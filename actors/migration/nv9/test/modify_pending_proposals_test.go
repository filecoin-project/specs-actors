package test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	power2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	vm2 "github.com/filecoin-project/specs-actors/v2/support/vm"

	"github.com/filecoin-project/specs-actors/v3/actors/builtin"
	"github.com/filecoin-project/specs-actors/v3/actors/builtin/exported"
	market3 "github.com/filecoin-project/specs-actors/v3/actors/builtin/market"
	miner3 "github.com/filecoin-project/specs-actors/v3/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v3/actors/migration/nv9"
	"github.com/filecoin-project/specs-actors/v3/actors/runtime"
	"github.com/filecoin-project/specs-actors/v3/actors/states"
	tutil "github.com/filecoin-project/specs-actors/v3/support/testing"
	vm3 "github.com/filecoin-project/specs-actors/v3/support/vm"
)

func TestUpdatePendingDealsMigration(t *testing.T) {
	ctx := context.Background()
	v := vm2.NewVMWithSingletons(ctx, t)
	addrs := vm2.CreateAccounts(ctx, t, v, 10, big.Mul(big.NewInt(100_000), vm2.FIL), 93837778)
	worker := addrs[0]

	minerBalance := big.Mul(big.NewInt(10_000), vm2.FIL)

	// create miner
	params := power2.CreateMinerParams{
		Owner:         worker,
		Worker:        worker,
		SealProofType: abi.RegisteredSealProof_StackedDrg32GiBV1_1,
		Peer:          abi.PeerID("not really a peer id"),
	}
	ret := vm2.ApplyOk(t, v, addrs[0], builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)

	minerAddrs, ok := ret.(*power2.CreateMinerReturn)
	require.True(t, ok)

	// add market collateral for clients and miner
	for i := 0; i < 9; i++ {
		collateral := big.Mul(big.NewInt(30), vm2.FIL)
		vm2.ApplyOk(t, v, addrs[i+1], builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &addrs[i+1])
		collateral = big.Mul(big.NewInt(64), vm2.FIL)
		vm2.ApplyOk(t, v, worker, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &minerAddrs.IDAddress)
	}

	// create 100 deals over 100 epochs to fill pending proposals structure
	dealStart := abi.ChainEpoch(252)
	var deals []*market2.PublishStorageDealsReturn
	for i := 0; i < 100; i++ {
		var err error
		v, err = v.WithEpoch(v.GetEpoch() + 1)
		require.NoError(t, err)
		deals = append(deals, publishDeal(t, v, worker, addrs[1+i%9], minerAddrs.IDAddress, fmt.Sprintf("deal%d", i),
			1<<26, false, dealStart, 210*builtin.EpochsInDay))
	}

	// run migration
	nextRoot, err := nv9.MigrateStateTree(ctx, v.Store(), v.StateRoot(), v.GetEpoch(), nv9.Config{})
	require.NoError(t, err)

	lookup := map[cid.Cid]runtime.VMActor{}
	for _, ba := range exported.BuiltinActors() {
		lookup[ba.Code()] = ba
	}

	v3, err := vm3.NewVMAtEpoch(ctx, lookup, v.Store(), nextRoot, v.GetEpoch()+1)
	require.NoError(t, err)

	// add 10 more deals after migration
	for i := 0; i < 10; i++ {
		var err error
		v3, err = v3.WithEpoch(v.GetEpoch() + 1)
		require.NoError(t, err)
		deals = append(deals, publishV3Deal(t, v3, worker, addrs[1+i%9], minerAddrs.IDAddress, fmt.Sprintf("deal1%d", i),
			1<<26, false, dealStart, 210*builtin.EpochsInDay))
	}

	// add even deals to a sector we will commit (to let the odd deals expire)
	var dealIDs []abi.DealID
	for i := 0; i < len(deals); i += 2 {
		dealIDs = append(dealIDs, deals[i].IDs[0])
	}

	// precommit capacity upgrade sector with deals
	sectorNumber := abi.SectorNumber(100)
	sealedCid := tutil.MakeCID("100", &miner3.SealedCIDPrefix)
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	preCommitParams := miner3.PreCommitSectorParams{
		SealProof:     sealProof,
		SectorNumber:  sectorNumber,
		SealedCID:     sealedCid,
		SealRandEpoch: v.GetEpoch() - 1,
		DealIDs:       dealIDs,
		Expiration:    v.GetEpoch() + 220*builtin.EpochsInDay,
	}
	vm3.ApplyOk(t, v3, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)

	// advance time to min seal duration
	proveTime := v3.GetEpoch() + miner3.PreCommitChallengeDelay + 1
	v3, _ = vm3.AdvanceByDeadlineTillEpoch(t, v3, minerAddrs.IDAddress, proveTime)

	// Prove commit sector after max seal duration
	v3, err = v3.WithEpoch(proveTime)
	require.NoError(t, err)
	proveCommitParams := miner3.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}
	vm3.ApplyOk(t, v3, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)

	// In the same epoch, trigger cron to validate prove commit
	// This activates deals hitting the pending deals structure.
	vm3.ApplyOk(t, v3, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// Run cron after deal start to expire pending deals
	v3, err = v3.WithEpoch(dealStart + 1)
	require.NoError(t, err)
	vm3.ApplyOk(t, v3, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// Assert that no invariants are broken. CheckStateInvariants used to assert that Pending Proposal value cid
	// matched its key, so this also checks that the market invariants have been corrected.
	stateTree, err := v3.GetStateTree()
	require.NoError(t, err)
	totalBalance, err := v3.GetTotalActorBalance()
	require.NoError(t, err)
	msgs, err := states.CheckStateInvariants(stateTree, totalBalance, v3.GetEpoch())
	require.NoError(t, err)

	assert.Equal(t, 0, len(msgs.Messages()), strings.Join(msgs.Messages(), "\n"))

}

func publishDeal(t *testing.T, v *vm2.VM, provider, dealClient, minerID addr.Address, dealLabel string,
	pieceSize abi.PaddedPieceSize, verifiedDeal bool, dealStart abi.ChainEpoch, dealLifetime abi.ChainEpoch,
) *market2.PublishStorageDealsReturn {
	deal := market2.DealProposal{
		PieceCID:             tutil.MakeCID(dealLabel, &market2.PieceCIDPrefix),
		PieceSize:            pieceSize,
		VerifiedDeal:         verifiedDeal,
		Client:               dealClient,
		Provider:             minerID,
		Label:                dealLabel,
		StartEpoch:           dealStart,
		EndEpoch:             dealStart + dealLifetime,
		StoragePricePerEpoch: abi.NewTokenAmount(1 << 20),
		ProviderCollateral:   big.Mul(big.NewInt(2), vm2.FIL),
		ClientCollateral:     big.Mul(big.NewInt(1), vm2.FIL),
	}

	publishDealParams := market2.PublishStorageDealsParams{
		Deals: []market2.ClientDealProposal{{
			Proposal:        deal,
			ClientSignature: crypto.Signature{},
		}},
	}
	ret, code := v.ApplyMessage(provider, builtin.StorageMarketActorAddr, big.Zero(), builtin.MethodsMarket.PublishStorageDeals, &publishDealParams)
	require.Equal(t, exitcode.Ok, code)

	expectedPublishSubinvocations := []vm2.ExpectInvocation{
		{To: minerID, Method: builtin.MethodsMiner.ControlAddresses, SubInvocations: []vm2.ExpectInvocation{}},
		{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward, SubInvocations: []vm2.ExpectInvocation{}},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower, SubInvocations: []vm2.ExpectInvocation{}},
	}

	vm2.ExpectInvocation{
		To:             builtin.StorageMarketActorAddr,
		Method:         builtin.MethodsMarket.PublishStorageDeals,
		SubInvocations: expectedPublishSubinvocations,
	}.Matches(t, v.LastInvocation())

	return ret.(*market2.PublishStorageDealsReturn)
}

func publishV3Deal(t *testing.T, v *vm3.VM, provider, dealClient, minerID addr.Address, dealLabel string,
	pieceSize abi.PaddedPieceSize, verifiedDeal bool, dealStart abi.ChainEpoch, dealLifetime abi.ChainEpoch,
) *market2.PublishStorageDealsReturn {
	deal := market3.DealProposal{
		PieceCID:             tutil.MakeCID(dealLabel, &market2.PieceCIDPrefix),
		PieceSize:            pieceSize,
		VerifiedDeal:         verifiedDeal,
		Client:               dealClient,
		Provider:             minerID,
		Label:                dealLabel,
		StartEpoch:           dealStart,
		EndEpoch:             dealStart + dealLifetime,
		StoragePricePerEpoch: abi.NewTokenAmount(1 << 20),
		ProviderCollateral:   big.Mul(big.NewInt(2), vm2.FIL),
		ClientCollateral:     big.Mul(big.NewInt(1), vm2.FIL),
	}

	publishDealParams := market3.PublishStorageDealsParams{
		Deals: []market2.ClientDealProposal{{
			Proposal:        deal,
			ClientSignature: crypto.Signature{},
		}},
	}
	ret, code := v.ApplyMessage(provider, builtin.StorageMarketActorAddr, big.Zero(), builtin.MethodsMarket.PublishStorageDeals, &publishDealParams)
	require.Equal(t, exitcode.Ok, code)

	expectedPublishSubinvocations := []vm3.ExpectInvocation{
		{To: minerID, Method: builtin.MethodsMiner.ControlAddresses, SubInvocations: []vm3.ExpectInvocation{}},
		{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward, SubInvocations: []vm3.ExpectInvocation{}},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower, SubInvocations: []vm3.ExpectInvocation{}},
	}

	vm3.ExpectInvocation{
		To:             builtin.StorageMarketActorAddr,
		Method:         builtin.MethodsMarket.PublishStorageDeals,
		SubInvocations: expectedPublishSubinvocations,
	}.Matches(t, v.LastInvocation())

	return ret.(*market2.PublishStorageDealsReturn)
}
