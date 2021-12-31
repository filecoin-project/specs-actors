package test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-state-types/rt"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"

	ipld2 "github.com/filecoin-project/specs-actors/v2/support/ipld"
	market6 "github.com/filecoin-project/specs-actors/v6/actors/builtin/market"
	power6 "github.com/filecoin-project/specs-actors/v6/actors/builtin/power"
	vm6 "github.com/filecoin-project/specs-actors/v6/support/vm"

	"github.com/filecoin-project/specs-actors/v7/actors/builtin"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/exported"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v7/actors/migration/nv15"
	"github.com/filecoin-project/specs-actors/v7/actors/util/adt"

	tutil "github.com/filecoin-project/specs-actors/v7/support/testing"
	"github.com/filecoin-project/specs-actors/v7/support/vm"

	addr "github.com/filecoin-project/go-address"
)

func TestDealLabelMigration(t *testing.T) {
	ctx := context.Background()
	log := nv15.TestLogger{TB: t}
	bs := ipld2.NewSyncBlockStoreInMemory()
	v := vm6.NewVMWithSingletons(ctx, t, bs)

	addrs := vm6.CreateAccounts(ctx, t, v, 2, big.Mul(big.NewInt(10_000), vm.FIL), 93837778)
	worker, client := addrs[0], addrs[1]

	minerBalance := big.Mul(big.NewInt(1_000), vm.FIL)
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1

	// create miner
	params := power6.CreateMinerParams{
		Owner:               worker,
		Worker:              worker,
		WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		Peer:                abi.PeerID("not really a peer id"),
	}
	ret := vm6.ApplyOk(t, v, addrs[0], builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)

	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	//
	// publish deals
	//

	// add market collateral for clients and miner
	collateral := big.Mul(big.NewInt(64), vm.FIL)
	vm6.ApplyOk(t, v, client, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &client)
	vm6.ApplyOk(t, v, worker, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &minerAddrs.IDAddress)

	// create 3 deals
	dealIDs := []abi.DealID{}
	dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	deals := publishDealv6(t, v, worker, client, minerAddrs.IDAddress, "deal1-activatedcronned", 1<<30, false, dealStart, 365*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)
	deals = publishDealv6(t, v, worker, client, minerAddrs.IDAddress, "deal2-activateduncronned", 1<<30, false, dealStart, 365*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)
	deals = publishDealv6(t, v, worker, client, minerAddrs.IDAddress, "deal3-unactivated", 1<<30, false, dealStart, 365*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)

	deal1ID := dealIDs[0]
	deal2ID := dealIDs[1]
	deal3ID := dealIDs[2]

	deal1CronTime := market.GenRandNextEpoch(dealStart, deal1ID)
	deal2CronTime := market.GenRandNextEpoch(dealStart, deal2ID)
	deal3CronTime := market.GenRandNextEpoch(dealStart, deal3ID)
	require.True(t, deal1CronTime < deal2CronTime && deal2CronTime < deal3CronTime)
	require.True(t, v.GetEpoch() < dealStart && dealStart < deal1CronTime)

	// precommit sector with deals
	sectorNumber := abi.SectorNumber(100)
	sealedCid := tutil.MakeCID("100", &miner.SealedCIDPrefix)
	preCommitParams := miner.PreCommitSectorParams{
		SealProof:     sealProof,
		SectorNumber:  sectorNumber,
		SealedCID:     sealedCid,
		SealRandEpoch: v.GetEpoch() - 1,
		DealIDs:       dealIDs[:2],
		Expiration:    v.GetEpoch() + 400*builtin.EpochsInDay,
	}
	vm6.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)

	// advance time to max seal duration
	proveTime := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	v, _ = vm6.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	// Prove commit sector after max seal duration- deal1 and deal2 get activated here
	v, err := v.WithEpoch(proveTime)
	require.NoError(t, err)
	proveCommitParams := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}
	vm6.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)

	// In the same epoch, trigger cron to validate prove commit
	vm6.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// advance time to when deal1 will be cronned
	v, err = v.WithEpoch(deal1CronTime)
	require.NoError(t, err)
	// run market cron to cron deal1, but not deal2 yet
	vm6.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// now do some assertions about what's in pendingproposals/proposals
	// getting various AMTs out of things
	adtStore := adt.WrapStore(ctx, cbor.NewCborStore(bs))
	var market6State market6.State
	require.NoError(t, v.GetState(builtin.StorageMarketActorAddr, &market6State))
	oldProposals, err := adt.AsArray(adtStore, market6State.Proposals, market6.ProposalsAmtBitwidth)
	require.NoError(t, err)
	oldStates, err := adt.AsArray(adtStore, market6State.States, market6.StatesAmtBitwidth)
	require.NoError(t, err)
	oldPendingProposals, err := adt.AsSet(adtStore, market6State.PendingProposals, builtin.DefaultHamtBitwidth)
	require.NoError(t, err)
	// deal1 will just be in states and proposals and not pendingproposals
	checkMarketProposalsEtcState(t, oldProposals, oldStates, oldPendingProposals, deal1ID, true, true, false, false)
	// deal2 will be in proposals pendingproposals and in states
	checkMarketProposalsEtcState(t, oldProposals, oldStates, oldPendingProposals, deal2ID, true, true, true, false)
	// deal3 will be in proposals and pendingproposals but not in states
	checkMarketProposalsEtcState(t, oldProposals, oldStates, oldPendingProposals, deal3ID, true, false, true, false)

	oldMarketActor, found, err := v.GetActor(builtin.StorageMarketActorAddr)
	require.NoError(t, err)
	require.True(t, found)

	startRoot := v.StateRoot()
	nextRoot, err := nv15.MigrateStateTree(ctx, adtStore, startRoot, abi.ChainEpoch(0), nv15.Config{MaxWorkers: 1}, log, nv15.NewMemMigrationCache())
	require.NoError(t, err)

	lookup := map[cid.Cid]rt.VMActor{}
	for _, ba := range exported.BuiltinActors() {
		lookup[ba.Code()] = ba
	}

	v7, err := vm.NewVMAtEpoch(ctx, lookup, v.Store(), nextRoot, v.GetEpoch()+1)
	require.NoError(t, err)

	// now do the assertions again about what's in pendingproposals/proposals
	// getting various AMTs out of things
	var marketState market.State
	require.NoError(t, v7.GetState(builtin.StorageMarketActorAddr, &marketState))
	proposals, err := adt.AsArray(adtStore, marketState.Proposals, market.ProposalsAmtBitwidth)
	require.NoError(t, err)
	states, err := adt.AsArray(adtStore, marketState.States, market.StatesAmtBitwidth)
	require.NoError(t, err)
	pendingProposals, err := adt.AsSet(adtStore, marketState.PendingProposals, builtin.DefaultHamtBitwidth)
	require.NoError(t, err)
	// deal1 will just be in states and proposals and not pendingproposals
	checkMarketProposalsEtcState(t, proposals, states, pendingProposals, deal1ID, true, true, false, true)
	// deal2 will be in proposals pendingproposals and in states
	checkMarketProposalsEtcState(t, proposals, states, pendingProposals, deal2ID, true, true, true, true)
	// deal3 will be in proposals and pendingproposals but not in states
	checkMarketProposalsEtcState(t, proposals, states, pendingProposals, deal3ID, true, false, true, true)

	// check that all three's labels are the same, just with changed types, before and after.
	require.NoError(t, checkSameLabel(oldProposals, proposals, deal1ID))
	require.NoError(t, checkSameLabel(oldProposals, proposals, deal2ID))
	require.NoError(t, checkSameLabel(oldProposals, proposals, deal3ID))

	var market7State market.State
	require.NoError(t, v7.GetState(builtin.StorageMarketActorAddr, &market7State))
	market.CheckStateInvariants(&market7State, v7.Store(), oldMarketActor.Balance, v.GetEpoch()+1)

}

func publishDealv6(t *testing.T, v *vm6.VM, provider, dealClient, minerID addr.Address, dealLabel string,
	pieceSize abi.PaddedPieceSize, verifiedDeal bool, dealStart abi.ChainEpoch, dealLifetime abi.ChainEpoch,
) *market6.PublishStorageDealsReturn {
	deal := market6.DealProposal{
		PieceCID:             tutil.MakeCID(dealLabel, &market.PieceCIDPrefix),
		PieceSize:            pieceSize,
		VerifiedDeal:         verifiedDeal,
		Client:               dealClient,
		Provider:             minerID,
		Label:                dealLabel,
		StartEpoch:           dealStart,
		EndEpoch:             dealStart + dealLifetime,
		StoragePricePerEpoch: abi.NewTokenAmount(1 << 20),
		ProviderCollateral:   big.Mul(big.NewInt(2), vm.FIL),
		ClientCollateral:     big.Mul(big.NewInt(1), vm.FIL),
	}

	publishDealParams := market6.PublishStorageDealsParams{
		Deals: []market6.ClientDealProposal{{
			Proposal: deal,
			ClientSignature: crypto.Signature{
				Type: crypto.SigTypeBLS,
			},
		}},
	}
	result := vm6.RequireApplyMessage(t, v, provider, builtin.StorageMarketActorAddr, big.Zero(), builtin.MethodsMarket.PublishStorageDeals, &publishDealParams, t.Name())
	require.Equal(t, exitcode.Ok, result.Code)

	expectedPublishSubinvocations := []vm6.ExpectInvocation{
		{To: minerID, Method: builtin.MethodsMiner.ControlAddresses, SubInvocations: []vm6.ExpectInvocation{}},
		{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward, SubInvocations: []vm6.ExpectInvocation{}},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower, SubInvocations: []vm6.ExpectInvocation{}},
	}

	if verifiedDeal {
		expectedPublishSubinvocations = append(expectedPublishSubinvocations, vm6.ExpectInvocation{
			To:             builtin.VerifiedRegistryActorAddr,
			Method:         builtin.MethodsVerifiedRegistry.UseBytes,
			SubInvocations: []vm6.ExpectInvocation{},
		})
	}

	vm6.ExpectInvocation{
		To:             builtin.StorageMarketActorAddr,
		Method:         builtin.MethodsMarket.PublishStorageDeals,
		SubInvocations: expectedPublishSubinvocations,
	}.Matches(t, v.LastInvocation())

	return result.Ret.(*market.PublishStorageDealsReturn)
}

func checkMarketProposalsEtcState(t *testing.T, proposals *adt.Array, states *adt.Array, pendingProposals *adt.Set,
	dealID abi.DealID, inProposals bool, inStates bool, inPendingProposals bool, isv6 bool) {
	var dealprop6 market.DealProposal
	var dealprop5 market6.DealProposal
	var found bool
	var err error
	if isv6 {
		found, err = proposals.Get(uint64(dealID), &dealprop6)
	} else {
		found, err = proposals.Get(uint64(dealID), &dealprop5)
	}
	require.NoError(t, err)
	require.Equal(t, found, inProposals)
	found, err = states.Get(uint64(dealID), nil)
	require.NoError(t, err)
	require.Equal(t, found, inStates)
	var dealpropcid cid.Cid
	if isv6 {
		dealpropcid, err = dealprop6.Cid()
	} else {
		dealpropcid, err = dealprop5.Cid()
	}
	require.NoError(t, err)
	found, err = pendingProposals.Has(abi.CidKey(dealpropcid))
	require.NoError(t, err)
	require.Equal(t, found, inPendingProposals)
}

func checkSameLabel(v5Proposals *adt.Array, v6Proposals *adt.Array, dealID abi.DealID) error {
	var dealprop5 market6.DealProposal
	var dealprop6 market.DealProposal
	found, err := v5Proposals.Get(uint64(dealID), &dealprop5)
	if !found || err != nil {
		return xerrors.Errorf("failed to look up dealID %v in validating deal label", dealID)
	}
	found, err = v6Proposals.Get(uint64(dealID), &dealprop6)
	if !found || err != nil {
		return xerrors.Errorf("failed to look up dealID %v in validating deal label", dealID)
	}
	if dealprop5.Label != string(dealprop6.Label) {
		return xerrors.Errorf("deal labels were not the same, modulo types, after migration.")
	}
	return nil
}
