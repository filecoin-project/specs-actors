package test

import (
	"context"
	"fmt"
	ipld2 "github.com/filecoin-project/specs-actors/v2/support/ipld"
	"github.com/filecoin-project/specs-actors/v6/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v7/actors/migration/nv15"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/stretchr/testify/require"

	market6 "github.com/filecoin-project/specs-actors/v6/actors/builtin/market"
	vm6 "github.com/filecoin-project/specs-actors/v6/support/vm"

	builtin6 "github.com/filecoin-project/specs-actors/v6/actors/builtin"
	miner6 "github.com/filecoin-project/specs-actors/v6/actors/builtin/miner"
	power6 "github.com/filecoin-project/specs-actors/v6/actors/builtin/power"

	tutil6 "github.com/filecoin-project/specs-actors/v6/support/testing"
	"github.com/filecoin-project/specs-actors/v7/support/vm6Util"

	"github.com/filecoin-project/go-address"
)

const sealProof = abi.RegisteredSealProof_StackedDrg32GiBV1_1


func publishDealv6(t *testing.T, v *vm6.VM, provider, dealClient, minerID address.Address, dealLabel string,
	pieceSize abi.PaddedPieceSize, verifiedDeal bool, dealStart abi.ChainEpoch, dealLifetime abi.ChainEpoch,
) *market6.PublishStorageDealsReturn {
	deal := market6.DealProposal{
		PieceCID:             tutil6.MakeCID(dealLabel, &market6.PieceCIDPrefix),
		PieceSize:            pieceSize,
		VerifiedDeal:         verifiedDeal,
		Client:               dealClient,
		Provider:             minerID,
		Label:                dealLabel,
		StartEpoch:           dealStart,
		EndEpoch:             dealStart + dealLifetime,
		StoragePricePerEpoch: abi.NewTokenAmount(1 << 20),
		ProviderCollateral:   big.Mul(big.NewInt(2), vm6.FIL),
		ClientCollateral:     big.Mul(big.NewInt(1), vm6.FIL),
	}

	publishDealParams := market6.PublishStorageDealsParams{
		Deals: []market6.ClientDealProposal{{
			Proposal: deal,
			ClientSignature: crypto.Signature{
				Type: crypto.SigTypeBLS,
			},
		}},
	}
	result := vm6.RequireApplyMessage(t, v, provider, builtin6.StorageMarketActorAddr, big.Zero(), builtin6.MethodsMarket.PublishStorageDeals, &publishDealParams, t.Name())
	require.Equal(t, exitcode.Ok, result.Code)

	expectedPublishSubinvocations := []vm6.ExpectInvocation{
		{To: minerID, Method: builtin6.MethodsMiner.ControlAddresses, SubInvocations: []vm6.ExpectInvocation{}},
		{To: builtin6.RewardActorAddr, Method: builtin6.MethodsReward.ThisEpochReward, SubInvocations: []vm6.ExpectInvocation{}},
		{To: builtin6.StoragePowerActorAddr, Method: builtin6.MethodsPower.CurrentTotalPower, SubInvocations: []vm6.ExpectInvocation{}},
	}

	if verifiedDeal {
		expectedPublishSubinvocations = append(expectedPublishSubinvocations, vm6.ExpectInvocation{
			To:             builtin6.VerifiedRegistryActorAddr,
			Method:         builtin6.MethodsVerifiedRegistry.UseBytes,
			SubInvocations: []vm6.ExpectInvocation{},
		})
	}

	vm6.ExpectInvocation{
		To:             builtin6.StorageMarketActorAddr,
		Method:         builtin6.MethodsMarket.PublishStorageDeals,
		SubInvocations: expectedPublishSubinvocations,
	}.Matches(t, v.LastInvocation())

	return result.Ret.(*market6.PublishStorageDealsReturn)
}

var seed = int64(93837778)
func createMiners(t *testing.T, ctx context.Context, v *vm6.VM, numMiners int) []vm6Util.MinerInfo {
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)

	workerAddresses := vm6.CreateAccounts(ctx, t, v, numMiners, big.Mul(big.NewInt(200_000_000), vm6.FIL), seed)
	seed += int64(numMiners)
	assert.Equal(t, len(workerAddresses), numMiners)

	var minerInfos []vm6Util.MinerInfo
	for _, workerAddress := range workerAddresses {
		params := power6.CreateMinerParams{
			Owner:               workerAddress,
			Worker:              workerAddress,
			WindowPoStProofType: wPoStProof,
			Peer:                abi.PeerID("not really a peer id"),
		}
		ret := vm6.ApplyOk(t, v, workerAddress, builtin6.StoragePowerActorAddr, big.Mul(big.NewInt(100_000_000), vm6.FIL), builtin6.MethodsPower.CreateMiner, &params)
		minerAddress, ok := ret.(*power6.CreateMinerReturn)
		require.True(t, ok)
		minerInfos = append(minerInfos, vm6Util.MinerInfo{ WorkerAddress: workerAddress, MinerAddress: minerAddress.IDAddress })
	}
	assert.Equal(t, len(minerInfos), numMiners)
	return minerInfos
}

func precommits(t *testing.T, v *vm6.VM, firstSectorNo int, numSectors int, minerInfos []vm6Util.MinerInfo, deals [][]abi.DealID) [][]*miner6.SectorPreCommitOnChainInfo {
	var precommitInfo [][]*miner6.SectorPreCommitOnChainInfo
	for i, minerInfo := range minerInfos {
		var dealIDs []abi.DealID = nil
		if deals != nil {
			dealIDs = deals[i]
		}
		precommits := vm6Util.PreCommitSectors(t, v, numSectors, miner6.PreCommitSectorBatchMaxSize, minerInfo.WorkerAddress, minerInfo.MinerAddress, sealProof, abi.SectorNumber(firstSectorNo), true, v.GetEpoch()+miner6.MaxSectorExpirationExtension, dealIDs)

		assert.Equal(t, len(precommits), numSectors)
		balances := vm6.GetMinerBalances(t, v, minerInfo.MinerAddress)
		assert.True(t, balances.PreCommitDeposit.GreaterThan(big.Zero()))
		precommitInfo = append(precommitInfo, precommits)
	}
	return precommitInfo
}

func createMinersAndSectorsV6(t *testing.T, ctx context.Context, ctxStore adt.Store, v *vm6.VM, firstSectorNo int, numMiners int, numSectors int, addDeals bool, minersToProve []vm6Util.MinerInfo) ([]vm6Util.MinerInfo, *vm6.VM) {
	minerInfos := createMiners(t, ctx, v, numMiners)
	if numSectors == 0 {
		return append(minersToProve, minerInfos...), v
	}

	var dealsArray [][]abi.DealID = nil
	if addDeals {
		for _, minerInfo := range minerInfos {
			deals := vm6Util.CreateDeals(t, 1, v, minerInfo.WorkerAddress, minerInfo.WorkerAddress, minerInfo.MinerAddress, sealProof)
			dealsArray = append(dealsArray, deals)
		}
	}

	precommitInfo := precommits(t, v , firstSectorNo, numSectors, minerInfos, dealsArray)

	// advance time to when we can prove-commit
	for i := 0; i < 3; i++ {
		v = vm6Util.ProveThenAdvanceOneDeadlineWithCron(t, v, ctxStore, minersToProve)
	}

	for i, minerInfo := range minerInfos {
		vm6Util.ProveCommitSectors(t, v, minerInfo.WorkerAddress, minerInfo.MinerAddress, precommitInfo[i], addDeals)
	}
	// v = vm6Util.AdvanceOneEpochWithCron(t, v)

	return append(minersToProve, minerInfos...), v
}

func TestCreateMiners(t *testing.T) {
	ctx := context.Background()
	bs := ipld2.NewSyncBlockStoreInMemory()
	v := vm6.NewVMWithSingletons(ctx, t, bs)
	ctxStore := adt.WrapBlockStore(ctx, bs)
	log := nv15.TestLogger{TB: t}

	v = vm6Util.AdvanceToEpochWithCron(t, v, 200)

	minerInfos, v := createMinersAndSectorsV6(t, ctx, ctxStore, v, 100, 100, 0, false, nil)
	minerInfos, v = createMinersAndSectorsV6(t, ctx, ctxStore, v, 100, 100, 100, true, minerInfos)
	_, v = createMinersAndSectorsV6(t, ctx, ctxStore, v, 10100, 2, 1000, true, minerInfos) // Bad miners who don't prove their sectors
	minerInfos, v = createMinersAndSectorsV6(t, ctx, ctxStore, v, 200100, 1, 10_000, true, minerInfos)
	// vm6Util.PrintMinerInfos(t, v, ctxStore, minerInfos)

	v = vm6Util.AdvanceOneDayWhileProving(t, v, ctxStore, minerInfos)

	networkStats := vm6.GetNetworkStats(t, v)
	fmt.Printf("BYTES COMMITTED %s\n", networkStats.TotalBytesCommitted)

	startRoot := v.StateRoot()
	cache := nv15.NewMemMigrationCache()
	_, err := nv15.MigrateStateTree(ctx, ctxStore, startRoot, v.GetEpoch(), nv15.Config{MaxWorkers: 1}, log, cache)
	require.NoError(t, err)

	minerInfos, v = createMinersAndSectorsV6(t, ctx, ctxStore, v, 100, 100, 0, false, nil)
	minerInfos, v = createMinersAndSectorsV6(t, ctx, ctxStore, v, 100, 100, 100, true, minerInfos)
	v = vm6Util.AdvanceOneDayWhileProving(t, v, ctxStore, minerInfos)

	cacheRoot, err := nv15.MigrateStateTree(ctx, ctxStore, v.StateRoot(), v.GetEpoch(), nv15.Config{MaxWorkers: 1}, log, cache)
	require.NoError(t, err)

	noCacheRoot, err := nv15.MigrateStateTree(ctx, ctxStore, v.StateRoot(), v.GetEpoch(), nv15.Config{MaxWorkers: 1}, log, nv15.NewMemMigrationCache())
	require.NoError(t, err)

	require.True(t, cacheRoot.Equals(noCacheRoot))

	//
	//lookup := map[cid.Cid]rt.VMActor{}
	//for _, ba := range exported7.BuiltinActors() {
	//	lookup[ba.Code()] = ba
	//}
	//
	//v7, err := vm7.NewVMAtEpoch(ctx, lookup, v.Store(), nextRoot, v.GetEpoch()+1)
	//require.NoError(t, err)
	//var marketState miner7.State
	//require.NoError(t, v7.GetState(builtin7.StorageMarketActorAddr, &marketState))
}