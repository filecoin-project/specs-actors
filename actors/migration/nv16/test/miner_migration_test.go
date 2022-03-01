package test

import (
	"context"
	vm7 "github.com/filecoin-project/specs-actors/v7/support/vm"
	"github.com/filecoin-project/specs-actors/v8/support/vm7Util"
	"strings"

	"github.com/filecoin-project/specs-actors/v8/actors/states"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/rt"

	builtin7 "github.com/filecoin-project/specs-actors/v7/actors/builtin"
	miner7 "github.com/filecoin-project/specs-actors/v7/actors/builtin/miner"
	power7 "github.com/filecoin-project/specs-actors/v7/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v7/actors/util/adt"

	"github.com/filecoin-project/specs-actors/v8/actors/builtin"
	"github.com/filecoin-project/specs-actors/v8/actors/builtin/exported"
	miner8 "github.com/filecoin-project/specs-actors/v8/actors/builtin/miner"
	power8 "github.com/filecoin-project/specs-actors/v8/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v8/actors/builtin/verifreg"
	"github.com/filecoin-project/specs-actors/v8/actors/migration/nv16"
	"github.com/filecoin-project/specs-actors/v8/support/ipld"
	vm8 "github.com/filecoin-project/specs-actors/v8/support/vm"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sealProof = abi.RegisteredSealProof_StackedDrg32GiBV1_1

var seed = int64(93837778)

func compareNetworkStats(t *testing.T, statsV7 vm7.NetworkStats, statsV8 vm8.NetworkStats) {
	comparePowerState(t, statsV7.State, statsV8.State)
	require.Equal(t, statsV7.TotalRawBytePower, statsV8.TotalRawBytePower)
	require.Equal(t, statsV7.TotalBytesCommitted, statsV8.TotalBytesCommitted)
	require.Equal(t, statsV7.TotalQualityAdjPower, statsV8.TotalQualityAdjPower)
	require.Equal(t, statsV7.TotalQABytesCommitted, statsV8.TotalQABytesCommitted)
	require.Equal(t, statsV7.TotalPledgeCollateral, statsV8.TotalPledgeCollateral)
	require.Equal(t, statsV7.ThisEpochRawBytePower, statsV8.ThisEpochRawBytePower)
	require.Equal(t, statsV7.ThisEpochQualityAdjPower, statsV8.ThisEpochQualityAdjPower)
	require.Equal(t, statsV7.ThisEpochPledgeCollateral, statsV8.ThisEpochPledgeCollateral)
	require.Equal(t, statsV7.MinerCount, statsV8.MinerCount)
	require.Equal(t, statsV7.MinerAboveMinPowerCount, statsV8.MinerAboveMinPowerCount)
	require.Equal(t, statsV7.ThisEpochReward, statsV8.ThisEpochReward)
	require.Equal(t, statsV7.ThisEpochRewardSmoothed.PositionEstimate, statsV8.ThisEpochRewardSmoothed.PositionEstimate)
	require.Equal(t, statsV7.ThisEpochRewardSmoothed.VelocityEstimate, statsV8.ThisEpochRewardSmoothed.VelocityEstimate)
	require.Equal(t, statsV7.ThisEpochBaselinePower, statsV8.ThisEpochBaselinePower)
	require.Equal(t, statsV7.TotalStoragePowerReward, statsV8.TotalStoragePowerReward)
	require.Equal(t, statsV7.TotalClientLockedCollateral, statsV8.TotalClientLockedCollateral)
	require.Equal(t, statsV7.TotalProviderLockedCollateral, statsV8.TotalProviderLockedCollateral)
	require.Equal(t, statsV7.TotalClientStorageFee, statsV8.TotalClientStorageFee)
}

func comparePowerState(t *testing.T, stateV7 power7.State, stateV8 power8.State) {
	require.Equal(t, stateV7.TotalRawBytePower, stateV8.TotalRawBytePower)
	require.Equal(t, stateV7.TotalBytesCommitted, stateV8.TotalBytesCommitted)
	require.Equal(t, stateV7.TotalQualityAdjPower, stateV8.TotalQualityAdjPower)
	require.Equal(t, stateV7.TotalQABytesCommitted, stateV8.TotalQABytesCommitted)
	require.Equal(t, stateV7.TotalPledgeCollateral, stateV8.TotalPledgeCollateral)
	require.Equal(t, stateV7.ThisEpochRawBytePower, stateV8.ThisEpochRawBytePower)
	require.Equal(t, stateV7.ThisEpochPledgeCollateral, stateV8.ThisEpochPledgeCollateral)
	require.Equal(t, stateV7.ThisEpochQAPowerSmoothed.PositionEstimate, stateV8.ThisEpochQAPowerSmoothed.PositionEstimate)
	require.Equal(t, stateV7.ThisEpochQAPowerSmoothed.VelocityEstimate, stateV8.ThisEpochQAPowerSmoothed.VelocityEstimate)
	require.Equal(t, stateV7.MinerCount, stateV8.MinerCount)
	require.Equal(t, stateV7.MinerAboveMinPowerCount, stateV8.MinerAboveMinPowerCount)
	require.Equal(t, stateV7.CronEventQueue, stateV8.CronEventQueue)
	require.Equal(t, stateV7.FirstCronEpoch, stateV8.FirstCronEpoch)
	require.Equal(t, stateV7.Claims, stateV8.Claims)
	require.Equal(t, stateV7.ProofValidationBatch, stateV8.ProofValidationBatch)
}

func createMiners(t *testing.T, ctx context.Context, v *vm7.VM, numMiners int) []vm7Util.MinerInfo {
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)

	workerAddresses := vm7.CreateAccounts(ctx, t, v, numMiners, big.Mul(big.NewInt(200_000_000), vm7.FIL), seed)
	seed += int64(numMiners)
	assert.Equal(t, len(workerAddresses), numMiners)

	var minerInfos []vm7Util.MinerInfo
	for _, workerAddress := range workerAddresses {
		params := power7.CreateMinerParams{
			Owner:               workerAddress,
			Worker:              workerAddress,
			WindowPoStProofType: wPoStProof,
			Peer:                abi.PeerID("not really a peer id"),
		}
		ret := vm7.ApplyOk(t, v, workerAddress, builtin7.StoragePowerActorAddr, big.Mul(big.NewInt(100_000_000), vm7.FIL), builtin7.MethodsPower.CreateMiner, &params)
		minerAddress, ok := ret.(*power7.CreateMinerReturn)
		require.True(t, ok)
		minerInfos = append(minerInfos, vm7Util.MinerInfo{WorkerAddress: workerAddress, MinerAddress: minerAddress.IDAddress})
	}
	assert.Equal(t, len(minerInfos), numMiners)
	return minerInfos
}

func precommits(t *testing.T, v *vm7.VM, firstSectorNo int, numSectors int, minerInfos []vm7Util.MinerInfo, deals [][]abi.DealID) [][]*miner7.SectorPreCommitOnChainInfo {
	var precommitInfo [][]*miner7.SectorPreCommitOnChainInfo
	for i, minerInfo := range minerInfos {
		var dealIDs []abi.DealID = nil
		if deals != nil {
			dealIDs = deals[i]
		}
		precommits := vm7Util.PreCommitSectors(t, v, numSectors, miner7.PreCommitSectorBatchMaxSize, minerInfo.WorkerAddress, minerInfo.MinerAddress, sealProof, abi.SectorNumber(firstSectorNo), true, v.GetEpoch()+miner7.MaxSectorExpirationExtension, dealIDs)

		assert.Equal(t, len(precommits), numSectors)
		balances := vm7.GetMinerBalances(t, v, minerInfo.MinerAddress)
		assert.True(t, balances.PreCommitDeposit.GreaterThan(big.Zero()))
		precommitInfo = append(precommitInfo, precommits)
	}
	return precommitInfo
}

func createMinersAndSectorsV7(t *testing.T, ctx context.Context, ctxStore adt.Store, v *vm7.VM, firstSectorNo int, numMiners int, numSectors int, addDeals bool, minersToProve []vm7Util.MinerInfo) ([]vm7Util.MinerInfo, *vm7.VM) {
	minerInfos := createMiners(t, ctx, v, numMiners)
	if numSectors == 0 {
		return append(minersToProve, minerInfos...), v
	}

	var dealsArray [][]abi.DealID = nil
	if addDeals {
		for _, minerInfo := range minerInfos {
			deals := vm7Util.CreateDeals(t, 1, v, minerInfo.WorkerAddress, minerInfo.WorkerAddress, minerInfo.MinerAddress, sealProof)
			dealsArray = append(dealsArray, deals)
		}
	}

	precommitInfo := precommits(t, v, firstSectorNo, numSectors, minerInfos, dealsArray)

	// advance time to when we can prove-commit
	for i := 0; i < 3; i++ {
		v = vm7Util.ProveThenAdvanceOneDeadlineWithCron(t, v, ctxStore, minersToProve)
	}

	for i, minerInfo := range minerInfos {
		vm7Util.ProveCommitSectors(t, v, minerInfo.WorkerAddress, minerInfo.MinerAddress, precommitInfo[i], addDeals)
	}

	return append(minersToProve, minerInfos...), v
}

func TestNv16Migration(t *testing.T) {
	ctx := context.Background()
	bs := ipld.NewBlockStoreInMemory()
	v := vm7.NewVMWithSingletons(ctx, t, bs)
	ctxStore := adt.WrapBlockStore(ctx, bs)
	log := nv16.TestLogger{TB: t}

	v = vm7Util.AdvanceToEpochWithCron(t, v, 200)

	minerInfos, v := createMinersAndSectorsV7(t, ctx, ctxStore, v, 100, 100, 0, false, nil)
	minerInfos, v = createMinersAndSectorsV7(t, ctx, ctxStore, v, 100, 100, 100, true, minerInfos)
	lazyMinerInfos, v := createMinersAndSectorsV7(t, ctx, ctxStore, v, 10100, 2, 1000, true, minerInfos) // Bad miners who don't prove their sectors
	minerInfos, v = createMinersAndSectorsV7(t, ctx, ctxStore, v, 200100, 1, 10_000, true, minerInfos)

	v = vm7Util.AdvanceOneDayWhileProving(t, v, ctxStore, minerInfos)

	startRoot := v.StateRoot()
	cache := nv16.NewMemMigrationCache()
	_, err := nv16.MigrateStateTree(ctx, ctxStore, startRoot, v.GetEpoch(), nv16.Config{MaxWorkers: 1}, log, cache)
	require.NoError(t, err)

	minerInfos, v = createMinersAndSectorsV7(t, ctx, ctxStore, v, 100, 100, 0, false, nil)
	minerInfos, v = createMinersAndSectorsV7(t, ctx, ctxStore, v, 100, 100, 100, true, minerInfos)
	v = vm7Util.AdvanceOneDayWhileProving(t, v, ctxStore, minerInfos)
	minerInfos = append(minerInfos, lazyMinerInfos...)

	cacheRoot, err := nv16.MigrateStateTree(ctx, ctxStore, v.StateRoot(), v.GetEpoch(), nv16.Config{MaxWorkers: 1}, log, cache)
	require.NoError(t, err)

	networkStatsBefore := vm7.GetNetworkStats(t, v)
	noCacheRoot, err := nv16.MigrateStateTree(ctx, ctxStore, v.StateRoot(), v.GetEpoch(), nv16.Config{MaxWorkers: 1}, log, nv16.NewMemMigrationCache())
	require.NoError(t, err)
	require.True(t, cacheRoot.Equals(noCacheRoot))

	lookup := map[cid.Cid]rt.VMActor{}
	for _, ba := range exported.BuiltinActors() {
		lookup[ba.Code()] = ba
	}

	v8, err := vm8.NewVMAtEpoch(ctx, lookup, ctxStore, noCacheRoot, v.GetEpoch())
	require.NoError(t, err)

	networkStatsAfter := vm8.GetNetworkStats(t, v8)
	compareNetworkStats(t, networkStatsBefore, networkStatsAfter)

	stateTree, err := v8.GetStateTree()
	require.NoError(t, err)
	totalBalance, err := v8.GetTotalActorBalance()
	require.NoError(t, err)
	acc, err := states.CheckStateInvariants(stateTree, totalBalance, v8.GetEpoch()-1)
	require.NoError(t, err)
	require.True(t, acc.IsEmpty(), strings.Join(acc.Messages(), "\n"))

	// Compare miner states
	for _, minerInfo := range minerInfos {
		var oldMinerState miner7.State
		err := v.GetState(minerInfo.MinerAddress, &oldMinerState)
		require.NoError(t, err)
		oldDeadlines, err := oldMinerState.LoadDeadlines(ctxStore)
		require.NoError(t, err)

		var newMinerState miner8.State
		err = v8.GetState(minerInfo.MinerAddress, &newMinerState)
		require.NoError(t, err)
		newDeadlines, err := newMinerState.LoadDeadlines(ctxStore)
		require.NoError(t, err)

		for i := 0; uint64(i) < miner7.WPoStPeriodDeadlines; i++ {
			oldDeadline, err := oldDeadlines.LoadDeadline(v.Store(), uint64(i))
			require.NoError(t, err)
			newDeadline, err := newDeadlines.LoadDeadline(v.Store(), uint64(i))
			require.NoError(t, err)

			require.Equal(t, oldDeadline.TotalSectors, newDeadline.TotalSectors)
			require.Equal(t, oldDeadline.LiveSectors, newDeadline.LiveSectors)
			require.Equal(t, oldDeadline.FaultyPower.Raw, newDeadline.FaultyPower.Raw)
			require.Equal(t, oldDeadline.FaultyPower.QA, newDeadline.FaultyPower.QA)
		}

		oldPower := vm7Util.MinerPower(t, v, ctxStore, minerInfo.MinerAddress)
		newPower := vm8.MinerPower(t, v8, minerInfo.MinerAddress)
		require.Equal(t, oldPower.Raw, newPower.Raw)
		require.Equal(t, oldPower.QA, newPower.QA)

		// Check if every single sector has null SectorKey
		err = newMinerState.ForEachSector(ctxStore, func(si *miner8.SectorOnChainInfo) {
			require.Nil(t, si.SectorKeyCID)
		})
		require.NoError(t, err)
	}

	// Check if the verified registry actor's RemoveDataCapProposalIDs is empty
	var verifRegState verifreg.State
	err = v8.GetState(builtin.VerifiedRegistryActorAddr, &verifRegState)
	require.NoError(t, err)
	proposalIDs, err := adt.AsMap(ctxStore, verifRegState.RemoveDataCapProposalIDs, builtin.DefaultHamtBitwidth)
	require.NoError(t, err)
	keys, err := proposalIDs.CollectKeys()
	require.NoError(t, err)
	require.Nil(t, keys)
}
