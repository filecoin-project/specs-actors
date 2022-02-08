package test

import (
	"context"
	"strings"

	"github.com/filecoin-project/specs-actors/v7/actors/states"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/rt"

	builtin6 "github.com/filecoin-project/specs-actors/v6/actors/builtin"
	miner6 "github.com/filecoin-project/specs-actors/v6/actors/builtin/miner"
	power6 "github.com/filecoin-project/specs-actors/v6/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v6/actors/util/adt"
	vm6 "github.com/filecoin-project/specs-actors/v6/support/vm"

	"github.com/filecoin-project/specs-actors/v7/actors/builtin"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/exported"
	miner7 "github.com/filecoin-project/specs-actors/v7/actors/builtin/miner"
	power7 "github.com/filecoin-project/specs-actors/v7/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/verifreg"
	"github.com/filecoin-project/specs-actors/v7/actors/migration/nv15"
	"github.com/filecoin-project/specs-actors/v7/support/ipld"
	vm7 "github.com/filecoin-project/specs-actors/v7/support/vm"
	"github.com/filecoin-project/specs-actors/v7/support/vm6Util"

	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sealProof = abi.RegisteredSealProof_StackedDrg32GiBV1_1

var seed = int64(93837778)

func compareNetworkStats(t *testing.T, statsV6 vm6.NetworkStats, statsV7 vm7.NetworkStats) {
	comparePowerState(t, statsV6.State, statsV7.State)
	require.Equal(t, statsV6.TotalRawBytePower, statsV7.TotalRawBytePower)
	require.Equal(t, statsV6.TotalBytesCommitted, statsV7.TotalBytesCommitted)
	require.Equal(t, statsV6.TotalQualityAdjPower, statsV7.TotalQualityAdjPower)
	require.Equal(t, statsV6.TotalQABytesCommitted, statsV7.TotalQABytesCommitted)
	require.Equal(t, statsV6.TotalPledgeCollateral, statsV7.TotalPledgeCollateral)
	require.Equal(t, statsV6.ThisEpochRawBytePower, statsV7.ThisEpochRawBytePower)
	require.Equal(t, statsV6.ThisEpochQualityAdjPower, statsV7.ThisEpochQualityAdjPower)
	require.Equal(t, statsV6.ThisEpochPledgeCollateral, statsV7.ThisEpochPledgeCollateral)
	require.Equal(t, statsV6.MinerCount, statsV7.MinerCount)
	require.Equal(t, statsV6.MinerAboveMinPowerCount, statsV7.MinerAboveMinPowerCount)
	require.Equal(t, statsV6.ThisEpochReward, statsV7.ThisEpochReward)
	require.Equal(t, statsV6.ThisEpochRewardSmoothed, statsV7.ThisEpochRewardSmoothed)
	require.Equal(t, statsV6.ThisEpochBaselinePower, statsV7.ThisEpochBaselinePower)
	require.Equal(t, statsV6.TotalStoragePowerReward, statsV7.TotalStoragePowerReward)
	require.Equal(t, statsV6.TotalClientLockedCollateral, statsV7.TotalClientLockedCollateral)
	require.Equal(t, statsV6.TotalProviderLockedCollateral, statsV7.TotalProviderLockedCollateral)
	require.Equal(t, statsV6.TotalClientStorageFee, statsV7.TotalClientStorageFee)
}

func comparePowerState(t *testing.T, stateV6 power6.State, stateV7 power7.State) {
	require.Equal(t, stateV6.TotalRawBytePower, stateV7.TotalRawBytePower)
	require.Equal(t, stateV6.TotalBytesCommitted, stateV7.TotalBytesCommitted)
	require.Equal(t, stateV6.TotalQualityAdjPower, stateV7.TotalQualityAdjPower)
	require.Equal(t, stateV6.TotalQABytesCommitted, stateV7.TotalQABytesCommitted)
	require.Equal(t, stateV6.TotalPledgeCollateral, stateV7.TotalPledgeCollateral)
	require.Equal(t, stateV6.ThisEpochRawBytePower, stateV7.ThisEpochRawBytePower)
	require.Equal(t, stateV6.ThisEpochPledgeCollateral, stateV7.ThisEpochPledgeCollateral)
	require.Equal(t, stateV6.ThisEpochPledgeCollateral, stateV7.ThisEpochPledgeCollateral)
	require.Equal(t, stateV6.ThisEpochQAPowerSmoothed, stateV7.ThisEpochQAPowerSmoothed)
	require.Equal(t, stateV6.MinerCount, stateV7.MinerCount)
	require.Equal(t, stateV6.MinerAboveMinPowerCount, stateV7.MinerAboveMinPowerCount)
	require.Equal(t, stateV6.CronEventQueue, stateV7.CronEventQueue)
	require.Equal(t, stateV6.FirstCronEpoch, stateV7.FirstCronEpoch)
	require.Equal(t, stateV6.Claims, stateV7.Claims)
	require.Equal(t, stateV6.ProofValidationBatch, stateV7.ProofValidationBatch)
}

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
		minerInfos = append(minerInfos, vm6Util.MinerInfo{WorkerAddress: workerAddress, MinerAddress: minerAddress.IDAddress})
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

	precommitInfo := precommits(t, v, firstSectorNo, numSectors, minerInfos, dealsArray)

	// advance time to when we can prove-commit
	for i := 0; i < 3; i++ {
		v = vm6Util.ProveThenAdvanceOneDeadlineWithCron(t, v, ctxStore, minersToProve)
	}

	for i, minerInfo := range minerInfos {
		vm6Util.ProveCommitSectors(t, v, minerInfo.WorkerAddress, minerInfo.MinerAddress, precommitInfo[i], addDeals)
	}

	return append(minersToProve, minerInfos...), v
}

func TestNv15Migration(t *testing.T) {
	ctx := context.Background()
	bs := ipld.NewBlockStoreInMemory()
	v := vm6.NewVMWithSingletons(ctx, t, bs)
	ctxStore := adt.WrapBlockStore(ctx, bs)
	log := nv15.TestLogger{TB: t}

	v = vm6Util.AdvanceToEpochWithCron(t, v, 200)

	minerInfos, v := createMinersAndSectorsV6(t, ctx, ctxStore, v, 100, 100, 0, false, nil)
	minerInfos, v = createMinersAndSectorsV6(t, ctx, ctxStore, v, 100, 100, 100, true, minerInfos)
	lazyMinerInfos, v := createMinersAndSectorsV6(t, ctx, ctxStore, v, 10100, 2, 1000, true, minerInfos) // Bad miners who don't prove their sectors
	minerInfos, v = createMinersAndSectorsV6(t, ctx, ctxStore, v, 200100, 1, 10_000, true, minerInfos)

	v = vm6Util.AdvanceOneDayWhileProving(t, v, ctxStore, minerInfos)

	startRoot := v.StateRoot()
	cache := nv15.NewMemMigrationCache()
	_, err := nv15.MigrateStateTree(ctx, ctxStore, startRoot, v.GetEpoch(), nv15.Config{MaxWorkers: 1}, log, cache)
	require.NoError(t, err)

	minerInfos, v = createMinersAndSectorsV6(t, ctx, ctxStore, v, 100, 100, 0, false, nil)
	minerInfos, v = createMinersAndSectorsV6(t, ctx, ctxStore, v, 100, 100, 100, true, minerInfos)
	v = vm6Util.AdvanceOneDayWhileProving(t, v, ctxStore, minerInfos)
	minerInfos = append(minerInfos, lazyMinerInfos...)

	cacheRoot, err := nv15.MigrateStateTree(ctx, ctxStore, v.StateRoot(), v.GetEpoch(), nv15.Config{MaxWorkers: 1}, log, cache)
	require.NoError(t, err)

	networkStatsBefore := vm6.GetNetworkStats(t, v)
	noCacheRoot, err := nv15.MigrateStateTree(ctx, ctxStore, v.StateRoot(), v.GetEpoch(), nv15.Config{MaxWorkers: 1}, log, nv15.NewMemMigrationCache())
	require.NoError(t, err)
	require.True(t, cacheRoot.Equals(noCacheRoot))

	lookup := map[cid.Cid]rt.VMActor{}
	for _, ba := range exported.BuiltinActors() {
		lookup[ba.Code()] = ba
	}

	v7, err := vm7.NewVMAtEpoch(ctx, lookup, ctxStore, noCacheRoot, v.GetEpoch())
	require.NoError(t, err)

	networkStatsAfter := vm7.GetNetworkStats(t, v7)
	compareNetworkStats(t, networkStatsBefore, networkStatsAfter)

	stateTree, err := v7.GetStateTree()
	require.NoError(t, err)
	totalBalance, err := v7.GetTotalActorBalance()
	require.NoError(t, err)
	acc, err := states.CheckStateInvariants(stateTree, totalBalance, v7.GetEpoch()-1)
	require.NoError(t, err)
	require.True(t, acc.IsEmpty(), strings.Join(acc.Messages(), "\n"))

	// Compare miner states
	for _, minerInfo := range minerInfos {
		var oldMinerState miner6.State
		err := v.GetState(minerInfo.MinerAddress, &oldMinerState)
		require.NoError(t, err)
		oldDeadlines, err := oldMinerState.LoadDeadlines(ctxStore)
		require.NoError(t, err)

		var newMinerState miner7.State
		err = v7.GetState(minerInfo.MinerAddress, &newMinerState)
		require.NoError(t, err)
		newDeadlines, err := newMinerState.LoadDeadlines(ctxStore)
		require.NoError(t, err)

		for i := 0; uint64(i) < miner6.WPoStPeriodDeadlines; i++ {
			oldDeadline, err := oldDeadlines.LoadDeadline(v.Store(), uint64(i))
			require.NoError(t, err)
			newDeadline, err := newDeadlines.LoadDeadline(v.Store(), uint64(i))
			require.NoError(t, err)

			require.Equal(t, oldDeadline.TotalSectors, newDeadline.TotalSectors)
			require.Equal(t, oldDeadline.LiveSectors, newDeadline.LiveSectors)
			require.Equal(t, oldDeadline.FaultyPower.Raw, newDeadline.FaultyPower.Raw)
			require.Equal(t, oldDeadline.FaultyPower.QA, newDeadline.FaultyPower.QA)
		}

		oldPower := vm6Util.MinerPower(t, v, ctxStore, minerInfo.MinerAddress)
		newPower := vm7.MinerPower(t, v7, minerInfo.MinerAddress)
		require.Equal(t, oldPower.Raw, newPower.Raw)
		require.Equal(t, oldPower.QA, newPower.QA)

		// Check if every single sector has null SectorKey
		err = newMinerState.ForEachSector(ctxStore, func(si *miner7.SectorOnChainInfo) {
			require.Nil(t, si.SectorKeyCID)
		})
		require.NoError(t, err)
	}

	// Check if the verified registry actor's RemoveDataCapProposalIDs is empty
	var verifRegState verifreg.State
	err = v7.GetState(builtin.VerifiedRegistryActorAddr, &verifRegState)
	require.NoError(t, err)
	proposalIDs, err := adt.AsMap(ctxStore, verifRegState.RemoveDataCapProposalIDs, builtin.DefaultHamtBitwidth)
	require.NoError(t, err)
	keys, err := proposalIDs.CollectKeys()
	require.NoError(t, err)
	require.Nil(t, keys)
}
