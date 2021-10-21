package test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/dline"
	"github.com/filecoin-project/go-state-types/exitcode"
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/filecoin-project/specs-actors/v7/actors/builtin"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v7/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/v7/actors/states"
	"github.com/filecoin-project/specs-actors/v7/support/ipld"
	tutil "github.com/filecoin-project/specs-actors/v7/support/testing"
	"github.com/filecoin-project/specs-actors/v7/support/vm"
)

func TestCommitPoStFlow(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())

	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	sectorSize, err := sealProof.SectorSize()
	require.NoError(t, err)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(10_000), builtin.TokenPrecision), 93837778)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	// advance vm so we can have seal randomness epoch in the past
	v, err = v.WithEpoch(200)
	require.NoError(t, err)

	sectorNumber := abi.SectorNumber(100)
	precommits := preCommitSectors(t, v, 1, 1, worker, minerAddrs.IDAddress, sealProof, sectorNumber, true)

	balances := vm.GetMinerBalances(t, v, minerAddrs.IDAddress)
	assert.True(t, balances.PreCommitDeposit.GreaterThan(big.Zero()))

	proveTime := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	//
	// overdue precommit
	//

	t.Run("missed prove commit results in precommit expiry and cleanup", func(t *testing.T) {
		// advance time to precommit clean up epoch
		cleanUpTime := proveTime + miner.ExpiredPreCommitCleanUpDelay
		v, dlInfo := vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, cleanUpTime)

		// advanced one more deadline so precommit clean up is reached
		tv, err := v.WithEpoch(dlInfo.Close)
		require.NoError(t, err)

		// run cron which should clean up precommit
		vm.ApplyOk(t, tv, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

		vm.ExpectInvocation{
			To:     builtin.CronActorAddr,
			Method: builtin.MethodsCron.EpochTick,
			SubInvocations: []vm.ExpectInvocation{
				{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.OnEpochTickEnd, SubInvocations: []vm.ExpectInvocation{
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
					{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.OnDeferredCronEvent, SubInvocations: []vm.ExpectInvocation{
						// The call to burnt funds indicates the overdue precommit has been penalized
						{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend, Value: vm.ExpectAttoFil(precommits[0].PreCommitDeposit)},
						// No re-enrollment of cron because burning of PCD discontinues miner cron scheduling
					}},
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
				}},
				{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick},
			},
		}.Matches(t, tv.LastInvocation())

		// precommit deposit has been reset
		balances := vm.GetMinerBalances(t, tv, minerAddrs.IDAddress)
		assert.Equal(t, big.Zero(), balances.InitialPledge)
		assert.Equal(t, big.Zero(), balances.PreCommitDeposit)

		// no power is added
		networkStats := vm.GetNetworkStats(t, tv)
		assert.Equal(t, big.Zero(), networkStats.TotalBytesCommitted)
		assert.Equal(t, big.Zero(), networkStats.TotalPledgeCollateral)
		assert.Equal(t, big.Zero(), networkStats.TotalRawBytePower)
		assert.Equal(t, big.Zero(), networkStats.TotalQualityAdjPower)
	})

	//
	// prove and verify
	//

	// Prove commit sector after max seal duration
	v, err = v.WithEpoch(proveTime)
	require.NoError(t, err)
	proveCommitParams := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}
	vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)

	vm.ExpectInvocation{
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.ProveCommitSector,
		Params: vm.ExpectObject(&proveCommitParams),
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.ComputeDataCommitment},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.SubmitPoRepForBulkVerify},
		},
	}.Matches(t, v.LastInvocation())

	// In the same epoch, trigger cron to validate prove commit
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	vm.ExpectInvocation{
		To:     builtin.CronActorAddr,
		Method: builtin.MethodsCron.EpochTick,
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.OnEpochTickEnd, SubInvocations: []vm.ExpectInvocation{
				{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
				// expect confirm sector proofs valid because we prove committed,
				// but not an on deferred cron event because this is not a deadline boundary
				{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.ConfirmSectorProofsValid, SubInvocations: []vm.ExpectInvocation{
					{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdatePledgeTotal},
				}},
				{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
			}},
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick},
		},
	}.Matches(t, v.Invocations()[1])

	// precommit deposit is released, ipr is added
	balances = vm.GetMinerBalances(t, v, minerAddrs.IDAddress)
	assert.True(t, balances.InitialPledge.GreaterThan(big.Zero()))
	assert.Equal(t, big.Zero(), balances.PreCommitDeposit)

	// power is unproven so network stats are unchanged
	networkStats := vm.GetNetworkStats(t, v)
	assert.Equal(t, big.Zero(), networkStats.TotalBytesCommitted)
	assert.True(t, networkStats.TotalPledgeCollateral.GreaterThan(big.Zero()))

	//
	// Submit PoSt
	//

	// advance to proving period
	dlInfo, pIdx, v := vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)
	var minerState miner.State
	err = v.GetState(minerAddrs.IDAddress, &minerState)
	require.NoError(t, err)

	sector, found, err := minerState.GetSector(v.Store(), sectorNumber)
	require.NoError(t, err)
	require.True(t, found)

	t.Run("submit PoSt succeeds", func(t *testing.T) {
		tv, err := v.WithEpoch(v.GetEpoch())
		require.NoError(t, err)

		partitions := []miner.PoStPartition{{
			Index:   pIdx,
			Skipped: bitfield.New(),
		}}
		sectorPower := miner.PowerForSector(sectorSize, sector)
		submitWindowPoSt(t, tv, worker, minerAddrs.IDAddress, dlInfo, partitions, sectorPower)

		// miner still has initial pledge
		balances = vm.GetMinerBalances(t, tv, minerAddrs.IDAddress)
		assert.True(t, balances.InitialPledge.GreaterThan(big.Zero()))

		// committed bytes are added (miner would have gained power if minimum requirement were met)
		networkStats := vm.GetNetworkStats(t, tv)
		assert.Equal(t, big.NewInt(int64(sectorSize)), networkStats.TotalBytesCommitted)
		assert.True(t, networkStats.TotalPledgeCollateral.GreaterThan(big.Zero()))

		// Trigger cron to keep reward accounting correct
		vm.ApplyOk(t, tv, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

		stateTree, err := tv.GetStateTree()
		require.NoError(t, err)
		totalBalance, err := tv.GetTotalActorBalance()
		require.NoError(t, err)
		acc, err := states.CheckStateInvariants(stateTree, totalBalance, tv.GetEpoch())
		require.NoError(t, err)
		assert.True(t, acc.IsEmpty(), strings.Join(acc.Messages(), "\n"))
	})

	t.Run("skip sector", func(t *testing.T) {
		tv, err := v.WithEpoch(v.GetEpoch())
		require.NoError(t, err)

		// Submit PoSt
		submitParams := miner.SubmitWindowedPoStParams{
			Deadline: dlInfo.Index,
			Partitions: []miner.PoStPartition{{
				Index:   pIdx,
				Skipped: bitfield.NewFromSet([]uint64{uint64(sectorNumber)}),
			}},
			Proofs: []proof.PoStProof{{
				PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			}},
			ChainCommitEpoch: dlInfo.Challenge,
			ChainCommitRand:  []byte(vm.RandString),
		}
		// PoSt is rejected for skipping all sectors.
		result := vm.RequireApplyMessage(t, tv, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams, t.Name())
		assert.Equal(t, exitcode.ErrIllegalArgument, result.Code)

		vm.ExpectInvocation{
			To:       minerAddrs.IDAddress,
			Method:   builtin.MethodsMiner.SubmitWindowedPoSt,
			Params:   vm.ExpectObject(&submitParams),
			Exitcode: exitcode.ErrIllegalArgument,
		}.Matches(t, tv.LastInvocation())

		// miner still has initial pledge
		balances = vm.GetMinerBalances(t, v, minerAddrs.IDAddress)
		assert.True(t, balances.InitialPledge.GreaterThan(big.Zero()))

		// network power is unchanged
		networkStats := vm.GetNetworkStats(t, tv)
		assert.Equal(t, big.Zero(), networkStats.TotalBytesCommitted)
		assert.True(t, networkStats.TotalPledgeCollateral.GreaterThan(big.Zero()))
	})

	t.Run("missed first PoSt deadline", func(t *testing.T) {
		// move to proving period end
		tv, err := v.WithEpoch(dlInfo.Last())
		require.NoError(t, err)

		// Run cron to detect missing PoSt
		vm.ApplyOk(t, tv, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

		vm.ExpectInvocation{
			To:     builtin.CronActorAddr,
			Method: builtin.MethodsCron.EpochTick,
			SubInvocations: []vm.ExpectInvocation{
				{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.OnEpochTickEnd, SubInvocations: []vm.ExpectInvocation{
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
					{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.OnDeferredCronEvent, SubInvocations: []vm.ExpectInvocation{
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
					}},
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
				}},
				{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick},
			},
		}.Matches(t, tv.LastInvocation())

		// network power is unchanged
		networkStats := vm.GetNetworkStats(t, tv)
		assert.Equal(t, big.Zero(), networkStats.TotalBytesCommitted)
		assert.True(t, networkStats.TotalPledgeCollateral.GreaterThan(big.Zero()))
	})
}

func TestMeasurePreCommitGas(t *testing.T) {
	// Number of sectors to pre-commit in each test.
	// We don't expect miners to have very large collections of outstanding pre-commits, but should
	// model at least the number for a maximally-aggregated prove-commit.
	// Smaller numbers show a much greater improvement to batching.
	sectorCount := 1000

	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	metrics := ipld.NewMetricsBlockStore(blkStore)
	v := vm.NewVMWithSingletons(ctx, t, metrics)
	v.SetStatsSource(metrics)

	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(100_000), builtin.TokenPrecision), 93837778)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	// Advance vm so we can have seal randomness epoch in the past
	v, err = v.WithEpoch(abi.ChainEpoch(200))
	require.NoError(t, err)

	// Note that the miner's pre-committed sector HAMT will increase in size and depth over the course of this
	// test, making later operations slightly more expensive.
	// The expected state of this structure is to have not many sectors in it.
	statsKey := vm.MethodKey{Code: builtin.StorageMinerActorCodeID, Method: builtin.MethodsMiner.PreCommitSectorBatch}
	p := message.NewPrinter(language.English)
	_, _ = p.Printf("Sector count\t%d\n", sectorCount)
	_, _ = p.Printf("Batch size\tIPLD cost\tCalls cost\tTotal per sector\tTotal per batch\n")
	for batchSize := 1; batchSize <= miner.PreCommitSectorBatchMaxSize; batchSize *= 2 {
		// Clone the VM so each batch get the same starting state.
		tv, err := v.WithEpoch(v.GetEpoch())
		require.NoError(t, err)
		firstSectorNo := abi.SectorNumber(sectorCount * batchSize)
		preCommitSectors(t, tv, sectorCount, batchSize, addrs[0], minerAddrs.IDAddress, sealProof, firstSectorNo, true)

		stats := tv.GetCallStats()
		precommitStats := stats[statsKey]
		ipldCost := ipldGas(precommitStats)
		callCost := callGas(precommitStats)
		perSector := (ipldCost + callCost) / uint64(sectorCount)
		perBatch := perSector * uint64(batchSize)
		_, _ = p.Printf("%d\t%d\t%d\t%d\t%d\n", batchSize, ipldCost, callCost, perSector, perBatch)
	}
	fmt.Println()
}

func TestMeasurePoRepGas(t *testing.T) {
	sectorCount := 819
	fmt.Printf("Batch Size = %d\n", sectorCount)
	printPoRepMsgGas(sectorCount)

	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	metrics := ipld.NewMetricsBlockStore(blkStore)
	v := vm.NewVMWithSingletons(ctx, t, metrics)
	v.SetStatsSource(metrics)
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)

	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(10_000), builtin.TokenPrecision), 93837778)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	// advance vm so we can have seal randomness epoch in the past
	v, err = v.WithEpoch(abi.ChainEpoch(200))
	require.NoError(t, err)

	//
	// precommit sectors
	//
	firstSectorNo := abi.SectorNumber(100)
	precommits := preCommitSectors(t, v, sectorCount, miner.PreCommitSectorBatchMaxSize, addrs[0], minerAddrs.IDAddress, sealProof, firstSectorNo, true)

	balances := vm.GetMinerBalances(t, v, minerAddrs.IDAddress)
	assert.True(t, balances.PreCommitDeposit.GreaterThan(big.Zero()))

	// advance time to max seal duration
	proveTime := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	//
	// prove and verify
	//
	v, err = v.WithEpoch(proveTime)
	require.NoError(t, err)
	sectorsProven := 0
	crons := 0
	// Prove sectors in batches of 200 to avoid going over the max 200 commits per miner per power cron invocation
	for sectorsProven < sectorCount {
		sectorsToProveThisCron := min(sectorCount-sectorsProven, power.MaxMinerProveCommitsPerEpoch)
		for i := 0; i < sectorsToProveThisCron; i++ {
			// Prove commit sector at a valid epoch
			proveCommitParams := miner.ProveCommitSectorParams{
				SectorNumber: precommits[i+sectorsProven].Info.SectorNumber,
			}
			vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)

			vm.ExpectInvocation{
				To:     minerAddrs.IDAddress,
				Method: builtin.MethodsMiner.ProveCommitSector,
				Params: vm.ExpectObject(&proveCommitParams),
				SubInvocations: []vm.ExpectInvocation{
					{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.ComputeDataCommitment},
					{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.SubmitPoRepForBulkVerify},
				},
			}.Matches(t, v.Invocations()[sectorsProven+crons+i])
		}
		sectorsProven += sectorsToProveThisCron

		proveCommitKey := vm.MethodKey{Code: builtin.StorageMinerActorCodeID, Method: builtin.MethodsMiner.ProveCommitSector}
		stats := v.GetCallStats()
		fmt.Printf("\n--------------------- Batch %d ---------------------\n", crons)
		printCallStats(proveCommitKey, stats[proveCommitKey], "")

		// In the same epoch, trigger cron to validate prove commits
		vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
		crons += 1
		cronKey := vm.MethodKey{Code: builtin.CronActorCodeID, Method: builtin.MethodsCron.EpochTick}
		stats = v.GetCallStats()
		printCallStats(cronKey, stats[cronKey], "")

		vm.ExpectInvocation{
			To:     builtin.CronActorAddr,
			Method: builtin.MethodsCron.EpochTick,
			SubInvocations: []vm.ExpectInvocation{
				{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.OnEpochTickEnd, SubInvocations: []vm.ExpectInvocation{
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
					// expect confirm sector proofs valid because we prove committed,
					// but not an on deferred cron event because this is not a deadline boundary
					{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.ConfirmSectorProofsValid, SubInvocations: []vm.ExpectInvocation{
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdatePledgeTotal},
					}},
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
				}},
				{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick},
			},
		}.Matches(t, v.Invocations()[sectorsProven+crons-1])
	}

	// precommit deposit is released, ipr is added
	balances = vm.GetMinerBalances(t, v, minerAddrs.IDAddress)
	assert.True(t, balances.InitialPledge.GreaterThan(big.Zero()))
	assert.Equal(t, big.Zero(), balances.PreCommitDeposit)

	// power is unproven so network stats are unchanged
	networkStats := vm.GetNetworkStats(t, v)
	assert.Equal(t, big.Zero(), networkStats.TotalBytesCommitted)
	assert.True(t, networkStats.TotalPledgeCollateral.GreaterThan(big.Zero()))
}

// Tests batch pre-commit and aggregate prove-commit in various interleavings and batch sizes.
func TestBatchOnboarding(t *testing.T) {
	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	metrics := ipld.NewMetricsBlockStore(blkStore)
	v := vm.NewVMWithSingletons(ctx, t, metrics)
	v.SetStatsSource(metrics)

	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	sectorSize, err := sealProof.SectorSize()
	require.NoError(t, err)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(10_000), builtin.TokenPrecision), 93837778)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	v, err = v.WithEpoch(abi.ChainEpoch(200))
	require.NoError(t, err)

	// A series of pre-commit and prove-commit actions intended to cover paths including:
	// - different pre-commit batch sizes
	// - different prove-commit aggregate sizes
	// - multiple pre-commit batches before proof
	// - proving only some of the pre-commits
	// - proving part of multiple pre-commit batches
	// - proving all pre-commits across multiple batches
	// - interleaving of pre- and prove-commit
	//
	// Sectors are still proven in the order of pre-commitment.
	var precommits []*miner.SectorPreCommitOnChainInfo
	nextSectorNo := abi.SectorNumber(0)
	preCommittedCount, provenCount := 0, 0
	for _, spec := range []struct {
		epochDelay               abi.ChainEpoch // epochs to advance since the prior action
		preCommitSectorCount     int            // sectors to batch pre-commit
		preCommitBatchSize       int            // batch size (multiple batches if committing more)
		proveCommitSectorCount   int            // sectors to aggregate prove-commit
		proveCommitAggregateSize int            // aggregate size (multiple aggregates if proving more)
	}{
		{ // Pre: 10, Proven: 0
			epochDelay:           0,
			preCommitSectorCount: 10,
			preCommitBatchSize:   miner.PreCommitSectorBatchMaxSize,
		},
		{ // Pre: 30, Proven: 0
			epochDelay:           1,
			preCommitSectorCount: 20,
			preCommitBatchSize:   12,
		},
		{ // Pre: 30, Proven: 8
			epochDelay:               miner.PreCommitChallengeDelay + 1,
			proveCommitSectorCount:   8,
			proveCommitAggregateSize: miner.MaxAggregatedSectors,
		},
		{ // Pre: 30, Proven: 16 (spanning pre-commit batches)
			epochDelay:               1,
			proveCommitSectorCount:   8,
			proveCommitAggregateSize: 4,
		},
		{ // Pre: 40, Proven: 16
			epochDelay:           1,
			preCommitSectorCount: 10,
			preCommitBatchSize:   4,
		},
		{ // Pre: 40, Proven: 40
			epochDelay:               miner.PreCommitChallengeDelay + 1,
			proveCommitSectorCount:   24,
			proveCommitAggregateSize: 10,
		},
	} {
		v, err = v.WithEpoch(v.GetEpoch() + spec.epochDelay)
		require.NoError(t, err)

		if spec.preCommitSectorCount > 0 {
			newPrecommits := preCommitSectors(t, v, spec.preCommitSectorCount, spec.preCommitBatchSize, worker, minerAddrs.IDAddress,
				sealProof, nextSectorNo, nextSectorNo == 0)
			precommits = append(precommits, newPrecommits...)
			nextSectorNo += abi.SectorNumber(spec.preCommitSectorCount)
			preCommittedCount += spec.preCommitSectorCount
		}

		if spec.proveCommitSectorCount > 0 {
			toProve := precommits[:spec.proveCommitSectorCount]
			precommits = precommits[spec.proveCommitSectorCount:]
			proveCommitSectors(t, v, worker, minerAddrs.IDAddress, toProve, spec.proveCommitAggregateSize)
			provenCount += spec.proveCommitSectorCount
		}
	}

	//
	// Window PoSt all proven sectors.
	//

	// The sectors are all in the same partition. Advance to it's proving window.
	dlInfo, pIdx, v := vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, abi.SectorNumber(0))
	var minerState miner.State
	err = v.GetState(minerAddrs.IDAddress, &minerState)
	require.NoError(t, err)

	sector, found, err := minerState.GetSector(v.Store(), abi.SectorNumber(0))
	require.NoError(t, err)
	require.True(t, found)

	partitions := []miner.PoStPartition{{
		Index:   pIdx,
		Skipped: bitfield.New(),
	}}
	newPower := miner.PowerForSector(sectorSize, sector).Mul(big.NewInt(int64(provenCount)))
	submitWindowPoSt(t, v, worker, minerAddrs.IDAddress, dlInfo, partitions, newPower)

	// Miner has initial pledge
	balances := vm.GetMinerBalances(t, v, minerAddrs.IDAddress)
	assert.True(t, balances.InitialPledge.GreaterThan(big.Zero()))

	// Committed bytes are added (miner would have gained power if minimum requirement were met)
	networkStats := vm.GetNetworkStats(t, v)
	assert.Equal(t, big.NewInt(int64(sectorSize)*int64(provenCount)), networkStats.TotalBytesCommitted)
	assert.True(t, networkStats.TotalPledgeCollateral.GreaterThan(big.Zero()))

	// Trigger cron to keep reward accounting correct
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	stateTree, err := v.GetStateTree()
	require.NoError(t, err)
	totalBalance, err := v.GetTotalActorBalance()
	require.NoError(t, err)
	acc, err := states.CheckStateInvariants(stateTree, totalBalance, v.GetEpoch())
	require.NoError(t, err)
	assert.True(t, acc.IsEmpty(), strings.Join(acc.Messages(), "\n"))
}

func TestAggregateOnePreCommitExpires(t *testing.T) {
	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	v := vm.NewVMWithSingletons(ctx, t, blkStore)
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(10_000), builtin.TokenPrecision), 93837778)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	// advance vm so we can have seal randomness epoch in the past
	v, err = v.WithEpoch(abi.ChainEpoch(200))
	require.NoError(t, err)

	//
	// precommit secotrs
	//
	firstSectorNo := abi.SectorNumber(100)
	// early precommit
	earlyPreCommitTime := v.GetEpoch()
	earlyPrecommits := preCommitSectors(t, v, 1, miner.PreCommitSectorBatchMaxSize, addrs[0], minerAddrs.IDAddress, sealProof, firstSectorNo, true)

	earlyPreCommitInvalid := earlyPreCommitTime + miner.MaxProveCommitDuration[sealProof] + abi.ChainEpoch(1)
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, earlyPreCommitInvalid)

	// later precommits
	laterPrecommits := preCommitSectors(t, v, 3, miner.PreCommitSectorBatchMaxSize, addrs[0], minerAddrs.IDAddress, sealProof, firstSectorNo+1, false)
	allPrecommits := append(earlyPrecommits, laterPrecommits...)
	sectorNosBf := precommitSectorNumbers(allPrecommits)

	// Advance minimum epochs past later precommits for later commits to be valid
	proveTime := v.GetEpoch() + miner.PreCommitChallengeDelay + abi.ChainEpoch(1)
	v, dlInfo := vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, dlInfo.Close)
	// Assert that precommit should not yet be cleaned up. This makes fixing this test easier if parameters change.
	require.True(t, proveTime < earlyPreCommitTime+miner.MaxProveCommitDuration[sealProof]+miner.ExpiredPreCommitCleanUpDelay)
	// Assert that we have a valid aggregate batch size
	aggSectorsCount, err := sectorNosBf.Count()
	require.NoError(t, err)
	require.True(t, aggSectorsCount >= miner.MinAggregatedSectors && aggSectorsCount < miner.MaxAggregatedSectors)

	proveCommitAggregateParams := miner.ProveCommitAggregateParams{
		SectorNumbers: sectorNosBf,
	}
	// Aggregate passes, proving the 2 unexpired commitments
	vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitAggregate, &proveCommitAggregateParams)
	vm.ExpectInvocation{
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.ProveCommitAggregate,
		Params: vm.ExpectObject(&proveCommitAggregateParams),
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.ComputeDataCommitment},
			{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdatePledgeTotal},
			{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
		},
	}.Matches(t, v.LastInvocation())

	balances := vm.GetMinerBalances(t, v, minerAddrs.IDAddress)
	assert.True(t, balances.InitialPledge.GreaterThan(big.Zero()))
	assert.True(t, balances.PreCommitDeposit.GreaterThan(big.Zero()))

}

func TestAggregateSizeLimits(t *testing.T) {
	overSizedBatch := 820
	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	v := vm.NewVMWithSingletons(ctx, t, blkStore)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(10_000), big.NewInt(1e18)), 93837778)

	// create miner
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	// advance vm so we can have seal randomness epoch in the past
	v, err = v.WithEpoch(abi.ChainEpoch(200))
	require.NoError(t, err)

	//
	// precommit sectors
	//
	firstSectorNo := abi.SectorNumber(100)
	precommits := preCommitSectors(t, v, overSizedBatch, miner.PreCommitSectorBatchMaxSize, addrs[0], minerAddrs.IDAddress, sealProof, firstSectorNo, true)
	balances := vm.GetMinerBalances(t, v, minerAddrs.IDAddress)
	assert.True(t, balances.PreCommitDeposit.GreaterThan(big.Zero()))

	// advance time to max seal duration
	proveTime := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	//
	// attempt proving with invalid args
	//

	// Fail with too many sectors
	v, err = v.WithEpoch(proveTime)
	require.NoError(t, err)
	sectorNosBf := precommitSectorNumbers(precommits)

	proveCommitAggregateTooManyParams := miner.ProveCommitAggregateParams{
		SectorNumbers: sectorNosBf,
	}
	res := vm.RequireApplyMessage(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitAggregate, &proveCommitAggregateTooManyParams, t.Name())
	assert.Equal(t, exitcode.ErrIllegalArgument, res.Code) // fail with too many aggregates

	// Fail with too few sectors
	tooFewSectorNosBf := precommitSectorNumbers(precommits[:miner.MinAggregatedSectors-1])
	proveCommitAggregateTooFewParams := miner.ProveCommitAggregateParams{
		SectorNumbers: tooFewSectorNosBf,
	}
	res = vm.RequireApplyMessage(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitAggregate, &proveCommitAggregateTooFewParams, t.Name())
	assert.Equal(t, exitcode.ErrIllegalArgument, res.Code)

	// Fail with proof too big
	justRightSectorNosBf := precommitSectorNumbers(precommits[:miner.MaxAggregatedSectors])
	proveCommitAggregateTooBigProofParams := miner.ProveCommitAggregateParams{
		SectorNumbers:  justRightSectorNosBf,
		AggregateProof: make([]byte, miner.MaxAggregateProofSize+1),
	}
	res = vm.RequireApplyMessage(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitAggregate, &proveCommitAggregateTooBigProofParams, t.Name())
	assert.Equal(t, exitcode.ErrIllegalArgument, res.Code)
}

func TestAggregateBadSender(t *testing.T) {
	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	v := vm.NewVMWithSingletons(ctx, t, blkStore)

	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	addrs := vm.CreateAccounts(ctx, t, v, 2, big.Mul(big.NewInt(10_000), builtin.TokenPrecision), 93837778)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	// advance vm so we can have seal randomness epoch in the past
	v, err = v.WithEpoch(abi.ChainEpoch(200))
	require.NoError(t, err)

	//
	// precommit good secotrs
	//
	firstSectorNo := abi.SectorNumber(100)
	// early precommit
	preCommitTime := v.GetEpoch()
	precommits := preCommitSectors(t, v, 4, miner.PreCommitSectorBatchMaxSize, addrs[0], minerAddrs.IDAddress, sealProof, firstSectorNo, true)

	//
	// attempt proving with invalid args
	//

	// advance time to max seal duration
	proveTime := preCommitTime + miner.MaxProveCommitDuration[sealProof]
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)
	v, err = v.WithEpoch(proveTime)
	require.NoError(t, err)

	sectorNosBf := precommitSectorNumbers(precommits)
	proveCommitAggregateParams := miner.ProveCommitAggregateParams{
		SectorNumbers: sectorNosBf,
	}
	res := vm.RequireApplyMessage(t, v, addrs[1], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitAggregate, &proveCommitAggregateParams, t.Name())
	assert.Equal(t, exitcode.ErrForbidden, res.Code)
}

func TestAggregateBadSectorNumber(t *testing.T) {
	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	v := vm.NewVMWithSingletons(ctx, t, blkStore)

	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)

	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(10_000), builtin.TokenPrecision), 93837778)

	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	// advance vm so we can have seal randomness epoch in the past
	v, err = v.WithEpoch(abi.ChainEpoch(200))
	require.NoError(t, err)

	//
	// precommit good secotrs
	//
	firstSectorNo := abi.SectorNumber(100)
	// early precommit
	preCommitTime := v.GetEpoch()
	precommits := preCommitSectors(t, v, 4, miner.PreCommitSectorBatchMaxSize, addrs[0], minerAddrs.IDAddress, sealProof, firstSectorNo, true)

	//
	// attempt proving with invalid args
	//

	// advance time to max seal duration
	proveTime := preCommitTime + miner.MaxProveCommitDuration[sealProof]
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)
	v, err = v.WithEpoch(proveTime)

	// construct invalid bitfield with a non-committed sector number > abi.MaxSectorNumber
	require.NoError(t, err)
	sectorNosBf := precommitSectorNumbers(precommits)
	sectorNosBf.Set(abi.MaxSectorNumber + 1)

	proveCommitAggregateTooManyParams := miner.ProveCommitAggregateParams{
		SectorNumbers: sectorNosBf,
	}
	res := vm.RequireApplyMessage(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitAggregate, &proveCommitAggregateTooManyParams, t.Name())
	assert.Equal(t, exitcode.ErrIllegalArgument, res.Code)
}

func TestMeasureAggregatePorepGas(t *testing.T) {
	sectorCount := 819
	fmt.Printf("batch size = %d\n", sectorCount)

	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	metrics := ipld.NewMetricsBlockStore(blkStore)
	v := vm.NewVMWithSingletons(ctx, t, metrics)
	v.SetStatsSource(metrics)

	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(10_000), builtin.TokenPrecision), 93837778)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	// advance vm so we can have seal randomness epoch in the past
	v, err = v.WithEpoch(abi.ChainEpoch(200))
	require.NoError(t, err)

	//
	// precommit sectors
	//
	firstSectorNo := abi.SectorNumber(100)
	precommits := preCommitSectors(t, v, sectorCount, miner.PreCommitSectorBatchMaxSize, addrs[0], minerAddrs.IDAddress, sealProof, firstSectorNo, true)
	balances := vm.GetMinerBalances(t, v, minerAddrs.IDAddress)
	assert.True(t, balances.PreCommitDeposit.GreaterThan(big.Zero()))

	// advance time to max seal duration
	proveTime := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	//
	// prove and verify
	//
	v, err = v.WithEpoch(proveTime)
	require.NoError(t, err)
	sectorNosBf := precommitSectorNumbers(precommits)

	proveCommitAggregateParams := miner.ProveCommitAggregateParams{
		SectorNumbers: sectorNosBf,
	}
	vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitAggregate, &proveCommitAggregateParams)
	vm.ExpectInvocation{
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.ProveCommitAggregate,
		Params: vm.ExpectObject(&proveCommitAggregateParams),
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.ComputeDataCommitment},
			{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdatePledgeTotal},
			{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
		},
	}.Matches(t, v.LastInvocation())

	proveCommitAggrKey := vm.MethodKey{Code: builtin.StorageMinerActorCodeID, Method: builtin.MethodsMiner.ProveCommitAggregate}
	stats := v.GetCallStats()
	printCallStats(proveCommitAggrKey, stats[proveCommitAggrKey], "")

	// In the same epoch, trigger cron to
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	cronKey := vm.MethodKey{Code: builtin.CronActorCodeID, Method: builtin.MethodsCron.EpochTick}
	stats = v.GetCallStats()
	printCallStats(cronKey, stats[cronKey], "")

	vm.ExpectInvocation{
		To:     builtin.CronActorAddr,
		Method: builtin.MethodsCron.EpochTick,
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.OnEpochTickEnd, SubInvocations: []vm.ExpectInvocation{
				{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
				// expect no confirm sector proofs valid because we prove committed with aggregation.
				// expect no on deferred cron event because this is not a deadline boundary
				{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
			}},
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick},
		},
	}.Matches(t, v.Invocations()[1])

	// precommit deposit is released, ipr is added
	balances = vm.GetMinerBalances(t, v, minerAddrs.IDAddress)
	assert.True(t, balances.InitialPledge.GreaterThan(big.Zero()))
	assert.Equal(t, big.Zero(), balances.PreCommitDeposit)

	// power is unproven so network stats are unchanged
	networkStats := vm.GetNetworkStats(t, v)
	assert.Equal(t, big.Zero(), networkStats.TotalBytesCommitted)
	assert.True(t, networkStats.TotalPledgeCollateral.GreaterThan(big.Zero()))
}

func preCommitSectors(t *testing.T, v *vm.VM, count, batchSize int, worker, mAddr address.Address, sealProof abi.RegisteredSealProof,
	sectorNumberBase abi.SectorNumber, expectCronEnrollment bool) []*miner.SectorPreCommitOnChainInfo {
	invocsCommon := []vm.ExpectInvocation{
		{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
	}
	invocFirst := vm.ExpectInvocation{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent}

	sectorIndex := 0
	for sectorIndex < count {
		msgSectorIndexStart := sectorIndex
		invocs := invocsCommon

		// Prepare message.
		params := miner.PreCommitSectorBatchParams{Sectors: make([]miner0.SectorPreCommitInfo, batchSize)}
		for j := 0; j < batchSize && sectorIndex < count; j++ {
			sectorNumber := sectorNumberBase + abi.SectorNumber(sectorIndex)
			sealedCid := tutil.MakeCID(fmt.Sprintf("%d", sectorNumber), &miner.SealedCIDPrefix)
			params.Sectors[j] = miner0.SectorPreCommitInfo{
				SealProof:     sealProof,
				SectorNumber:  sectorNumber,
				SealedCID:     sealedCid,
				SealRandEpoch: v.GetEpoch() - 1,
				DealIDs:       nil,
				Expiration:    v.GetEpoch() + miner.MinSectorExpiration + miner.MaxProveCommitDuration[sealProof] + 100,
			}
			sectorIndex++
		}
		if sectorIndex == count && sectorIndex%batchSize != 0 {
			// Trim the last, partial batch.
			params.Sectors = params.Sectors[:sectorIndex%batchSize]
		}

		// Finalize invocation expectation list
		if len(params.Sectors) > 1 {
			aggFee := miner.AggregatePreCommitNetworkFee(len(params.Sectors), big.Zero())
			invocs = append(invocs, vm.ExpectInvocation{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend, Value: &aggFee})
		}
		if expectCronEnrollment && msgSectorIndexStart == 0 {
			invocs = append(invocs, invocFirst)
		}
		vm.ApplyOk(t, v, worker, mAddr, big.Zero(), builtin.MethodsMiner.PreCommitSectorBatch, &params)
		vm.ExpectInvocation{
			To:             mAddr,
			Method:         builtin.MethodsMiner.PreCommitSectorBatch,
			Params:         vm.ExpectObject(&params),
			SubInvocations: invocs,
		}.Matches(t, v.LastInvocation())
	}

	// Extract chain state.
	var minerState miner.State
	err := v.GetState(mAddr, &minerState)
	require.NoError(t, err)

	precommits := make([]*miner.SectorPreCommitOnChainInfo, count)
	for i := 0; i < count; i++ {
		precommit, found, err := minerState.GetPrecommittedSector(v.Store(), sectorNumberBase+abi.SectorNumber(i))
		require.NoError(t, err)
		require.True(t, found)
		precommits[i] = precommit
	}
	return precommits
}

// Proves pre-committed sectors as batches of aggSize.
func proveCommitSectors(t *testing.T, v *vm.VM, worker, actor address.Address, precommits []*miner.SectorPreCommitOnChainInfo, aggSize int) {
	for len(precommits) > 0 {
		batchSize := min(aggSize, len(precommits))
		toProve := precommits[:batchSize]
		precommits = precommits[batchSize:]

		sectorNosBf := precommitSectorNumbers(toProve)
		proveCommitAggregateParams := miner.ProveCommitAggregateParams{
			SectorNumbers: sectorNosBf,
		}
		vm.ApplyOk(t, v, worker, actor, big.Zero(), builtin.MethodsMiner.ProveCommitAggregate, &proveCommitAggregateParams)
		vm.ExpectInvocation{
			To:     actor,
			Method: builtin.MethodsMiner.ProveCommitAggregate,
			Params: vm.ExpectObject(&proveCommitAggregateParams),
			SubInvocations: []vm.ExpectInvocation{
				{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.ComputeDataCommitment},
				{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
				{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
				{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdatePledgeTotal},
				{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
			},
		}.Matches(t, v.LastInvocation())
	}
}

// Submits a Window PoSt for partitions in a deadline.
func submitWindowPoSt(t *testing.T, v *vm.VM, worker, actor address.Address, dlInfo *dline.Info, partitions []miner.PoStPartition,
	newPower miner.PowerPair) {
	submitParams := miner.SubmitWindowedPoStParams{
		Deadline:   dlInfo.Index,
		Partitions: partitions,
		Proofs: []proof.PoStProof{{
			PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		}},
		ChainCommitEpoch: dlInfo.Challenge,
		ChainCommitRand:  []byte(vm.RandString),
	}
	vm.ApplyOk(t, v, worker, actor, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)

	updatePowerParams := &power.UpdateClaimedPowerParams{
		RawByteDelta:         newPower.Raw,
		QualityAdjustedDelta: newPower.QA,
	}

	vm.ExpectInvocation{
		To:     actor,
		Method: builtin.MethodsMiner.SubmitWindowedPoSt,
		Params: vm.ExpectObject(&submitParams),
		SubInvocations: []vm.ExpectInvocation{
			{
				To:     builtin.StoragePowerActorAddr,
				Method: builtin.MethodsPower.UpdateClaimedPower,
				Params: vm.ExpectObject(updatePowerParams),
			},
		},
	}.Matches(t, v.LastInvocation())
}

// Returns a bitfield of the sector numbers from a collection pre-committed sectors.
func precommitSectorNumbers(precommits []*miner.SectorPreCommitOnChainInfo) bitfield.BitField {
	intSectorNumbers := make([]uint64, len(precommits))
	for i := range precommits {
		intSectorNumbers[i] = uint64(precommits[i].Info.SectorNumber)
	}
	return bitfield.NewFromSet(intSectorNumbers)
}

func printCallStats(method vm.MethodKey, stats *vm.CallStats, indent string) { // nolint:unused
	p := message.NewPrinter(language.English) // For readable large numbers
	_, _ = p.Printf("%s%v:%d: calls: %d  gets: %d  puts: %d  read: %d  written: %d  avg gets: %.2f, avg puts: %.2f\n",
		indent, builtin.ActorNameByCode(method.Code), method.Method, stats.Calls, stats.Reads, stats.Writes,
		stats.ReadBytes, stats.WriteBytes, float32(stats.Reads)/float32(stats.Calls),
		float32(stats.Writes)/float32(stats.Calls))

	_, _ = p.Printf("%s%v:%d: ipld gas: %d call gas: %d\n", indent, builtin.ActorNameByCode(method.Code), method.Method,
		ipldGas(stats), callGas(stats))

	if stats.SubStats == nil {
		return
	}

	for m, s := range stats.SubStats {
		printCallStats(m, s, indent+"  ")
	}
}

func ipldGas(stats *vm.CallStats) uint64 {
	gasGetObj := uint64(75242)
	gasPutObj := uint64(84070)
	gasPutPerByte := uint64(1)
	gasStorageMultiplier := uint64(1300)
	return stats.Reads*gasGetObj + stats.Writes*gasPutObj + stats.WriteBytes*gasPutPerByte*gasStorageMultiplier
}

func callGas(stats *vm.CallStats) uint64 {
	gasPerCall := uint64(29233)
	return stats.Calls * gasPerCall
}

// Using gas params from filecoin v12 and assumptions about parameters to ProveCommitAggregate print an estimate
// of the gas charged for the on chain ProveCommitAggregate message.
func printPoRepMsgGas(batchSize int) {
	// Ignore message fields and sector number bytes for both.
	// Ignoring non-parma message fields under estimates both by the same amount
	// Ignoring sector numbers/bitfields underestimates current porep compared to aggregate
	// which is the right direction for finding a starting bound (we can optimize later)
	onChainMessageComputeBase := 38863
	onChainMessageStorageBase := 36
	onChainMessageStoragePerByte := 1
	storageGasMultiplier := 1300
	msgBytes := 1920
	msgGas := onChainMessageComputeBase + (onChainMessageStorageBase+onChainMessageStoragePerByte*msgBytes)*storageGasMultiplier

	allMsgsGas := batchSize * msgGas
	fmt.Printf("%d batchsize: all proof param byte gas: %d\n", batchSize, allMsgsGas)
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
