package test

import (
	"context"
	"strconv"
	"testing"

	"github.com/filecoin-project/specs-actors/v8/actors/util/adt"

	"github.com/filecoin-project/specs-actors/v8/actors/builtin/power"

	"github.com/filecoin-project/go-address"

	"github.com/filecoin-project/go-bitfield"
	tutil "github.com/filecoin-project/specs-actors/v8/support/testing"

	"github.com/filecoin-project/go-state-types/exitcode"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v8/actors/builtin"
	"github.com/filecoin-project/specs-actors/v8/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v8/support/ipld"
	"github.com/filecoin-project/specs-actors/v8/support/vm"
)

// ---- Success cases ----

// Tests that an active CC sector can be correctly upgraded, and the expected state changes occur
func TestSimplePathSuccess(t *testing.T) {
	createMinerAndUpgradeASector(t)
}

// Tests a successful upgrade, followed by the sector going faulty and recovering
func TestFullPathSuccess(t *testing.T) {
	v, sectorInfo, worker, minerAddrs, deadlineIndex, partitionIndex, ss := createMinerAndUpgradeASector(t)
	sectorNumber := sectorInfo.SectorNumber

	// submit post successfully
	deadlineInfo, _, v := vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)
	vm.SubmitPoSt(t, v, minerAddrs.IDAddress, worker, deadlineInfo, partitionIndex)

	// move out of the sector's deadline
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, v.GetEpoch()+miner.WPoStChallengeWindow)
	v = vm.AdvanceOneEpochWithCron(t, v)
	require.True(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, deadlineIndex, partitionIndex, sectorNumber))

	// miss next post, lose power, become faulty :'(
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, v.GetEpoch()+miner.WPoStProvingPeriod)
	require.False(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, deadlineIndex, partitionIndex, sectorNumber))
	require.True(t, vm.CheckSectorFaulty(t, v, minerAddrs.IDAddress, deadlineIndex, partitionIndex, sectorNumber))

	minerPower := vm.MinerPower(t, v, minerAddrs.IDAddress)
	require.True(t, minerPower.IsZero())

	// enter a deadline where recovery declaration is valid
	v = vm.AdvanceOneEpochWithCron(t, v)
	vm.DeclareRecovery(t, v, minerAddrs.IDAddress, worker, deadlineIndex, partitionIndex, sectorNumber)

	deadlineInfo, partitionIndex, v = vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)

	vm.SubmitPoSt(t, v, minerAddrs.IDAddress, worker, deadlineInfo, partitionIndex)

	require.True(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, deadlineIndex, partitionIndex, sectorNumber))
	require.False(t, vm.CheckSectorFaulty(t, v, minerAddrs.IDAddress, deadlineIndex, partitionIndex, sectorNumber))
	minerPower = vm.MinerPower(t, v, minerAddrs.IDAddress)
	require.Equal(t, ss, minerPower.Raw.Uint64())
}

func TestUpgradeAndMissPoSt(t *testing.T) {
	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	v := vm.NewVMWithSingletons(ctx, t, blkStore)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(100_000), big.NewInt(1e18)), 93837778)

	// create miner
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	ss, err := sealProof.SectorSize()
	require.NoError(t, err)
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	// advance vm so we can have seal randomness epoch in the past
	v, err = v.WithEpoch(abi.ChainEpoch(200))
	require.NoError(t, err)

	v, deadlineIndex, partitionIndex, sectorNumber := createSector(t, v, worker, minerAddrs.IDAddress, 100, sealProof)

	// make some deals
	dealIDs := createDeals(t, 1, v, worker, worker, minerAddrs.IDAddress, sealProof)

	// replicaUpdate the sector

	replicaUpdate := miner.ReplicaUpdate{
		SectorID:           sectorNumber,
		Deadline:           deadlineIndex,
		Partition:          partitionIndex,
		NewSealedSectorCID: tutil.MakeCID("replica", &miner.SealedCIDPrefix),
		Deals:              dealIDs,
		UpdateProofType:    abi.RegisteredUpdateProof_StackedDrg32GiBV1,
	}

	vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(),
		builtin.MethodsMiner.ProveReplicaUpdates,
		&miner.ProveReplicaUpdatesParams{Updates: []miner.ReplicaUpdate{replicaUpdate}})

	powerAfterUpdate := vm.MinerPower(t, v, minerAddrs.IDAddress)
	require.False(t, powerAfterUpdate.Raw.IsZero())

	// immediately miss post, lose power, become faulty
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, v.GetEpoch()+miner.WPoStProvingPeriod)
	require.False(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, deadlineIndex, partitionIndex, sectorNumber))
	require.True(t, vm.CheckSectorFaulty(t, v, minerAddrs.IDAddress, deadlineIndex, partitionIndex, sectorNumber))

	dl := vm.DeadlineState(t, v, minerAddrs.IDAddress, deadlineIndex)
	require.Equal(t, dl.FaultyPower.Raw, powerAfterUpdate.Raw)

	emptySectorsSnapshotArrayCid, err := adt.StoreEmptyArray(v.Store(), miner.SectorsAmtBitwidth)
	require.NoError(t, err)
	require.Equal(t, dl.SectorsSnapshot, emptySectorsSnapshotArrayCid)

	minerPower := vm.MinerPower(t, v, minerAddrs.IDAddress)
	require.True(t, minerPower.IsZero())

	// enter a deadline where recovery declaration is valid
	v = vm.AdvanceOneEpochWithCron(t, v)
	vm.DeclareRecovery(t, v, minerAddrs.IDAddress, worker, deadlineIndex, partitionIndex, sectorNumber)

	deadlineInfo, partitionIndex, v := vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)

	vm.SubmitPoSt(t, v, minerAddrs.IDAddress, worker, deadlineInfo, partitionIndex)

	require.True(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, deadlineIndex, partitionIndex, sectorNumber))
	require.False(t, vm.CheckSectorFaulty(t, v, minerAddrs.IDAddress, deadlineIndex, partitionIndex, sectorNumber))
	minerPower = vm.MinerPower(t, v, minerAddrs.IDAddress)
	require.Equal(t, uint64(ss), minerPower.Raw.Uint64())
}

// ---- Failure cases ----

// Tests that a sector in an immutable deadline cannot be upgraded
func TestImmutableDeadlineFailure(t *testing.T) {
	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	v := vm.NewVMWithSingletons(ctx, t, blkStore)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(100_000), big.NewInt(1e18)), 93837778)

	// create miner
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	ss, err := sealProof.SectorSize()
	require.NoError(t, err)

	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	// advance vm so we can have seal randomness epoch in the past
	v, err = v.WithEpoch(abi.ChainEpoch(200))
	require.NoError(t, err)

	v, deadlineIndex, partitionIndex, sectorNumber := createSector(t, v, worker, minerAddrs.IDAddress, 100, sealProof)

	// sanity checks about the sector created

	oldSectorInfo := vm.SectorInfo(t, v, minerAddrs.RobustAddress, sectorNumber)
	require.Equal(t, 0, len(oldSectorInfo.DealIDs))
	require.Nil(t, oldSectorInfo.SectorKeyCID)
	minerPower := vm.MinerPower(t, v, minerAddrs.IDAddress)
	require.Equal(t, uint64(ss), minerPower.Raw.Uint64())

	// make some unverified deals
	dealIDs := createDeals(t, 1, v, worker, worker, minerAddrs.IDAddress, sealProof)

	// Advance back into the sector's deadline
	_, _, v = vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)

	// replicaUpdate the sector

	replicaUpdate := miner.ReplicaUpdate{
		SectorID:           sectorNumber,
		Deadline:           deadlineIndex,
		Partition:          partitionIndex,
		NewSealedSectorCID: tutil.MakeCID("replica1", &miner.SealedCIDPrefix),
		Deals:              dealIDs,
		UpdateProofType:    abi.RegisteredUpdateProof_StackedDrg32GiBV1,
	}

	vm.ApplyCode(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(),
		builtin.MethodsMiner.ProveReplicaUpdates,
		&miner.ProveReplicaUpdatesParams{Updates: []miner.ReplicaUpdate{replicaUpdate}}, exitcode.ErrIllegalArgument)
}

func TestUnhealthySectorFailure(t *testing.T) {
	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	v := vm.NewVMWithSingletons(ctx, t, blkStore)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(100_000), big.NewInt(1e18)), 93837778)

	// create miner
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	v, deadlineIndex, partitionIndex, sectorNumber := createSector(t, v, worker, minerAddrs.IDAddress, 100, sealProof)

	// make some deals
	dealIDs := createDeals(t, 1, v, worker, worker, minerAddrs.IDAddress, sealProof)

	// ffw 1 day, missing posts
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, v.GetEpoch()+miner.WPoStProvingPeriod)
	require.False(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, deadlineIndex, partitionIndex, sectorNumber))

	replicaUpdate := miner.ReplicaUpdate{
		SectorID:           sectorNumber,
		Deadline:           deadlineIndex,
		Partition:          partitionIndex,
		NewSealedSectorCID: tutil.MakeCID("replica", &miner.SealedCIDPrefix),
		Deals:              dealIDs,
		UpdateProofType:    abi.RegisteredUpdateProof_StackedDrg32GiBV1,
	}

	vm.ApplyCode(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(),
		builtin.MethodsMiner.ProveReplicaUpdates,
		&miner.ProveReplicaUpdatesParams{Updates: []miner.ReplicaUpdate{replicaUpdate}}, exitcode.ErrIllegalArgument)
}

func TestTerminatedSectorFailure(t *testing.T) {
	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	v := vm.NewVMWithSingletons(ctx, t, blkStore)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(100_000), big.NewInt(1e18)), 93837778)

	// create miner
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	v, dlIdx, pIdx, sectorNumber := createSector(t, v, worker, minerAddrs.IDAddress, 100, sealProof)

	// make some deals
	dealIDs := createDeals(t, 1, v, worker, worker, minerAddrs.IDAddress, sealProof)

	// Terminate Sector
	vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.TerminateSectors, &miner.TerminateSectorsParams{
		Terminations: []miner.TerminationDeclaration{{
			Deadline:  dlIdx,
			Partition: pIdx,
			Sectors:   bitfield.NewFromSet([]uint64{uint64(sectorNumber)}),
		}},
	})
	require.False(t, vm.CheckSectorActive(t, v, minerAddrs.RobustAddress, dlIdx, pIdx, sectorNumber))

	replicaUpdate := miner.ReplicaUpdate{
		SectorID:           sectorNumber,
		Deadline:           dlIdx,
		Partition:          pIdx,
		NewSealedSectorCID: tutil.MakeCID("replica", &miner.SealedCIDPrefix),
		Deals:              dealIDs,
		UpdateProofType:    abi.RegisteredUpdateProof_StackedDrg32GiBV1,
	}

	vm.ApplyCode(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(),
		builtin.MethodsMiner.ProveReplicaUpdates,
		&miner.ProveReplicaUpdatesParams{Updates: []miner.ReplicaUpdate{replicaUpdate}}, exitcode.ErrIllegalArgument)
}

func TestBadBatchSizeFailure(t *testing.T) {
	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	v := vm.NewVMWithSingletons(ctx, t, blkStore)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(100_000), big.NewInt(1e18)), 93837778)

	// create miner
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	v, deadlineIndex, partitionIndex, sectorNumber := createSector(t, v, worker, minerAddrs.IDAddress, 100, sealProof)

	// make some deals
	dealIDs := createDeals(t, 1, v, worker, worker, minerAddrs.IDAddress, sealProof)

	// fail to replicaUpdate more sectors than batch size

	updates := make([]miner.ReplicaUpdate, miner.ProveReplicaUpdatesMaxSize+1)
	for i := range updates {
		updates[i] = miner.ReplicaUpdate{
			SectorID:           sectorNumber,
			Deadline:           deadlineIndex,
			Partition:          partitionIndex,
			NewSealedSectorCID: tutil.MakeCID("replica", &miner.SealedCIDPrefix),
			Deals:              dealIDs,
		}
	}

	vm.ApplyCode(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(),
		builtin.MethodsMiner.ProveReplicaUpdates,
		&miner.ProveReplicaUpdatesParams{Updates: updates}, exitcode.ErrIllegalArgument)
}

func TestNoDisputeuteAfterUpgrade(t *testing.T) {
	v, _, worker, minerAddrs, dlIdx, _, _ := createMinerAndUpgradeASector(t)

	disputeParams := &miner.DisputeWindowedPoStParams{
		Deadline:  dlIdx,
		PoStIndex: 0,
	}

	vm.ApplyCode(t, v, worker, minerAddrs.IDAddress, big.Zero(), builtin.MethodsMiner.DisputeWindowedPoSt, disputeParams, exitcode.ErrIllegalArgument)

	vm.ExpectInvocation{
		To:             minerAddrs.IDAddress,
		Method:         builtin.MethodsMiner.DisputeWindowedPoSt,
		SubInvocations: nil,
		Exitcode:       exitcode.ErrIllegalArgument,
	}.Matches(t, v.LastInvocation())
}

func TestUpgradeBadPostDispute(t *testing.T) {
	v, sectorInfo, worker, minerAddrs, dlIdx, pIdx, _ := createMinerAndUpgradeASector(t)

	deadlineInfo, _, v := vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorInfo.SectorNumber)

	vm.SubmitInvalidPoSt(t, v, minerAddrs.IDAddress, worker, deadlineInfo, pIdx)
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, v.GetEpoch()+miner.WPoStChallengeWindow*2)

	disputeParams := &miner.DisputeWindowedPoStParams{
		Deadline:  dlIdx,
		PoStIndex: 0,
	}

	vm.ApplyOk(t, v, worker, minerAddrs.IDAddress, big.Zero(), builtin.MethodsMiner.DisputeWindowedPoSt, disputeParams)

	vm.ExpectInvocation{
		To:             minerAddrs.IDAddress,
		Method:         builtin.MethodsMiner.DisputeWindowedPoSt,
		SubInvocations: nil,
		Exitcode:       0,
	}.Matches(t, v.LastInvocation())
}

func TestBadPostUpgradeDispute(t *testing.T) {
	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	v := vm.NewVMWithSingletons(ctx, t, blkStore)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(100_000), big.NewInt(1e18)), 93837778)

	// create miner
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	_, err := sealProof.SectorSize()
	require.NoError(t, err)

	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	// advance vm so we can have seal randomness epoch in the past
	v, err = v.WithEpoch(abi.ChainEpoch(200))
	require.NoError(t, err)

	v, dlIdx, pIdx, sectorNumber := createSector(t, v, worker, minerAddrs.IDAddress, 100, sealProof)

	deadlineInfo, _, v := vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)

	vm.SubmitInvalidPoSt(t, v, minerAddrs.IDAddress, worker, deadlineInfo, pIdx)
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, v.GetEpoch()+miner.WPoStChallengeWindow*2)

	// make some unverified deals
	dealIDs := createDeals(t, 1, v, worker, worker, minerAddrs.IDAddress, sealProof)

	// replicaUpdate the sector

	replicaUpdate := miner.ReplicaUpdate{
		SectorID:           sectorNumber,
		Deadline:           dlIdx,
		Partition:          pIdx,
		NewSealedSectorCID: tutil.MakeCID("replica1", &miner.SealedCIDPrefix),
		Deals:              dealIDs,
		UpdateProofType:    abi.RegisteredUpdateProof_StackedDrg32GiBV1,
	}

	ret := vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(),
		builtin.MethodsMiner.ProveReplicaUpdates,
		&miner.ProveReplicaUpdatesParams{Updates: []miner.ReplicaUpdate{replicaUpdate}})

	updatedSectors, ok := ret.(*bitfield.BitField)
	require.True(t, ok)
	count, err := updatedSectors.Count()
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)

	isSet, err := updatedSectors.IsSet(uint64(sectorNumber))
	require.NoError(t, err)
	require.True(t, isSet)

	disputeParams := &miner.DisputeWindowedPoStParams{
		Deadline:  dlIdx,
		PoStIndex: 0,
	}

	vm.ApplyOk(t, v, worker, minerAddrs.IDAddress, big.Zero(), builtin.MethodsMiner.DisputeWindowedPoSt, disputeParams)

	vm.ExpectInvocation{
		To:             minerAddrs.IDAddress,
		Method:         builtin.MethodsMiner.DisputeWindowedPoSt,
		SubInvocations: nil,
		Exitcode:       0,
	}.Matches(t, v.LastInvocation())
}

// Tests that an active CC sector can be correctly upgraded, and then the sector can be terminated
func TestTerminateAfterUpgrade(t *testing.T) {
	v, sectorInfo, worker, minerAddrs, dlIdx, pIdx, _ := createMinerAndUpgradeASector(t)
	sectorNumber := sectorInfo.SectorNumber

	// Terminate Sector
	v, err := v.WithEpoch(v.GetEpoch() + 1)
	require.NoError(t, err)

	vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.TerminateSectors, &miner.TerminateSectorsParams{
		Terminations: []miner.TerminationDeclaration{{
			Deadline:  dlIdx,
			Partition: pIdx,
			Sectors:   bitfield.NewFromSet([]uint64{uint64(sectorNumber)}),
		}},
	})

	// expect power, market and miner to be in base state
	minerBalances := vm.GetMinerBalances(t, v, minerAddrs.IDAddress)
	assert.Equal(t, big.Zero(), minerBalances.InitialPledge)
	assert.Equal(t, big.Zero(), minerBalances.PreCommitDeposit)

	// expect network stats to reflect power has been removed from sector
	stats := vm.GetNetworkStats(t, v)
	assert.Equal(t, int64(0), stats.MinerAboveMinPowerCount)
	assert.Equal(t, big.Zero(), stats.TotalRawBytePower)
	assert.Equal(t, big.Zero(), stats.TotalQualityAdjPower)
	assert.Equal(t, big.Zero(), stats.TotalBytesCommitted)
	assert.Equal(t, big.Zero(), stats.TotalQABytesCommitted)
	assert.Equal(t, big.Zero(), stats.TotalPledgeCollateral)
}

// Tests that an active CC sector can be correctly upgraded, and then the sector can be terminated
func TestExtendAfterUpdgrade(t *testing.T) {
	v, sectorInfo, worker, minerAddrs, dlIdx, pIdx, _ := createMinerAndUpgradeASector(t)

	extensionParams := &miner.ExtendSectorExpirationParams{
		Extensions: []miner.ExpirationExtension{{
			Deadline:      dlIdx,
			Partition:     pIdx,
			Sectors:       bitfield.NewFromSet([]uint64{uint64(sectorInfo.SectorNumber)}),
			NewExpiration: v.GetEpoch() + miner.MaxSectorExpirationExtension - 1,
		}},
	}

	vm.ApplyOk(t, v, worker, minerAddrs.IDAddress, big.Zero(), builtin.MethodsMiner.ExtendSectorExpiration, extensionParams)

	vm.ExpectInvocation{
		To:             minerAddrs.IDAddress,
		Method:         builtin.MethodsMiner.ExtendSectorExpiration,
		SubInvocations: nil,
	}.Matches(t, v.LastInvocation())

	var mStateFinal miner.State
	require.NoError(t, v.GetState(minerAddrs.IDAddress, &mStateFinal))
	infoFinal, found, err := mStateFinal.GetSector(v.Store(), sectorInfo.SectorNumber)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, abi.ChainEpoch(miner.MaxSectorExpirationExtension-1), infoFinal.Expiration-infoFinal.Activation)
}

func TestWrongDeadlineIndexFailure(t *testing.T) {
	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	v := vm.NewVMWithSingletons(ctx, t, blkStore)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(100_000), big.NewInt(1e18)), 93837778)

	// create miner
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1

	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	// advance vm so we can have seal randomness epoch in the past
	v, err = v.WithEpoch(abi.ChainEpoch(200))
	require.NoError(t, err)

	v, deadlineIndex, partitionIndex, sectorNumber := createSector(t, v, worker, minerAddrs.IDAddress, 100, sealProof)
	oldSectorInfo := vm.SectorInfo(t, v, minerAddrs.RobustAddress, sectorNumber)

	// make some unverified deals
	dealIDs := createDeals(t, 1, v, worker, worker, minerAddrs.IDAddress, sealProof)

	// replicaUpdate the sector

	replicaUpdate := miner.ReplicaUpdate{
		SectorID:           sectorNumber,
		Deadline:           deadlineIndex + 1,
		Partition:          partitionIndex,
		NewSealedSectorCID: tutil.MakeCID("replica1", &miner.SealedCIDPrefix),
		Deals:              dealIDs,
		UpdateProofType:    abi.RegisteredUpdateProof_StackedDrg32GiBV1,
	}

	ret := vm.ApplyCode(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(),
		builtin.MethodsMiner.ProveReplicaUpdates,
		&miner.ProveReplicaUpdatesParams{Updates: []miner.ReplicaUpdate{replicaUpdate}},
		exitcode.ErrIllegalArgument)

	_, ok := ret.(*bitfield.BitField)
	require.False(t, ok)

	newSectorInfo := vm.SectorInfo(t, v, minerAddrs.RobustAddress, sectorNumber)
	require.Equal(t, *oldSectorInfo, *newSectorInfo)
	require.NotEqual(t, replicaUpdate.NewSealedSectorCID, newSectorInfo.SealedCID)
}

func TestWrongPartitionIndexFailure(t *testing.T) {
	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	v := vm.NewVMWithSingletons(ctx, t, blkStore)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(100_000), big.NewInt(1e18)), 93837778)

	// create miner
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1

	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	// advance vm so we can have seal randomness epoch in the past
	v, err = v.WithEpoch(abi.ChainEpoch(200))
	require.NoError(t, err)

	v, deadlineIndex, partitionIndex, sectorNumber := createSector(t, v, worker, minerAddrs.IDAddress, 100, sealProof)
	oldSectorInfo := vm.SectorInfo(t, v, minerAddrs.RobustAddress, sectorNumber)

	// make some unverified deals
	dealIDs := createDeals(t, 1, v, worker, worker, minerAddrs.IDAddress, sealProof)

	// replicaUpdate the sector

	replicaUpdate := miner.ReplicaUpdate{
		SectorID:           sectorNumber,
		Deadline:           deadlineIndex,
		Partition:          partitionIndex + 1,
		NewSealedSectorCID: tutil.MakeCID("replica1", &miner.SealedCIDPrefix),
		Deals:              dealIDs,
		UpdateProofType:    abi.RegisteredUpdateProof_StackedDrg32GiBV1,
	}

	ret := vm.ApplyCode(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(),
		builtin.MethodsMiner.ProveReplicaUpdates,
		&miner.ProveReplicaUpdatesParams{Updates: []miner.ReplicaUpdate{replicaUpdate}},
		exitcode.ErrNotFound)

	_, ok := ret.(*bitfield.BitField)
	require.False(t, ok)

	newSectorInfo := vm.SectorInfo(t, v, minerAddrs.RobustAddress, sectorNumber)
	require.Equal(t, *oldSectorInfo, *newSectorInfo)
	require.NotEqual(t, replicaUpdate.NewSealedSectorCID, newSectorInfo.SealedCID)
}

func TestProveReplicaUpdateMultiDline(t *testing.T) {
	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	v := vm.NewVMWithSingletons(ctx, t, blkStore)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(100_000), big.NewInt(1e18)), 93837778)
	/* create miner */
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	_, err := sealProof.SectorSize()
	require.NoError(t, err)

	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	v, err = v.WithEpoch(abi.ChainEpoch(1440)) // something offset far away from deadline 0 and 1
	require.NoError(t, err)

	/* Commit enough sectors to pack two partitions */
	moreThanOnePartition := 2400
	batchSize := 100
	firstSectorNumberP1 := abi.SectorNumber(0)
	firstSectorNumberP2 := abi.SectorNumber(builtin.PoStProofPolicies[abi.RegisteredPoStProof_StackedDrgWindow32GiBV1].WindowPoStPartitionSectors)

	newPrecommits := preCommitSectors(t, v, moreThanOnePartition, batchSize, worker, minerAddrs.IDAddress, sealProof, abi.SectorNumber(0), true, v.GetEpoch()+miner.MaxSectorExpirationExtension)
	var precommits []*miner.SectorPreCommitOnChainInfo
	precommits = append(precommits, newPrecommits...)
	toProve := precommits

	proveTime := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)
	proveCommitSectors(t, v, worker, minerAddrs.IDAddress, toProve, batchSize)

	/* This is a mess, but it just ensures activation of both partitions by posting, cronning and checking */

	// advance to proving period and submit post for first partition
	dlInfo, pIdx, v := vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, firstSectorNumberP1)

	// first partition shouldn't be active until PoSt
	require.False(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, dlInfo.Index, pIdx, firstSectorNumberP1))
	vm.SubmitPoSt(t, v, minerAddrs.IDAddress, worker, dlInfo, pIdx)

	// move into the next deadline so that the created sector is mutable
	v, currDlInfo := vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, v.GetEpoch()+miner.WPoStChallengeWindow)
	assert.Equal(t, uint64(1), currDlInfo.Index)
	v = vm.AdvanceOneEpochWithCron(t, v)

	// hooray, first partition is now active
	require.True(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, dlInfo.Index, pIdx, firstSectorNumberP1))
	require.True(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, dlInfo.Index, pIdx, firstSectorNumberP1+1))
	require.True(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, dlInfo.Index, pIdx, firstSectorNumberP1+2))
	require.True(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, dlInfo.Index, pIdx, firstSectorNumberP1+2300))
	// second partition shouldn't be active until PoSt
	require.False(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, currDlInfo.Index, 0, firstSectorNumberP2))
	vm.SubmitPoSt(t, v, minerAddrs.IDAddress, worker, currDlInfo, 0)
	// move into the next deadline so that the created sector is mutable
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, v.GetEpoch()+miner.WPoStChallengeWindow)
	v = vm.AdvanceOneEpochWithCron(t, v)
	require.True(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, currDlInfo.Index, 0, firstSectorNumberP2))

	/* Replica Update across two deadlines */
	oldSectorCommRP1 := vm.SectorInfo(t, v, minerAddrs.RobustAddress, firstSectorNumberP1).SealedCID
	oldSectorCommRP2 := vm.SectorInfo(t, v, minerAddrs.RobustAddress, firstSectorNumberP2).SealedCID

	dealIDs := createDeals(t, 2, v, worker, worker, minerAddrs.IDAddress, sealProof)
	replicaUpdate1 := miner.ReplicaUpdate{
		SectorID:           firstSectorNumberP1,
		Deadline:           0,
		Partition:          0,
		NewSealedSectorCID: tutil.MakeCID("replica1", &miner.SealedCIDPrefix),
		Deals:              dealIDs[0:1],
		UpdateProofType:    abi.RegisteredUpdateProof_StackedDrg32GiBV1,
	}

	replicaUpdate2 := miner.ReplicaUpdate{
		SectorID:           firstSectorNumberP2,
		Deadline:           1,
		Partition:          0,
		NewSealedSectorCID: tutil.MakeCID("replica2", &miner.SealedCIDPrefix),
		Deals:              dealIDs[1:],
		UpdateProofType:    abi.RegisteredUpdateProof_StackedDrg32GiBV1,
	}

	// When this bug is fixed this should become vm.ApplyOk
	ret := vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(),
		builtin.MethodsMiner.ProveReplicaUpdates,
		&miner.ProveReplicaUpdatesParams{Updates: []miner.ReplicaUpdate{replicaUpdate1, replicaUpdate2}})

	updatedSectors, ok := ret.(*bitfield.BitField)
	require.True(t, ok)
	count, err := updatedSectors.Count()
	require.NoError(t, err)
	require.Equal(t, uint64(2), count)

	isSet, err := updatedSectors.IsSet(uint64(firstSectorNumberP1))
	require.NoError(t, err)
	require.True(t, isSet)

	isSet, err = updatedSectors.IsSet(uint64(firstSectorNumberP2))
	require.NoError(t, err)
	require.True(t, isSet)

	newSectorInfoP1 := vm.SectorInfo(t, v, minerAddrs.RobustAddress, firstSectorNumberP1)
	require.Equal(t, dealIDs[0], newSectorInfoP1.DealIDs[0])
	require.Equal(t, oldSectorCommRP1, *newSectorInfoP1.SectorKeyCID)
	require.Equal(t, replicaUpdate1.NewSealedSectorCID, newSectorInfoP1.SealedCID)
	newSectorInfoP2 := vm.SectorInfo(t, v, minerAddrs.RobustAddress, firstSectorNumberP2)
	require.Equal(t, dealIDs[1], newSectorInfoP2.DealIDs[0])
	require.Equal(t, oldSectorCommRP2, *newSectorInfoP2.SectorKeyCID)
	require.Equal(t, replicaUpdate2.NewSealedSectorCID, newSectorInfoP2.SealedCID)
}

func TestDealIncludedInMultipleSectors(t *testing.T) {
	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	v := vm.NewVMWithSingletons(ctx, t, blkStore)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(100_000), big.NewInt(1e18)), 93837778)

	// create miner
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	_, err := sealProof.SectorSize()
	require.NoError(t, err)

	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	// advance vm so we can have seal randomness epoch in the past
	v, err = v.WithEpoch(abi.ChainEpoch(200))
	require.NoError(t, err)

	//
	// preCommit two sectors
	//
	precommit := preCommitSectors(t, v, 2, miner.PreCommitSectorBatchMaxSize, worker, minerAddrs.IDAddress, sealProof, 100, true, v.GetEpoch()+miner.MaxSectorExpirationExtension)

	assert.Equal(t, len(precommit), 2)
	balances := vm.GetMinerBalances(t, v, minerAddrs.IDAddress)
	assert.True(t, balances.PreCommitDeposit.GreaterThan(big.Zero()))

	// advance time to max seal duration
	proveTime := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	// proveCommit the sector
	sectorNumber1 := precommit[0].Info.SectorNumber
	sectorNumber2 := precommit[1].Info.SectorNumber

	v, err = v.WithEpoch(proveTime)
	require.NoError(t, err)

	proveCommit1 := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber1,
	}
	proveCommit2 := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber2,
	}

	_ = vm.ApplyOk(t, v, worker, minerAddrs.IDAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommit1)
	_ = vm.ApplyOk(t, v, worker, minerAddrs.IDAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommit2)

	// In the same epoch, trigger cron to validate prove commit
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// advance to proving period and submit post
	dlInfo, pIdx, v := vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber1)
	dlIdx := dlInfo.Index

	// sector shouldn't be active until PoSt
	require.False(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, dlInfo.Index, pIdx, sectorNumber1))
	require.False(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, dlInfo.Index, pIdx, sectorNumber2))
	vm.SubmitPoSt(t, v, minerAddrs.IDAddress, worker, dlInfo, pIdx)

	// move into the next deadline so that the created sector is mutable
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, v.GetEpoch()+miner.WPoStChallengeWindow)
	v = vm.AdvanceOneEpochWithCron(t, v)

	// hooray, sector is now active
	require.True(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, dlInfo.Index, pIdx, sectorNumber1))
	require.True(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, dlInfo.Index, pIdx, sectorNumber2))

	// make some unverified deals
	dealIDs := createDeals(t, 1, v, worker, worker, minerAddrs.IDAddress, sealProof)

	// replicaUpdate the sector

	replicaUpdate1 := miner.ReplicaUpdate{
		SectorID:           sectorNumber1,
		Deadline:           dlIdx,
		Partition:          pIdx,
		NewSealedSectorCID: tutil.MakeCID("replica1", &miner.SealedCIDPrefix),
		Deals:              dealIDs,
		UpdateProofType:    abi.RegisteredUpdateProof_StackedDrg32GiBV1,
	}

	replicaUpdate2 := miner.ReplicaUpdate{
		SectorID:           sectorNumber2,
		Deadline:           dlIdx,
		Partition:          pIdx,
		NewSealedSectorCID: tutil.MakeCID("replica1", &miner.SealedCIDPrefix),
		Deals:              dealIDs,
		UpdateProofType:    abi.RegisteredUpdateProof_StackedDrg32GiBV1,
	}

	ret := vm.ApplyCode(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(),
		builtin.MethodsMiner.ProveReplicaUpdates,
		&miner.ProveReplicaUpdatesParams{Updates: []miner.ReplicaUpdate{replicaUpdate1, replicaUpdate2}},
		0)

	updatedSectors, ok := ret.(*bitfield.BitField)
	require.True(t, ok)
	count, err := updatedSectors.Count()
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)

	isSet, err := updatedSectors.IsSet(uint64(sectorNumber1))
	require.NoError(t, err)
	require.True(t, isSet)
	isSet, err = updatedSectors.IsSet(uint64(sectorNumber2))
	require.NoError(t, err)
	require.False(t, isSet)

	newSectorInfo := vm.SectorInfo(t, v, minerAddrs.RobustAddress, sectorNumber1)
	require.Equal(t, 1, len(newSectorInfo.DealIDs))
	require.Equal(t, dealIDs[0], newSectorInfo.DealIDs[0])
	require.Equal(t, replicaUpdate1.NewSealedSectorCID, newSectorInfo.SealedCID)

	newSectorInfo2 := vm.SectorInfo(t, v, minerAddrs.RobustAddress, sectorNumber2)
	require.Equal(t, 0, len(newSectorInfo2.DealIDs))
	require.NotEqual(t, replicaUpdate2.NewSealedSectorCID, newSectorInfo2.SealedCID)
}

func createDeals(t *testing.T, numberOfDeals int, v *vm.VM, clientAddress address.Address, workerAddress address.Address, minerAddress address.Address, sealProof abi.RegisteredSealProof) []abi.DealID {
	// add market collateral for client and miner
	collateral := big.Mul(big.NewInt(int64(3*numberOfDeals)), vm.FIL)
	vm.ApplyOk(t, v, clientAddress, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &clientAddress)
	collateral = big.Mul(big.NewInt(int64(64*numberOfDeals)), vm.FIL)
	vm.ApplyOk(t, v, workerAddress, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &minerAddress)

	var dealIDs []abi.DealID
	for i := 0; i < numberOfDeals; i++ {
		dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
		deals := publishDeal(t, v, workerAddress, workerAddress, minerAddress, "dealLabel"+strconv.Itoa(i), 32<<30, false, dealStart, 180*builtin.EpochsInDay)
		dealIDs = append(dealIDs, deals.IDs...)
	}

	return dealIDs
}

// This method produces an active, mutable sector, by:
// - PreCommiting a sector
// - fastforwarding time and ProveCommitting it
// - fastforwarding to its Proving period and PoSting it
// - fastforwarding out of the proving period into a new deadline
func createSector(t *testing.T, v *vm.VM, workerAddress address.Address, minerAddress address.Address, firstSectorNo abi.SectorNumber, sealProof abi.RegisteredSealProof) (*vm.VM, uint64, uint64, abi.SectorNumber) {

	//
	// preCommit a sector
	//
	precommit := preCommitSectors(t, v, 1, miner.PreCommitSectorBatchMaxSize, workerAddress, minerAddress, sealProof, firstSectorNo, true, v.GetEpoch()+miner.MaxSectorExpirationExtension)

	assert.Equal(t, len(precommit), 1)
	balances := vm.GetMinerBalances(t, v, minerAddress)
	assert.True(t, balances.PreCommitDeposit.GreaterThan(big.Zero()))

	// advance time to max seal duration
	proveTime := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddress, proveTime)

	// proveCommit the sector

	sectorNumber := precommit[0].Info.SectorNumber

	v, err := v.WithEpoch(proveTime)
	require.NoError(t, err)

	proveCommit := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}

	vm.ApplyOk(t, v, workerAddress, minerAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommit)

	// In the same epoch, trigger cron to validate prove commit
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// advance to proving period and submit post
	dlInfo, pIdx, v := vm.AdvanceTillProvingDeadline(t, v, minerAddress, sectorNumber)

	// sector shouldn't be active until PoSt
	require.False(t, vm.CheckSectorActive(t, v, minerAddress, dlInfo.Index, pIdx, sectorNumber))
	vm.SubmitPoSt(t, v, minerAddress, workerAddress, dlInfo, pIdx)

	// move into the next deadline so that the created sector is mutable
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddress, v.GetEpoch()+miner.WPoStChallengeWindow)
	v = vm.AdvanceOneEpochWithCron(t, v)

	// hooray, sector is now active
	require.True(t, vm.CheckSectorActive(t, v, minerAddress, dlInfo.Index, pIdx, sectorNumber))

	return v, dlInfo.Index, pIdx, sectorNumber
}

// This function contains the simple success path
func createMinerAndUpgradeASector(t *testing.T) (*vm.VM, *miner.SectorOnChainInfo, address.Address, *power.CreateMinerReturn, uint64, uint64, uint64) {
	ctx := context.Background()
	blkStore := ipld.NewBlockStoreInMemory()
	v := vm.NewVMWithSingletons(ctx, t, blkStore)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(100_000), big.NewInt(1e18)), 93837778)

	// create miner
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	ss, err := sealProof.SectorSize()
	require.NoError(t, err)

	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := createMiner(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	// advance vm so we can have seal randomness epoch in the past
	v, err = v.WithEpoch(abi.ChainEpoch(200))
	require.NoError(t, err)

	v, deadlineIndex, partitionIndex, sectorNumber := createSector(t, v, worker, minerAddrs.IDAddress, 100, sealProof)

	// sanity checks about the sector created

	oldSectorInfo := vm.SectorInfo(t, v, minerAddrs.RobustAddress, sectorNumber)
	require.Equal(t, 0, len(oldSectorInfo.DealIDs))
	require.Nil(t, oldSectorInfo.SectorKeyCID)
	minerPower := vm.MinerPower(t, v, minerAddrs.IDAddress)
	require.Equal(t, uint64(ss), minerPower.Raw.Uint64())

	// make some unverified deals
	dealIDs := createDeals(t, 1, v, worker, worker, minerAddrs.IDAddress, sealProof)

	// replicaUpdate the sector

	replicaUpdate := miner.ReplicaUpdate{
		SectorID:           sectorNumber,
		Deadline:           deadlineIndex,
		Partition:          partitionIndex,
		NewSealedSectorCID: tutil.MakeCID("replica1", &miner.SealedCIDPrefix),
		Deals:              dealIDs,
		UpdateProofType:    abi.RegisteredUpdateProof_StackedDrg32GiBV1,
	}

	ret := vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(),
		builtin.MethodsMiner.ProveReplicaUpdates,
		&miner.ProveReplicaUpdatesParams{Updates: []miner.ReplicaUpdate{replicaUpdate}})

	updatedSectors, ok := ret.(*bitfield.BitField)
	require.True(t, ok)
	count, err := updatedSectors.Count()
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)

	isSet, err := updatedSectors.IsSet(uint64(sectorNumber))
	require.NoError(t, err)
	require.True(t, isSet)

	newSectorInfo := vm.SectorInfo(t, v, minerAddrs.RobustAddress, sectorNumber)
	require.Equal(t, 1, len(newSectorInfo.DealIDs))
	require.Equal(t, dealIDs[0], newSectorInfo.DealIDs[0])
	require.Equal(t, oldSectorInfo.SealedCID, *newSectorInfo.SectorKeyCID)
	require.Equal(t, replicaUpdate.NewSealedSectorCID, newSectorInfo.SealedCID)

	minerPower = vm.MinerPower(t, v, minerAddrs.IDAddress)
	require.Equal(t, uint64(ss), minerPower.Raw.Uint64())

	return v, newSectorInfo, worker, minerAddrs, deadlineIndex, partitionIndex, uint64(ss)
}
