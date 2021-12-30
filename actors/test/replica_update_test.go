package test

import (
	"context"
	"strconv"
	"testing"

	"github.com/filecoin-project/go-address"

	"github.com/filecoin-project/go-bitfield"
	tutil "github.com/filecoin-project/specs-actors/v7/support/testing"

	"github.com/filecoin-project/go-state-types/exitcode"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v7/actors/builtin"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v7/support/ipld"
	"github.com/filecoin-project/specs-actors/v7/support/vm"
)

// ---- Success cases ----

// Tests that an active CC sector can be correctly upgraded, and the expected state changes occur
func TestSimplePathSuccess(t *testing.T) {
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

	updatedSectors := ret.(*bitfield.BitField)
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
}

// Tests a successful upgrade, followed by the sector going faulty and recovering
func TestFullPathSuccess(t *testing.T) {
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

	// make some deals
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

	minerPower = vm.MinerPower(t, v, minerAddrs.IDAddress)
	require.True(t, minerPower.IsZero())

	// enter a deadline where recovery declaration is valid
	v = vm.AdvanceOneEpochWithCron(t, v)
	vm.DeclareRecovery(t, v, minerAddrs.IDAddress, worker, deadlineIndex, partitionIndex, sectorNumber)

	deadlineInfo, partitionIndex, v = vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)

	vm.SubmitPoSt(t, v, minerAddrs.IDAddress, worker, deadlineInfo, partitionIndex)

	require.True(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, deadlineIndex, partitionIndex, sectorNumber))
	require.False(t, vm.CheckSectorFaulty(t, v, minerAddrs.IDAddress, deadlineIndex, partitionIndex, sectorNumber))
	minerPower = vm.MinerPower(t, v, minerAddrs.IDAddress)
	require.Equal(t, uint64(ss), minerPower.Raw.Uint64())
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

	// immediately miss post, lose power, become faulty
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, v.GetEpoch()+miner.WPoStProvingPeriod)
	require.False(t, vm.CheckSectorActive(t, v, minerAddrs.IDAddress, deadlineIndex, partitionIndex, sectorNumber))
	require.True(t, vm.CheckSectorFaulty(t, v, minerAddrs.IDAddress, deadlineIndex, partitionIndex, sectorNumber))

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

	_ = vm.ApplyOk(t, v, workerAddress, minerAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommit)

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
