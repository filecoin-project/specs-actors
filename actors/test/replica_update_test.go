package test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/specs-actors/v7/actors/runtime/proof"

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

func TestReplicaUpdateSuccess(t *testing.T) {
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

	//
	// preCommit a single sector
	//
	firstSectorNo := abi.SectorNumber(100)
	precommit := preCommitSectors(t, v, 1, miner.PreCommitSectorBatchMaxSize, addrs[0], minerAddrs.IDAddress, sealProof, firstSectorNo, true, v.GetEpoch()+miner.MaxSectorExpirationExtension)
	assert.Equal(t, len(precommit), 1)
	balances := vm.GetMinerBalances(t, v, minerAddrs.IDAddress)
	assert.True(t, balances.PreCommitDeposit.GreaterThan(big.Zero()))

	// advance time to max seal duration
	proveTime := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	// proveCommit the sector

	sectorNumber := precommit[0].Info.SectorNumber

	v, err = v.WithEpoch(proveTime)
	require.NoError(t, err)

	proveCommit := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}

	_ = vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommit)

	// In the same epoch, trigger cron to validate prove commit
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// advance to proving period and submit post
	dlInfo, pIdx, v := vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)

	submitParams := miner.SubmitWindowedPoStParams{
		Deadline: dlInfo.Index,
		Partitions: []miner.PoStPartition{{
			Index:   pIdx,
			Skipped: bitfield.New(),
		}},
		Proofs: []proof.PoStProof{{
			PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		}},
		ChainCommitEpoch: dlInfo.Challenge,
		ChainCommitRand:  []byte(vm.RandString),
	}

	vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)

	// add market collateral for clients and miner
	collateral := big.Mul(big.NewInt(3), vm.FIL)
	vm.ApplyOk(t, v, worker, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &worker)
	collateral = big.Mul(big.NewInt(64), vm.FIL)
	vm.ApplyOk(t, v, worker, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &minerAddrs.IDAddress)

	// we need a deal

	dealIDs := []abi.DealID{}
	dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	deals := publishDeal(t, v, worker, worker, minerAddrs.IDAddress, "deal1", 32<<30, false, dealStart, 180*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)

	// replicaUpdate the sector

	replicaUpdate := miner.ReplicaUpdate{
		SectorID:           sectorNumber,
		Deadline:           dlInfo.Index,
		Partition:          pIdx,
		NewSealedSectorCID: tutil.MakeCID("replica", &miner.SealedCIDPrefix),
		Deals:              dealIDs,
		UpdateProofType:    abi.RegisteredUpdateProof_StackedDrg32GiBV1,
	}

	ret := vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(),
		builtin.MethodsMiner.ProveReplicaUpdates,
		&miner.ProveReplicaUpdatesParams{Updates: []miner.ReplicaUpdate{replicaUpdate}})

	updatedSectors := ret.(bitfield.BitField)
	count, err := updatedSectors.Count()
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)

	isSet, err := updatedSectors.IsSet(uint64(sectorNumber))
	require.NoError(t, err)
	require.True(t, isSet)

	info := vm.SectorInfo(t, v, minerAddrs.RobustAddress, sectorNumber)
	require.Equal(t, 1, len(info.DealIDs))
	require.Equal(t, dealIDs[0], info.DealIDs[0])
}

func TestReplicaUpdateFailures(t *testing.T) {
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

	// fail to replicaUpdate more sectors than batch size

	updates := make([]miner.ReplicaUpdate, miner.ProveReplicaUpdatesMaxSize+1)
	for i := range updates {
		updates[i] = miner.ReplicaUpdate{
			SectorID:           abi.SectorNumber(i),
			NewSealedSectorCID: tutil.MakeCID("replica", &miner.SealedCIDPrefix),
		}
	}

	_ = vm.ApplyCode(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(),
		builtin.MethodsMiner.ProveReplicaUpdates,
		&miner.ProveReplicaUpdatesParams{Updates: updates}, exitcode.ErrIllegalArgument)
}
