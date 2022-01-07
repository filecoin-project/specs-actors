package vm6Util

import (
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/dline"
	"github.com/filecoin-project/go-state-types/exitcode"
	xc "github.com/filecoin-project/go-state-types/exitcode"
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v6/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/v6/actors/util/adt"
	tutil "github.com/filecoin-project/specs-actors/v6/support/testing"
	vm6 "github.com/filecoin-project/specs-actors/v6/support/vm"
	"github.com/filecoin-project/specs-actors/v7/support/vm"
	"github.com/stretchr/testify/require"
	"testing"
)

// find the sector info for the given id
func SectorInfo(t *testing.T, v *vm6.VM, minerIDAddress address.Address, sectorNumber abi.SectorNumber) *miner.SectorOnChainInfo {
	var minerState miner.State
	err := v.GetState(minerIDAddress, &minerState)
	require.NoError(t, err)

	info, found, err := minerState.GetSector(v.Store(), sectorNumber)
	require.NoError(t, err)
	require.True(t, found)
	return info
}

// returns true if the sector is healthy
func CheckSectorActive(t *testing.T, v *vm6.VM, minerIDAddress address.Address, deadlineIndex uint64, partitionIndex uint64, sectorNumber abi.SectorNumber) bool {
	var minerState miner.State
	err := v.GetState(minerIDAddress, &minerState)
	require.NoError(t, err)

	active, err := CheckMinerSectorActive(&minerState, v.Store(), deadlineIndex, partitionIndex, sectorNumber, true)
	require.NoError(t, err)
	return active
}

// returns true if the sector is faulty -- a slightly more specific check than CheckSectorActive
func CheckSectorFaulty(t *testing.T, v *vm6.VM, minerIDAddress address.Address, deadlineIndex uint64, partitionIndex uint64, sectorNumber abi.SectorNumber) bool {
	var st miner.State
	require.NoError(t, v.GetState(minerIDAddress, &st))

	deadlines, err := st.LoadDeadlines(v.Store())
	require.NoError(t, err)

	deadline, err := deadlines.LoadDeadline(v.Store(), deadlineIndex)
	require.NoError(t, err)

	partition, err := deadline.LoadPartition(v.Store(), partitionIndex)
	require.NoError(t, err)

	isFaulty, err := partition.Faults.IsSet(uint64(sectorNumber))
	require.NoError(t, err)

	return isFaulty
}

func AdvanceByDeadlineTillEpochWhileProving(t *testing.T, v *vm6.VM, minerIDAddress address.Address, workerAddress address.Address, sectorNumber abi.SectorNumber, e abi.ChainEpoch) *vm6.VM {
	var dlInfo *dline.Info
	var pIdx uint64
	for v.GetEpoch() < e {
		dlInfo, pIdx, v = vm6.AdvanceTillProvingDeadline(t, v, minerIDAddress, sectorNumber)
		SubmitPoSt(t, v, minerIDAddress, workerAddress, dlInfo, pIdx)
		v, _ = vm6.AdvanceByDeadlineTillIndex(t, v, minerIDAddress, dlInfo.Index+2%miner.WPoStPeriodDeadlines)
	}

	return v
}

func DeclareRecovery(t *testing.T, v *vm6.VM, minerAddress, workerAddress address.Address, deadlineIndex uint64, partitionIndex uint64, sectorNumber abi.SectorNumber) {
	recoverParams := miner.RecoveryDeclaration{
		Deadline:  deadlineIndex,
		Partition: partitionIndex,
		Sectors:   bitfield.NewFromSet([]uint64{uint64(sectorNumber)}),
	}

	vm6.ApplyOk(t, v, workerAddress, minerAddress, big.Zero(), builtin.MethodsMiner.DeclareFaultsRecovered, &miner.DeclareFaultsRecoveredParams{
		Recoveries: []miner.RecoveryDeclaration{recoverParams},
	})
}

func SubmitPoSt(t *testing.T, v *vm6.VM, minerAddress, workerAddress address.Address, dlInfo *dline.Info, partitionIndex uint64) {
	submitParams := miner.SubmitWindowedPoStParams{
		Deadline: dlInfo.Index,
		Partitions: []miner.PoStPartition{{
			Index:   partitionIndex,
			Skipped: bitfield.New(),
		}},
		Proofs: []proof.PoStProof{{
			PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		}},
		ChainCommitEpoch: dlInfo.Challenge,
		ChainCommitRand:  []byte(vm.RandString),
	}

	vm6.ApplyOk(t, v, workerAddress, minerAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)
}


// Advances to the next epoch, running cron.
func AdvanceOneEpochWithCron(t *testing.T, v *vm6.VM) *vm6.VM {
	result := vm6.RequireApplyMessage(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil, t.Name())

	require.Equal(t, exitcode.Ok, result.Code)
	v, err := v.WithEpoch(v.GetEpoch() + 1)
	require.NoError(t, err)
	return v
}


func PreCommitSectors(t *testing.T, v *vm6.VM, count, batchSize int, worker, mAddr address.Address, sealProof abi.RegisteredSealProof, sectorNumberBase abi.SectorNumber, expectCronEnrollment bool, expiration abi.ChainEpoch) []*miner.SectorPreCommitOnChainInfo {
	invocsCommon := []vm6.ExpectInvocation{
		{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
	}
	invocFirst := vm6.ExpectInvocation{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent}

	sectorIndex := 0
	for sectorIndex < count {
		msgSectorIndexStart := sectorIndex
		invocs := invocsCommon

		// Prepare message.
		params := miner.PreCommitSectorBatchParams{Sectors: make([]miner0.SectorPreCommitInfo, batchSize)}
		if expiration < 0 {
			expiration = v.GetEpoch() + miner.MinSectorExpiration + miner.MaxProveCommitDuration[sealProof] + 100
		}

		for j := 0; j < batchSize && sectorIndex < count; j++ {
			sectorNumber := sectorNumberBase + abi.SectorNumber(sectorIndex)
			sealedCid := tutil.MakeCID(fmt.Sprintf("%d", sectorNumber), &miner.SealedCIDPrefix)
			params.Sectors[j] = miner0.SectorPreCommitInfo{
				SealProof:     sealProof,
				SectorNumber:  sectorNumber,
				SealedCID:     sealedCid,
				SealRandEpoch: v.GetEpoch() - 1,
				DealIDs:       nil,
				Expiration:    expiration,
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
			invocs = append(invocs, vm6.ExpectInvocation{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend, Value: &aggFee})
		}
		if expectCronEnrollment && msgSectorIndexStart == 0 {
			invocs = append(invocs, invocFirst)
		}
		vm6.ApplyOk(t, v, worker, mAddr, big.Zero(), builtin.MethodsMiner.PreCommitSectorBatch, &params)
		vm6.ExpectInvocation{
			To:             mAddr,
			Method:         builtin.MethodsMiner.PreCommitSectorBatch,
			Params:         vm6.ExpectObject(&params),
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



// Returns an error if the target sector cannot be found, or some other bad state is reached.
// Returns false if the target sector is faulty, terminated, or unproven
// Returns true otherwise
func CheckMinerSectorActive(st *miner.State, store adt.Store, dlIdx, pIdx uint64, sector abi.SectorNumber, requireProven bool) (bool, error) {
	dls, err := st.LoadDeadlines(store)
	if err != nil {
		return false, err
	}

	dl, err := dls.LoadDeadline(store, dlIdx)
	if err != nil {
		return false, err
	}

	partition, err := dl.LoadPartition(store, pIdx)
	if err != nil {
		return false, err
	}

	if exists, err := partition.Sectors.IsSet(uint64(sector)); err != nil {
		return false, xc.ErrIllegalState.Wrapf("failed to decode sectors bitfield (deadline %d, partition %d): %w", dlIdx, pIdx, err)
	} else if !exists {
		return false, xc.ErrNotFound.Wrapf("sector %d not a member of partition %d, deadline %d", sector, pIdx, dlIdx)
	}

	if faulty, err := partition.Faults.IsSet(uint64(sector)); err != nil {
		return false, xc.ErrIllegalState.Wrapf("failed to decode faults bitfield (deadline %d, partition %d): %w", dlIdx, pIdx, err)
	} else if faulty {
		return false, nil
	}

	if terminated, err := partition.Terminated.IsSet(uint64(sector)); err != nil {
		return false, xc.ErrIllegalState.Wrapf("failed to decode terminated bitfield (deadline %d, partition %d): %w", dlIdx, pIdx, err)
	} else if terminated {
		return false, nil
	}

	if unproven, err := partition.Unproven.IsSet(uint64(sector)); err != nil {
		return false, xc.ErrIllegalState.Wrapf("failed to decode unproven bitfield (deadline %d, partition %d): %w", dlIdx, pIdx, err)
	} else if unproven && requireProven {
		return false, nil
	}

	return true, nil
}