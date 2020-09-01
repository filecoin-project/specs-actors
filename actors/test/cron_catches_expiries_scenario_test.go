package test_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
	vm "github.com/filecoin-project/specs-actors/support/vm"

	"github.com/filecoin-project/go-bitfield"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCronCatchedCCExpirationsAtDeadlineBoundary(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t)
	addrs := vm.CreateAccounts(ctx, t, v, 2, big.Mul(big.NewInt(10_000), vm.FIL), 93837778)
	worker, unverifiedClient := addrs[0], addrs[1]

	minerBalance := big.Mul(big.NewInt(1_000), vm.FIL)
	sectorNumber := abi.SectorNumber(100)
	sealedCid := tutil.MakeCID("100", &miner.SealedCIDPrefix)
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1

	// create miner
	params := power.CreateMinerParams{
		Owner:         worker,
		Worker:        worker,
		SealProofType: sealProof,
		Peer:          abi.PeerID("not really a peer id"),
	}
	ret, code := v.ApplyMessage(addrs[0], builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)
	require.Equal(t, exitcode.Ok, code)

	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	// precommit sector
	preCommitParams := miner.SectorPreCommitInfo{
		SealProof:     sealProof,
		SectorNumber:  sectorNumber,
		SealedCID:     sealedCid,
		SealRandEpoch: v.GetEpoch() - 1,
		DealIDs:       nil,
		Expiration:    v.GetEpoch() + 200*builtin.EpochsInDay,
	}
	_, code = v.ApplyMessage(addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)
	require.Equal(t, exitcode.Ok, code)

	// advance time to max seal duration
	proveTime := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	// Prove commit sector after max seal duration
	v, err := v.WithEpoch(proveTime)
	require.NoError(t, err)
	proveCommitParams := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}
	_, code = v.ApplyMessage(worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)
	require.Equal(t, exitcode.Ok, code)

	// In the same epoch, trigger cron to validate prove commit
	_, code = v.ApplyMessage(builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
	require.Equal(t, exitcode.Ok, code)

	// advance to proving period and submit post
	dlInfo, pIdx, v := vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)

	submitParams := miner.SubmitWindowedPoStParams{
		Deadline: dlInfo.Index,
		Partitions: []miner.PoStPartition{{
			Index:   pIdx,
			Skipped: bitfield.New(),
		}},
		Proofs: []abi.PoStProof{{
			PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		}},
		ChainCommitRand: []byte("not really random"),
	}

	_, code = v.ApplyMessage(addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)
	require.Equal(t, exitcode.Ok, code)

	// add market collateral for client and miner
	collateral := big.Mul(big.NewInt(3), vm.FIL)
	_, code = v.ApplyMessage(unverifiedClient, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &unverifiedClient)
	require.Equal(t, exitcode.Ok, code)
	collateral = big.Mul(big.NewInt(64), vm.FIL)
	_, code = v.ApplyMessage(worker, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &minerAddrs.IDAddress)
	require.Equal(t, exitcode.Ok, code)

	// create a deal required by upgrade sector
	dealIDs := []abi.DealID{}
	dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	deals := publishDeal(t, v, worker, unverifiedClient, minerAddrs.IDAddress, "deal1", 1<<30, false, dealStart, 181*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)

	// precommit capacity upgrade sector with deals
	upgradeSectorNumber := abi.SectorNumber(101)
	upgradeSealedCid := tutil.MakeCID("101", &miner.SealedCIDPrefix)
	preCommitParams = miner.SectorPreCommitInfo{
		SealProof:              sealProof,
		SectorNumber:           upgradeSectorNumber,
		SealedCID:              upgradeSealedCid,
		SealRandEpoch:          v.GetEpoch() - 1,
		DealIDs:                dealIDs,
		Expiration:             v.GetEpoch() + 220*builtin.EpochsInDay,
		ReplaceCapacity:        true,
		ReplaceSectorDeadline:  dlInfo.Index,
		ReplaceSectorPartition: pIdx,
		ReplaceSectorNumber:    sectorNumber,
	}
	_, code = v.ApplyMessage(addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)
	require.Equal(t, exitcode.Ok, code)

	// advance time to end of original sector's first proving deadline after minimum prove time has past
	proveTime = v.GetEpoch() + miner.PreCommitChallengeDelay + 1
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)
	dlInfo, _, v = vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)

	// prove original sector so it won't be faulted
	_, code = v.ApplyMessage(addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)
	require.Equal(t, exitcode.Ok, code)

	// one epoch before deadline close (i.e. Last) is where we might see a problem with cron scheduling of expirations
	v, err = v.WithEpoch(dlInfo.Last())
	require.NoError(t, err)

	// miner still has power for old sector
	sectorPower := vm.PowerForMinerSector(t, v, minerAddrs.IDAddress, sectorNumber)
	minerPower := vm.MinerPower(t, v, minerAddrs.IDAddress)
	assert.Equal(t, sectorPower.Raw, minerPower.Raw)
	assert.Equal(t, sectorPower.QA, minerPower.QA)

	// Prove commit sector after max seal duration
	proveCommitParams = miner.ProveCommitSectorParams{
		SectorNumber: upgradeSectorNumber,
	}
	_, code = v.ApplyMessage(worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)
	require.Equal(t, exitcode.Ok, code)

	// In the same epoch, trigger cron to validate prove commit
	// Replaced sector should be terminated at end of deadline it was replace in, so it should be terminated
	// by this call. This requires the miner's proving period handling to be run after commit verification.
	_, code = v.ApplyMessage(builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
	require.Equal(t, exitcode.Ok, code)

	// Loss of power indicates original sector has been terminated at correct time.
	minerPower = vm.MinerPower(t, v, minerAddrs.IDAddress)
	assert.Equal(t, big.Zero(), minerPower.Raw)
	assert.Equal(t, big.Zero(), minerPower.QA)
}
