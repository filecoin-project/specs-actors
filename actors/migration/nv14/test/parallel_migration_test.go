package test_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/dline"
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	ipld2 "github.com/filecoin-project/specs-actors/v2/support/ipld"
	miner5 "github.com/filecoin-project/specs-actors/v5/actors/builtin/miner"
	power5 "github.com/filecoin-project/specs-actors/v5/actors/builtin/power"
	vm5 "github.com/filecoin-project/specs-actors/v5/support/vm"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v6/actors/migration/nv14"
	"github.com/filecoin-project/specs-actors/v6/actors/runtime/proof"
	adt5 "github.com/filecoin-project/specs-actors/v6/actors/util/adt"
	tutil "github.com/filecoin-project/specs-actors/v6/support/testing"
	"github.com/filecoin-project/specs-actors/v6/support/vm"
	cbor "github.com/ipfs/go-ipld-cbor"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestParallelMigrationCalls(t *testing.T) {
	// Construct simple prior state tree over a synchronized store
	ctx := context.Background()
	log := nv14.TestLogger{TB: t}
	bs := ipld2.NewSyncBlockStoreInMemory()
	vm := vm5.NewVMWithSingletons(ctx, t, bs)

	// Run migration
	adtStore := adt5.WrapStore(ctx, cbor.NewCborStore(bs))
	startRoot := vm.StateRoot()
	endRootSerial, err := nv14.MigrateStateTree(ctx, adtStore, startRoot, abi.ChainEpoch(0), nv14.Config{MaxWorkers: 1}, log, nv14.NewMemMigrationCache())
	require.NoError(t, err)

	// Migrate in parallel
	var endRootParallel1, endRootParallel2 cid.Cid
	grp, ctx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		var err1 error
		endRootParallel1, err1 = nv14.MigrateStateTree(ctx, adtStore, startRoot, abi.ChainEpoch(0), nv14.Config{MaxWorkers: 2}, log, nv14.NewMemMigrationCache())
		return err1
	})
	grp.Go(func() error {
		var err2 error
		endRootParallel2, err2 = nv14.MigrateStateTree(ctx, adtStore, startRoot, abi.ChainEpoch(0), nv14.Config{MaxWorkers: 2}, log, nv14.NewMemMigrationCache())
		return err2
	})
	require.NoError(t, grp.Wait())
	assert.Equal(t, endRootSerial, endRootParallel1)
	assert.Equal(t, endRootParallel1, endRootParallel2)
}

func TestEarlyTerminationInCronBeforeAndAfterMigration(t *testing.T) {

	ctx := context.Background()
	bs := ipld2.NewSyncBlockStoreInMemory()
	v := vm5.NewVMWithSingletons(ctx, t, bs)

	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)

	require.NoError(t, err)
	addrs := vm5.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(10_000), builtin.TokenPrecision), 93837778)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := CreateMinerV5(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	// Pre commit two sectors
	v, err = v.WithEpoch(200)
	require.NoError(t, err)
	sectorNumber := abi.SectorNumber(100)
	PreCommitSectorsV5(t, v, 2, 1, worker, minerAddrs.IDAddress, sealProof, sectorNumber, true)
	balances := vm5.GetMinerBalances(t, v, minerAddrs.IDAddress)
	assert.True(t, balances.PreCommitDeposit.GreaterThan(big.Zero()))
	proveTime := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	v, _ = vm5.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	// Prove commit two sectors
	v, err = v.WithEpoch(proveTime)
	require.NoError(t, err)
	proveCommitParams := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}
	vm5.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)
	proveCommitParams.SectorNumber = sectorNumber + 1
	vm5.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)
	// In the same epoch, trigger cron to validate prove commit
	vm5.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// PoSt
	sectorSize, err := sealProof.SectorSize()
	dlInfo, pIdx, v := vm5.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)
	var minerState miner.State
	err = v.GetState(minerAddrs.IDAddress, &minerState)
	require.NoError(t, err)
	sector1, found, err := minerState.GetSector(v.Store(), sectorNumber)
	require.NoError(t, err)
	require.True(t, found)
	sector2, found, err := minerState.GetSector(v.Store(), sectorNumber+1)
	require.NoError(t, err)
	require.True(t, found)
	tv, err := v.WithEpoch(v.GetEpoch())
	require.NoError(t, err)
	partitions := []miner.PoStPartition{{
		Index:   pIdx,
		Skipped: bitfield.New(),
	}}
	sectorPower := miner.PowerForSector(sectorSize, sector1)
	sectorPower = sectorPower.Add(miner.PowerForSector(sectorSize, sector2))
	SubmitWindowPoStV5(t, tv, worker, minerAddrs.IDAddress, dlInfo, partitions, sectorPower)

	// Fault sector 100

	// Migrate
	log := nv14.TestLogger{TB: t}
	adtStore := adt5.WrapStore(ctx, cbor.NewCborStore(bs))
	startRoot := v.StateRoot()
	endRootSerial, err := nv14.MigrateStateTree(ctx, adtStore, startRoot, abi.ChainEpoch(0), nv14.Config{MaxWorkers: 1}, log, nv14.NewMemMigrationCache())
	require.NoError(t, err)
	_ = endRootSerial

	// Fault sector 101

}

func CreateMinerV5(t *testing.T, v *vm5.VM, owner, worker address.Address, wPoStProof abi.RegisteredPoStProof, balance abi.TokenAmount) *power.CreateMinerReturn {
	params := power5.CreateMinerParams{
		Owner:               owner,
		Worker:              worker,
		WindowPoStProofType: wPoStProof,
		Peer:                abi.PeerID("not really a peer id"),
	}
	ret := vm5.ApplyOk(t, v, worker, builtin.StoragePowerActorAddr, balance, builtin.MethodsPower.CreateMiner, &params)
	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)
	return minerAddrs
}

func PreCommitSectorsV5(t *testing.T, v *vm5.VM, count, batchSize int, worker, mAddr address.Address, sealProof abi.RegisteredSealProof,
	sectorNumberBase abi.SectorNumber, expectCronEnrollment bool) []*miner.SectorPreCommitOnChainInfo {
	invocsCommon := []vm5.ExpectInvocation{
		{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
	}
	invocFirst := vm5.ExpectInvocation{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent}

	sectorIndex := 0
	for sectorIndex < count {
		msgSectorIndexStart := sectorIndex
		invocs := invocsCommon

		// Prepare message.
		params := miner5.PreCommitSectorBatchParams{Sectors: make([]miner0.SectorPreCommitInfo, batchSize)}
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
			invocs = append(invocs, vm5.ExpectInvocation{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend, Value: &aggFee})
		}
		if expectCronEnrollment && msgSectorIndexStart == 0 {
			invocs = append(invocs, invocFirst)
		}
		vm5.ApplyOk(t, v, worker, mAddr, big.Zero(), builtin.MethodsMiner.PreCommitSectorBatch, &params)
		vm5.ExpectInvocation{
			To:             mAddr,
			Method:         builtin.MethodsMiner.PreCommitSectorBatch,
			Params:         vm5.ExpectObject(&params),
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

func SubmitWindowPoStV5(t *testing.T, v *vm5.VM, worker, actor address.Address, dlInfo *dline.Info, partitions []miner.PoStPartition,
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
	vm5.ApplyOk(t, v, worker, actor, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)

	updatePowerParams := &power.UpdateClaimedPowerParams{
		RawByteDelta:         newPower.Raw,
		QualityAdjustedDelta: newPower.QA,
	}

	vm5.ExpectInvocation{
		To:     actor,
		Method: builtin.MethodsMiner.SubmitWindowedPoSt,
		Params: vm5.ExpectObject(&submitParams),
		SubInvocations: []vm5.ExpectInvocation{
			{
				To:     builtin.StoragePowerActorAddr,
				Method: builtin.MethodsPower.UpdateClaimedPower,
				Params: vm5.ExpectObject(updatePowerParams),
			},
		},
	}.Matches(t, v.LastInvocation())
}
