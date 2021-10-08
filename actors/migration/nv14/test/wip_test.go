package test_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/rt"
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	ipld2 "github.com/filecoin-project/specs-actors/v2/support/ipld"
	builtin5 "github.com/filecoin-project/specs-actors/v5/actors/builtin"
	miner5 "github.com/filecoin-project/specs-actors/v5/actors/builtin/miner"
	vm5 "github.com/filecoin-project/specs-actors/v5/support/vm"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/exported"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v6/actors/migration/nv14"
	adt5 "github.com/filecoin-project/specs-actors/v6/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v6/support/vm"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecoverFaultsFromBeforeAfterMigration(t *testing.T) {

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

	v, sectorNumbers, sectors := commitProveCronNSectorsV5(t, v, 2, worker, minerAddrs.IDAddress, sealProof)
	sector1Num := sectorNumbers[0]
	sector2Num := sectorNumbers[1]
	sector1 := sectors[0]

	// Gather initial IP. This will only change when sectors are terminated
	var minerState miner.State
	err = v.GetState(minerAddrs.IDAddress, &minerState)
	require.NoError(t, err)
	ip := minerState.InitialPledge

	// Fault sector 100 by skipping in post in the next proving period
	dlInfo, pIdx, v := vm5.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sector1Num)
	partitions := []miner.PoStPartition{{
		Index:   pIdx,
		Skipped: bitfield.NewFromSet([]uint64{uint64(sector1Num)}),
	}}
	v, err = v.WithEpoch(dlInfo.Last()) // run cron on deadline end to fault
	require.NoError(t, err)
	sectorSize, err := sealProof.SectorSize()
	require.NoError(t, err)

	SubmitWindowPoStV5(t, v, worker, minerAddrs.IDAddress, dlInfo, partitions, miner.PowerForSector(sectorSize, sector1).Neg())
	vm5.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// Migrate
	log := nv14.TestLogger{TB: t}
	adtStore := adt5.WrapStore(ctx, cbor.NewCborStore(bs))
	startRoot := v.StateRoot()
	nextRoot, err := nv14.MigrateStateTree(ctx, adtStore, startRoot, abi.ChainEpoch(0), nv14.Config{MaxWorkers: 1}, log, nv14.NewMemMigrationCache())
	require.NoError(t, err)

	lookup := map[cid.Cid]rt.VMActor{}
	for _, ba := range exported.BuiltinActors() {
		lookup[ba.Code()] = ba
	}

	v6, err := vm.NewVMAtEpoch(ctx, lookup, v.Store(), nextRoot, v.GetEpoch()+1)
	require.NoError(t, err)

	// Fault sector 101
	d, p := vm.SectorDeadline(t, v6, minerAddrs.IDAddress, sector2Num)
	v6, _ = vm.AdvanceByDeadlineTillIndex(t, v6, minerAddrs.IDAddress, d+2) // move out of deadline so fault can go through
	dfParams := &miner5.DeclareFaultsParams{
		Faults: []miner0.FaultDeclaration{
			{
				Deadline:  d,
				Partition: p,
				Sectors:   bitfield.NewFromSet([]uint64{uint64(sector2Num)}),
			},
		},
	}
	vm.ApplyOk(t, v6, worker, minerAddrs.IDAddress, big.Zero(), builtin5.MethodsMiner.DeclareFaults, dfParams)

	// Run for 3 epochs as faulty
	for i := 0; i < 3; i++ {
		v6 = checkAndAdvanceDeadline(t, v6, minerAddrs.IDAddress, sector1Num, ip)
	}
	err = v6.GetState(minerAddrs.IDAddress, &minerState)
	require.NoError(t, err)
	// one sector has been terminated
	oneSectorIP := big.Div(ip, big.NewInt(2))
	assert.Equal(t, oneSectorIP, minerState.InitialPledge)

	secondSectorTerminationPP := int(miner.FaultMaxAge/miner.WPoStProvingPeriod) + 1 // +1 because second sector faults 1 PP after first
	for i := int(miner5.FaultMaxAge / miner5.WPoStProvingPeriod); i < secondSectorTerminationPP; i++ {
		v6 = checkAndAdvanceDeadline(t, v6, minerAddrs.IDAddress, sector2Num, oneSectorIP)
	}
	err = v6.GetState(minerAddrs.IDAddress, &minerState)
	require.NoError(t, err)
	// both sectors terminated
	assert.Equal(t, big.Zero(), minerState.InitialPledge)

}
