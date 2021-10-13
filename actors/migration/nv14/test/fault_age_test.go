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
	"github.com/filecoin-project/go-state-types/rt"
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	ipld2 "github.com/filecoin-project/specs-actors/v2/support/ipld"
	miner5 "github.com/filecoin-project/specs-actors/v5/actors/builtin/miner"
	power5 "github.com/filecoin-project/specs-actors/v5/actors/builtin/power"
	vm5 "github.com/filecoin-project/specs-actors/v5/support/vm"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/exported"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v6/actors/migration/nv14"
	"github.com/filecoin-project/specs-actors/v6/actors/runtime/proof"
	adt5 "github.com/filecoin-project/specs-actors/v6/actors/util/adt"
	tutil "github.com/filecoin-project/specs-actors/v6/support/testing"
	"github.com/filecoin-project/specs-actors/v6/support/vm"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFaultManyMigrateRecoverOneFaultAgain(t *testing.T) {

	ctx := context.Background()
	bs := ipld2.NewSyncBlockStoreInMemory()
	v := vm5.NewVMWithSingletons(ctx, t, bs)

	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	sectorSize, err := sealProof.SectorSize()
	require.NoError(t, err)

	require.NoError(t, err)
	addrs := vm5.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(10_000), builtin.TokenPrecision), 93837778)
	owner, worker := addrs[0], addrs[0]
	minerAddrs := CreateMinerV5(t, v, owner, worker, wPoStProof, big.Mul(big.NewInt(10_000), vm.FIL))

	v, sectorNumbers, sectors := commitProveCronNSectorsV5(t, v, 10, worker, minerAddrs.IDAddress, sealProof)
	sector1Num := sectorNumbers[0]
	sector1 := sectors[0]

	// Gather initial IP. This will only change when sectors are terminated
	var minerState miner.State
	err = v.GetState(minerAddrs.IDAddress, &minerState)
	require.NoError(t, err)
	ip := minerState.InitialPledge

	// Fault all sectors by missing a post
	v, err = v.WithEpoch(v.GetEpoch() + 1)
	require.NoError(t, err)
	dlInfo, _, v := vm5.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sector1Num)
	v, err = v.WithEpoch(dlInfo.Last()) // run cron on deadline end to fault
	require.NoError(t, err)
	vm5.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
	v, err = v.WithEpoch(v.GetEpoch() + 1)
	require.NoError(t, err)

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

	// Recover one sector
	d, p := vm.SectorDeadline(t, v6, minerAddrs.IDAddress, sector1Num)
	recoverParams := miner.DeclareFaultsRecoveredParams{
		Recoveries: []miner0.RecoveryDeclaration{
			{
				Deadline:  d,
				Partition: p,
				Sectors:   bitfield.NewFromSet([]uint64{uint64(sector1Num), uint64(sector1Num)}),
			},
		},
	}
	faultedSectorNumbers := make([]uint64, 9)
	for i, num := range sectorNumbers {
		if i > 0 {
			faultedSectorNumbers[i-1] = uint64(num)
		}
	}
	partitions := []miner.PoStPartition{{
		Index:   p,
		Skipped: bitfield.NewFromSet(faultedSectorNumbers),
	}}
	vm.ApplyOk(t, v6, worker, minerAddrs.IDAddress, big.Zero(), builtin.MethodsMiner.DeclareFaultsRecovered, &recoverParams)
	dlInfo, _, v6 = vm.AdvanceTillProvingDeadline(t, v6, minerAddrs.IDAddress, sector1Num)
	vm.SubmitWindowPoSt(t, v6, worker, minerAddrs.IDAddress, dlInfo, partitions, miner.PowerForSectors(sectorSize, []*miner.SectorOnChainInfo{sector1}))

	faultedInvocs := []vm.ExpectInvocation{
		{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
	}
	newFaultInvocs := []vm.ExpectInvocation{
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdateClaimedPower},
		{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
	}
	terminatedInvocs := []vm.ExpectInvocation{
		{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
		{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdatePledgeTotal},
	}
	// continue faulty for > miner5.FaultMaxAge and watch only faults before migration expire
	faultyPPs := 2 * int(miner5.FaultMaxAge/miner.WPoStProvingPeriod)
	preMigrationFaultPeriod := int(miner5.FaultMaxAge / miner.WPoStProvingPeriod)
	// once the first FaultMaxAge epochs have passed only 1 of 10 sectors is left
	oneSectorIP := big.Div(ip, big.NewInt(10))
	assert.True(t, faultyPPs > preMigrationFaultPeriod-1)
	for i := 0; i < faultyPPs; i++ {
		invocs := faultedInvocs
		if i == preMigrationFaultPeriod-1 {
			invocs = terminatedInvocs
		} else if i == 1 { // no wposts so recovered sector faults after one period
			invocs = newFaultInvocs
		}
		if i > preMigrationFaultPeriod-1 {
			ip = oneSectorIP
		}
		v6 = checkAndAdvanceDeadline(t, v6, minerAddrs.IDAddress, sector1Num, ip, invocs)
	}
	err = v6.GetState(minerAddrs.IDAddress, &minerState)
	require.NoError(t, err)
	// Nine out of ten sectors has been terminated.  Over miner5.FaultMaxAge epochs have passed
	assert.Equal(t, oneSectorIP, minerState.InitialPledge)
}

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
	dfParams := &miner.DeclareFaultsParams{
		Faults: []miner0.FaultDeclaration{
			{
				Deadline:  d,
				Partition: p,
				Sectors:   bitfield.NewFromSet([]uint64{uint64(sector2Num)}),
			},
		},
	}
	vm.ApplyOk(t, v6, worker, minerAddrs.IDAddress, big.Zero(), builtin.MethodsMiner.DeclareFaults, dfParams)

	twoFaultedInvocs := []vm.ExpectInvocation{
		{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
	}

	// continue faulty for 5 proving periods (less than miner5.FaultMaxAge and miner.FaultMaxAge+1)
	faultyPPs := 5
	require.True(t, faultyPPs < int(miner5.FaultMaxAge/miner.WPoStProvingPeriod))
	require.True(t, faultyPPs < int(miner.FaultMaxAge/miner.WPoStProvingPeriod)+1)
	for i := 0; i < faultyPPs; i++ {
		v6 = checkAndAdvanceDeadline(t, v6, minerAddrs.IDAddress, sector1Num, ip, twoFaultedInvocs)
	}

	// Recover both sectors
	recoverParams := miner.DeclareFaultsRecoveredParams{
		Recoveries: []miner0.RecoveryDeclaration{
			{
				Deadline:  d,
				Partition: p,
				Sectors:   bitfield.NewFromSet([]uint64{uint64(sector1Num), uint64(sector2Num)}),
			},
		},
	}
	vm.ApplyOk(t, v6, worker, minerAddrs.IDAddress, big.Zero(), builtin.MethodsMiner.DeclareFaultsRecovered, &recoverParams)

	// On next post sectors should come back to life
	v6, dlInfo = vm.AdvanceByDeadlineTillIndex(t, v6, minerAddrs.IDAddress, d)
	v6, err = v6.WithEpoch(dlInfo.Last())
	require.NoError(t, err)
	partitions = []miner.PoStPartition{{
		Index:   p,
		Skipped: bitfield.New(),
	}}
	vm.SubmitWindowPoSt(t, v6, worker, minerAddrs.IDAddress, dlInfo, partitions, miner.PowerForSectors(sectorSize, sectors))
	// cron shows no faults
	vm.ApplyOk(t, v6, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
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
	}.Matches(t, v6.Invocations()[1])

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
	dfParams := &miner.DeclareFaultsParams{
		Faults: []miner0.FaultDeclaration{
			{
				Deadline:  d,
				Partition: p,
				Sectors:   bitfield.NewFromSet([]uint64{uint64(sector2Num)}),
			},
		},
	}
	vm.ApplyOk(t, v6, worker, minerAddrs.IDAddress, big.Zero(), builtin.MethodsMiner.DeclareFaults, dfParams)

	// Assemble different cron call signatures
	twoFaultedInvocs := []vm.ExpectInvocation{
		{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
	}
	oneFaultedInvocs := []vm.ExpectInvocation{
		{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
	}
	sectorOneTermInvocs := []vm.ExpectInvocation{
		twoFaultedInvocs[0],
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
		{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdatePledgeTotal},
	}
	sectorTwoTermInvocs := []vm.ExpectInvocation{
		oneFaultedInvocs[0],
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
		{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdatePledgeTotal},
	}

	// in miner5.FaultMaxAge epochs sector1 is terminated
	// since both sectors are faulty no posts need to be submitted
	for i := 0; i < int(miner5.FaultMaxAge/miner5.WPoStProvingPeriod); i++ {
		invocs := twoFaultedInvocs
		if i == int(miner5.FaultMaxAge/miner5.WPoStProvingPeriod)-1 {
			invocs = sectorOneTermInvocs
		}
		v6 = checkAndAdvanceDeadline(t, v6, minerAddrs.IDAddress, sector1Num, ip, invocs)
	}
	err = v6.GetState(minerAddrs.IDAddress, &minerState)
	require.NoError(t, err)
	// one sector has been terminated
	oneSectorIP := big.Div(ip, big.NewInt(2))
	assert.Equal(t, oneSectorIP, minerState.InitialPledge)

	secondSectorTerminationPP := int(miner.FaultMaxAge/miner.WPoStProvingPeriod) + 1 // +1 because second sector faults 1 PP after first
	for i := int(miner5.FaultMaxAge / miner5.WPoStProvingPeriod); i < secondSectorTerminationPP; i++ {
		invocs := oneFaultedInvocs
		if i == secondSectorTerminationPP-1 {
			invocs = sectorTwoTermInvocs
		}
		v6 = checkAndAdvanceDeadline(t, v6, minerAddrs.IDAddress, sector2Num, oneSectorIP, invocs)
	}
	err = v6.GetState(minerAddrs.IDAddress, &minerState)
	require.NoError(t, err)
	// both sectors terminated
	assert.Equal(t, big.Zero(), minerState.InitialPledge)

}

func checkAndAdvanceDeadline(t *testing.T, v *vm.VM, mAddr address.Address, sectorNum abi.SectorNumber, ipExpected abi.TokenAmount, cronSubInvocs []vm.ExpectInvocation) *vm.VM {
	var minerState miner.State
	err := v.GetState(mAddr, &minerState)
	require.NoError(t, err)
	assert.Equal(t, ipExpected, minerState.InitialPledge)
	dlInfo, _, v := vm.AdvanceTillProvingDeadline(t, v, mAddr, sectorNum)
	v, err = v.WithEpoch(dlInfo.Last())
	require.NoError(t, err)
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	vm.ExpectInvocation{
		To:     builtin.CronActorAddr,
		Method: builtin.MethodsCron.EpochTick,
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.OnEpochTickEnd, SubInvocations: []vm.ExpectInvocation{
				{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
				{To: mAddr, Method: builtin.MethodsMiner.OnDeferredCronEvent, SubInvocations: cronSubInvocs},
				{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
			}},
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick},
		},
	}.Matches(t, v.Invocations()[0])

	// get out of this deadline to advance to the next one in the next iteration
	v, err = v.WithEpoch(dlInfo.Close)
	require.NoError(t, err)
	return v
}

func commitProveCronNSectorsV5(t *testing.T, v *vm5.VM, n int, worker, mAddr address.Address, sealProof abi.RegisteredSealProof) (*vm5.VM, []abi.SectorNumber, []*miner.SectorOnChainInfo) {
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)
	partitionsSector, err := builtin.PoStProofWindowPoStPartitionSectors(wPoStProof)
	require.NoError(t, err)
	require.True(t, n < int(partitionsSector))
	// Pre commit n sectors
	v, err = v.WithEpoch(200)
	require.NoError(t, err)
	sectorNumberStart := abi.SectorNumber(100)
	PreCommitSectorsV5(t, v, n, 1, worker, mAddr, sealProof, sectorNumberStart, true)
	balances := vm5.GetMinerBalances(t, v, mAddr)
	assert.True(t, balances.PreCommitDeposit.GreaterThan(big.Zero()))
	proveTime := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	v, _ = vm5.AdvanceByDeadlineTillEpoch(t, v, mAddr, proveTime)

	// Prove commit two sectors
	v, err = v.WithEpoch(proveTime)
	require.NoError(t, err)
	sectorNumbers := make([]abi.SectorNumber, n)
	for i := 0; i < n; i++ {
		num := sectorNumberStart + abi.SectorNumber(i)
		sectorNumbers[i] = num
		proveCommitParams := miner.ProveCommitSectorParams{
			SectorNumber: num,
		}
		vm5.ApplyOk(t, v, worker, mAddr, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)
	}
	// In same epoch run cron to trigger proof confirmation and deadline assignment
	vm5.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	sectorSize, err := sealProof.SectorSize()
	require.NoError(t, err)
	dlInfo, pIdx, v := vm5.AdvanceTillProvingDeadline(t, v, mAddr, sectorNumberStart)
	var minerState miner.State
	err = v.GetState(mAddr, &minerState)
	require.NoError(t, err)

	// PoSt
	powerAdded := miner.NewPowerPairZero()
	sectors := make([]*miner.SectorOnChainInfo, 0)
	for _, num := range sectorNumbers {
		sector, found, err := minerState.GetSector(v.Store(), num)
		require.NoError(t, err)
		sectors = append(sectors, sector)
		require.True(t, found)
		powerAdded = powerAdded.Add(miner.PowerForSector(sectorSize, sector))
	}
	v, err = v.WithEpoch(v.GetEpoch())
	require.NoError(t, err)
	partitions := []miner.PoStPartition{{
		Index:   pIdx,
		Skipped: bitfield.New(),
	}}
	SubmitWindowPoStV5(t, v, worker, mAddr, dlInfo, partitions, powerAdded)
	v, err = v.WithEpoch(dlInfo.Last()) // run cron on deadline end to process post
	require.NoError(t, err)
	vm5.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
	return v, sectorNumbers, sectors
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
