package miner_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitions(t *testing.T) {
	sectors := []*miner.SectorOnChainInfo{
		testSector(2, 1, 50, 60, 1000),
		testSector(3, 2, 51, 61, 1001),
		testSector(7, 3, 52, 62, 1002),
		testSector(8, 4, 53, 63, 1003),
		testSector(11, 5, 54, 64, 1004),
		testSector(13, 6, 55, 65, 1005),
	}
	sectorSize := abi.SectorSize(32 << 30)

	t.Run("adds sectors and reports sector stats", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		partition := emptyPartition(t, rt)

		quantSpec := miner.NewQuantSpec(4, 1)
		power, err := partition.AddSectors(adt.AsStore(rt), sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		expectedPower := miner.PowerForSectors(sectorSize, sectors)
		assert.True(t, expectedPower.Equals(power))

		assertPartitionState(t, partition, sectors, sectorSize, bf(1, 2, 3, 4, 5, 6), bf(), bf(), bf())

		// assert sectors have been arranged into 3 groups
		assertPartitionExpirationQueue(t, rt, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(1, 2)},
			{expiration: 9, sectors: bf(3, 4)},
			{expiration: 13, sectors: bf(5, 6)},
		})
	})

	t.Run("adds faults", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		partition := emptyPartition(t, rt)

		quantSpec := miner.NewQuantSpec(4, 1)
		_, err := partition.AddSectors(adt.AsStore(rt), sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		faultSet := bf(4, 5)
		faultSectors := selectSectors(t, sectors, faultSet)
		power, err := partition.AddFaults(adt.AsStore(rt), faultSet, faultSectors, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		expectedFaultyPower := miner.PowerForSectors(sectorSize, faultSectors)
		assert.True(t, expectedFaultyPower.Equals(power))

		assertPartitionState(t, partition, sectors, sectorSize, bf(1, 2, 3, 4, 5, 6), bf(4, 5), bf(), bf())

		// moves faulty sectors after expiration to earlier group
		assertPartitionExpirationQueue(t, rt, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(1, 2)},
			{expiration: 9, sectors: bf(3, 4, 5)},
			{expiration: 13, sectors: bf(6)},
		})
	})

	t.Run("adds recoveries", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		partition := emptyPartition(t, rt)

		quantSpec := miner.NewQuantSpec(4, 1)
		_, err := partition.AddSectors(adt.AsStore(rt), sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		// make 4, 5 and 6 faulty
		faultSet := bitfield.NewFromSet([]uint64{4, 5, 6})
		faultSectors := selectSectors(t, sectors, faultSet)
		_, err = partition.AddFaults(adt.AsStore(rt), faultSet, faultSectors, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// add 4 and 5 as recoveries
		recoverSet := bitfield.NewFromSet([]uint64{4, 5})
		recoverSectors := selectSectors(t, sectors, recoverSet)
		recoveredPower := miner.PowerForSectors(sectorSize, recoverSectors)
		err = partition.AddRecoveries(recoverSet, recoveredPower)
		require.NoError(t, err)

		assertPartitionState(t, partition, sectors, sectorSize, bf(1, 2, 3, 4, 5, 6), bf(4, 5, 6), bf(4, 5), bf())
	})

	t.Run("remove recoveries", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		partition := emptyPartition(t, rt)

		quantSpec := miner.NewQuantSpec(4, 1)
		_, err := partition.AddSectors(adt.AsStore(rt), sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		// make 4, 5 and 6 faulty
		faultSet := bitfield.NewFromSet([]uint64{4, 5, 6})
		faultSectors := selectSectors(t, sectors, faultSet)
		_, err = partition.AddFaults(adt.AsStore(rt), faultSet, faultSectors, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// add 4 and 5 as recoveries
		recoverSet := bitfield.NewFromSet([]uint64{4, 5})
		recoverSectors := selectSectors(t, sectors, recoverSet)
		recoveredPower := miner.PowerForSectors(sectorSize, recoverSectors)
		err = partition.AddRecoveries(recoverSet, recoveredPower)
		require.NoError(t, err)

		// remove zero recoveries does nothing
		err = partition.RemoveRecoveries(bf(), miner.NewPowerPairZero())
		require.NoError(t, err)

		assertPartitionState(t, partition, sectors, sectorSize, bf(1, 2, 3, 4, 5, 6), bf(4, 5, 6), bf(4, 5), bf())

		// removing sector 5 alters recovery set and recovery power
		removedPower := miner.PowerForSectors(sectorSize, sectors[4:5])
		err = partition.RemoveRecoveries(bf(5), removedPower)
		require.NoError(t, err)

		assertPartitionState(t, partition, sectors, sectorSize, bf(1, 2, 3, 4, 5, 6), bf(4, 5, 6), bf(4), bf())
	})

	t.Run("recovers faults", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		partition := emptyPartition(t, rt)

		quantSpec := miner.NewQuantSpec(4, 1)
		_, err := partition.AddSectors(adt.AsStore(rt), sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		// make 4, 5 and 6 faulty
		faultSet := bitfield.NewFromSet([]uint64{4, 5, 6})
		faultSectors := selectSectors(t, sectors, faultSet)
		_, err = partition.AddFaults(adt.AsStore(rt), faultSet, faultSectors, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// add 4 and 5 as recoveries
		recoverSet := bitfield.NewFromSet([]uint64{4, 5})
		recoverSectors := selectSectors(t, sectors, recoverSet)
		recoveryPower := miner.PowerForSectors(sectorSize, recoverSectors)
		err = partition.AddRecoveries(recoverSet, recoveryPower)
		require.NoError(t, err)

		// mark recoveries as recovered recover sectors
		recoveredPower, err := partition.RecoverFaults(adt.AsStore(rt), recoverSet, recoverSectors, sectorSize, quantSpec)
		require.NoError(t, err)

		// recovered power should equal power of recovery sectors
		assert.True(t, recoveryPower.Equals(recoveredPower))

		// state should be as if recovered sectors were never faults
		assertPartitionState(t, partition, sectors, sectorSize, bf(1, 2, 3, 4, 5, 6), bf(6), bf(), bf())

		// restores recovered expirations to original state (unrecovered sector 6 still expires early)
		assertPartitionExpirationQueue(t, rt, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(1, 2)},
			{expiration: 9, sectors: bf(3, 4, 6)},
			{expiration: 13, sectors: bf(5)},
		})
	})

	t.Run("reschedules expirations", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		partition := emptyPartition(t, rt)

		quantSpec := miner.NewQuantSpec(4, 1)
		_, err := partition.AddSectors(adt.AsStore(rt), sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		// recovered power should equal power of recovery sectors
		sectorsToMove := selectSectors(t, sectors, bf(2, 4, 6))
		err = partition.RescheduleExpirations(adt.AsStore(rt), 18, sectorsToMove, sectorSize, quantSpec)
		require.NoError(t, err)

		// partition power and sector categorization should remain the same
		assertPartitionState(t, partition, sectors, sectorSize, bf(1, 2, 3, 4, 5, 6), bf(), bf(), bf())

		// sectors should move to new expiration group
		assertPartitionExpirationQueue(t, rt, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(1)},
			{expiration: 9, sectors: bf(3)},
			{expiration: 13, sectors: bf(5)},
			{expiration: 21, sectors: bf(2, 4, 6)},
		})
	})

	t.Run("replace sectors", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		partition := emptyPartition(t, rt)

		quantSpec := miner.NewQuantSpec(4, 1)
		_, err := partition.AddSectors(adt.AsStore(rt), sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		// remove 3 sectors starting with 2
		oldSectors := sectors[1:4]
		oldSectorPower := miner.PowerForSectors(sectorSize, oldSectors)
		oldSectorPledge := int64(1001 + 1002 + 1003)

		// replace 2 and add 2 new sectors
		newSectors := []*miner.SectorOnChainInfo{
			testSector(10, 2, 150, 260, 3000),
			testSector(10, 7, 151, 261, 3001),
			testSector(18, 8, 152, 262, 3002),
		}
		newSectorPower := miner.PowerForSectors(sectorSize, newSectors)
		newSectorPledge := int64(3000 + 3001 + 3002)

		powerDelta, pledgeDelta, err := partition.ReplaceSectors(adt.AsStore(rt), oldSectors, newSectors, sectorSize, quantSpec)
		require.NoError(t, err)

		expectedPowerDelta := newSectorPower.Sub(oldSectorPower)
		assert.True(t, expectedPowerDelta.Equals(powerDelta))
		assert.Equal(t, abi.NewTokenAmount(newSectorPledge-oldSectorPledge), pledgeDelta)

		// partition state should contain new sectors and not old sectors
		sectorsAfterReplace := append(append(sectors[:1:1], sectors[4:]...), newSectors...)
		assertPartitionState(t, partition, sectorsAfterReplace, sectorSize, bf(1, 2, 5, 6, 7, 8), bf(), bf(), bf())

		// sector 2 should be moved, 3 and 4 should be removed, and 7 and 8 added
		assertPartitionExpirationQueue(t, rt, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(1)},
			{expiration: 13, sectors: bf(2, 5, 6, 7)},
			{expiration: 21, sectors: bf(8)},
		})
	})

	t.Run("replace sectors errors when attempting to replace inactive sector", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		partition := emptyPartition(t, rt)

		quantSpec := miner.NewQuantSpec(4, 1)
		_, err := partition.AddSectors(adt.AsStore(rt), sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		// fault sector 2
		faultSet := bf(2)
		faultSectors := selectSectors(t, sectors, faultSet)
		_, err = partition.AddFaults(adt.AsStore(rt), faultSet, faultSectors, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// remove 3 sectors starting with 2
		oldSectors := sectors[1:4]

		// replace sector 2
		newSectors := []*miner.SectorOnChainInfo{
			testSector(10, 2, 150, 260, 3000),
		}

		_, _, err = partition.ReplaceSectors(adt.AsStore(rt), oldSectors, newSectors, sectorSize, quantSpec)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "refusing to replace inactive sectors")
	})

	t.Run("terminate sectors", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		partition := emptyPartition(t, rt)

		quantSpec := miner.NewQuantSpec(4, 1)
		_, err := partition.AddSectors(adt.AsStore(rt), sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		// fault sector 3, 4, 5 and 6
		faultSet := bf(3, 4, 5, 6)
		faultSectors := selectSectors(t, sectors, faultSet)
		_, err = partition.AddFaults(adt.AsStore(rt), faultSet, faultSectors, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// mark 4and 5 as a recoveries
		recoverSet := bf(4, 5)
		recoverSectors := selectSectors(t, sectors, recoverSet)
		recoveredPower := miner.PowerForSectors(sectorSize, recoverSectors)
		err = partition.AddRecoveries(recoverSet, recoveredPower)
		require.NoError(t, err)

		// now terminate 1, 3 and 5
		terminations := bf(1, 3, 5)
		terminatedSectors := selectSectors(t, sectors, terminations)
		terminationEpoch := abi.ChainEpoch(3)
		powerDelta, err := partition.TerminateSectors(adt.AsStore(rt), terminationEpoch, terminatedSectors, sectorSize, quantSpec)
		require.NoError(t, err)

		expectedPowerDelta := miner.PowerForSectors(sectorSize, terminatedSectors)
		assert.True(t, expectedPowerDelta.Equals(powerDelta))

		// expect partition state to no longer reflect power and pledge from terminated sectors and terminations to contain new sectors
		unterminatedSectors := selectSectors(t, sectors, bf(2, 4, 6))
		assertPartitionState(t, partition, unterminatedSectors, sectorSize, bf(1, 2, 3, 4, 5, 6), bf(4, 6), bf(4), terminations)

		// sectors should move to new expiration group
		assertPartitionExpirationQueue(t, rt, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(2)},
			{expiration: 9, sectors: bf(4, 6)},
		})

		// sectors should be added to early termination bitfield queue
		queue, err := miner.LoadBitfieldQueue(adt.AsStore(rt), partition.EarlyTerminated, quantSpec)
		require.NoError(t, err)

		ExpectBQ().
			Add(terminationEpoch, 1, 3, 5).
			Equals(t, queue)
	})

	t.Run("pop expiring sectors", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		partition := emptyPartition(t, rt)

		quantSpec := miner.NewQuantSpec(4, 1)
		_, err := partition.AddSectors(adt.AsStore(rt), sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		// add one fault with an early termination
		_, err = partition.AddFaults(adt.AsStore(rt), bf(4), sectors[3:4], abi.ChainEpoch(2), sectorSize, quantSpec)
		require.NoError(t, err)

		// pop first expiration set
		expireEpoch := abi.ChainEpoch(5)
		expset, err := partition.PopExpiredSectors(adt.AsStore(rt), expireEpoch, quantSpec)
		require.NoError(t, err)

		assertBitfieldsEqual(t, expset.OnTimeSectors, bf(1, 2))
		assertBitfieldsEqual(t, expset.EarlySectors, bf(4))
		assert.Equal(t, abi.NewTokenAmount(1000+1001), expset.OnTimePledge)

		// active power only contains power from non-faulty sectors
		assert.True(t, expset.ActivePower.Equals(miner.PowerForSectors(sectorSize, sectors[:2])))

		// faulty power comes from early termination
		assert.True(t, expset.FaultyPower.Equals(miner.PowerForSectors(sectorSize, sectors[3:4])))

		// expect sectors to be moved to terminations
		unterminatedSectors := selectSectors(t, sectors, bf(3, 4, 5, 6))
		assertPartitionState(t, partition, unterminatedSectors, sectorSize, bf(1, 2, 3, 4, 5, 6), bf(), bf(), bf(1, 2, 4))

		// sectors should move to new expiration group
		assertPartitionExpirationQueue(t, rt, partition, quantSpec, []expectExpirationGroup{
			{expiration: 9, sectors: bf(3)},
			{expiration: 13, sectors: bf(5, 6)},
		})

		// sectors should be added to early termination bitfield queue
		queue, err := miner.LoadBitfieldQueue(adt.AsStore(rt), partition.EarlyTerminated, quantSpec)
		require.NoError(t, err)

		// only early termination appears in bitfield queue
		ExpectBQ().
			Add(expireEpoch, 4).
			Equals(t, queue)
	})

	t.Run("pop expiring sectors errors if a recovery exists", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		partition := emptyPartition(t, rt)

		quantSpec := miner.NewQuantSpec(4, 1)
		_, err := partition.AddSectors(adt.AsStore(rt), sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		// add a recovery
		err = partition.AddRecoveries(bf(5), miner.PowerForSectors(sectorSize, sectors[4:5]))
		require.NoError(t, err)

		// pop first expiration set
		expireEpoch := abi.ChainEpoch(5)
		_, err = partition.PopExpiredSectors(adt.AsStore(rt), expireEpoch, quantSpec)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected recoveries while processing expirations")
	})

	t.Run("records missing PoSt", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		partition := emptyPartition(t, rt)

		quantSpec := miner.NewQuantSpec(4, 1)
		_, err := partition.AddSectors(adt.AsStore(rt), sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		// make 4, 5 and 6 faulty
		faultSet := bitfield.NewFromSet([]uint64{4, 5, 6})
		faultSectors := selectSectors(t, sectors, faultSet)
		_, err = partition.AddFaults(adt.AsStore(rt), faultSet, faultSectors, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// add 4 and 5 as recoveries
		recoverSet := bitfield.NewFromSet([]uint64{4, 5})
		recoverSectors := selectSectors(t, sectors, recoverSet)
		recoveredPower := miner.PowerForSectors(sectorSize, recoverSectors)
		err = partition.AddRecoveries(recoverSet, recoveredPower)
		require.NoError(t, err)

		// record entire partition as faulted
		newFaultPower, failedRecoveryPower, err := partition.RecordMissedPost(adt.AsStore(rt), abi.ChainEpoch(6), quantSpec)
		require.NoError(t, err)

		expectedNewFaultPower := miner.PowerForSectors(sectorSize, sectors[:3])
		assert.True(t, expectedNewFaultPower.Equals(newFaultPower))

		expectedFailedRecoveryPower := miner.PowerForSectors(sectorSize, sectors[3:5])
		assert.True(t, expectedFailedRecoveryPower.Equals(failedRecoveryPower))

		// everything is now faulty
		assertPartitionState(t, partition, sectors, sectorSize, bf(1, 2, 3, 4, 5, 6), bf(1, 2, 3, 4, 5, 6), bf(), bf())

		// everything not in first expiration group is now in second because fault expiration quantized to 9
		assertPartitionExpirationQueue(t, rt, partition, quantSpec, []expectExpirationGroup{
			{expiration: 5, sectors: bf(1, 2)},
			{expiration: 9, sectors: bf(3, 4, 5, 6)},
		})
	})

	t.Run("pops early terminations", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		partition := emptyPartition(t, rt)

		quantSpec := miner.NewQuantSpec(4, 1)
		_, err := partition.AddSectors(adt.AsStore(rt), sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		// fault sector 3, 4, 5 and 6
		faultSet := bf(3, 4, 5, 6)
		faultSectors := selectSectors(t, sectors, faultSet)
		_, err = partition.AddFaults(adt.AsStore(rt), faultSet, faultSectors, abi.ChainEpoch(7), sectorSize, quantSpec)
		require.NoError(t, err)

		// mark 4and 5 as a recoveries
		recoverSet := bf(4, 5)
		recoverSectors := selectSectors(t, sectors, recoverSet)
		recoveredPower := miner.PowerForSectors(sectorSize, recoverSectors)
		err = partition.AddRecoveries(recoverSet, recoveredPower)
		require.NoError(t, err)

		// now terminate 1, 3 and 5
		terminations := bf(1, 3, 5)
		terminatedSectors := selectSectors(t, sectors, terminations)
		terminationEpoch := abi.ChainEpoch(3)
		_, err = partition.TerminateSectors(adt.AsStore(rt), terminationEpoch, terminatedSectors, sectorSize, quantSpec)
		require.NoError(t, err)

		// pop first termination
		result, hasMore, err := partition.PopEarlyTerminations(adt.AsStore(rt), 1)
		require.NoError(t, err)

		// expect first sector to be in early terminations
		assertBitfieldsEqual(t, bf(1), result.Sectors[terminationEpoch])

		// expect more results
		assert.True(t, hasMore)

		// expect terminations to still contain 3 and 5
		queue, err := miner.LoadBitfieldQueue(adt.AsStore(rt), partition.EarlyTerminated, quantSpec)
		require.NoError(t, err)

		// only early termination appears in bitfield queue
		ExpectBQ().
			Add(terminationEpoch, 3, 5).
			Equals(t, queue)

		// pop the rest
		result, hasMore, err = partition.PopEarlyTerminations(adt.AsStore(rt), 5)
		require.NoError(t, err)

		// expect 3 and 5
		assertBitfieldsEqual(t, bf(3, 5), result.Sectors[terminationEpoch])

		// expect no more results
		assert.False(t, hasMore)

		// expect early terminations to be empty
		queue, err = miner.LoadBitfieldQueue(adt.AsStore(rt), partition.EarlyTerminated, quantSpec)
		require.NoError(t, err)
		ExpectBQ().Equals(t, queue)
	})
}

type expectExpirationGroup struct {
	expiration abi.ChainEpoch
	sectors    *bitfield.BitField
}

func assertPartitionExpirationQueue(t *testing.T, rt *mock.Runtime, partition *miner.Partition, quant miner.QuantSpec, groups []expectExpirationGroup) {
	queue, err := miner.LoadExpirationQueue(adt.AsStore(rt), partition.ExpirationsEpochs, quant)
	require.NoError(t, err)

	for _, group := range groups {
		requireNoExpirationGroupsBefore(t, group.expiration, queue)
		set, err := queue.PopUntil(group.expiration)
		require.NoError(t, err)

		// we pnly care whether the sectors are in the queue or not. ExpirationQueue tests can deal with early or on time.
		allSectors, err := bitfield.MergeBitFields(set.OnTimeSectors, set.EarlySectors)
		require.NoError(t, err)
		assertBitfieldsEqual(t, group.sectors, allSectors)
	}
}

func assertPartitionState(t *testing.T,
	partition *miner.Partition,
	sectors []*miner.SectorOnChainInfo,
	sectorSize abi.SectorSize,
	allSectorIds *bitfield.BitField,
	faults *bitfield.BitField,
	recovering *bitfield.BitField,
	terminations *bitfield.BitField) {

	faultyPower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, faults))
	assert.True(t, partition.FaultyPower.Equals(faultyPower))
	recoveringPower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, recovering))
	assert.True(t, partition.RecoveringPower.Equals(recoveringPower))
	livePower := miner.PowerForSectors(sectorSize, sectors)
	assert.True(t, partition.LivePower.Equals(livePower))
	activePower := livePower.Sub(faultyPower)
	assert.True(t, activePower.Equals(partition.ActivePower()))

	assertBitfieldsEqual(t, faults, partition.Faults)
	assertBitfieldsEqual(t, recovering, partition.Recoveries)
	assertBitfieldsEqual(t, terminations, partition.Terminated)
	assertBitfieldsEqual(t, allSectorIds, partition.Sectors)

	live, err := partition.LiveSectors()
	require.NoError(t, err)

	expectedLive, err := bitfield.SubtractBitField(allSectorIds, terminations)
	require.NoError(t, err)
	assertBitfieldsEqual(t, expectedLive, live)

	nonActiveIds, err := bitfield.MergeBitFields(faults, terminations)
	require.NoError(t, err)
	expectedActiveIds, err := bitfield.SubtractBitField(allSectorIds, nonActiveIds)
	require.NoError(t, err)

	active, err := partition.ActiveSectors()
	require.NoError(t, err)
	assertBitfieldsEqual(t, expectedActiveIds, active)
}

func bf(secNos ...uint64) *bitfield.BitField {
	return bitfield.NewFromSet(secNos)
}

func selectSectors(t *testing.T, sectors []*miner.SectorOnChainInfo, field *bitfield.BitField) []*miner.SectorOnChainInfo {
	included := []*miner.SectorOnChainInfo{}
	for _, s := range sectors {
		set, err := field.IsSet(uint64(s.SectorNumber))
		require.NoError(t, err)
		if set {
			included = append(included, s)
		}
	}
	return included
}

func emptyPartition(t *testing.T, rt *mock.Runtime) *miner.Partition {
	store := adt.AsStore(rt)
	root, err := adt.MakeEmptyArray(store).Root()
	require.NoError(t, err)

	return miner.ConstructPartition(root)
}

func assertBitfieldsEqual(t *testing.T, bf1 *bitfield.BitField, bf2 *bitfield.BitField) {
	count, err := bf2.Count()
	require.NoError(t, err)

	err = bf1.ForEach(func(v uint64) error {
		other, err := bf2.First()
		require.NoError(t, err)

		assert.Equal(t, other, v)

		bf2, err = bf2.Slice(1, count-1)
		require.NoError(t, err)
		count -= 1
		return nil
	})
	require.NoError(t, err)

	// assert no bits left in second bitfield.
	assert.Equal(t, uint64(0), count)
}
