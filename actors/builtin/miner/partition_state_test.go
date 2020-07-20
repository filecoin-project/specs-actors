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
	sectorSize := abi.SectorSize(32 * 1 << 30)

	t.Run("adds sectors and reports sector stats", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		partition := emptyPartition(t, rt)

		power, err := partition.AddSectors(adt.AsStore(rt), sectors, sectorSize, miner.NewQuantSpec(4, 1))
		require.NoError(t, err)

		expectedPower := miner.PowerForSectors(sectorSize, sectors)
		assert.True(t, expectedPower.Equals(power))

		assertPartitionState(t, partition, sectors, sectorSize, bf(), bf(), bf())
	})

	t.Run("adds faults", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		partition := emptyPartition(t, rt)

		quantSpec := miner.NewQuantSpec(4, 1)
		_, err := partition.AddSectors(adt.AsStore(rt), sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		faultSet := bitfield.NewFromSet([]uint64{4, 5})
		faultSectors := selectSectors(t, sectors, faultSet)
		power, err := partition.AddFaults(adt.AsStore(rt), faultSet, faultSectors, abi.ChainEpoch(1000), sectorSize, quantSpec)
		require.NoError(t, err)

		expectedFaultyPower := miner.PowerForSectors(sectorSize, faultSectors)
		assert.True(t, expectedFaultyPower.Equals(power))

		assertPartitionState(t, partition, sectors, sectorSize, bf(4, 5), bf(), bf())
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
		_, err = partition.AddFaults(adt.AsStore(rt), faultSet, faultSectors, abi.ChainEpoch(1000), sectorSize, quantSpec)
		require.NoError(t, err)

		// add 4 and 5 as recoveries
		recoverSet := bitfield.NewFromSet([]uint64{4, 5})
		recoverSectors := selectSectors(t, sectors, recoverSet)
		recoveredPower := miner.PowerForSectors(sectorSize, recoverSectors)
		err = partition.AddRecoveries(recoverSet, recoveredPower)
		require.NoError(t, err)

		assertPartitionState(t, partition, sectors, sectorSize, bf(4, 5, 6), bf(4, 5), bf())
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
		_, err = partition.AddFaults(adt.AsStore(rt), faultSet, faultSectors, abi.ChainEpoch(1000), sectorSize, quantSpec)
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
		assertPartitionState(t, partition, sectors, sectorSize, bf(6), bf(), bf())
	})
}

func assertPartitionState(t *testing.T,
	partition *miner.Partition,
	sectors []*miner.SectorOnChainInfo,
	sectorSize abi.SectorSize,
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

	allSectorIds := bf(sectorIDs(sectors)...)
	assertBitfieldsEqual(t, faults, partition.Faults)
	assertBitfieldsEqual(t, recovering, partition.Recoveries)
	assertBitfieldsEqual(t, terminations, partition.Terminated)
	assertBitfieldsEqual(t, allSectorIds, partition.Sectors)

	live, err := partition.LiveSectors()
	require.NoError(t, err)
	assertBitfieldsEqual(t, allSectorIds, live)

	expectedActiveIds, err := bitfield.SubtractBitField(allSectorIds, faults)
	require.NoError(t, err)

	active, err := partition.ActiveSectors()
	require.NoError(t, err)
	assertBitfieldsEqual(t, expectedActiveIds, active)
}

func sectorIDs(sectors []*miner.SectorOnChainInfo) []uint64 {
	allSectorIds := make([]uint64, len(sectors))
	for i, sector := range sectors {
		allSectorIds[i] = uint64(sector.SectorNumber)
	}
	return allSectorIds
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
