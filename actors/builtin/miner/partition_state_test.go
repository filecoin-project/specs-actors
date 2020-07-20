package miner

import (
	"context"
	"github.com/filecoin-project/go-bitfield"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitions(t *testing.T) {
	sectors := []*SectorOnChainInfo{
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

		power, err := partition.AddSectors(adt.AsStore(rt), sectors, sectorSize, QuantSpec{unit: 4, offset: 1})
		require.NoError(t, err)

		expectedPower := PowerForSectors(sectorSize, sectors)
		assert.True(t, expectedPower.Equals(power))

		assert.True(t, partition.FaultyPower.Equals(NewPowerPairZero()))
		assert.True(t, partition.RecoveringPower.Equals(NewPowerPairZero()))
		assert.True(t, partition.LivePower.Equals(expectedPower))
		assert.True(t, expectedPower.Equals(partition.ActivePower()))

		assertBitfieldEmpty(t, partition.Faults)
		assertBitfieldEmpty(t, partition.Recoveries)
		assertBitfieldEmpty(t, partition.Terminated)
		assertBitfieldEquals(t, partition.Sectors, 1, 2, 3, 4, 5, 6)

		active, err := partition.ActiveSectors()
		require.NoError(t, err)
		assertBitfieldEquals(t, active, 1, 2, 3, 4, 5, 6)

		live, err := partition.LiveSectors()
		require.NoError(t, err)
		assertBitfieldEquals(t, live, 1, 2, 3, 4, 5, 6)
	})

	t.Run("adds faults", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		partition := emptyPartition(t, rt)

		quantSpec := QuantSpec{unit: 4, offset: 1}
		_, err := partition.AddSectors(adt.AsStore(rt), sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		faults := bitfield.NewFromSet([]uint64{4, 5})
		_, err = partition.AddFaults(adt.AsStore(rt), faults, selectSectors(t, sectors, faults), abi.ChainEpoch(1000), sectorSize, quantSpec)
		require.NoError(t, err)
	})
}

func selectSectors(t *testing.T, sectors []*SectorOnChainInfo, field *bitfield.BitField) []*SectorOnChainInfo {
	included := []*SectorOnChainInfo{}
	for _, s := range sectors {
		set, err := field.IsSet(uint64(s.SectorNumber))
		require.NoError(t, err)
		if set {
			included = append(included, s)
		}
	}
	return included
}

func emptyPartition(t *testing.T, rt *mock.Runtime) *Partition {
	store := adt.AsStore(rt)
	root, err := adt.MakeEmptyArray(store).Root()
	require.NoError(t, err)

	return ConstructPartition(root)
}
