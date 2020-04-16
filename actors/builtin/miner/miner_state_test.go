package miner_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
)

func TestProvingPeriodStart(t *testing.T) {
	t.Run("returns 0 when epoch is 0", func(t *testing.T) {
		minerState := miner.State{Info: miner.MinerInfo{ProvingPeriodBoundary: abi.ChainEpoch(0)}}
		currentEpoch := abi.ChainEpoch(0)
		ppStart, started := minerState.DeadlineInfo(currentEpoch)

		// expect a multiple of ProvingPeriodBoundary
		assert.Equal(t, abi.ChainEpoch(0), ppStart)
		assert.True(t, started)
	})

	t.Run("boundary inside first proving period", func(t *testing.T) {
		minerState := miner.State{Info: miner.MinerInfo{ProvingPeriodBoundary: abi.ChainEpoch(7)}}
		ppStart, started := minerState.DeadlineInfo(abi.ChainEpoch(6))
		assert.Equal(t, -(miner.WPoStProvingPeriod-7), ppStart)
		assert.False(t, started)

		ppStart, started = minerState.DeadlineInfo(abi.ChainEpoch(7))
		assert.Equal(t, abi.ChainEpoch(7), ppStart)
		assert.True(t, started)

		ppStart, started = minerState.DeadlineInfo(abi.ChainEpoch(8))
		assert.Equal(t, abi.ChainEpoch(7), ppStart)
		assert.True(t, started)
	})

	t.Run("right on period boundary", func(t *testing.T) {
		minerState := miner.State{Info: miner.MinerInfo{ProvingPeriodBoundary: abi.ChainEpoch(12)}}
		ppStart, started := minerState.DeadlineInfo(miner.WPoStProvingPeriod + 12)

		assert.Equal(t, miner.WPoStProvingPeriod+12, ppStart)
		assert.True(t, started)
	})
}

func TestPartitionsForDeadline(t *testing.T) {
	t.Run("empty deadlines", func(t *testing.T) {
		dl := newDeadlineWithPartitions(t, [miner.WPoStPeriodDeadlines]uint64{})
		firstIndex, sectorCount, err := miner.PartitionsForDeadline(dl, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), firstIndex)
		assert.Equal(t, uint64(0), sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, miner.WPoStPeriodDeadlines-1)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), firstIndex)
		assert.Equal(t, uint64(0), sectorCount)
	})

	t.Run("single sector at first deadline", func(t *testing.T) {
		dl := newDeadlineWithPartitions(t, [miner.WPoStPeriodDeadlines]uint64{1})
		firstIndex, sectorCount, err := miner.PartitionsForDeadline(dl, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), firstIndex)
		assert.Equal(t, uint64(1), sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, 1)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), firstIndex)
		assert.Zero(t, sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, miner.WPoStPeriodDeadlines - 1)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), firstIndex)
		assert.Zero(t, sectorCount)
	})

	t.Run("single sector at non-first deadline", func(t *testing.T) {
		dl := newDeadlineWithPartitions(t, [miner.WPoStPeriodDeadlines]uint64{0, 1})
		firstIndex, sectorCount, err := miner.PartitionsForDeadline(dl, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), firstIndex)
		assert.Equal(t, uint64(0), sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, 1)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), firstIndex)
		assert.Equal(t, uint64(1), sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, 2)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), firstIndex)
		assert.Equal(t, uint64(0), sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, miner.WPoStPeriodDeadlines - 1)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), firstIndex)
		assert.Equal(t, uint64(0), sectorCount)
	})

	t.Run("deadlines with one full partitions", func(t *testing.T) {
		dl := newDeadlineWithPartitions(t, fullSinglePartitionGen)
		firstIndex, sectorCount, err := miner.PartitionsForDeadline(dl, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), firstIndex)
		assert.Equal(t, partSize, sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, 1)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), firstIndex)
		assert.Equal(t, partSize, sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, miner.WPoStPeriodDeadlines - 1)
		require.NoError(t, err)
		assert.Equal(t, uint64(miner.WPoStPeriodDeadlines-1), firstIndex)
		assert.Equal(t, partSize, sectorCount)
	})

	t.Run("partial partitions", func(t *testing.T) {
		dl := newDeadlineWithPartitions(t, [miner.WPoStPeriodDeadlines]uint64{
			0: partSize - 1,
			1: partSize,
			2: partSize - 2,
			3: partSize,
			4: partSize - 3,
			5: partSize,
		})
		firstIndex, sectorCount, err := miner.PartitionsForDeadline(dl, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), firstIndex)
		assert.Equal(t, partSize-1, sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, 1)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), firstIndex)
		assert.Equal(t, partSize, sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, 2)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), firstIndex)
		assert.Equal(t, partSize-2, sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, 5)
		require.NoError(t, err)
		assert.Equal(t, uint64(5), firstIndex)
		assert.Equal(t, partSize, sectorCount)
	})

	t.Run("multiple partitions", func(t *testing.T) {
		dl := newDeadlineWithPartitions(t, [miner.WPoStPeriodDeadlines]uint64{
			0: partSize,       // 1 partition 1 total
			1: partSize * 2,   // 2 partitions 3 total
			2: partSize*4 - 1, // 4 partitions 7 total
			3: partSize * 6,   // 6 partitions 13 total
			4: partSize*8 - 1, // 8 partitions 21 total
			5: partSize * 9,   // 9 partitions 30 total
		})

		firstIndex, sectorCount, err := miner.PartitionsForDeadline(dl, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), firstIndex)
		assert.Equal(t, partSize, sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, 1)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), firstIndex)
		assert.Equal(t, partSize*2, sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, 2)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), firstIndex)
		assert.Equal(t, partSize*4-1, sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, 3)
		require.NoError(t, err)
		assert.Equal(t, uint64(7), firstIndex)
		assert.Equal(t, partSize*6, sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, 4)
		require.NoError(t, err)
		assert.Equal(t, uint64(13), firstIndex)
		assert.Equal(t, partSize*8-1, sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, 5)
		require.NoError(t, err)
		assert.Equal(t, uint64(21), firstIndex)
		assert.Equal(t, partSize*9, sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, miner.WPoStPeriodDeadlines - 1)
		require.NoError(t, err)
		assert.Equal(t, uint64(30), firstIndex)
		assert.Equal(t, uint64(0), sectorCount)
	})
}

//
// Deadlines Utils
//

const partSize = uint64(miner.WPoStPartitionSectors)

var fullSinglePartitionGen = [miner.WPoStPeriodDeadlines]uint64{
	partSize, partSize, partSize, partSize,
	partSize, partSize, partSize, partSize,
	partSize, partSize, partSize, partSize,
	partSize, partSize, partSize, partSize,
	partSize, partSize, partSize, partSize,
	partSize, partSize, partSize, partSize,
	partSize, partSize, partSize, partSize,
	partSize, partSize, partSize, partSize,
	partSize, partSize, partSize, partSize,
	partSize, partSize, partSize, partSize,
	partSize, partSize, partSize, partSize,
	partSize, partSize, partSize, partSize,
}

// accepts an array were the value at each index indicates how many sectors are in the partition of the returned Deadlines
// Example:
// gen := [miner.WPoStPeriodDeadlines]uint64{1, 42, 89, 0} returns a deadline with:
// 1  sectors at deadlineIdx 0
// 42 sectors at deadlineIdx 1
// 89 sectors at deadlineIdx 2
// 0  sectors at deadlineIdx 3-47
func newDeadlineWithPartitions(t *testing.T, gen [miner.WPoStPeriodDeadlines]uint64) *miner.Deadlines {
	// ensure there are no duplicate sectors across partitions
	var sectorIdx uint64
	deadline := miner.ConstructDeadlines()
	for partition, numSectors := range gen {
		var sectors []uint64
		for i := uint64(0); i < numSectors; i++ {
			sectors = append(sectors, sectorIdx)
			sectorIdx++
		}
		require.NoError(t, deadline.AddToDeadline(uint64(partition), sectors...))
	}
	return deadline
}
