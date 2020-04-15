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
		minerState := miner.State{ProvingPeriodBoundary: abi.ChainEpoch(0)}
		currentEpoch := abi.ChainEpoch(0)
		ppStart, started := minerState.ProvingPeriodStart(currentEpoch)

		// expect a multiple of ProvingPeriodBoundary
		assert.Equal(t, abi.ChainEpoch(0), ppStart)
		assert.True(t, started)
	})

	t.Run("returns 0 when epoch is less than proving period", func(t *testing.T) {
		minerState := miner.State{ProvingPeriodBoundary: abi.ChainEpoch(7)}
		currentEpoch := abi.ChainEpoch(6)
		ppStart, started := minerState.ProvingPeriodStart(currentEpoch)

		// expect a multiple of ProvingPeriodBoundary
		assert.Equal(t, abi.ChainEpoch(0), ppStart)
		assert.True(t, started)
	})

	t.Run("returns multiple of proving period start when epoch is greater than proving period start", func(t *testing.T) {
		minerState := miner.State{ProvingPeriodBoundary: abi.ChainEpoch(25)}
		currentEpoch := abi.ChainEpoch(72)
		ppStart, started := minerState.ProvingPeriodStart(currentEpoch)

		// expect a multiple of ProvingPeriodBoundary
		assert.Equal(t, abi.ChainEpoch(50), ppStart)
		assert.True(t, started)
	})

	t.Run("returns 0 when epoch is factor of proving period", func(t *testing.T) {
		minerState := miner.State{ProvingPeriodBoundary: abi.ChainEpoch(12)}
		currentEpoch := abi.ChainEpoch(144)
		ppStart, started := minerState.ProvingPeriodStart(currentEpoch)

		// expect a multiple of ProvingPeriodBoundary
		assert.Equal(t, abi.ChainEpoch(144), ppStart)
		assert.True(t, started)
	})

}

func TestPartitionsForDeadline(t *testing.T) {
	t.Run("deadline with single sector in first partition index", func(t *testing.T) {
		dl := newDeadlineWithPartitions(t, [miner.WPoStPeriodDeadlines]uint64{1})
		partsForDl, sectorCount, err := dl.PartitionsForDeadline(0)
		require.NoError(t, err)

		assert.Equal(t, uint64(1), partsForDl)
		assert.Equal(t, uint64(1), sectorCount)

		partsForDl, sectorCount, err = dl.PartitionsForDeadline(1)
		require.NoError(t, err)

		assert.Zero(t, partsForDl)
		assert.Zero(t, sectorCount)
	})

	t.Run("deadline with each partition index containing a single full partition", func(t *testing.T) {
		dl := newDeadlineWithPartitions(t, fullSinglePartitionGen)
		partsForDl, sectorCount, err := dl.PartitionsForDeadline(47)
		require.NoError(t, err)

		assert.Equal(t, uint64(47), partsForDl)
		assert.Equal(t, partSize, sectorCount)
	})

	t.Run("deadline with some partition indexs containing full partitions with others containing partial", func(t *testing.T) {
		dl := newDeadlineWithPartitions(t, [miner.WPoStPeriodDeadlines]uint64{
			0: partSize - 1,
			1: partSize,
			2: partSize - 2,
			3: partSize,
			4: partSize - 3,
			5: partSize,
		})
		partsForDl, sectorCount, err := dl.PartitionsForDeadline(5)
		require.NoError(t, err)

		assert.Equal(t, uint64(5), partsForDl)
		assert.Equal(t, partSize, sectorCount)
	})

	t.Run("deadline with multi partition at indexs, last index is partial partition", func(t *testing.T) {
		dl := newDeadlineWithPartitions(t, [miner.WPoStPeriodDeadlines]uint64{
			0: partSize,        // 1 partition 1 total
			1: partSize * 2,    // 2 partitions 3 total
			2: partSize * 4,    // 4 partitions 7 total
			3: partSize * 6,    // 6 partitions 13 total
			4: partSize * 8,    // 8 partitions 21 total
			5: partSize*10 - 1, // 9 partitions 30 total
		})

		// index 0
		partsForDl, sectorCount, err := dl.PartitionsForDeadline(0)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), partsForDl)
		assert.Equal(t, partSize, sectorCount)

		// index 1
		partsForDl, sectorCount, err = dl.PartitionsForDeadline(1)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), partsForDl)
		assert.Equal(t, partSize*2, sectorCount)

		// index 2
		partsForDl, sectorCount, err = dl.PartitionsForDeadline(2)
		require.NoError(t, err)
		assert.Equal(t, uint64(7), partsForDl)
		assert.Equal(t, partSize*4, sectorCount)

		// index 3
		partsForDl, sectorCount, err = dl.PartitionsForDeadline(3)
		require.NoError(t, err)
		assert.Equal(t, uint64(13), partsForDl)
		assert.Equal(t, partSize*6, sectorCount)

		// index 4
		partsForDl, sectorCount, err = dl.PartitionsForDeadline(4)
		require.NoError(t, err)
		assert.Equal(t, uint64(21), partsForDl)
		assert.Equal(t, partSize*8, sectorCount)

		// index 5
		partsForDl, sectorCount, err = dl.PartitionsForDeadline(5)
		require.NoError(t, err)
		assert.Equal(t, uint64(30), partsForDl)
		assert.Equal(t, partSize*10-1, sectorCount)
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
	var sectorIdx abi.SectorNumber
	deadline := new(miner.Deadlines)
	for partition, numSectors := range gen {
		var sectors []abi.SectorNumber
		for i := uint64(0); i < numSectors; i++ {
			sectors = append(sectors, sectorIdx)
			sectorIdx++
		}
		require.NoError(t, deadline.AddToDeadline(partition, sectors...))
	}
	return deadline
}
