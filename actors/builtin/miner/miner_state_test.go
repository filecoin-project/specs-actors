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

		assert.Equal(t, abi.ChainEpoch(0), ppStart)
		assert.True(t, started)
	})

	t.Run("returns 0 when epoch is less than proving period", func(t *testing.T) {
		minerState := miner.State{ProvingPeriodBoundary: abi.ChainEpoch(7)}
		currentEpoch := abi.ChainEpoch(6)
		ppStart, started := minerState.ProvingPeriodStart(currentEpoch)

		assert.Equal(t, abi.ChainEpoch(0), ppStart)
		assert.True(t, started)
	})

	t.Run("returns 0 when epoch is factor of proving period", func(t *testing.T) {
		minerState := miner.State{ProvingPeriodBoundary: abi.ChainEpoch(144)}
		currentEpoch := abi.ChainEpoch(12)
		ppStart, started := minerState.ProvingPeriodStart(currentEpoch)

		assert.Equal(t, abi.ChainEpoch(0), ppStart)
		assert.True(t, started)
	})

	t.Run("returns multiple of proving period start when epoch is greater than proving period start", func(t *testing.T) {
		minerState := miner.State{ProvingPeriodBoundary: abi.ChainEpoch(25)}
		currentEpoch := abi.ChainEpoch(72)
		ppStart, started := minerState.ProvingPeriodStart(currentEpoch)

		assert.Equal(t, abi.ChainEpoch(50), ppStart)
		assert.True(t, started)
	})

	// XXX: tests for negative blanace and flase return are impossible given quantize impl
}

func TestComputePartitionsSectors(t *testing.T) {

}

func TestPartitionsForDeadline(t *testing.T) {
	t.Run("deadline with single sector in first partition index", func(t *testing.T) {
		dl := newDeadlineWithPartitions(t, [miner.WPoStPeriodDeadlines]uint64{1})
		firstPart, sectorCount, err := dl.PartitionsForDeadline(0)
		require.NoError(t, err)

		assert.Zero(t, firstPart)
		assert.Equal(t, uint64(1), sectorCount)

		firstPart, sectorCount, err = dl.PartitionsForDeadline(1)
		require.NoError(t, err)

		assert.Zero(t, firstPart)
		assert.Zero(t, sectorCount)
	})

	t.Run("deadline with some partition indexs containing single full partition", func(t *testing.T) {
		dl := newDeadlineWithPartitions(t, [miner.WPoStPeriodDeadlines]uint64{
			0: partSize - 1,
			1: partSize,
			2: partSize - 1,
			3: partSize,
			4: partSize - 1,
			5: partSize,
		})
		firstPart, sectorCount, err := dl.PartitionsForDeadline(5)
		require.NoError(t, err)

		assert.Equal(t, uint64(2), firstPart)
		assert.Equal(t, partSize, sectorCount)
	})

	t.Run("deadline with each partition index containing a single full partition", func(t *testing.T) {
		dl := newDeadlineWithPartitions(t, fullSinglePartitionGen)
		firstPart, sectorCount, err := dl.PartitionsForDeadline(23)
		require.NoError(t, err)

		assert.Equal(t, uint64(23), firstPart)
		assert.Equal(t, partSize, sectorCount)
	})

	t.Run("deadline with multi partition at first partition index", func(t *testing.T) {
		fullSinglePartitionGen[0] *= 2 + 1
		dl := newDeadlineWithPartitions(t, fullSinglePartitionGen)
		firstPart, sectorCount, err := dl.PartitionsForDeadline(23)
		require.NoError(t, err)

		assert.Equal(t, uint64(25), firstPart)
		assert.Equal(t, partSize, sectorCount)

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
