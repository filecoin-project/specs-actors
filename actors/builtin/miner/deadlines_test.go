package miner_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
)

func TestProvingPeriodDeadlines(t *testing.T) {
	PP := miner.WPoStProvingPeriod
	CW := miner.WPoStChallengeWindow
	DLS := miner.WPoStPeriodDeadlines

	t.Run("boundary zero", func(t *testing.T) {
		boundary := abi.ChainEpoch(0)

		// First proving period.
		di := assertDeadlineInfo(t, boundary, 0, 0, 0, 0)
		assert.Equal(t, -miner.WPoStChallengeLookback, di.Challenge)
		assert.Equal(t, -miner.FaultDeclarationCutoff, di.FaultCutoff)
		assert.True(t, di.IsOpen())
		assert.True(t, di.FaultCutoffPassed())

		assertDeadlineInfo(t, boundary, 1, 0, 0, 0)
		// Final epoch of deadline 0.
		assertDeadlineInfo(t, boundary, CW-1, 0, 0, 0)
		// First epoch of deadline 1
		assertDeadlineInfo(t, boundary, CW, 0, 1, CW)
		assertDeadlineInfo(t, boundary, CW+1, 0, 1, CW)
		// Final epoch of deadline 1
		assertDeadlineInfo(t, boundary, CW*2-1, 0, 1, CW)
		// First epoch of deadline 2
		assertDeadlineInfo(t, boundary, CW*2, 0, 2, CW*2)

		// Last epoch of last deadline
		assertDeadlineInfo(t, boundary, PP-1, 0, DLS-1, PP-CW)

		// Second proving period
		// First epoch of deadline 0
		di = assertDeadlineInfo(t, boundary, PP, PP, 0, PP)
		assert.Equal(t, PP-miner.WPoStChallengeLookback, di.Challenge)
		assert.Equal(t, PP-miner.FaultDeclarationCutoff, di.FaultCutoff)

		// Final epoch of deadline 0.
		assertDeadlineInfo(t, boundary, PP+CW-1, PP, 0, PP+0)
		// First epoch of deadline 1
		assertDeadlineInfo(t, boundary, PP+CW, PP, 1, PP+CW)
		assertDeadlineInfo(t, boundary, PP+CW+1, PP, 1, PP+CW)
	})

	t.Run("boundary non-zero", func(t *testing.T) {
		boundary := CW*2 + 2 // Arbitrary not aligned with challenge window.
		initialPPStart := boundary - PP
		firstDlIndex := miner.WPoStPeriodDeadlines - uint64(boundary/CW) - 1
		firstDlOpen := initialPPStart + CW*abi.ChainEpoch(firstDlIndex)

		require.True(t, boundary < PP)
		require.True(t, initialPPStart < 0)
		require.True(t, firstDlOpen < 0)

		// Incomplete initial proving period.
		// At epoch zero, the initial deadlines in the period have already passed and we're part way through
		// another one.
		di := assertDeadlineInfo(t, boundary, 0, initialPPStart, firstDlIndex, firstDlOpen)
		assert.Equal(t, firstDlOpen-miner.WPoStChallengeLookback, di.Challenge)
		assert.Equal(t, firstDlOpen-miner.FaultDeclarationCutoff, di.FaultCutoff)
		assert.True(t, di.IsOpen())
		assert.True(t, di.FaultCutoffPassed())

		// Epoch 1
		assertDeadlineInfo(t, boundary, 1, initialPPStart, firstDlIndex, firstDlOpen)

		// Epoch 2 rolls over to third-last challenge window
		assertDeadlineInfo(t, boundary, 2, initialPPStart, firstDlIndex+1, firstDlOpen+CW)
		assertDeadlineInfo(t, boundary, 3, initialPPStart, firstDlIndex+1, firstDlOpen+CW)

		// Last epoch of second-last window.
		assertDeadlineInfo(t, boundary, 2+CW-1, initialPPStart, firstDlIndex+1, firstDlOpen+CW)
		// First epoch of last challenge window.
		assertDeadlineInfo(t, boundary, 2+CW, initialPPStart, firstDlIndex+2, firstDlOpen+CW*2)
		// Last epoch of last challenge window.
		assert.Equal(t, miner.WPoStPeriodDeadlines-1, firstDlIndex+2)
		assertDeadlineInfo(t, boundary, 2+2*CW-1, initialPPStart, firstDlIndex+2, firstDlOpen+CW*2)

		// First epoch of next proving period.
		assertDeadlineInfo(t, boundary, 2+2*CW, initialPPStart+PP, 0, initialPPStart+PP)
		assertDeadlineInfo(t, boundary, 2+2*CW+1, initialPPStart+PP, 0, initialPPStart+PP)
	})
}

func assertDeadlineInfo(t *testing.T, boundary, current, periodStart abi.ChainEpoch, index uint64, deadlineOpen abi.ChainEpoch) *miner.DeadlineInfo {
	expected := makeDeadline(current, periodStart, index, deadlineOpen)
	actual, started := miner.ComputeProvingPeriodDeadline(boundary, current)
	assert.Equal(t, actual.PeriodStart >= 0, started)
	assert.True(t, actual.IsOpen())
	assert.False(t, actual.HasElapsed())
	assert.Equal(t, expected, actual)
	return actual
}

func makeDeadline(currEpoch, periodStart abi.ChainEpoch, deadline uint64, deadlineOpen abi.ChainEpoch) *miner.DeadlineInfo {
	return &miner.DeadlineInfo{
		CurrentEpoch: currEpoch,
		PeriodStart:  periodStart,
		Index:        deadline,
		Open:         deadlineOpen,
		Close:        deadlineOpen + miner.WPoStChallengeWindow,
		Challenge:    deadlineOpen - miner.WPoStChallengeLookback,
		FaultCutoff:  deadlineOpen - miner.FaultDeclarationCutoff,
	}
}

func TestPartitionsForDeadline(t *testing.T) {
	t.Run("empty deadlines", func(t *testing.T) {
		dl := deadlineWithSectors(t, [miner.WPoStPeriodDeadlines]uint64{})
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
		dl := deadlineWithSectors(t, [miner.WPoStPeriodDeadlines]uint64{1})
		firstIndex, sectorCount, err := miner.PartitionsForDeadline(dl, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), firstIndex)
		assert.Equal(t, uint64(1), sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, 1)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), firstIndex)
		assert.Zero(t, sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, miner.WPoStPeriodDeadlines-1)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), firstIndex)
		assert.Zero(t, sectorCount)
	})

	t.Run("single sector at non-first deadline", func(t *testing.T) {
		dl := deadlineWithSectors(t, [miner.WPoStPeriodDeadlines]uint64{0, 1})
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

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, miner.WPoStPeriodDeadlines-1)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), firstIndex)
		assert.Equal(t, uint64(0), sectorCount)
	})

	t.Run("deadlines with one full partitions", func(t *testing.T) {
		dl := deadlinesWithFullPartitions(t, 1)
		firstIndex, sectorCount, err := miner.PartitionsForDeadline(dl, 0)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), firstIndex)
		assert.Equal(t, partSize, sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, 1)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), firstIndex)
		assert.Equal(t, partSize, sectorCount)

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, miner.WPoStPeriodDeadlines-1)
		require.NoError(t, err)
		assert.Equal(t, miner.WPoStPeriodDeadlines-1, firstIndex)
		assert.Equal(t, partSize, sectorCount)
	})

	t.Run("partial partitions", func(t *testing.T) {
		dl := deadlineWithSectors(t, [miner.WPoStPeriodDeadlines]uint64{
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
		dl := deadlineWithSectors(t, [miner.WPoStPeriodDeadlines]uint64{
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

		firstIndex, sectorCount, err = miner.PartitionsForDeadline(dl, miner.WPoStPeriodDeadlines-1)
		require.NoError(t, err)
		assert.Equal(t, uint64(30), firstIndex)
		assert.Equal(t, uint64(0), sectorCount)
	})
}

//
// Deadlines Utils
//

const partSize = miner.WPoStPartitionSectors

func deadlinesWithFullPartitions(t *testing.T, n uint64) *miner.Deadlines {
	gen := [miner.WPoStPeriodDeadlines]uint64{}
	for i := range gen {
		gen[i] = partSize * n
	}
	return deadlineWithSectors(t, gen)
}

// accepts an array were the value at each index indicates how many sectors are in the partition of the returned Deadlines
// Example:
// gen := [miner.WPoStPeriodDeadlines]uint64{1, 42, 89, 0} returns a deadline with:
// 1  sectors at deadlineIdx 0
// 42 sectors at deadlineIdx 1
// 89 sectors at deadlineIdx 2
// 0  sectors at deadlineIdx 3-47
func deadlineWithSectors(t *testing.T, gen [miner.WPoStPeriodDeadlines]uint64) *miner.Deadlines {
	// ensure there are no duplicate sectors across partitions
	var sectorIdx uint64
	dls := miner.ConstructDeadlines()
	for partition, numSectors := range gen {
		var sectors []uint64
		for i := uint64(0); i < numSectors; i++ {
			sectors = append(sectors, sectorIdx)
			sectorIdx++
		}
		require.NoError(t, dls.AddToDeadline(uint64(partition), sectors...))
	}
	return dls
}
