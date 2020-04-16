package miner

import (
	"testing"

	"github.com/minio/blake2b-simd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/abi"
	tutils "github.com/filecoin-project/specs-actors/support/testing"
)

func TestAssignProvingPeriodBoundary(t *testing.T) {
	addr1 := tutils.NewActorAddr(t, "Bumfuzzle")
	addr2 := tutils.NewActorAddr(t, "Dinglebop")
	startEpoch := abi.ChainEpoch(1)
	hash := func(data []byte) [32]byte {
		hasher, err := blake2b.New(&blake2b.Config{Size: 32})
		require.NoError(t, err)
		_, err = hasher.Write(data)
		require.NoError(t, err)
		digest := hasher.Sum(nil)
		var out [32]byte
		copy(out[:], digest[:32])
		return out
	}

	// ensure the values are different for different addresses
	assignedEpoch1, err := assignProvingPeriodBoundary(addr1, startEpoch, hash)
	assert.NoError(t, err)
	assert.True(t, assignedEpoch1 < WPoStProvingPeriod)

	assignedEpoch2, err := assignProvingPeriodBoundary(addr2, startEpoch, hash)
	assert.NoError(t, err)
	assert.True(t, assignedEpoch2 < WPoStProvingPeriod)

	assert.NotEqual(t, assignedEpoch1, assignedEpoch2)

	// ensure we (probably) don't return an epoch larger than WPoStProvingPeriod
	for i := 0; i < 10_000; i++ {
		assignedEpoch, err := assignProvingPeriodBoundary(addr1, abi.ChainEpoch(i), hash)
		assert.NoError(t, err)
		assert.True(t, assignedEpoch < WPoStProvingPeriod)

	}
}

// NB: WPoStChallengeWindow = 72
func TestComputeCurrentDeadline(t *testing.T) {
	// Every epoch is during some deadline's challenge window.
	t.Run("simple case", func(t *testing.T) {
		ppperiodStart := abi.ChainEpoch(160)
		currentEpoch := abi.ChainEpoch(160)

		actualDeadlineIdx, actualChallengeEpoch := ComputeCurrentDeadline(ppperiodStart, currentEpoch)

		expectedDeadlineIdx := uint64(0)
		expectedChallengeEpoch := abi.ChainEpoch(160)
		assert.Equal(t, expectedDeadlineIdx, actualDeadlineIdx, "Expected: %v, Actual: %v", expectedDeadlineIdx, actualDeadlineIdx)
		assert.Equal(t, expectedChallengeEpoch, actualChallengeEpoch, "Expected: %v, Actual: %v", expectedChallengeEpoch, actualChallengeEpoch)
	})

	t.Run("Middle Point", func(t *testing.T) {
		ppperiodStart := abi.ChainEpoch(160)
		currentEpoch := abi.ChainEpoch(1888)

		actualDeadlineIdx, actualChallengeEpoch := ComputeCurrentDeadline(ppperiodStart, currentEpoch)

		expectedDeadlineIdx := uint64(24)
		expectedChallengeEpoch := abi.ChainEpoch(1888)
		assert.Equal(t, expectedDeadlineIdx, actualDeadlineIdx, "Expected: %v, Actual: %v", expectedDeadlineIdx, actualDeadlineIdx)
		assert.Equal(t, expectedChallengeEpoch, actualChallengeEpoch, "Expected: %v, Actual: %v", expectedChallengeEpoch, actualChallengeEpoch)
	})

	t.Run("Last epoch in proving period", func(t *testing.T) {
		ppperiodStart := abi.ChainEpoch(160)
		currentEpoch := abi.ChainEpoch(3615)

		actualDeadlineIdx, actualChallengeEpoch := ComputeCurrentDeadline(ppperiodStart, currentEpoch)

		expectedDeadlineIdx := uint64(47)
		expectedChallengeEpoch := abi.ChainEpoch(3544)
		assert.Equal(t, expectedDeadlineIdx, actualDeadlineIdx, "Expected: %v, Actual: %v", expectedDeadlineIdx, actualDeadlineIdx)
		assert.Equal(t, expectedChallengeEpoch, actualChallengeEpoch, "Expected: %v, Actual: %v", expectedChallengeEpoch, actualChallengeEpoch)
	})

	t.Run("Rollover to new proving period", func(t *testing.T) {
		ppperiodStart := abi.ChainEpoch(160)
		currentEpoch := abi.ChainEpoch(3616)

		actualDeadlineIdx, actualChallengeEpoch := ComputeCurrentDeadline(ppperiodStart, currentEpoch)

		expectedDeadlineIdx := uint64(0)
		expectedChallengeEpoch := abi.ChainEpoch(3616)
		assert.Equal(t, expectedDeadlineIdx, actualDeadlineIdx, "Expected: %v, Actual: %v", expectedDeadlineIdx, actualDeadlineIdx)
		assert.Equal(t, expectedChallengeEpoch, actualChallengeEpoch, "Expected: %v, Actual: %v", expectedChallengeEpoch, actualChallengeEpoch)
	})

	t.Run("Negative challenge epoch", func(t *testing.T) {
		ppperiodStart := abi.ChainEpoch(160)
		currentEpoch := abi.ChainEpoch(15)

		actualDeadlineIdx, actualChallengeEpoch := ComputeCurrentDeadline(ppperiodStart, currentEpoch)

		expectedDeadlineIdx := uint64(45)
		expectedChallengeEpoch := abi.ChainEpoch(-56)
		assert.Equal(t, expectedDeadlineIdx, actualDeadlineIdx, "Expected: %v, Actual: %v", expectedDeadlineIdx, actualDeadlineIdx)
		assert.Equal(t, expectedChallengeEpoch, actualChallengeEpoch, "Expected: %v, Actual: %v", expectedChallengeEpoch, actualChallengeEpoch)
	})

	t.Run("Rollover from negative to positive", func(t *testing.T) {
		ppperiodStart := abi.ChainEpoch(160)
		currentEpoch := abi.ChainEpoch(16)

		actualDeadlineIdx, actualChallengeEpoch := ComputeCurrentDeadline(ppperiodStart, currentEpoch)

		expectedDeadlineIdx := uint64(46)
		expectedChallengeEpoch := abi.ChainEpoch(16)
		assert.Equal(t, expectedDeadlineIdx, actualDeadlineIdx, "Expected: %v, Actual: %v", expectedDeadlineIdx, actualDeadlineIdx)
		assert.Equal(t, expectedChallengeEpoch, actualChallengeEpoch, "Expected: %v, Actual: %v", expectedChallengeEpoch, actualChallengeEpoch)

	})

}
