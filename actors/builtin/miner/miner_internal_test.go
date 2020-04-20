package miner

import (
	"testing"

	"github.com/minio/blake2b-simd"
	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/actors/abi"
	tutils "github.com/filecoin-project/specs-actors/support/testing"
)

func TestAssignProvingPeriodBoundary(t *testing.T) {
	addr1 := tutils.NewActorAddr(t, "a")
	addr2 := tutils.NewActorAddr(t, "b")
	startEpoch := abi.ChainEpoch(1)

	// ensure the values are different for different addresses
	b1, err := assignProvingPeriodBoundary(addr1, startEpoch, blake2b.Sum256)
	assert.NoError(t, err)
	assert.True(t, b1 >= 0)
	assert.True(t, b1 < WPoStProvingPeriod)

	b2, err := assignProvingPeriodBoundary(addr2, startEpoch, blake2b.Sum256)
	assert.NoError(t, err)
	assert.True(t, b2 >= 0)
	assert.True(t, b2 < WPoStProvingPeriod)

	assert.NotEqual(t, b1, b2)

	// Ensure boundaries are always less than a proving period.
	for i := 0; i < 10_000; i++ {
		boundary, err := assignProvingPeriodBoundary(addr1, abi.ChainEpoch(i), blake2b.Sum256)
		assert.NoError(t, err)
		assert.True(t, boundary >= 0)
		assert.True(t, boundary < WPoStProvingPeriod)
	}
}
