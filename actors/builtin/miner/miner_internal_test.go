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
