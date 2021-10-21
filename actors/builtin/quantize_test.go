package builtin_test

import (
	"testing"

	. "github.com/filecoin-project/specs-actors/v7/actors/builtin"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/stretchr/testify/assert"
)

func TestQuantizeUp(t *testing.T) {
	t.Run("no quantization", func(t *testing.T) {
		q := NoQuantization
		assert.Equal(t, abi.ChainEpoch(0), q.QuantizeUp(0))
		assert.Equal(t, abi.ChainEpoch(1), q.QuantizeUp(1))
		assert.Equal(t, abi.ChainEpoch(2), q.QuantizeUp(2))
		assert.Equal(t, abi.ChainEpoch(123456789), q.QuantizeUp(123456789))
	})
	t.Run("zero offset", func(t *testing.T) {
		assert.Equal(t, abi.ChainEpoch(50), QuantizeUp(42, 10, 0))
		assert.Equal(t, abi.ChainEpoch(16000), QuantizeUp(16000, 100, 0))
		assert.Equal(t, abi.ChainEpoch(0), QuantizeUp(-5, 10, 0))
		assert.Equal(t, abi.ChainEpoch(-50), QuantizeUp(-50, 10, 0))
		assert.Equal(t, abi.ChainEpoch(-50), QuantizeUp(-53, 10, 0))
	})

	t.Run("non zero offset", func(t *testing.T) {
		assert.Equal(t, abi.ChainEpoch(6), QuantizeUp(4, 5, 1))
		assert.Equal(t, abi.ChainEpoch(1), QuantizeUp(0, 5, 1))
		assert.Equal(t, abi.ChainEpoch(-4), QuantizeUp(-6, 5, 1))
		assert.Equal(t, abi.ChainEpoch(4), QuantizeUp(2, 10, 4))
	})

	t.Run("offset seed bigger than unit is normalized", func(t *testing.T) {
		assert.Equal(t, abi.ChainEpoch(13), QuantizeUp(9, 5, 28)) // offset should be 3
		assert.Equal(t, abi.ChainEpoch(10000), QuantizeUp(10000, 100, 2000000))
	})
}

func TestQuantizeDown(t *testing.T) {
	t.Run("no quantization", func(t *testing.T) {
		q := NoQuantization
		assert.Equal(t, abi.ChainEpoch(0), q.QuantizeDown(0))
		assert.Equal(t, abi.ChainEpoch(1), q.QuantizeDown(1))
		assert.Equal(t, abi.ChainEpoch(1337), q.QuantizeDown(1337))
	})
	t.Run("zero offset", func(t *testing.T) {
		q := NewQuantSpec(abi.ChainEpoch(10), abi.ChainEpoch(0))
		assert.Equal(t, abi.ChainEpoch(6660), q.QuantizeDown(6666))
		assert.Equal(t, abi.ChainEpoch(50), q.QuantizeDown(50))
		assert.Equal(t, abi.ChainEpoch(50), q.QuantizeDown(59))
	})

	t.Run("non zero offset", func(t *testing.T) {
		q := NewQuantSpec(abi.ChainEpoch(10), abi.ChainEpoch(1))
		assert.Equal(t, abi.ChainEpoch(11), q.QuantizeDown(20))
		assert.Equal(t, abi.ChainEpoch(11), q.QuantizeDown(11))
	})
}
