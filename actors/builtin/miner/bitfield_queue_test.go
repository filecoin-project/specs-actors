package miner

import (
	"context"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBitfieldQueue(t *testing.T) {
	t.Run("adds values to empty queue", func(t *testing.T) {
		queue := emptyBitfieldQueue(t)

		values := []uint64{1, 2, 3, 4}
		epoch := abi.ChainEpoch(42)
		queue.AddToQueueValues(epoch, values...)

		ExpectBQ().
			Add(epoch, values...).
			Equals(t, queue)
	})

	t.Run("adds bitfield to empty queue", func(t *testing.T) {
		queue := emptyBitfieldQueue(t)

		values := []uint64{1, 2, 3, 4}
		epoch := abi.ChainEpoch(42)

		queue.AddToQueue(epoch, bitfield.NewFromSet(values))

		ExpectBQ().
			Add(epoch, values...).
			Equals(t, queue)
	})

	t.Run("merges values withing same epoch", func(t *testing.T) {
		queue := emptyBitfieldQueue(t)

		epoch := abi.ChainEpoch(42)

		queue.AddToQueueValues(epoch, 1, 3)
		queue.AddToQueueValues(epoch, 2, 4)

		ExpectBQ().
			Add(epoch, 1, 2, 3, 4).
			Equals(t, queue)
	})

	t.Run("adds values to different epochs", func(t *testing.T) {
		queue := emptyBitfieldQueue(t)

		epoch1 := abi.ChainEpoch(42)
		epoch2 := abi.ChainEpoch(93)

		queue.AddToQueueValues(epoch1, 1, 3)
		queue.AddToQueueValues(epoch2, 2, 4)

		ExpectBQ().
			Add(epoch1, 1, 3).
			Add(epoch2, 2, 4).
			Equals(t, queue)
	})

	t.Run("PouUntil from empty queue returns empty bitfield", func(t *testing.T) {
		queue := emptyBitfieldQueue(t)

		// TODO: broken pending https://github.com/filecoin-project/go-amt-ipld/issues/18
		//emptyQueue, err := queue.Root()
		//require.NoError(t, err)

		next, err := queue.PopUntil(42)
		require.NoError(t, err)

		// no values are returned
		count, err := next.Count()
		require.NoError(t, err)
		assert.Equal(t, 0, int(count))

		// queue is still empty
		//root, err := queue.Root()
		//assert.Equal(t, emptyQueue, root)
	})

	t.Run("PopUntil does nothing if 'until' parameter before first value", func(t *testing.T) {
		queue := emptyBitfieldQueue(t)

		epoch1 := abi.ChainEpoch(42)
		epoch2 := abi.ChainEpoch(93)

		queue.AddToQueueValues(epoch1, 1, 3)
		queue.AddToQueueValues(epoch2, 2, 4)

		next, err := queue.PopUntil(epoch1 - 1)
		require.NoError(t, err)

		// no values are returned
		count, err := next.Count()
		require.NoError(t, err)
		assert.Equal(t, 0, int(count))

		// queue remains the same
		ExpectBQ().
			Add(epoch1, 1, 3).
			Add(epoch2, 2, 4).
			Equals(t, queue)
	})

	t.Run("PopUntil removes and returns entries before and including target epoch", func(t *testing.T) {
		queue := emptyBitfieldQueue(t)

		epoch1 := abi.ChainEpoch(42)
		epoch2 := abi.ChainEpoch(93)
		epoch3 := abi.ChainEpoch(182)
		epoch4 := abi.ChainEpoch(203)

		queue.AddToQueueValues(epoch1, 1, 3)
		queue.AddToQueueValues(epoch2, 5)
		queue.AddToQueueValues(epoch3, 6, 7, 8)
		queue.AddToQueueValues(epoch4, 2, 4)

		// required to make for each work
		_, err := queue.Root()
		require.NoError(t, err)

		next, err := queue.PopUntil(epoch2)
		require.NoError(t, err)

		// values from first two epochs are returned
		assertBitfieldEquals(t, next, 1, 3, 5)

		// queue only contains remaining values
		ExpectBQ().
			Add(epoch3, 6, 7, 8).
			Add(epoch4, 2, 4).
			Equals(t, queue)

		// subsequent call to epoch less than next does nothing.
		next, err = queue.PopUntil(epoch3 - 1)
		require.NoError(t, err)

		// no values are returned
		assertBitfieldEquals(t, next, []uint64{}...)

		// queue only contains remaining values
		ExpectBQ().
			Add(epoch3, 6, 7, 8).
			Add(epoch4, 2, 4).
			Equals(t, queue)
	})
}

func emptyBitfieldQueue(t *testing.T) BitfieldQueue {
	rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
	store := adt.AsStore(rt)
	root, err := adt.MakeEmptyArray(store).Root()
	require.NoError(t, err)

	queue, err := LoadBitfieldQueue(store, root, NoQuantization)
	require.NoError(t, err)
	return queue
}

type bqExpectation struct {
	expected map[abi.ChainEpoch][]uint64
}

func ExpectBQ() *bqExpectation {
	return &bqExpectation{expected: make(map[abi.ChainEpoch][]uint64)}
}

func (bqe *bqExpectation) Add(epoch abi.ChainEpoch, values ...uint64) *bqExpectation {
	bqe.expected[epoch] = values
	return bqe
}

func (bqe *bqExpectation) Equals(t *testing.T, q BitfieldQueue) {
	require.Equal(t, uint64(len(bqe.expected)), q.Length())

	q.ForEach(func(epoch abi.ChainEpoch, bf *bitfield.BitField) error {
		values, ok := bqe.expected[epoch]
		require.True(t, ok)

		assertBitfieldEquals(t, bf, values...)
		return nil
	})
}

func assertBitfieldEquals(t *testing.T, bf *bitfield.BitField, values ...uint64) {
	count, err := bf.Count()
	require.NoError(t, err)
	require.Equal(t, len(values), int(count))

	bf.ForEach(func(v uint64) error {
		assert.Equal(t, values[0], v)
		values = values[1:]
		return nil
	})
}
