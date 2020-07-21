package miner_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeadlines(t *testing.T) {
	sectors := []*miner.SectorOnChainInfo{
		testSector(2, 1, 50, 60, 1000),
		testSector(3, 2, 51, 61, 1001),
		testSector(7, 3, 52, 62, 1002),
		testSector(8, 4, 53, 63, 1003),

		testSector(11, 5, 54, 64, 1004),
		testSector(13, 6, 55, 65, 1005),
		testSector(8, 7, 56, 66, 1006),
		testSector(8, 8, 57, 67, 1007),

		testSector(8, 9, 58, 68, 1008),
	}

	sectorSize := abi.SectorSize(32 << 30)
	quantSpec := miner.NewQuantSpec(4, 1)
	partitionSize := uint64(4)
	builder := mock.NewBuilder(context.Background(), address.Undef)

	expDlSt := expectedDeadlineState{
		quant:         quantSpec,
		partitionSize: partitionSize,
		sectorSize:    sectorSize,
		sectors:       sectors,
	}

	//
	// Define some basic test scenarios that build one each other.
	//

	// Adds sectors
	//
	// Partition 1: sectors 1, 2, 3, 4
	// Partition 2: sectors 5, 6, 7, 8
	// Partition 3: sectors 9
	addSectors := func(t *testing.T, rt *mock.Runtime, dl *miner.Deadline) {
		store := adt.AsStore(rt)
		power, err := dl.AddSectors(store, partitionSize, sectors, sectorSize, quantSpec)
		require.NoError(t, err)

		expectedPower := miner.PowerForSectors(sectorSize, sectors)
		assert.True(t, expectedPower.Equals(power))
		expDlSt.withPartitions(
			bf(1, 2, 3, 4),
			bf(5, 6, 7, 8),
			bf(9),
		).assert(t, rt, dl)
	}

	// Adds sectors according to addSectors, then terminates them:
	//
	// From partition 0: sectors 1 & 3
	// From partition 1: sectors 6
	addThenTerminate := func(t *testing.T, rt *mock.Runtime, dl *miner.Deadline) {
		addSectors(t, rt, dl)

		store := adt.AsStore(rt)
		removedPower, err := dl.TerminateSectors(store, 15, map[uint64][]*miner.SectorOnChainInfo{
			0: selectSectors(t, sectors, bf(1, 3)),
			1: selectSectors(t, sectors, bf(6)),
		}, sectorSize, quantSpec)
		require.NoError(t, err)

		expectedPower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, bf(1, 3, 6)))
		require.True(t, expectedPower.Equals(removedPower), "expDlSt to remove power for terminated sectors")

		expDlSt.withTerminations(1, 3, 6).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, rt, dl)
	}

	// Adds and terminates sectors according to the previous two functions,
	// then pops early terminations.
	addThenTerminateThenPopEarly := func(t *testing.T, rt *mock.Runtime, dl *miner.Deadline) {
		addThenTerminate(t, rt, dl)

		store := adt.AsStore(rt)
		earlyTerminations, more, err := dl.PopEarlyTerminations(store, 100, 100)
		require.NoError(t, err)
		assert.False(t, more)
		assert.Equal(t, uint64(2), earlyTerminations.PartitionsProcessed)
		assert.Equal(t, uint64(3), earlyTerminations.SectorsProcessed)
		assert.Len(t, earlyTerminations.Sectors, 1)
		assertBitfieldEquals(t, earlyTerminations.Sectors[15], 1, 3, 6)

		// Popping early terminations doesn't affect the terminations bitfield.
		expDlSt.withTerminations(1, 3, 6).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, rt, dl)
	}

	// Runs the above scenarios, then removes partition 0.
	addThenTerminateThenRemovePartition := func(t *testing.T, rt *mock.Runtime, dl *miner.Deadline) {
		addThenTerminateThenPopEarly(t, rt, dl)

		store := adt.AsStore(rt)
		live, dead, removedPower, err := dl.RemovePartitions(store, bf(0), quantSpec)
		require.NoError(t, err, "should have removed partitions")
		assertBitfieldEquals(t, live, 2, 4)
		assertBitfieldEquals(t, dead, 1, 3)
		livePower := miner.PowerForSectors(sectorSize, selectSectors(t, sectors, live))
		require.True(t, livePower.Equals(removedPower))

		expDlSt.withTerminations(6).
			withPartitions(
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, rt, dl)
	}

	// Test the basic scenarios (technically, we could just run the final one).

	t.Run("adds sectors", func(t *testing.T) {
		rt := builder.Build(t)
		dl := emptyDeadline(t, rt)
		addSectors(t, rt, dl)
	})

	t.Run("terminates sectors", func(t *testing.T) {
		rt := builder.Build(t)
		dl := emptyDeadline(t, rt)
		addThenTerminate(t, rt, dl)
	})

	t.Run("pops early terminations", func(t *testing.T) {
		rt := builder.Build(t)
		dl := emptyDeadline(t, rt)

		addThenTerminateThenPopEarly(t, rt, dl)
	})

	t.Run("removes partitions", func(t *testing.T) {
		rt := builder.Build(t)
		dl := emptyDeadline(t, rt)

		addThenTerminateThenRemovePartition(t, rt, dl)
	})

	//
	// Now, build on these basic scenarios with some "what ifs".
	//

	t.Run("cannot remove partitions with early terminations", func(t *testing.T) {
		rt := builder.Build(t)
		dl := emptyDeadline(t, rt)
		addThenTerminate(t, rt, dl)

		store := adt.AsStore(rt)
		_, _, _, err := dl.RemovePartitions(store, bf(0), quantSpec)
		require.Error(t, err, "should have failed to remove a partition with early terminations")
	})

	t.Run("cannot remove missing partition", func(t *testing.T) {
		rt := builder.Build(t)
		dl := emptyDeadline(t, rt)

		addThenTerminateThenRemovePartition(t, rt, dl)

		store := adt.AsStore(rt)
		_, _, _, err := dl.RemovePartitions(store, bf(2), quantSpec)
		require.Error(t, err, "should have failed to remove missing partition")
	})

	t.Run("removing no partitions does nothing", func(t *testing.T) {
		rt := builder.Build(t)
		dl := emptyDeadline(t, rt)

		addThenTerminateThenPopEarly(t, rt, dl)

		store := adt.AsStore(rt)
		live, dead, removedPower, err := dl.RemovePartitions(store, bf(), quantSpec)
		require.NoError(t, err, "should not have failed to remove no partitions")
		require.True(t, removedPower.IsZero())
		assertBitfieldEquals(t, live)
		assertBitfieldEquals(t, dead)

		// Popping early terminations doesn't affect the terminations bitfield.
		expDlSt.withTerminations(1, 3, 6).
			withPartitions(
				bf(1, 2, 3, 4),
				bf(5, 6, 7, 8),
				bf(9),
			).assert(t, rt, dl)
	})

	t.Run("fails to remove partitions with faulty sectors", func(t *testing.T) {
		rt := builder.Build(t)

		dl := emptyDeadline(t, rt)

		addSectors(t, rt, dl)

		store := adt.AsStore(rt)

		// Mark faulty.
		{

			partitions, err := dl.PartitionsArray(store)
			require.NoError(t, err)

			var part miner.Partition

			// mark partition 3 faulty
			{
				found, err := partitions.Get(2, &part)
				require.NoError(t, err)
				require.True(t, found)

				_, _, err = part.RecordMissedPost(store, 17, quantSpec)
				require.NoError(t, err)

				err = partitions.Set(2, &part)
				require.NoError(t, err)

				err = dl.AddExpirationPartitions(store, 17, []uint64{2}, quantSpec)
				require.NoError(t, err)
			}

			dl.Partitions, err = partitions.Root()
			require.NoError(t, err)

			expDlSt.withFaults(9).
				withPartitions(
					bf(1, 2, 3, 4),
					bf(5, 6, 7, 8),
					bf(9),
				).assert(t, rt, dl)
		}

		// Try to remove sectors.
		{
			_, _, _, err := dl.RemovePartitions(store, bf(2), quantSpec)
			require.Error(t, err, "should have failed to remove a partition with faults")
		}
	})

}

func emptyDeadline(t *testing.T, rt *mock.Runtime) *miner.Deadline {
	store := adt.AsStore(rt)
	root, err := adt.MakeEmptyArray(store).Root()
	require.NoError(t, err)

	return miner.ConstructDeadline(root)
}

// Helper type for validating deadline state.
//
// All methods take and the state by _value_ so one can (and should) construct a
// sane base-state.
type expectedDeadlineState struct {
	quant         miner.QuantSpec
	sectorSize    abi.SectorSize
	partitionSize uint64
	sectors       []*miner.SectorOnChainInfo

	faults       *bitfield.BitField
	recovering   *bitfield.BitField
	terminations *bitfield.BitField
	posts        *bitfield.BitField

	partitionSectors []*bitfield.BitField
}

func (s expectedDeadlineState) withFaults(faults ...uint64) expectedDeadlineState {
	s.faults = bf(faults...)
	return s
}
func (s expectedDeadlineState) withRecovering(recovering ...uint64) expectedDeadlineState {
	s.recovering = bf(recovering...)
	return s
}
func (s expectedDeadlineState) withTerminations(terminations ...uint64) expectedDeadlineState {
	s.terminations = bf(terminations...)
	return s
}
func (s expectedDeadlineState) withPosts(posts ...uint64) expectedDeadlineState {
	s.posts = bf(posts...)
	return s
}
func (s expectedDeadlineState) withPartitions(partitions ...*bitfield.BitField) expectedDeadlineState {
	s.partitionSectors = partitions
	return s
}

// Assert that the deadline's state correct.
func (s expectedDeadlineState) assert(t *testing.T, rt *mock.Runtime, dl *miner.Deadline) {
	orEmpty := func(bf *bitfield.BitField) *bitfield.BitField {
		if bf == nil {
			bf = bitfield.NewFromSet(nil)
		}
		return bf
	}

	faults := orEmpty(s.faults)
	recovering := orEmpty(s.recovering)
	terminations := orEmpty(s.terminations)
	posts := orEmpty(s.posts)

	store := adt.AsStore(rt)
	partitions, err := dl.PartitionsArray(store)
	require.NoError(t, err)

	require.Equal(t, uint64(len(s.partitionSectors)), partitions.Length())

	expectedDeadlineExpQueue := make(map[abi.ChainEpoch][]uint64)
	var partitionsWithEarlyTerminations []uint64

	expectPartIndex := int64(0)
	var partition miner.Partition
	err = partitions.ForEach(&partition, func(partIdx int64) error {
		require.Equal(t, expectPartIndex, partIdx)
		expectPartIndex++

		partSectorNos := s.partitionSectors[partIdx]

		partFaults, err := bitfield.IntersectBitField(faults, partSectorNos)
		require.NoError(t, err)

		partRecovering, err := bitfield.IntersectBitField(recovering, partSectorNos)
		require.NoError(t, err)

		partTerminations, err := bitfield.IntersectBitField(terminations, partSectorNos)
		require.NoError(t, err)

		assertPartitionState(t,
			store,
			&partition, s.quant, s.sectorSize, s.sectors, partSectorNos, partFaults, partRecovering, partTerminations)

		earlyTerminated, err := adt.AsArray(store, partition.EarlyTerminated)
		require.NoError(t, err)
		if earlyTerminated.Length() > 0 {
			partitionsWithEarlyTerminations = append(partitionsWithEarlyTerminations, uint64(partIdx))
		}

		// The partition's expiration queue is already tested by the
		// partition tests.
		//
		// Here, we're making sure it's consistent with the deadline's queue.
		q, err := adt.AsArray(store, partition.ExpirationsEpochs)
		require.NoError(t, err)
		err = q.ForEach(nil, func(epoch int64) error {
			expectedDeadlineExpQueue[abi.ChainEpoch(epoch)] = append(
				expectedDeadlineExpQueue[abi.ChainEpoch(epoch)],
				uint64(partIdx),
			)
			return nil
		})
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, err)

	allSectors, err := bitfield.MultiMerge(s.partitionSectors...)
	require.NoError(t, err)

	allSectorsCount, err := allSectors.Count()
	require.NoError(t, err)

	deadSectorsCount, err := terminations.Count()
	require.NoError(t, err)

	require.Equal(t, dl.LiveSectors, allSectorsCount-deadSectorsCount)
	require.Equal(t, dl.TotalSectors, allSectorsCount)

	assertBitfieldsEqual(t, dl.PostSubmissions, posts)

	// Validate expiration queue. The deadline expiration queue is a
	// superset of the partition expiration queues because we never remove
	// from it.
	{
		expirationEpochs, err := adt.AsArray(store, dl.ExpirationsEpochs)
		require.NoError(t, err)
		for epoch, partitions := range expectedDeadlineExpQueue {
			var bf abi.BitField
			found, err := expirationEpochs.Get(uint64(epoch), &bf)
			require.NoError(t, err)
			require.True(t, found)
			for _, p := range partitions {
				present, err := bf.IsSet(p)
				require.NoError(t, err)
				assert.True(t, present)
			}
		}
	}

	// Validate early terminations.
	assertBitfieldEquals(t, dl.EarlyTerminations, partitionsWithEarlyTerminations...)
}
