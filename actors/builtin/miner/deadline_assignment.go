package miner

import (
	"container/heap"
)

// Helper types for deadline assignment.
type deadlineAssignmentInfo struct {
	index       int
	liveSectors uint64
}

func (dai *deadlineAssignmentInfo) isFull(partitionSize int) bool {
	return (dai.liveSectors % uint64(partitionSize)) == 0
}

type deadlineAssignmentHeap struct {
	partitionSize int
	deadlines     []*deadlineAssignmentInfo
}

func (dah *deadlineAssignmentHeap) Len() int {
	return len(dah.deadlines)
}

func (dah *deadlineAssignmentHeap) Swap(i, j int) {
	dah.deadlines[i], dah.deadlines[j] = dah.deadlines[j], dah.deadlines[i]
}

func (dah *deadlineAssignmentHeap) Less(i, j int) bool {
	a, b := dah.deadlines[i], dah.deadlines[j]

	aIsFull := a.isFull(dah.partitionSize)
	bIsFull := b.isFull(dah.partitionSize)

	// Sort by fullness first.
	if !aIsFull && bIsFull {
		return true
	} else if aIsFull && !bIsFull {
		return false
	}

	// Then by total live sectors.
	if a.liveSectors < b.liveSectors {
		return true
	} else if a.liveSectors > b.liveSectors {
		return false
	}

	// TODO: Randomize by index instead of simply sorting.
	// https://github.com/filecoin-project/specs-actors/issues/432
	return a.index < b.index
}

func (dah *deadlineAssignmentHeap) Push(x interface{}) {
	dah.deadlines = append(dah.deadlines, x.(*deadlineAssignmentInfo))
}

func (dah *deadlineAssignmentHeap) Pop() interface{} {
	last := dah.deadlines[len(dah.deadlines)-1]
	dah.deadlines[len(dah.deadlines)-1] = nil
	dah.deadlines = dah.deadlines[:len(dah.deadlines)-1]
	return last
}

// Assigns partitions to deadlines, first filling partial partitions, then
// adding new partitions to deadlines with the fewest live sectors.
func assignDeadlines(
	partitionSize uint64,
	deadlines *[WPoStPeriodDeadlines]*Deadline,
	sectors []*SectorOnChainInfo,
) (changes [WPoStPeriodDeadlines][]*SectorOnChainInfo) {
	// Build a heap
	dlHeap := deadlineAssignmentHeap{
		partitionSize: int(partitionSize),
		deadlines:     make([]*deadlineAssignmentInfo, 0, len(deadlines)),
	}

	for dlIdx, dl := range deadlines {
		if dl != nil {
			dlHeap.deadlines = append(dlHeap.deadlines, &deadlineAssignmentInfo{
				index:       dlIdx,
				liveSectors: dl.LiveSectors,
			})
		}
	}

	heap.Init(&dlHeap)

	// Assign sectors to deadlines.
	for len(sectors) > 0 {
		info := dlHeap.deadlines[0]

		// Fill up any partial sectors first.
		size := int((partitionSize - info.liveSectors%partitionSize))
		if size == 0 {
			size = int(partitionSize)
		}
		if size > len(sectors) {
			size = len(sectors)
		}
		changes[info.index] = append(changes[info.index], sectors[:size]...)
		sectors = sectors[size:]

		info.liveSectors += uint64(size)
		info.liveSectors += uint64(size)
		heap.Fix(&dlHeap, 0)
	}
	return changes
}
