package miner

import (
	"fmt"

	"github.com/filecoin-project/specs-actors/actors/abi"
)

// Deadline calculations with respect to a current epoch.
// "Deadline" refers to the window during which proofs may be submitted.
// Windows are non-overlapping ranges [Open, Close), but the challenge epoch for a window occurs before
// the window opens.
type DeadlineInfo struct {
	CurrentEpoch abi.ChainEpoch // Epoch at which this info was calculated.
	PeriodStart  abi.ChainEpoch // First epoch of the proving period (<= CurrentEpoch).
	Index        uint64         // Current deadline index, in [0..WPoStProvingPeriodDeadlines), or WPoStProvingPeriodDeadlines if period elapsed.
	Open         abi.ChainEpoch // First epoch from which a proof may be submitted, inclusive (>= CurrentEpoch).
	Close        abi.ChainEpoch // First epoch from which a proof may no longer be submitted, exclusive (>= Open).
	Challenge    abi.ChainEpoch // Epoch at which to sample the chain for challenge (< Open).
	FaultCutoff  abi.ChainEpoch // First epoch at which a fault declaration is rejected (< Open).
}

// Whether the proving period has begun.
func (d *DeadlineInfo) PeriodStarted() bool {
	return d.CurrentEpoch >= d.PeriodStart
}

// Whether the proving period has elapsed.
func (d *DeadlineInfo) PeriodElapsed() bool {
	return d.CurrentEpoch >= d.NextPeriodStart()
}

// Whether the current deadline is currently open.
func (d *DeadlineInfo) IsOpen() bool {
	return d.CurrentEpoch >= d.Open && d.CurrentEpoch < d.Close
}

// Whether the current deadline has already closed.
func (d *DeadlineInfo) HasElapsed() bool {
	return d.CurrentEpoch >= d.Close
}

// Whether the deadline's fault cutoff has passed.
func (d *DeadlineInfo) FaultCutoffPassed() bool {
	return d.CurrentEpoch >= d.FaultCutoff
}

// The last epoch in the proving period.
func (d *DeadlineInfo) PeriodEnd() abi.ChainEpoch {
	return d.PeriodStart + WPoStProvingPeriod - 1
}

// The first epoch in the next proving period.
func (d *DeadlineInfo) NextPeriodStart() abi.ChainEpoch {
	return d.PeriodStart + WPoStProvingPeriod
}

// Returns deadline-related calculations for a proving period start and current epoch.
func ComputeProvingPeriodDeadline(periodStart, currEpoch abi.ChainEpoch) *DeadlineInfo {
	periodProgress := currEpoch - periodStart
	if periodProgress >= WPoStProvingPeriod {
		// Proving period has completely elapsed.
		// Return deadline info for a no-duration deadline immediately after the last real one.
		afterLastDeadline := periodStart + WPoStProvingPeriod
		return &DeadlineInfo{
			CurrentEpoch: currEpoch,
			PeriodStart:  periodStart,
			Index:        WPoStPeriodDeadlines,
			Open:         afterLastDeadline,
			Close:        afterLastDeadline,
			Challenge:    afterLastDeadline,
			FaultCutoff:  0,
		}
	}

	deadlineIdx := uint64(periodProgress / WPoStChallengeWindow)
	if periodProgress < 0 { // Period not yet started.
		deadlineIdx = 0
	}
	deadlineOpen := periodStart + (abi.ChainEpoch(deadlineIdx) * WPoStChallengeWindow)

	return &DeadlineInfo{
		CurrentEpoch: currEpoch,
		PeriodStart:  periodStart,
		Index:        deadlineIdx,
		Open:         deadlineOpen,
		Close:        deadlineOpen + WPoStChallengeWindow,
		Challenge:    deadlineOpen - WPoStChallengeLookback,
		FaultCutoff:  deadlineOpen - FaultDeclarationCutoff,
	}
}

// Computes the first partition index and number of sectors for a deadline.
// Partitions are numbered globally for the miner, not per-deadline.
// If the deadline has no sectors, the first partition index is the index that a partition at that deadline would
// have, if non-empty (and sectorCount is zero).
func PartitionsForDeadline(d *Deadlines, partitionSize, deadlineIdx uint64) (firstPartition, sectorCount uint64, _ error) {
	if deadlineIdx >= WPoStPeriodDeadlines {
		return 0, 0, fmt.Errorf("invalid deadline index %d for %d deadlines", deadlineIdx, WPoStPeriodDeadlines)
	}
	var partitionCountSoFar uint64
	for i := uint64(0); i < WPoStPeriodDeadlines; i++ {
		partitionCount, thisSectorCount, err := DeadlineCount(d, partitionSize, i)
		if err != nil {
			return 0, 0, err
		}
		if i == deadlineIdx {
			return partitionCountSoFar, thisSectorCount, nil
		}
		partitionCountSoFar += partitionCount
	}
	return 0, 0, nil
}

// Counts the partitions (including up to one partial) and sectors at a deadline.
func DeadlineCount(d *Deadlines, partitionSize, deadlineIdx uint64) (partitionCount, sectorCount uint64, err error) {
	if deadlineIdx >= WPoStPeriodDeadlines {
		return 0, 0, fmt.Errorf("invalid deadline index %d for %d deadlines", deadlineIdx, WPoStPeriodDeadlines)
	}
	sectorCount, err = d.Due[deadlineIdx].Count()
	if err != nil {
		return 0, 0, err
	}

	partitionCount = sectorCount / partitionSize
	if sectorCount%partitionSize != 0 {
		partitionCount++
	}
	return
}

// Computes a bitfield of the sector numbers included in a sequence of partitions due at some deadline.
// Fails if any partition is not due at the provided deadline.
func ComputePartitionsSectors(d *Deadlines, partitionSize uint64, deadlineIndex uint64, partitions []uint64) ([]*abi.BitField, error) {
	deadlineFirstPartition, deadlineSectorCount, err := PartitionsForDeadline(d, partitionSize, deadlineIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to count partitions for deadline %d: %w", deadlineIndex, err)
	}
	// ceil(deadlineSectorCount / partitionSize)
	deadlinePartitionCount := (deadlineSectorCount + partitionSize - 1) / partitionSize

	// Work out which sector numbers the partitions correspond to.
	deadlineSectors := d.Due[deadlineIndex]
	var partitionsSectors []*abi.BitField
	for _, pIdx := range partitions {
		if pIdx < deadlineFirstPartition || pIdx >= deadlineFirstPartition+deadlinePartitionCount {
			return nil, fmt.Errorf("invalid partition %d at deadline %d with first %d, count %d",
				pIdx, deadlineIndex, deadlineFirstPartition, deadlinePartitionCount)
		}

		// Slice out the sectors corresponding to this partition from the deadline's sector bitfield.
		sectorOffset := (pIdx - deadlineFirstPartition) * partitionSize
		sectorCount := min64(partitionSize, deadlineSectorCount-sectorOffset)
		partitionSectors, err := deadlineSectors.Slice(sectorOffset, sectorCount)
		if err != nil {
			return nil, fmt.Errorf("failed to slice deadline %d, size %d, offset %d, count %d",
				deadlineIndex, deadlineSectorCount, sectorOffset, sectorCount)
		}
		partitionsSectors = append(partitionsSectors, partitionSectors)
	}
	return partitionsSectors, nil
}

// Assigns a sequence of sector numbers to deadlines by:
// - filling any non-full partitions, in round-robin order across the deadlines
// - repeatedly adding a new partition to the deadline with the fewest partitions
// When multiple partitions share the minimal sector count, one is chosen at random (from a seed).
func AssignNewSectors(deadlines *Deadlines, partitionSize uint64, newSectors []uint64, seed abi.Randomness) error {
	nextNewSector := uint64(0)

	// Assigns up to `count` sectors to `deadline` and advances `nextNewSector`.
	assignToDeadline := func(count uint64, deadline uint64) error {
		countToAdd := min64(count, uint64(len(newSectors))-nextNewSector)
		sectorsToAdd := newSectors[nextNewSector : nextNewSector+countToAdd]
		err := deadlines.AddToDeadline(deadline, sectorsToAdd...)
		if err != nil {
			return fmt.Errorf("failed to add %d sectors to deadline %d: %w", countToAdd, deadline, err)
		}
		nextNewSector += countToAdd
		return nil
	}

	// Iterate deadlines and fill any partial partitions. There's no great advantage to filling more- or less-
	// full ones first, so they're filled in sequence order.
	// Meanwhile, record the partition count at each deadline.
	deadlinePartitionCounts := make([]uint64, WPoStPeriodDeadlines)
	for i := uint64(0); i < WPoStPeriodDeadlines && nextNewSector < uint64(len(newSectors)); i++ {
		partitionCount, sectorCount, err := DeadlineCount(deadlines, partitionSize, i)
		if err != nil {
			return fmt.Errorf("failed to count sectors in partition %d: %w", i, err)
		}
		deadlinePartitionCounts[i] = partitionCount

		gap := partitionSize - (sectorCount % partitionSize)
		if gap != partitionSize {
			err = assignToDeadline(gap, i)
			if err != nil {
				return err
			}
		}
	}

	// While there remain new sectors to assign, fill a new partition at each deadline in round-robin fashion.
	// TODO WPOST (follow-up): fill less-full deadlines first, randomize when equally full.
	targetDeadline := uint64(0)
	for nextNewSector < uint64(len(newSectors)) {
		err := assignToDeadline(partitionSize, targetDeadline)
		if err != nil {
			return err
		}
		targetDeadline = (targetDeadline + 1) % WPoStPeriodDeadlines
	}
	return nil
}
