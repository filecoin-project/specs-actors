package miner

import (
	"errors"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

// Deadline calculations with respect to a current epoch.
// "Deadline" refers to the window during which proofs may be submitted.
// Windows are non-overlapping ranges [Open, Close), but the challenge epoch for a window occurs before
// the window opens.
// The current epoch may not necessarily lie within the deadline or proving period represented here.
type DeadlineInfo struct {
	CurrentEpoch abi.ChainEpoch // Epoch at which this info was calculated.
	PeriodStart  abi.ChainEpoch // First epoch of the proving period (<= CurrentEpoch).
	Index        uint64         // A deadline index, in [0..WPoStProvingPeriodDeadlines) unless period elapsed.
	Open         abi.ChainEpoch // First epoch from which a proof may be submitted (>= CurrentEpoch).
	Close        abi.ChainEpoch // First epoch from which a proof may no longer be submitted (>= Open).
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

// The last epoch in the proving period.
func (d *DeadlineInfo) PeriodEnd() abi.ChainEpoch {
	return d.PeriodStart + WPoStProvingPeriod - 1
}

// The first epoch in the next proving period.
func (d *DeadlineInfo) NextPeriodStart() abi.ChainEpoch {
	return d.PeriodStart + WPoStProvingPeriod
}

// Whether the current deadline is currently open.
func (d *DeadlineInfo) IsOpen() bool {
	return d.CurrentEpoch >= d.Open && d.CurrentEpoch < d.Close
}

// Whether the current deadline has already closed.
func (d *DeadlineInfo) HasElapsed() bool {
	return d.CurrentEpoch >= d.Close
}

// The last epoch during which a proof may be submitted.
func (d *DeadlineInfo) Last() abi.ChainEpoch {
	return d.Close - 1
}

// Epoch at which the subsequent deadline opens.
func (d *DeadlineInfo) NextOpen() abi.ChainEpoch {
	return d.Close
}

// Whether the deadline's fault cutoff has passed.
func (d *DeadlineInfo) FaultCutoffPassed() bool {
	return d.CurrentEpoch >= d.FaultCutoff
}

// Returns the next instance of this deadline that has not yet elapsed.
func (d *DeadlineInfo) NextNotElapsed() *DeadlineInfo {
	next := d
	for next.HasElapsed() {
		next = NewDeadlineInfo(next.NextPeriodStart(), d.Index, d.CurrentEpoch)
	}
	return next
}

func (d *DeadlineInfo) QuantSpec() QuantSpec {
	return NewQuantSpec(WPoStProvingPeriod, d.Last())
}

// Returns deadline-related calculations for a deadline in some proving period and the current epoch.
func NewDeadlineInfo(periodStart abi.ChainEpoch, deadlineIdx uint64, currEpoch abi.ChainEpoch) *DeadlineInfo {
	if deadlineIdx < WPoStPeriodDeadlines {
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
	} else {
		// Return deadline info for a no-duration deadline immediately after the last real one.
		afterLastDeadline := periodStart + WPoStProvingPeriod
		return &DeadlineInfo{
			CurrentEpoch: currEpoch,
			PeriodStart:  periodStart,
			Index:        deadlineIdx,
			Open:         afterLastDeadline,
			Close:        afterLastDeadline,
			Challenge:    afterLastDeadline,
			FaultCutoff:  0,
		}
	}
}

// FindSector returns the deadline and partition index for a sector number.
// It returns an error if the sector number is not tracked by deadlines.
func FindSector(store adt.Store, deadlines *Deadlines, sectorNum abi.SectorNumber) (uint64, uint64, error) {
	for dlIdx := range deadlines.Due {
		dl, err := deadlines.LoadDeadline(store, uint64(dlIdx))
		if err != nil {
			return 0, 0, err
		}

		partitions, err := adt.AsArray(store, dl.Partitions)
		if err != nil {
			return 0, 0, err
		}
		var partition Partition

		partIdx := uint64(0)
		stopErr := errors.New("stop")
		err = partitions.ForEach(&partition, func(i int64) error {
			found, err := partition.Sectors.IsSet(uint64(sectorNum))
			if err != nil {
				return err
			}
			if found {
				partIdx = uint64(i)
				return stopErr
			}
			return nil
		})
		if err == stopErr {
			return uint64(dlIdx), partIdx, nil
		} else if err != nil {
			return 0, 0, err
		}

	}
	return 0, 0, xerrors.Errorf("sector %d not due at any deadline", sectorNum)
}

// Returns true if the deadline at the given index is currently mutable.
func deadlineIsMutable(provingPeriodStart abi.ChainEpoch, dlIdx uint64, currentEpoch abi.ChainEpoch) bool {
	// Get the next non-elapsed deadline (i.e., the next time we care about
	// mutations to the deadline).
	dlInfo := NewDeadlineInfo(provingPeriodStart, dlIdx, currentEpoch).NextNotElapsed()
	// Ensure that the current epoch is at least one challenge window before
	// that deadline opens.
	return currentEpoch < dlInfo.Open-WPoStChallengeWindow
}
