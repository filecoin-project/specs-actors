package miner

import (
	"fmt"
	"io"

	"github.com/filecoin-project/go-bitfield"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

// ExpirationSet is a collection of sector numbers that are expiring, either due to
// expected "on-time" expiration at the end of their life, or unexpected "early" termination
// due to being faulty for too long consecutively.
// The power for the expiring sectors is divided into active and currently-faulty.
// Note that there is not a direct correspondence between on-time sectors and active power;
// a sector may be faulty but expiring on-time if it faults just prior to expected termination.
// Early sectors are always faulty, and active power always represents on-time sectors.
type ExpirationSet struct {
	OnTimeSectors *abi.BitField
	EarlySectors  *abi.BitField
	ActivePower   PowerPair
	FaultyPower   PowerPair
}

func NewExpirationSetEmpty() *ExpirationSet {
	return NewExpirationSet(abi.NewBitField(), abi.NewBitField(), NewPowerPairZero(), NewPowerPairZero())
}

func NewExpirationSet(onTimeSectors, earlySectors *abi.BitField, activePower, faultyPower PowerPair) *ExpirationSet {
	return &ExpirationSet{
		OnTimeSectors: onTimeSectors,
		EarlySectors:  earlySectors,
		ActivePower:   activePower,
		FaultyPower:   faultyPower,
	}
}

// Adds sectors and power to the expiration set in place.
func (es *ExpirationSet) Add(onTimeSectors, earlySectors *abi.BitField, activePower, faultyPower PowerPair) error {
	var err error
	if es.OnTimeSectors, err = bitfield.MergeBitFields(es.OnTimeSectors, onTimeSectors); err != nil {
		return err
	}
	if es.EarlySectors, err = bitfield.MergeBitFields(es.EarlySectors, earlySectors); err != nil {
		return err
	}
	es.ActivePower = es.ActivePower.Add(activePower)
	es.FaultyPower = es.FaultyPower.Add(faultyPower)
	return nil
}

// Removes sectors and power from the expiration set in place.
func (es *ExpirationSet) Remove(onTimeSectors, earlySectors *abi.BitField, activePower, faultyPower PowerPair) error {
	var err error
	if es.OnTimeSectors, err = bitfield.SubtractBitField(es.OnTimeSectors, onTimeSectors); err != nil {
		return err
	}
	if es.EarlySectors, err = bitfield.SubtractBitField(es.EarlySectors, earlySectors); err != nil {
		return err
	}
	// XXX: check underflow
	es.ActivePower = es.ActivePower.Sub(activePower)
	es.FaultyPower = es.FaultyPower.Sub(faultyPower)
	return nil
}

func (es *ExpirationSet) IsEmpty() (empty bool, err error) {
	if empty, err = es.OnTimeSectors.IsEmpty(); err != nil {
		return false, err
	} else if empty {
		if empty, err = es.EarlySectors.IsEmpty(); err != nil {
			return false, err
		}
		return empty, nil
	} else {
		return false, nil
	}
	// XXX: check zero power?
}

// A queue of expiration sets by epoch, representing the on-time or early termination epoch for a collection of sectors.
// Wraps an AMT[ChainEpoch]*ExpirationSet.
type expirationQueue struct {
	*adt.Array
}

func loadExpirationQueue(store adt.Store, root cid.Cid) (expirationQueue, error) {
	arr, err := adt.AsArray(store, root)
	if err != nil {
		return expirationQueue{}, xerrors.Errorf("failed to load epoch queue %v: %w", root, err)
	}
	return expirationQueue{arr}, nil
}

// Adds new (non-faulty) sectors to the queue entry for an on-time expiration epoch, with active power.
// XXX no export
func (q expirationQueue) AddActiveSectorsAt(sectors *abi.BitField, epoch abi.ChainEpoch, power PowerPair) error {
	return q.add(epoch, sectors, abi.NewBitField(), power, NewPowerPairZero())
}

// Adds a collection of sectors to their on-time target expiration entries.
// The sectors are assumed to be active (non-faulty).
// Returns the sector numbers, power, and pledge added.
func (q expirationQueue) AddActiveSectors(sectors []*SectorOnChainInfo, ssize abi.SectorSize) (*abi.BitField, PowerPair, abi.TokenAmount, error) {
	totalPower := NewPowerPairZero()
	totalPledge := big.Zero()
	var totalSectors []*abi.BitField
	noEarlySectors := abi.NewBitField()
	noFaultyPower := NewPowerPairZero()
	for _, group := range groupSectorsByExpiration(ssize, sectors) {
		snos := bitfield.NewFromSet(group.sectors)
		if err := q.add(group.epoch, snos, noEarlySectors, group.power, noFaultyPower); err != nil {
			return nil, NewPowerPairZero(), big.Zero(), xerrors.Errorf("failed to record new sector expirations: %w", err)
		}
		totalSectors = append(totalSectors, snos)
		totalPower = totalPower.Add(group.power)
		totalPledge = big.Add(totalPledge, group.pledge)
	}
	snos, err := bitfield.MultiMerge(totalSectors...)
	if err != nil {
		return nil, NewPowerPairZero(), big.Zero(), err
	}
	return snos, totalPower, totalPledge, nil
}

// Reschedules some sectors to a new expiration epoch.
// The sectors being rescheduled are assumed to be not faulty, and hence are removed from and re-scheduled for on-time
// rather than early expiration.
// The sectors' power and pledge are assumed not to change, despite the new expiration.
func (q expirationQueue) RescheduleExpirations(newExpiration abi.ChainEpoch, sectors []*SectorOnChainInfo, ssize abi.SectorSize) error {
	snos, power, _, err := q.removeActiveSectors(sectors, ssize)
	if err != nil {
		return xerrors.Errorf("failed to remove sector expirations: %w", err)
	}

	if err = q.AddActiveSectorsAt(snos, newExpiration, power); err != nil {
		return xerrors.Errorf("failed to record new sector expirations: %w", err)
	}
	return nil
}

// Re-schedules sectors to expire at an early expiration epoch, if they wouldn't expire before then anyway.
// The sectors must not be currently faulty, so must be registered as expiring on-time rather than early.
// Returns the total power represented by the sectors.
func (q expirationQueue) RescheduleAsFaults(newExpiration abi.ChainEpoch, sectors []*SectorOnChainInfo, ssize abi.SectorSize) (PowerPair, error) {
	var sectorsTotal []uint64
	expiringPower := NewPowerPairZero()
	rescheduledPower := NewPowerPairZero()

	// Group sectors by their target expiration, then remove from existing queue entries according to those groups.
	for _, group := range groupSectorsByExpiration(ssize, sectors) {
		var err error
		var es ExpirationSet
		if err = q.mustGet(group.epoch, &es); err != nil {
			return NewPowerPairZero(), err
		}
		if group.epoch <= newExpiration {
			// Don't reschedule sectors that are already due to expire on-time before the fault-driven expiration,
			// but do represent their power as now faulty.
			es.ActivePower = es.ActivePower.Sub(group.power)
			es.FaultyPower = es.FaultyPower.Add(group.power)
			expiringPower = expiringPower.Add(group.power)
		} else {
			// Remove sectors from on-time expiry and active power.
			sectorsBf := bitfield.NewFromSet(group.sectors)
			if es.OnTimeSectors, err = bitfield.SubtractBitField(es.OnTimeSectors, sectorsBf); err != nil {
				return NewPowerPairZero(), err
			}
			es.ActivePower = es.ActivePower.Sub(group.power)

			// Accumulate the sectors and power removed.
			sectorsTotal = append(sectorsTotal, group.sectors...)
			rescheduledPower = rescheduledPower.Add(group.power)
		}
		if err = q.mustUpdateOrDelete(group.epoch, &es); err != nil {
			return NewPowerPairZero(), err
		}
	}

	// Add sectors to new expiration as early-terminating and faulty.
	if err := q.add(newExpiration, abi.NewBitField(), bitfield.NewFromSet(sectorsTotal), NewPowerPairZero(), rescheduledPower); err != nil {
		return NewPowerPairZero(), err
	}

	return rescheduledPower.Add(expiringPower), nil
}

// Re-schedules *all* sectors to expire at an early expiration epoch, if they wouldn't expire before then anyway.
func (q expirationQueue) RescheduleAllAsFaults(faultExpiration abi.ChainEpoch) error {
	var rescheduedEpochs []uint64
	var rescheduledSectors []*abi.BitField
	rescheduledPower := NewPowerPairZero()

	var es ExpirationSet
	if err := q.Array.ForEach(&es, func(e int64) error {
		epoch := abi.ChainEpoch(e)
		if epoch <= faultExpiration {
			// Regardless of whether the sectors were expiring on-time or early, all the power is now faulty.
			es.FaultyPower = es.FaultyPower.Add(es.ActivePower)
			es.ActivePower = NewPowerPairZero()
			if err := q.mustUpdate(epoch, &es); err != nil {
				return err
			}
		} else {
			rescheduedEpochs = append(rescheduedEpochs, uint64(epoch))
			rescheduledSectors = append(rescheduledSectors, es.OnTimeSectors, es.EarlySectors)
			rescheduledPower = rescheduledPower.Add(es.ActivePower)
			rescheduledPower = rescheduledPower.Add(es.FaultyPower)
		}
		return nil
	}); err != nil {
		return err
	}

	// Add rescheduled sectors to new expiration as early-terminating and faulty.
	allRescheduled, err := bitfield.MultiMerge(rescheduledSectors...)
	if err != nil {
		return xerrors.Errorf("failed to merge rescheduled sectors: %w", err)
	}
	if err = q.add(faultExpiration, abi.NewBitField(), allRescheduled, NewPowerPairZero(), rescheduledPower); err != nil {
		return err
	}

	// Trim the rescheduled epochs from the queue.
	if err = q.BatchDelete(rescheduedEpochs); err != nil {
		return err
	}

	return nil
}

// Removes sectors from any queue entries in which they appear that are earlier then their scheduled expiration epoch,
// and schedules them at their expected termination epoch.
// Power for the sectors is changes from faulty to active (whether rescheduled or not).
// Returns the newly-recovered power.
func (q expirationQueue) RescheduleRecovered(sectors []*SectorOnChainInfo, ssize abi.SectorSize) (PowerPair, error) {
	remaining := make(map[abi.SectorNumber]struct{}, len(sectors))
	for _, s := range sectors {
		remaining[s.SectorNumber] = struct{}{}
	}

	// Traverse the expiration queue once to find each recovering sector and remove it from early/faulty there.
	var sectorsRescheduled []*SectorOnChainInfo
	recoveredPower := NewPowerPairZero()
	if err := q.traverseMutate(func(epoch abi.ChainEpoch, es *ExpirationSet) (changed, keepGoing bool, err error) {
		onTimeSectors, err := es.OnTimeSectors.AllMap(SectorsMax)
		if err != nil {
			return false, false, err
		}
		earlySectors, err := es.EarlySectors.AllMap(SectorsMax)
		if err != nil {
			return false, false, err
		}

		// This loop could alternatively be done by constructing bitfields and intersecting them, but it's not
		// clear that would be much faster (O(max(N, M)) vs O(N+M)).
		// If faults are correlated, the first queue entry likely has them all anyway.
		// The length of sectors has a maximum of one partition size.
		for _, sector := range sectors {
			sno := uint64(sector.SectorNumber)
			power := PowerForSector(ssize, sector)
			var found bool
			if _, found = onTimeSectors[sno]; found {
				// If the sector expires on-time at this epoch, leave it here but change faulty power to active.
				es.FaultyPower = es.FaultyPower.Sub(power)
				es.ActivePower = es.ActivePower.Add(power)
			} else if _, found = earlySectors[sno]; found {
				// If the sector expires early at this epoch, remove it for re-scheduling.
				es.EarlySectors.Unset(sno)
				es.FaultyPower = es.FaultyPower.Sub(power)
				sectorsRescheduled = append(sectorsRescheduled, sector)
			}
			if found {
				changed = true
				delete(remaining, sector.SectorNumber)
				recoveredPower = recoveredPower.Add(power)
			}
		}

		return changed, len(remaining) > 0, nil
	}); err != nil {
		return NewPowerPairZero(), err
	}
	// XXX: assert that remaining is now empty, i.e. all were found?

	// Re-schedule the removed sectors to their target expiration.
	if _, _, _, err := q.AddActiveSectors(sectorsRescheduled, ssize); err != nil {
		return NewPowerPairZero(), err
	}
	return recoveredPower, nil
}

// Removes some sectors and adds some others.
// The sectors being replace must not be faulty, so must be scheduled on-time rather than early expiration.
// The sectors added are assumed to be not faulty.
// Returns the old a new sector number bitfields, and delta to power and pledge, new minus old.
func (q expirationQueue) ReplaceSectors(oldSectors, newSectors []*SectorOnChainInfo, ssize abi.SectorSize) (*abi.BitField, *abi.BitField, PowerPair, abi.TokenAmount, error) {
	oldSnos, oldPower, oldPledge, err := q.removeActiveSectors(oldSectors, ssize)
	if err != nil {
		return nil, nil, NewPowerPairZero(), big.Zero(), xerrors.Errorf("failed to remove replaced sectors: %w", err)
	}
	newSnos, newPower, newPledge, err := q.AddActiveSectors(newSectors, ssize)
	if err != nil {
		return nil, nil, NewPowerPairZero(), big.Zero(), xerrors.Errorf("failed to add replacement sectors: %w", err)
	}
	return oldSnos, newSnos, newPower.Sub(oldPower), big.Sub(newPledge, oldPledge), nil
}

// Remove some sectors from the queue.
// The sectors may be active or faulty, and scheduled either for on-time or early termination.
// Returns the aggregate of removed sectors and power, and recovering power.
func (q expirationQueue) RemoveSectors(sectors []*SectorOnChainInfo, faults *abi.BitField, recovering *abi.BitField,
	ssize abi.SectorSize) (*ExpirationSet, PowerPair, error) {
	remaining := make(map[abi.SectorNumber]struct{}, len(sectors))
	for _, s := range sectors {
		remaining[s.SectorNumber] = struct{}{}
	}
	faultsMap, err := faults.AllMap(SectorsMax)
	if err != nil {
		return nil, NewPowerPairZero(), xerrors.Errorf("failed to expand faults: %w", err)
	}
	recoveringMap, err := recovering.AllMap(SectorsMax)
	if err != nil {
		return nil, NewPowerPairZero(), xerrors.Errorf("failed to expand recoveries: %w", err)
	}

	removed := NewExpirationSetEmpty()
	recoveringPower := NewPowerPairZero()
	if err = q.traverseMutate(func(epoch abi.ChainEpoch, es *ExpirationSet) (changed, keepGoing bool, err error) {
		onTimeSectors, err := es.OnTimeSectors.AllMap(SectorsMax)
		if err != nil {
			return false, false, err
		}
		earlySectors, err := es.EarlySectors.AllMap(SectorsMax)
		if err != nil {
			return false, false, err
		}

		// This loop could alternatively be done by constructing bitfields and intersecting them, but it's not
		// clear that would be much faster (O(max(N, M)) vs O(N+M)).
		// The length of sectors has a maximum of one partition size.
		for _, sector := range sectors {
			sno := uint64(sector.SectorNumber)
			var found bool
			if _, found = onTimeSectors[sno]; found {
				es.OnTimeSectors.Unset(sno)
				removed.OnTimeSectors.Set(sno)
			} else if _, found = earlySectors[sno]; found {
				es.EarlySectors.Unset(sno)
				removed.EarlySectors.Set(sno)
			}
			if found {
				delete(remaining, sector.SectorNumber)
				changed = true

				power := PowerForSector(ssize, sector)
				if _, f := faultsMap[sno]; f {
					es.FaultyPower = es.FaultyPower.Sub(power)
					removed.FaultyPower = removed.FaultyPower.Add(power)
				} else {
					es.ActivePower = es.ActivePower.Sub(power)
					removed.ActivePower = removed.ActivePower.Add(power)
				}
				if _, r := recoveringMap[sno]; r {
					recoveringPower = recoveringPower.Add(power)
				}

			}
		}

		return changed, len(remaining) > 0, nil
	}); err != nil {
		return nil, recoveringPower, err
	}
	// XXX: assert that remaining is now empty, i.e. all were found?

	return removed, recoveringPower, nil
}

// Removes and aggregates entries from the queue up to and including some epoch.
func (q expirationQueue) PopUntil(until abi.ChainEpoch) (*ExpirationSet, error) {
	var onTimeSectors []*abi.BitField
	var earlySectors []*abi.BitField
	activePower := NewPowerPairZero()
	faultyPower := NewPowerPairZero()

	var poppedKeys []uint64
	var thisValue ExpirationSet
	stopErr := fmt.Errorf("stop")
	if err := q.Array.ForEach(&thisValue, func(i int64) error {
		if abi.ChainEpoch(i) > until {
			return stopErr
		}
		poppedKeys = append(poppedKeys, uint64(i))
		onTimeSectors = append(onTimeSectors, thisValue.OnTimeSectors)
		earlySectors = append(earlySectors, thisValue.EarlySectors)
		activePower = activePower.Add(thisValue.ActivePower)
		faultyPower = faultyPower.Add(thisValue.ActivePower)
		return nil
	}); err != nil && err != stopErr {
		return nil, err
	}

	if err := q.Array.BatchDelete(poppedKeys); err != nil {
		return nil, err
	}

	allOnTime, err := bitfield.MultiMerge(onTimeSectors...)
	if err != nil {
		return nil, err
	}
	allEarly, err := bitfield.MultiMerge(earlySectors...)
	if err != nil {
		return nil, err
	}
	return NewExpirationSet(allOnTime, allEarly, activePower, faultyPower), nil
}

func (q expirationQueue) add(epoch abi.ChainEpoch, onTimeSectors, earlySectors *abi.BitField, activePower, faultyPower PowerPair) error {
	es, err := q.mayGet(epoch)
	if err != nil {
		return err
	}

	if err = es.Add(onTimeSectors, earlySectors, activePower, faultyPower); err != nil {
		return xerrors.Errorf("failed to add expiration values for epoch %v: %w", epoch, err)
	}

	return q.mustUpdate(epoch, es)
}

// XXX: this doesn't check that the sectors were actually there.
func (q expirationQueue) remove(epoch abi.ChainEpoch, onTimeSectors, earlySectors *abi.BitField, activePower, faultyPower PowerPair) error {
	var es ExpirationSet
	if err := q.mustGet(epoch, &es); err != nil {
		return err
	}

	if err := es.Remove(onTimeSectors, earlySectors, activePower, faultyPower); err != nil {
		return xerrors.Errorf("failed to remove expiration values for queue epoch %v: %w", epoch, err)
	}

	return q.mustUpdateOrDelete(epoch, &es)
}

func (q expirationQueue) removeActiveSectors(sectors []*SectorOnChainInfo, ssize abi.SectorSize) (*abi.BitField, PowerPair, abi.TokenAmount, error) {
	removedSnos := abi.NewBitField()
	removedPower := NewPowerPairZero()
	removedPledge := big.Zero()
	noEarlySectors := abi.NewBitField()
	noFaultyPower := NewPowerPairZero()

	// Group sectors by their expiration, then remove from existing queue entries according to those groups.
	for _, group := range groupSectorsByExpiration(ssize, sectors) {
		sectorsBf := bitfield.NewFromSet(group.sectors)
		if err := q.remove(group.epoch, sectorsBf, noEarlySectors, group.power, noFaultyPower); err != nil {
			return nil, NewPowerPairZero(), big.Zero(), err
		}
		for _, n := range group.sectors {
			removedSnos.Set(n)
		}
		removedPower = removedPower.Add(group.power)
		removedPledge = big.Add(removedPledge, group.pledge)
	}
	return removedSnos, removedPower, removedPledge, nil
}

// Traverses the entire queue with a callback function that may mutate entries.
// Iff the function returns that it changed an entry, the new entry will be re-written in the queue. Any changed
// entries that become empty are removed after iteration completes.
func (q expirationQueue) traverseMutate(f func(epoch abi.ChainEpoch, es *ExpirationSet) (changed, keepGoing bool, err error)) error {
	var es ExpirationSet
	var epochsEmptied []uint64
	errStop := fmt.Errorf("stop")
	if err := q.Array.ForEach(&es, func(epoch int64) error {
		changed, keepGoing, err := f(abi.ChainEpoch(epoch), &es)
		if err != nil {
			return err
		} else if changed {
			if emptied, err := es.IsEmpty(); err != nil {
				return err
			} else if emptied {
				epochsEmptied = append(epochsEmptied, uint64(epoch))
			} else if err = q.mustUpdate(abi.ChainEpoch(epoch), &es); err != nil {
				return err
			}
		}

		if !keepGoing {
			return errStop
		}
		return nil
	}); err != nil && err != errStop {
		return err
	}
	if err := q.Array.BatchDelete(epochsEmptied); err != nil {
		return err
	}
	return nil
}

func (q expirationQueue) mayGet(key abi.ChainEpoch) (*ExpirationSet, error) {
	es := NewExpirationSetEmpty()
	if _, err := q.Array.Get(uint64(key), es); err != nil {
		return nil, xerrors.Errorf("failed to lookup queue epoch %v: %w", key, err)
	}
	return es, nil
}

func (q expirationQueue) mustGet(key abi.ChainEpoch, es *ExpirationSet) error {
	if found, err := q.Array.Get(uint64(key), es); err != nil {
		return xerrors.Errorf("failed to lookup queue epoch %v: %w", key, err)
	} else if !found {
		return xerrors.Errorf("missing expected expiration set at epoch %v", key)
	}
	return nil
}

func (q expirationQueue) mustUpdate(epoch abi.ChainEpoch, es *ExpirationSet) error {
	if err := q.Array.Set(uint64(epoch), es); err != nil {
		return xerrors.Errorf("failed to set queue epoch %v: %w", epoch, err)
	}
	return nil
}

// Since this might delete the node, it's not safe for use inside an iteration.
func (q expirationQueue) mustUpdateOrDelete(epoch abi.ChainEpoch, es *ExpirationSet) error {
	if empty, err := es.IsEmpty(); err != nil {
		return err
	} else if empty {
		if err = q.Array.Delete(uint64(epoch)); err != nil {
			return xerrors.Errorf("failed to delete queue epoch %d: %w", epoch, err)
		}
	} else if err = q.Array.Set(uint64(epoch), es); err != nil {
		return xerrors.Errorf("failed to set queue epoch %v: %w", epoch, err)
	}
	return nil
}

func (e *ExpirationSet) MarshalCBOR(io.Writer) error {
	panic("implement me")
}
func (e *ExpirationSet) UnmarshalCBOR(io.Reader) error {
	panic("implement me")
}
