package miner

import (
	"fmt"
	"math"
	"sort"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/v8/actors/builtin"
	"github.com/filecoin-project/specs-actors/v8/actors/util"
	"github.com/filecoin-project/specs-actors/v8/actors/util/adt"
)

// ExpirationSet is a collection of sector numbers that are expiring, either due to
// expected "on-time" expiration at the end of their life, or unexpected "early" termination
// due to being faulty for too long consecutively.
// Note that there is not a direct correspondence between on-time sectors and active sectors;
// a sector may be faulty but expiring on-time if it faults just prior to expected termination.
// Early sectors are always faulty, and active sectors always represent on-time sectors.
type ExpirationSet struct {
	OnTimeSectors bitfield.BitField // Sectors expiring "on time" at the end of their committed life
	EarlySectors  bitfield.BitField // Sectors expiring "early" due to being faulty for too long
	OnTimePledge  abi.TokenAmount   // Pledge total for the on-time sectors
	ActiveCount   uint64            // Count of sectors that are currently active (not faulty)
	FaultyCount   uint64            // Count of sectors that are currently faulty
}

func NewExpirationSetEmpty() *ExpirationSet {
	set, _ := NewExpirationSet(bitfield.New(), bitfield.New(), big.Zero(), 0, 0)
	return set
}

func NewExpirationSet(onTimeSectors, earlySectors bitfield.BitField, onTimePledge abi.TokenAmount, activeCount, faultyCount uint64) (*ExpirationSet, error) {
	set := &ExpirationSet{
		OnTimeSectors: onTimeSectors,
		EarlySectors:  earlySectors,
		OnTimePledge:  onTimePledge,
		ActiveCount:   activeCount,
		FaultyCount:   faultyCount,
	}
	if err := set.ValidateState(); err != nil {
		return nil, err
	}
	return set, nil
}

// Adds sectors to the expiration set in place.
func (es *ExpirationSet) Add(onTimeSectors, earlySectors bitfield.BitField, onTimePledge abi.TokenAmount, activeCount, faultyCount uint64) error {
	// Check overflow.
	if onTimePledge.LessThan(big.Zero()) {
		return xerrors.Errorf("negative pledge added to expiration set: %+v", es)
	}
	if activeCount > math.MaxUint64-es.ActiveCount {
		return xerrors.Errorf("expiration set active count overflow: %+v", es)
	}
	if faultyCount > math.MaxUint64-es.FaultyCount {
		return xerrors.Errorf("expiration set active count overflow: %+v", es)
	}

	var err error
	if es.OnTimeSectors, err = bitfield.MergeBitFields(es.OnTimeSectors, onTimeSectors); err != nil {
		return err
	}
	if es.EarlySectors, err = bitfield.MergeBitFields(es.EarlySectors, earlySectors); err != nil {
		return err
	}
	es.OnTimePledge = big.Add(es.OnTimePledge, onTimePledge)
	es.ActiveCount += activeCount
	es.FaultyCount += faultyCount
	return es.ValidateState()
}

// Removes sectors from the expiration set in place.
func (es *ExpirationSet) Remove(onTimeSectors, earlySectors bitfield.BitField, onTimePledge abi.TokenAmount, activeCount, faultyCount uint64) error {
	// Check for sector intersection. This could be cheaper with a combined intersection/difference method used below.
	if found, err := util.BitFieldContainsAll(es.OnTimeSectors, onTimeSectors); err != nil {
		return err
	} else if !found {
		return xerrors.Errorf("removing on-time sectors %v not contained in %v", onTimeSectors, es.OnTimeSectors)
	}
	if found, err := util.BitFieldContainsAll(es.EarlySectors, earlySectors); err != nil {
		return err
	} else if !found {
		return xerrors.Errorf("removing early sectors %v not contained in %v", earlySectors, es.EarlySectors)
	}

	var err error
	if es.OnTimeSectors, err = bitfield.SubtractBitField(es.OnTimeSectors, onTimeSectors); err != nil {
		return err
	}
	if es.EarlySectors, err = bitfield.SubtractBitField(es.EarlySectors, earlySectors); err != nil {
		return err
	}

	// Check underflow.
	if onTimePledge.LessThan(big.Zero()) {
		return xerrors.Errorf("negative pledge removed from expiration set: %+v", es)
	}
	if es.OnTimePledge.LessThan(onTimePledge) {
		return xerrors.Errorf("expiration set pledge underflow: %+v", es)
	}
	if es.ActiveCount < activeCount || es.FaultyCount < faultyCount {
		return xerrors.Errorf("expiration set sector count underflow: %+v", es)
	}

	es.OnTimePledge = big.Sub(es.OnTimePledge, onTimePledge)
	es.ActiveCount -= activeCount
	es.FaultyCount -= faultyCount
	return es.ValidateState()
}

// A set is empty if it has no sectors.
// The pledge is not checked, but expected to be zero.
func (es *ExpirationSet) IsEmpty() bool {
	return es.ActiveCount == 0 && es.FaultyCount == 0
}

// Counts all sectors in the expiration set.
func (es *ExpirationSet) Count() (count uint64, err error) {
	if es.ActiveCount > math.MaxUint64-es.FaultyCount {
		return 0, xerrors.Errorf("overflow adding expiration set counts: %+v", es)
	}
	return es.ActiveCount + es.FaultyCount, nil
}

// validates a set of assertions that must hold for expiration sets
func (es *ExpirationSet) ValidateState() error {
	if es.OnTimePledge.LessThan(big.Zero()) {
		return xerrors.Errorf("expiration set with negative pledge: %+v", es)
	}
	onTime, err := es.OnTimeSectors.Count()
	if err != nil {
		return err
	}
	early, err := es.EarlySectors.Count()
	if err != nil {
		return err
	}

	if onTime > math.MaxUint64-early {
		return xerrors.Errorf("overflow adding expiration set bitfield counts: %+v", es)
	}
	if es.ActiveCount > math.MaxUint64-es.FaultyCount {
		return xerrors.Errorf("overflow adding expiration set state counts: %+v", es)
	}
	if onTime+early != es.ActiveCount+es.FaultyCount {
		return xerrors.Errorf("expiration set inconsistent, on-time (%d) + early (%d) != active (%d) + faulty (%d)",
			onTime, early, es.ActiveCount, es.FaultyCount)
	}
	return nil
}

// A queue of expiration sets by epoch, representing the on-time or early termination epoch for a collection of sectors.
// Wraps an AMT[ChainEpoch]*ExpirationSet.
// Keys in the queue are quantized (upwards), modulo some offset, to reduce the cardinality of keys.
type ExpirationQueue struct {
	*adt.Array
	quant builtin.QuantSpec
}

// An internal limit on the cardinality of a bitfield in a queue entry.
// This must be at least large enough to support the maximum number of sectors in a partition.
// It would be a bit better to derive this number from an enumeration over all partition sizes.
const entrySectorsMax = 10_000

// Loads a queue root.
// Epochs provided to subsequent method calls will be quantized upwards to quanta mod offsetSeed before being
// written to/read from queue entries.
func LoadExpirationQueue(store adt.Store, root cid.Cid, quant builtin.QuantSpec, bitwidth int) (ExpirationQueue, error) {
	arr, err := adt.AsArray(store, root, bitwidth)
	if err != nil {
		return ExpirationQueue{}, xerrors.Errorf("failed to load epoch queue %v: %w", root, err)
	}
	return ExpirationQueue{arr, quant}, nil
}

// Adds a collection of sectors to their on-time target expiration entries (quantized).
// The sectors are assumed to be active (non-faulty).
// Returns the sector numbers, count thereof, and pledge added.
func (q ExpirationQueue) AddActiveSectors(sectors []*SectorOnChainInfo) (bitfield.BitField, abi.TokenAmount, error) {
	totalPledge := big.Zero()
	var totalSectors []bitfield.BitField
	noEarlySectors := bitfield.New()
	for _, group := range groupNewSectorsByDeclaredExpiration(sectors, q.quant) {
		snos := bitfield.NewFromSet(group.sectors)
		if err := q.add(group.epoch, snos, noEarlySectors, uint64(len(group.sectors)), 0, group.pledge); err != nil {
			return bitfield.BitField{}, big.Zero(), xerrors.Errorf("failed to record new sector expirations: %w", err)
		}
		totalSectors = append(totalSectors, snos)
		totalPledge = big.Add(totalPledge, group.pledge)
	}
	snos, err := bitfield.MultiMerge(totalSectors...)
	if err != nil {
		return bitfield.BitField{}, big.Zero(), err
	}
	return snos, totalPledge, nil
}

// Re-schedules sectors to expire at an early expiration epoch (quantized), if they wouldn't expire before then anyway.
// The sectors must not be currently faulty, so must be registered as expiring on-time rather than early.
// The pledge for the now-early sectors is removed from the queue.
func (q ExpirationQueue) RescheduleAsFaults(newExpiration abi.ChainEpoch, sectors []*SectorOnChainInfo) error {
	var sectorsTotal []uint64
	rescheduledCount := uint64(0)

	// Group sectors by their target expiration, then remove from existing queue entries according to those groups.
	groups, err := q.findSectorsByExpiration(sectors)
	if err != nil {
		return err
	}

	for _, group := range groups {
		var err error
		groupCount := uint64(len(group.sectors))
		if group.epoch <= q.quant.QuantizeUp(newExpiration) {
			// Don't reschedule sectors that are already due to expire on-time before the fault-driven expiration,
			// but do represent them as now faulty.
			// Their pledge remains as "on-time".
			group.expirationSet.ActiveCount -= groupCount
			group.expirationSet.FaultyCount += groupCount
		} else {
			// Remove sectors from on-time expiry and active sets.
			sectorsBf := bitfield.NewFromSet(group.sectors)
			if group.expirationSet.OnTimeSectors, err = bitfield.SubtractBitField(group.expirationSet.OnTimeSectors, sectorsBf); err != nil {
				return err
			}
			group.expirationSet.OnTimePledge = big.Sub(group.expirationSet.OnTimePledge, group.pledge)
			group.expirationSet.ActiveCount -= groupCount

			// Accumulate the sectors removed.
			sectorsTotal = append(sectorsTotal, group.sectors...)
			rescheduledCount += groupCount
		}
		if err = q.mustUpdateOrDelete(group.epoch, group.expirationSet); err != nil {
			return err
		}

		if err = group.expirationSet.ValidateState(); err != nil {
			return err
		}
	}

	if len(sectorsTotal) > 0 {
		// Add sectors to new expiration as early-terminating and faulty.
		earlySectors := bitfield.NewFromSet(sectorsTotal)
		noOnTimeSectors := bitfield.New()
		noOnTimePledge := abi.NewTokenAmount(0)
		noActiveCount := uint64(0)
		if err := q.add(newExpiration, noOnTimeSectors, earlySectors, noActiveCount, rescheduledCount, noOnTimePledge); err != nil {
			return err
		}
	}
	return nil
}

// Re-schedules *all* sectors to expire at an early expiration epoch, if they wouldn't expire before then anyway.
func (q ExpirationQueue) RescheduleAllAsFaults(faultExpiration abi.ChainEpoch) error {
	var rescheduledEpochs []uint64
	var rescheduledSectors []bitfield.BitField
	rescheduledCount := uint64(0)

	var es ExpirationSet
	if err := q.Array.ForEach(&es, func(e int64) error {
		epoch := abi.ChainEpoch(e)
		if epoch <= q.quant.QuantizeUp(faultExpiration) {
			// Regardless of whether the sectors were expiring on-time or early, they are all now faulty.
			// Pledge is still on-time.
			es.FaultyCount += es.ActiveCount
			es.ActiveCount = 0
			if err := q.mustUpdate(epoch, &es); err != nil {
				return err
			}
		} else {
			rescheduledEpochs = append(rescheduledEpochs, uint64(epoch))
			// sanity check to make sure we're not trying to re-schedule already faulty sectors.
			if isEmpty, err := es.EarlySectors.IsEmpty(); err != nil {
				return xerrors.Errorf("failed to determine if epoch had early expirations: %w", err)
			} else if !isEmpty {
				return xerrors.Errorf("attempted to re-schedule early expirations to an even earlier epoch")
			}
			rescheduledSectors = append(rescheduledSectors, es.OnTimeSectors)
			rescheduledCount += es.ActiveCount + es.FaultyCount
		}

		if err := es.ValidateState(); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	// If we didn't reschedule anything, we're done.
	if len(rescheduledEpochs) == 0 {
		return nil
	}

	// Add rescheduled sectors to new expiration as early-terminating and faulty.
	allRescheduled, err := bitfield.MultiMerge(rescheduledSectors...)
	if err != nil {
		return xerrors.Errorf("failed to merge rescheduled sectors: %w", err)
	}
	noOnTimeSectors := bitfield.New()
	noActiveCount := uint64(0)
	noOnTimePledge := abi.NewTokenAmount(0)
	if err = q.add(faultExpiration, noOnTimeSectors, allRescheduled, noActiveCount, rescheduledCount, noOnTimePledge); err != nil {
		return err
	}

	// Trim the rescheduled epochs from the queue.
	if err = q.BatchDelete(rescheduledEpochs, true); err != nil {
		return err
	}

	return nil
}

// Removes sectors from any queue entries in which they appear that are earlier then their scheduled expiration epoch,
// and schedules them at their expected termination epoch.
// Pledge for the sectors is re-added as on-time.
// The sectors are changed from faulty to active (whether rescheduled or not).
// Returns the count of newly-recovered sectors. Fails if any sectors are not found in the queue.
func (q ExpirationQueue) RescheduleRecovered(sectors []*SectorOnChainInfo) (uint64, error) {
	remaining := make(map[abi.SectorNumber]struct{}, len(sectors))
	for _, s := range sectors {
		remaining[s.SectorNumber] = struct{}{}
	}

	// Traverse the expiration queue once to find each recovering sector and remove it from early/faulty there.
	// We expect this to find all recovering sectors within the first FaultMaxAge/WPoStProvingPeriod entries
	// (i.e. 14 for 14-day faults), but if something has gone wrong it's safer not to fail if that's not met.
	var sectorsRescheduled []*SectorOnChainInfo
	recoveredCount := uint64(0)
	if err := q.traverseMutate(func(epoch abi.ChainEpoch, es *ExpirationSet) (changed, keepGoing bool, err error) {
		onTimeSectors, err := es.OnTimeSectors.AllMap(entrySectorsMax)
		if err != nil {
			return false, false, err
		}
		earlySectors, err := es.EarlySectors.AllMap(entrySectorsMax)
		if err != nil {
			return false, false, err
		}

		// This loop could alternatively be done by constructing bitfields and intersecting them, but it's not
		// clear that would be much faster (O(max(N, M)) vs O(N+M)).
		// If faults are correlated, the first queue entry likely has them all anyway.
		// The length of sectors has a maximum of one partition size.
		for _, sector := range sectors {
			sno := uint64(sector.SectorNumber)
			var found bool
			if _, found = onTimeSectors[sno]; found {
				// If the sector expires on-time at this epoch, leave it here but change faulty to active.
				// The pledge is already part of the on-time pledge at this entry.
				es.FaultyCount -= 1
				es.ActiveCount += 1
			} else if _, found = earlySectors[sno]; found {
				// If the sector expires early at this epoch, remove it for re-scheduling.
				// It's not part of the on-time pledge number here.
				es.EarlySectors.Unset(sno)
				es.FaultyCount -= 1
				sectorsRescheduled = append(sectorsRescheduled, sector)
			}
			if found {
				recoveredCount += 1
				delete(remaining, sector.SectorNumber)
				changed = true
			}
		}

		if err = es.ValidateState(); err != nil {
			return false, false, err
		}

		return changed, len(remaining) > 0, nil
	}); err != nil {
		return 0, err
	}
	if len(remaining) > 0 {
		return 0, xerrors.Errorf("sectors not found in expiration queue: %v", remaining)
	}

	// Re-schedule the removed sectors to their target expiration.
	if _, _, err := q.AddActiveSectors(sectorsRescheduled); err != nil {
		return 0, err
	}
	return recoveredCount, nil
}

// Removes some sectors and adds some others.
// The sectors being replaced must not be faulty, so must be scheduled for on-time rather than early expiration.
// The sectors added are assumed to be not faulty.
// Returns the old and new sector number bitfields, and delta to pledge (new minus old).
func (q ExpirationQueue) ReplaceSectors(oldSectors, newSectors []*SectorOnChainInfo) (bitfield.BitField, bitfield.BitField, abi.TokenAmount, error) {
	oldSnos, oldPledge, err := q.removeActiveSectors(oldSectors)
	if err != nil {
		return bitfield.BitField{}, bitfield.BitField{}, big.Zero(), xerrors.Errorf("failed to remove replaced sectors: %w", err)
	}
	newSnos, newPledge, err := q.AddActiveSectors(newSectors)
	if err != nil {
		return bitfield.BitField{}, bitfield.BitField{}, big.Zero(), xerrors.Errorf("failed to add replacement sectors: %w", err)
	}
	return oldSnos, newSnos, big.Sub(newPledge, oldPledge), nil
}

// Remove some sectors from the queue.
// The sectors may be active or faulty, and scheduled either for on-time or early termination.
// Returns the aggregate of removed sectors.
// Fails if any sectors are not found in the queue.
func (q ExpirationQueue) RemoveSectors(sectors []*SectorOnChainInfo, faults bitfield.BitField) (*ExpirationSet, error) {
	remaining := make(map[abi.SectorNumber]struct{}, len(sectors))
	for _, s := range sectors {
		remaining[s.SectorNumber] = struct{}{}
	}
	faultsMap, err := faults.AllMap(AddressedSectorsMax)
	if err != nil {
		return nil, xerrors.Errorf("failed to expand faults: %w", err)
	}

	// results
	removed := NewExpirationSetEmpty()

	// Split into faulty and non-faulty. We process non-faulty sectors first
	// because they always expire on-time so we know where to find them.
	var (
		nonFaultySectors []*SectorOnChainInfo
		faultySectors    []*SectorOnChainInfo
	)
	for _, sector := range sectors {
		if _, found := faultsMap[uint64(sector.SectorNumber)]; found {
			faultySectors = append(faultySectors, sector)
			continue
		}
		nonFaultySectors = append(nonFaultySectors, sector)
		// remove them from "remaining", we're going to process them below.
		delete(remaining, sector.SectorNumber)
	}

	// Remove non-faulty sectors.
	removed.ActiveCount = uint64(len(nonFaultySectors))
	removed.OnTimeSectors, removed.OnTimePledge, err = q.removeActiveSectors(nonFaultySectors)
	if err != nil {
		return nil, xerrors.Errorf("failed to remove on-time recoveries: %w", err)
	}

	// Finally, remove faulty sectors (on time and not). These sectors can
	// only appear within the first 14 days (fault max age). Given that this
	// queue is quantized, we should be able to stop traversing the queue
	// after 14 entries.
	if err = q.traverseMutate(func(epoch abi.ChainEpoch, es *ExpirationSet) (changed, keepGoing bool, err error) {
		onTimeSectors, err := es.OnTimeSectors.AllMap(entrySectorsMax)
		if err != nil {
			return false, false, err
		}
		earlySectors, err := es.EarlySectors.AllMap(entrySectorsMax)
		if err != nil {
			return false, false, err
		}

		// This loop could alternatively be done by constructing bitfields and intersecting them, but it's not
		// clear that would be much faster (O(max(N, M)) vs O(N+M)).
		// The length of sectors has a maximum of one partition size.
		for _, sector := range faultySectors {
			sno := uint64(sector.SectorNumber)
			var found bool
			if _, found = onTimeSectors[sno]; found {
				es.OnTimeSectors.Unset(sno)
				removed.OnTimeSectors.Set(sno)
				es.OnTimePledge = big.Sub(es.OnTimePledge, sector.InitialPledge)
				removed.OnTimePledge = big.Add(removed.OnTimePledge, sector.InitialPledge)
			} else if _, found = earlySectors[sno]; found {
				es.EarlySectors.Unset(sno)
				removed.EarlySectors.Set(sno)
			}
			if found {
				if _, f := faultsMap[sno]; f {
					es.FaultyCount -= 1
					removed.FaultyCount += 1
				} else {
					es.ActiveCount -= 1
					removed.ActiveCount += 1
				}
				delete(remaining, sector.SectorNumber)
				changed = true
			}
		}

		if err = es.ValidateState(); err != nil {
			return false, false, err
		}

		return changed, len(remaining) > 0, nil
	}); err != nil {
		return nil, err
	}
	if len(remaining) > 0 {
		return NewExpirationSetEmpty(), xerrors.Errorf("sectors not found in expiration queue: %v", remaining)
	}

	return removed, nil
}

// Removes and aggregates entries from the queue up to and including some epoch.
func (q ExpirationQueue) PopUntil(until abi.ChainEpoch) (*ExpirationSet, error) {
	var onTimeSectors []bitfield.BitField
	var earlySectors []bitfield.BitField
	activeCount := uint64(0)
	faultyCount := uint64(0)
	onTimePledge := big.Zero()

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
		activeCount += thisValue.ActiveCount
		faultyCount += thisValue.FaultyCount
		onTimePledge = big.Add(onTimePledge, thisValue.OnTimePledge)
		return nil
	}); err != nil && err != stopErr {
		return nil, err
	}

	if err := q.Array.BatchDelete(poppedKeys, true); err != nil {
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
	return NewExpirationSet(allOnTime, allEarly, onTimePledge, activeCount, faultyCount)
}

func (q ExpirationQueue) add(rawEpoch abi.ChainEpoch, onTimeSectors, earlySectors bitfield.BitField, activeCount, faultyCount uint64,
	pledge abi.TokenAmount) error {
	epoch := q.quant.QuantizeUp(rawEpoch)
	es, err := q.mayGet(epoch)
	if err != nil {
		return err
	}

	if err = es.Add(onTimeSectors, earlySectors, pledge, activeCount, faultyCount); err != nil {
		return xerrors.Errorf("failed to add expiration values for epoch %v: %w", epoch, err)
	}

	return q.mustUpdate(epoch, es)
}

func (q ExpirationQueue) remove(rawEpoch abi.ChainEpoch, onTimeSectors, earlySectors bitfield.BitField, activeCount, faultyCount uint64,
	pledge abi.TokenAmount) error {
	epoch := q.quant.QuantizeUp(rawEpoch)
	var es ExpirationSet
	if found, err := q.Array.Get(uint64(epoch), &es); err != nil {
		return xerrors.Errorf("failed to lookup queue epoch %v: %w", epoch, err)
	} else if !found {
		return xerrors.Errorf("missing expected expiration set at epoch %v", epoch)
	}

	if err := es.Remove(onTimeSectors, earlySectors, pledge, activeCount, faultyCount); err != nil {
		return xerrors.Errorf("failed to remove expiration values for queue epoch %v: %w", epoch, err)
	}

	return q.mustUpdateOrDelete(epoch, &es)
}

func (q ExpirationQueue) removeActiveSectors(sectors []*SectorOnChainInfo) (bitfield.BitField, abi.TokenAmount, error) {
	removedSnos := bitfield.New()
	removedPledge := big.Zero()
	noEarlySectors := bitfield.New()
	noFaultyCount := uint64(0)

	// Group sectors by their expiration, then remove from existing queue entries according to those groups.
	groups, err := q.findSectorsByExpiration(sectors)
	if err != nil {
		return bitfield.BitField{}, big.Zero(), err
	}

	for _, group := range groups {
		sectorsBf := bitfield.NewFromSet(group.sectors)
		if err := q.remove(group.epoch, sectorsBf, noEarlySectors, uint64(len(group.sectors)), noFaultyCount, group.pledge); err != nil {
			return bitfield.BitField{}, big.Zero(), err
		}
		for _, n := range group.sectors {
			removedSnos.Set(n)
		}
		removedPledge = big.Add(removedPledge, group.pledge)
	}
	return removedSnos, removedPledge, nil
}

// Traverses the entire queue with a callback function that may mutate entries.
// Iff the function returns that it changed an entry, the new entry will be re-written in the queue. Any changed
// entries that become empty are removed after iteration completes.
func (q ExpirationQueue) traverseMutate(f func(epoch abi.ChainEpoch, es *ExpirationSet) (changed, keepGoing bool, err error)) error {
	var es ExpirationSet
	var epochsEmptied []uint64
	errStop := fmt.Errorf("stop")
	if err := q.Array.ForEach(&es, func(epoch int64) error {
		changed, keepGoing, err := f(abi.ChainEpoch(epoch), &es)
		if err != nil {
			return err
		} else if changed {
			if emptied := es.IsEmpty(); err != nil {
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
	if err := q.Array.BatchDelete(epochsEmptied, true); err != nil {
		return err
	}
	return nil
}

func (q ExpirationQueue) traverse(f func(epoch abi.ChainEpoch, es *ExpirationSet) (keepGoing bool, err error)) error {
	return q.traverseMutate(func(epoch abi.ChainEpoch, es *ExpirationSet) (bool, bool, error) {
		keepGoing, err := f(epoch, es)
		return false, keepGoing, err
	})
}

func (q ExpirationQueue) mayGet(key abi.ChainEpoch) (*ExpirationSet, error) {
	es := NewExpirationSetEmpty()
	if _, err := q.Array.Get(uint64(key), es); err != nil {
		return nil, xerrors.Errorf("failed to lookup queue epoch %v: %w", key, err)
	}
	return es, nil
}

func (q ExpirationQueue) mustUpdate(epoch abi.ChainEpoch, es *ExpirationSet) error {
	if err := q.Array.Set(uint64(epoch), es); err != nil {
		return xerrors.Errorf("failed to set queue epoch %v: %w", epoch, err)
	}
	return nil
}

// Since this might delete the node, it's not safe for use inside an iteration.
func (q ExpirationQueue) mustUpdateOrDelete(epoch abi.ChainEpoch, es *ExpirationSet) error {
	if es.IsEmpty() {
		if err := q.Array.Delete(uint64(epoch)); err != nil {
			return xerrors.Errorf("failed to delete queue epoch %d: %w", epoch, err)
		}
	} else if err := q.Array.Set(uint64(epoch), es); err != nil {
		return xerrors.Errorf("failed to set queue epoch %v: %w", epoch, err)
	}
	return nil
}

type sectorEpochSet struct {
	epoch   abi.ChainEpoch
	sectors []uint64
	pledge  abi.TokenAmount
}

type sectorExpirationSet struct {
	sectorEpochSet
	expirationSet *ExpirationSet
}

// Takes a slice of sector infos and returns sector info sets grouped and
// sorted by expiration epoch, quantized.
//
// Note: While the result is sorted by epoch, the order of per-epoch sectors is maintained.
func groupNewSectorsByDeclaredExpiration(sectors []*SectorOnChainInfo, quant builtin.QuantSpec) []sectorEpochSet {
	sectorsByExpiration := make(map[abi.ChainEpoch][]*SectorOnChainInfo)

	for _, sector := range sectors {
		qExpiration := quant.QuantizeUp(sector.Expiration)
		sectorsByExpiration[qExpiration] = append(sectorsByExpiration[qExpiration], sector)
	}

	sectorEpochSets := make([]sectorEpochSet, 0, len(sectorsByExpiration))

	// This map iteration is non-deterministic but safe because we sort by epoch below.
	for expiration, epochSectors := range sectorsByExpiration { //nolint:nomaprange // result is subsequently sorted
		sectorNumbers := make([]uint64, len(epochSectors))
		totalPledge := big.Zero()
		for i, sector := range epochSectors {
			sectorNumbers[i] = uint64(sector.SectorNumber)
			totalPledge = big.Add(totalPledge, sector.InitialPledge)
		}
		sectorEpochSets = append(sectorEpochSets, sectorEpochSet{
			epoch:   expiration,
			sectors: sectorNumbers,
			pledge:  totalPledge,
		})
	}

	sort.Slice(sectorEpochSets, func(i, j int) bool {
		return sectorEpochSets[i].epoch < sectorEpochSets[j].epoch
	})
	return sectorEpochSets
}

// Groups sectors into sets based on their Expiration field.
// If sectors are not found in the expiration set corresponding to their expiration field
// (i.e. they have been rescheduled) traverse expiration sets to for groups where these
// sectors actually expire.
// Groups will be returned in expiration order, earliest first.
func (q *ExpirationQueue) findSectorsByExpiration(sectors []*SectorOnChainInfo) ([]sectorExpirationSet, error) {
	declaredExpirations := make(map[abi.ChainEpoch]bool, len(sectors))
	sectorsByNumber := make(map[uint64]*SectorOnChainInfo, len(sectors))
	allRemaining := make(map[uint64]struct{})
	expirationGroups := make([]sectorExpirationSet, 0, len(declaredExpirations))

	for _, sector := range sectors {
		qExpiration := q.quant.QuantizeUp(sector.Expiration)
		declaredExpirations[qExpiration] = true
		allRemaining[uint64(sector.SectorNumber)] = struct{}{}
		sectorsByNumber[uint64(sector.SectorNumber)] = sector
	}

	// Traverse expiration sets first by expected expirations. This will find all groups if no sectors have been rescheduled.
	// This map iteration is non-deterministic but safe because we sort by epoch below.
	for expiration := range declaredExpirations { //nolint:nomaprange // result is subsequently sorted
		es, err := q.mayGet(expiration)
		if err != nil {
			return nil, err
		}

		// create group from overlap
		var group sectorExpirationSet
		group, allRemaining, err = groupExpirationSet(sectorsByNumber, allRemaining, es, expiration)
		if err != nil {
			return nil, err
		}
		if len(group.sectors) > 0 {
			expirationGroups = append(expirationGroups, group)
		}
	}

	// If sectors remain, traverse next in epoch order. Remaining sectors should be rescheduled to expire soon, so
	// this traversal should exit early.
	if len(allRemaining) > 0 {
		err := q.traverse(func(epoch abi.ChainEpoch, es *ExpirationSet) (bool, error) {
			// If this set's epoch is one of our declared epochs, we've already processed it in the loop above,
			// so skip processing here. Sectors rescheduled to this epoch would have been included in the earlier processing.
			if _, found := declaredExpirations[epoch]; found {
				return true, nil
			}

			// Sector should not be found in EarlyExpirations which holds faults. An implicit assumption
			// of grouping is that it only returns active sectors. ExpirationQueue should not
			// provide operations that allow this to happen.
			if err := assertNoEarlySectors(allRemaining, es); err != nil {
				return true, err
			}

			var group sectorExpirationSet
			var err error
			group, allRemaining, err = groupExpirationSet(sectorsByNumber, allRemaining, es, epoch)
			if err != nil {
				return false, err
			}
			if len(group.sectors) > 0 {
				expirationGroups = append(expirationGroups, group)
			}

			return len(allRemaining) > 0, nil
		})
		if err != nil {
			return nil, err
		}
	}

	if len(allRemaining) > 0 {
		return nil, xerrors.New("some sectors not found in expiration queue")
	}

	// sort groups, earliest first.
	sort.Slice(expirationGroups, func(i, j int) bool {
		return expirationGroups[i].epoch < expirationGroups[j].epoch
	})
	return expirationGroups, nil
}

// Takes a slice of sector infos a bitfield of sector numbers and returns a single group for all bitfield sectors
// Also returns a bitfield containing sectors not found in expiration set.
// This method mutates includeSet by removing sector numbers of sectors found in expiration set.
func groupExpirationSet(sectors map[uint64]*SectorOnChainInfo, includeSet map[uint64]struct{}, es *ExpirationSet,
	expiration abi.ChainEpoch) (sectorExpirationSet, map[uint64]struct{}, error) {
	var sectorNumbers []uint64
	totalPledge := big.Zero()
	err := es.OnTimeSectors.ForEach(func(u uint64) error {
		if _, found := includeSet[u]; found {
			sector := sectors[u]
			sectorNumbers = append(sectorNumbers, u)
			totalPledge = big.Add(totalPledge, sector.InitialPledge)
			delete(includeSet, u)
		}
		return nil
	})
	if err != nil {
		return sectorExpirationSet{}, nil, err
	}

	return sectorExpirationSet{
		sectorEpochSet: sectorEpochSet{
			epoch:   expiration,
			sectors: sectorNumbers,
			pledge:  totalPledge,
		},
		expirationSet: es,
	}, includeSet, nil
}

// assertNoEarlySectors checks for an invalid overlap between a bitfield an a set's early sectors.
func assertNoEarlySectors(set map[uint64]struct{}, es *ExpirationSet) error {
	return es.EarlySectors.ForEach(func(u uint64) error {
		if _, found := set[u]; found {
			return xerrors.Errorf("Invalid attempt to group sector %d with an early expiration", u)
		}
		return nil
	})
}
