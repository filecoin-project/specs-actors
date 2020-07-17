package miner

import (
	"errors"

	"github.com/filecoin-project/go-bitfield"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

type Partition struct {
	// Sector numbers in this partition, including faulty and terminated sectors.
	Sectors *abi.BitField
	// Subset of sectors detected/declared faulty and not yet recovered (excl. from PoSt).
	// Faults ∩ Terminated = ∅
	Faults *abi.BitField
	// Subset of faulty sectors expected to recover on next PoSt
	// Recoveries ∩ Terminated = ∅
	Recoveries *abi.BitField
	// Subset of sectors terminated but not yet removed from partition (excl. from PoSt)
	Terminated *abi.BitField
	// Maps epochs sectors that expire in or before that epoch.
	// An expiration may be an "on-time" scheduled expiration, or early "faulty" expiration.
	// Keys are quantized to last-in-deadline epochs.
	ExpirationsEpochs cid.Cid // AMT[ChainEpoch]ExpirationSet
	// Subset of terminated that were before their committed expiration epoch, by termination epoch.
	// Termination fees have not yet been calculated or paid and associated deals have not yet been
	// canceled but effective power has already been adjusted.
	// Not quantized.
	EarlyTerminated cid.Cid // AMT[ChainEpoch]BitField

	// Power of not-yet-terminated sectors (incl faulty).
	LivePower PowerPair
	// Power of currently-faulty sectors. FaultyPower <= LivePower.
	FaultyPower PowerPair
	// Power of expected-to-recover sectors. RecoveringPower <= FaultyPower.
	RecoveringPower PowerPair
}

// Value type for a pair of raw and QA power.
type PowerPair struct {
	Raw abi.StoragePower
	QA  abi.StoragePower
}

// A set of sectors associated with a given epoch.
func ConstructPartition(emptyArray cid.Cid) *Partition {
	return &Partition{
		Sectors:           abi.NewBitField(),
		Faults:            abi.NewBitField(),
		Recoveries:        abi.NewBitField(),
		Terminated:        abi.NewBitField(),
		ExpirationsEpochs: emptyArray,
		EarlyTerminated:   emptyArray,
		LivePower:         NewPowerPairZero(),
		FaultyPower:       NewPowerPairZero(),
		RecoveringPower:   NewPowerPairZero(),
	}
}

// Live sectors are those that are not terminated (but may be faulty).
func (p *Partition) LiveSectors() (*abi.BitField, error) {
	live, err := bitfield.SubtractBitField(p.Sectors, p.Terminated)
	if err != nil {
		return nil, xerrors.Errorf("failed to compute live sectors: %w", err)
	}
	return live, nil

}

// Active sectors are those that are neither terminated nor faulty, i.e. actively contributing power.
func (p *Partition) ActiveSectors() (*abi.BitField, error) {
	live, err := p.LiveSectors()
	if err != nil {
		return nil, err
	}
	active, err := bitfield.SubtractBitField(live, p.Faults)
	if err != nil {
		return nil, xerrors.Errorf("failed to compute active sectors: %w", err)
	}
	return active, err
}

// Active power is power of non-faulty sectors.
func (p *Partition) ActivePower() PowerPair {
	return p.LivePower.Sub(p.FaultyPower)
}

// AddSectors adds new sectors to the partition.
// The sectors are "live", neither faulty, recovering, nor terminated.
// Each new sector's expiration is scheduled shortly after its target expiration epoch.
func (p *Partition) AddSectors(store adt.Store, sectors []*SectorOnChainInfo, ssize abi.SectorSize, quant QuantSpec) (PowerPair, error) {
	expirations, err := LoadExpirationQueue(store, p.ExpirationsEpochs, quant)
	if err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to load sector expirations: %w", err)
	}
	snos, power, _, err := expirations.AddActiveSectors(sectors, ssize)
	if err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to record new sector expirations: %w", err)
	}
	if p.ExpirationsEpochs, err = expirations.Root(); err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to store sector expirations: %w", err)
	}

	// Update other metadata using the calculated totals.
	if p.Sectors, err = bitfield.MergeBitFields(p.Sectors, snos); err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to record new sector numbers: %w", err)
	}
	p.LivePower = p.LivePower.Add(power)
	// No change to faults, recoveries, or terminations.
	// No change to faulty or recovering power.
	return power, nil
}

// Records a set of sectors as faulty.
// The sectors are added to the Faults bitfield and the FaultyPower is increased.
// The sectors' expirations are rescheduled to the fault expiration epoch, as "early" (if not expiring earlier).
// The sectors must not be already faulty.
// Returns the power of the now-faulty sectors.
func (p *Partition) AddFaults(store adt.Store, sectorNos *abi.BitField, sectors []*SectorOnChainInfo, faultExpiration abi.ChainEpoch,
	ssize abi.SectorSize, quant QuantSpec) (PowerPair, error) {
	var err error
	// Load expiration queue
	queue, err := LoadExpirationQueue(store, p.ExpirationsEpochs, quant)
	if err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to load partition queue: %w", err)
	}

	// Reschedule faults
	power, err := queue.RescheduleAsFaults(faultExpiration, sectors, ssize)
	if err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to add faults to partition queue: %w", err)
	}

	// Save expiration queue
	if p.ExpirationsEpochs, err = queue.Root(); err != nil {
		return NewPowerPairZero(), err
	}

	// Update partition metadata
	if p.Faults, err = bitfield.MergeBitFields(p.Faults, sectorNos); err != nil {
		return NewPowerPairZero(), err
	}
	// The sectors must not have been previously faulty or recovering.
	// No change to recoveries or terminations.

	p.FaultyPower = p.FaultyPower.Add(power)
	// No change to live or recovering power.
	return power, nil
}

// Removes sector numbers from faults and thus from recoveries.
// The sectors are removed from the Faults and Recovering bitfields, and FaultyPower and RecoveringPower reduced.
// The sectors are re-scheduled for expiration shortly after their target expiration epoch.
// Consistency between the partition totals and queue depend on the reported sectors actually being faulty and recovering.
// Returns the power of the now-recovered sectors.
func (p *Partition) RecoverFaults(store adt.Store, recovered *abi.BitField, sectors []*SectorOnChainInfo,
	ssize abi.SectorSize, quant QuantSpec) (PowerPair, error) {
	// Load expiration queue
	queue, err := LoadExpirationQueue(store, p.ExpirationsEpochs, quant)
	if err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to load partition queue: %w", err)
	}
	// Reschedule recovered
	power, err := queue.RescheduleRecovered(sectors, ssize)
	if err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to reschedule faults in partition queue: %w", err)
	}
	// Save expiration queue
	if p.ExpirationsEpochs, err = queue.Root(); err != nil {
		return NewPowerPairZero(), err
	}

	// Update partition metadata
	if newFaults, err := bitfield.SubtractBitField(p.Faults, recovered); err != nil {
		return NewPowerPairZero(), err
	} else {
		p.Faults = newFaults
	}
	if newRecoveries, err := bitfield.SubtractBitField(p.Recoveries, recovered); err != nil {
		return NewPowerPairZero(), err
	} else {
		p.Recoveries = newRecoveries
	}
	// No change to live power.
	p.FaultyPower = p.FaultyPower.Sub(power)
	p.RecoveringPower = p.RecoveringPower.Sub(power)

	return power, err
}

// Adds sectors to recoveries and recovering power. Assumes sectors are faulty but not already present in recoveries.
func (p *Partition) AddRecoveries(sectorNos *abi.BitField, power PowerPair) (err error) {
	empty, err := sectorNos.IsEmpty()
	if err != nil {
		return err
	}
	if empty {
		return nil
	}
	p.Recoveries, err = bitfield.MergeBitFields(p.Recoveries, sectorNos)
	if err != nil {
		return err
	}
	p.RecoveringPower = p.RecoveringPower.Add(power)
	// No change to faults, or terminations.
	// No change to faulty power.
	return nil
}

// Removes sectors from recoveries and recovering power. Assumes sectors are currently faulty and recovering..
func (p *Partition) RemoveRecoveries(sectorNos *abi.BitField, power PowerPair) (err error) {
	empty, err := sectorNos.IsEmpty()
	if err != nil {
		return err
	}
	if empty {
		return nil
	}
	p.Recoveries, err = bitfield.SubtractBitField(p.Recoveries, sectorNos)
	if err != nil {
		return err
	}
	p.RecoveringPower = p.RecoveringPower.Sub(power)
	// No change to faults, or terminations.
	// No change to faulty power.
	return nil
}

// RescheduleExpirations moves expiring sectors to the target expiration.
// The power of the rescheduled sectors is assumed to have not changed since initial scheduling.
func (p *Partition) RescheduleExpirations(store adt.Store, newExpiration abi.ChainEpoch, sectors []*SectorOnChainInfo,
	ssize abi.SectorSize, quant QuantSpec) error {
	expirations, err := LoadExpirationQueue(store, p.ExpirationsEpochs, quant)
	if err != nil {
		return xerrors.Errorf("failed to load sector expirations: %w", err)
	}
	if err = expirations.RescheduleExpirations(newExpiration, sectors, ssize); err != nil {
		return err
	}
	p.ExpirationsEpochs, err = expirations.Root()
	return err
}

// Replaces a number of "old" sectors with new ones.
// The old sectors must not be faulty or terminated.
// If the same sector is both removed and added, this permits rescheduling *with a change in power*,
// unlike RescheduleExpirations.
// Returns the delta to power and pledge requirement.
func (p *Partition) ReplaceSectors(store adt.Store, oldSectors, newSectors []*SectorOnChainInfo,
	ssize abi.SectorSize, quant QuantSpec) (PowerPair, abi.TokenAmount, error) {
	expirations, err := LoadExpirationQueue(store, p.ExpirationsEpochs, quant)
	if err != nil {
		return NewPowerPairZero(), big.Zero(), xerrors.Errorf("failed to load sector expirations: %w", err)
	}
	oldSnos, newSnos, powerDelta, pledgeDelta, err := expirations.ReplaceSectors(oldSectors, newSectors, ssize)
	if err != nil {
		return NewPowerPairZero(), big.Zero(), xerrors.Errorf("failed to replace sector expirations: %w", err)
	}
	if p.ExpirationsEpochs, err = expirations.Root(); err != nil {
		return NewPowerPairZero(), big.Zero(), xerrors.Errorf("failed to save sector expirations: %w", err)
	}

	// Check the sectors being removed are active (alive, not faulty).
	active, err := p.ActiveSectors()
	if err != nil {
		return NewPowerPairZero(), big.Zero(), err
	}
	allActive, err := abi.BitFieldContainsAll(active, oldSnos)
	if err != nil {
		return NewPowerPairZero(), big.Zero(), xerrors.Errorf("failed to check for active sectors: %w", err)
	} else if !allActive {
		return NewPowerPairZero(), big.Zero(), xerrors.Errorf("refusing to replace inactive sectors in %v (active: %v)", oldSnos, active)
	}

	// Update partition metadata.
	if p.Sectors, err = bitfield.SubtractBitField(p.Sectors, oldSnos); err != nil {
		return NewPowerPairZero(), big.Zero(), xerrors.Errorf("failed to remove replaced sectors: %w", err)
	}
	if p.Sectors, err = bitfield.MergeBitFields(p.Sectors, newSnos); err != nil {
		return NewPowerPairZero(), big.Zero(), xerrors.Errorf("failed to add replaced sectors: %w", err)
	}
	p.LivePower = p.LivePower.Add(powerDelta)
	// No change to faults, recoveries, or terminations.
	// No change to faulty or recovering power.
	return powerDelta, pledgeDelta, nil
}

// Record the epoch of any sectors expiring early, for termination fee calculation later.
func (p *Partition) recordEarlyTermination(store adt.Store, epoch abi.ChainEpoch, sectors *bitfield.BitField) error {
	etQueue, err := LoadBitfieldQueue(store, p.EarlyTerminated, NoQuantization)
	if err != nil {
		return xerrors.Errorf("failed to load early termination queue: %w", err)
	}
	if err = etQueue.AddToQueue(epoch, sectors); err != nil {
		return xerrors.Errorf("failed to add to early termination queue: %w", err)
	}
	if p.EarlyTerminated, err = etQueue.Root(); err != nil {
		return xerrors.Errorf("failed to save early termination queue: %w", err)
	}
	return nil
}

// Marks a collection of sectors as terminated.
// The sectors are removed from Faults and Recoveries.
// The epoch of termination is recorded for future termination fee calculation.
func (p *Partition) TerminateSectors(store adt.Store, epoch abi.ChainEpoch, sectors []*SectorOnChainInfo,
	ssize abi.SectorSize, quant QuantSpec) (PowerPair, error) {
	expirations, err := LoadExpirationQueue(store, p.ExpirationsEpochs, quant)
	if err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to load sector expirations: %w", err)
	}
	removed, removedRecovering, err := expirations.RemoveSectors(sectors, p.Faults, p.Recoveries, ssize)
	if err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to remove sector expirations: %w", err)
	}
	if p.ExpirationsEpochs, err = expirations.Root(); err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to save sector expirations: %w", err)
	}

	removedSectors, err := bitfield.MergeBitFields(removed.OnTimeSectors, removed.EarlySectors)
	if err != nil {
		return NewPowerPairZero(), err
	}

	// Record early termination.
	err = p.recordEarlyTermination(store, epoch, removedSectors)
	if err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to record early sector termination: %w", err)
	}

	// Update partition metadata.
	if p.Faults, err = bitfield.SubtractBitField(p.Faults, removedSectors); err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to remove terminated sectors from faults: %w", err)
	}
	if p.Recoveries, err = bitfield.SubtractBitField(p.Recoveries, removedSectors); err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to remove terminated sectors from recoveries: %w", err)
	}
	if p.Terminated, err = bitfield.MergeBitFields(p.Terminated, removedSectors); err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to add terminated sectors: %w", err)
	}

	powerDelta := removed.ActivePower.Add(removed.FaultyPower)
	p.LivePower = p.LivePower.Add(powerDelta)
	p.FaultyPower = p.FaultyPower.Sub(removed.FaultyPower)
	p.RecoveringPower = p.RecoveringPower.Sub(removedRecovering)

	return powerDelta, nil
}

// PopExpiredSectors traverses the expiration queue up to and including some epoch, and marks all expiring
// sectors as terminated.
// Returns the expired sector aggregates.
func (p *Partition) PopExpiredSectors(store adt.Store, until abi.ChainEpoch, quant QuantSpec) (*ExpirationSet, error) {
	expirations, err := LoadExpirationQueue(store, p.ExpirationsEpochs, quant)
	if err != nil {
		return nil, xerrors.Errorf("failed to load expiration queue: %w", err)
	}
	popped, err := expirations.PopUntil(until)
	if err != nil {
		return nil, xerrors.Errorf("failed to pop expiration queue until %d: %w", until, err)
	}
	if p.ExpirationsEpochs, err = expirations.Root(); err != nil {
		return nil, err
	}

	expiredSectors, err := bitfield.MergeBitFields(popped.OnTimeSectors, popped.EarlySectors)
	if err != nil {
		return nil, err
	}

	// There shouldn't be any recovering sectors or power if this is invoked at deadline end.
	// Either the partition was PoSted and the recovering became recovered, or the partition was not PoSted
	// and all recoveries retracted.
	// No recoveries may be posted until the deadline is closed.
	noRecoveries, err := p.Recoveries.IsEmpty()
	if err != nil {
		return nil, err
	} else if !noRecoveries {
		return nil, xerrors.Errorf("unexpected recoveries while processing expirations")
	}
	if !p.RecoveringPower.IsZero() {
		return nil, xerrors.Errorf("unexpected recovering power while processing expirations")
	}
	// Nothing expiring now should have already terminated.
	alreadyTerminated, err := abi.BitFieldContainsAny(p.Terminated, expiredSectors)
	if err != nil {
		return nil, err
	} else if alreadyTerminated {
		return nil, xerrors.Errorf("expiring sectors already terminated")
	}

	// Mark the sectors as terminated and subtract sector power.
	if p.Terminated, err = bitfield.MergeBitFields(p.Terminated, expiredSectors); err != nil {
		return nil, xerrors.Errorf("failed to merge expired sectors: %w", err)
	}
	if p.Faults, err = bitfield.SubtractBitField(p.Faults, expiredSectors); err != nil {
		return nil, err
	}
	p.LivePower = p.LivePower.Sub(popped.ActivePower)
	p.FaultyPower = p.FaultyPower.Sub(popped.FaultyPower)

	// Record the epoch of any sectors expiring early, for termination fee calculation later.
	err = p.recordEarlyTermination(store, until, popped.EarlySectors)
	if err != nil {
		return nil, xerrors.Errorf("failed to record early terminations: %w", err)
	}

	return popped, nil
}

// Marks all non-faulty sectors in the partition as faulty and clears recoveries, updating power memos appropriately.
// All sectors' expirations are rescheduled to the fault expiration, as "early" (if not expiring earlier)
// Returns the power of the newly faulty and failed recovery sectors.
func (p *Partition) RecordMissedPost(store adt.Store, faultExpiration abi.ChainEpoch, quant QuantSpec) (newFaultPower, failedRecoveryPower PowerPair, err error) {
	// Collapse tail of queue into the last entry, and mark all power faulty.
	// Load expiration queue
	queue, err := LoadExpirationQueue(store, p.ExpirationsEpochs, quant)
	if err != nil {
		return NewPowerPairZero(), NewPowerPairZero(), xerrors.Errorf("failed to load partition queue: %w", err)
	}
	if err = queue.RescheduleAllAsFaults(faultExpiration); err != nil {
		return NewPowerPairZero(), NewPowerPairZero(), xerrors.Errorf("failed to reschedule all as faults: %w", err)
	}
	// Save expiration queue
	if p.ExpirationsEpochs, err = queue.Root(); err != nil {
		return NewPowerPairZero(), NewPowerPairZero(), err
	}

	// Compute faulty power for penalization. New faulty power is the total power minus already faulty.
	newFaultPower = p.LivePower.Sub(p.FaultyPower)
	failedRecoveryPower = p.RecoveringPower

	// Update partition metadata
	allFaults, err := p.LiveSectors()
	if err != nil {
		return NewPowerPairZero(), NewPowerPairZero(), err
	}
	p.Faults = allFaults
	p.Recoveries = abi.NewBitField()
	p.FaultyPower = p.LivePower
	p.RecoveringPower = NewPowerPairZero()

	return newFaultPower, failedRecoveryPower, nil
}

func (p *Partition) PopEarlyTerminations(store adt.Store, maxSectors uint64) (result TerminationResult, hasMore bool, err error) {
	stopErr := errors.New("stop iter")

	// Load early terminations.
	earlyTerminatedQ, err := LoadBitfieldQueue(store, p.EarlyTerminated, NoQuantization)
	if err != nil {
		return TerminationResult{}, false, err
	}

	var (
		processed        []uint64
		remainingSectors *abi.BitField
		remainingEpoch   abi.ChainEpoch
	)

	result.PartitionsProcessed = 1
	result.Sectors = make(map[abi.ChainEpoch]*abi.BitField)

	if err = earlyTerminatedQ.ForEach(func(epoch abi.ChainEpoch, sectors *bitfield.BitField) error {
		toProcess := sectors
		count, err := sectors.Count()
		if err != nil {
			return xerrors.Errorf("failed to count early terminations: %w", err)
		}

		limit := maxSectors - result.SectorsProcessed

		if limit < count {
			toProcess, err = sectors.Slice(0, limit)
			if err != nil {
				return xerrors.Errorf("failed to slice early terminations: %w", err)
			}

			rest, err := bitfield.SubtractBitField(sectors, toProcess)
			if err != nil {
				return xerrors.Errorf("failed to subtract processed early terminations: %w", err)
			}
			remainingSectors = rest
			remainingEpoch = epoch

			result.SectorsProcessed += limit
		} else {
			processed = append(processed, uint64(epoch))
			result.SectorsProcessed += count
		}

		result.Sectors[epoch] = toProcess

		if result.SectorsProcessed < maxSectors {
			return nil
		}
		return stopErr
	}); err != nil && err != stopErr {
		return TerminationResult{}, false, xerrors.Errorf("failed to walk early terminations queue: %w", err)
	}

	// Update early terminations
	err = earlyTerminatedQ.BatchDelete(processed)
	if err != nil {
		return TerminationResult{}, false, xerrors.Errorf("failed to remove entries from early terminations queue: %w", err)
	}

	if remainingSectors != nil {
		err = earlyTerminatedQ.Set(uint64(remainingEpoch), remainingSectors)
		if err != nil {
			return TerminationResult{}, false, xerrors.Errorf("failed to update remaining entry early terminations queue: %w", err)
		}
	}

	// Save early terminations.
	p.EarlyTerminated, err = earlyTerminatedQ.Root()
	if err != nil {
		return TerminationResult{}, false, xerrors.Errorf("failed to store early terminations queue: %w", err)
	}
	return result, earlyTerminatedQ.Length() > 0, nil
}

//
// PowerPair
//

func NewPowerPairZero() PowerPair {
	return NewPowerPair(big.Zero(), big.Zero())
}

func NewPowerPair(raw, qa abi.StoragePower) PowerPair {
	return PowerPair{Raw: raw, QA: qa}
}

func (pp *PowerPair) IsZero() bool {
	return pp.Raw.IsZero() && pp.QA.IsZero()
}

func (pp *PowerPair) Add(other PowerPair) PowerPair {
	return PowerPair{
		Raw: big.Add(pp.Raw, other.Raw),
		QA:  big.Add(pp.QA, other.QA),
	}
}

func (pp *PowerPair) Sub(other PowerPair) PowerPair {
	return PowerPair{
		Raw: big.Sub(pp.Raw, other.Raw),
		QA:  big.Sub(pp.QA, other.QA),
	}
}

func (pp *PowerPair) Neg() PowerPair {
	return PowerPair{
		Raw: pp.Raw.Neg(),
		QA:  pp.QA.Neg(),
	}
}

func (pp *PowerPair) Equals(other PowerPair) bool {
	return pp.Raw.Equals(other.Raw) && pp.QA.Equals(other.QA)
}
