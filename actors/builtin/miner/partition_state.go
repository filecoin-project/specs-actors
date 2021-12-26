package miner

import (
	"errors"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	xc "github.com/filecoin-project/go-state-types/exitcode"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/v8/actors/builtin"
	"github.com/filecoin-project/specs-actors/v8/actors/util"
	"github.com/filecoin-project/specs-actors/v8/actors/util/adt"
)

type Partition struct {
	// Sector numbers in this partition, including faulty, unproven, and terminated sectors.
	Sectors bitfield.BitField
	// Unproven sectors in this partition. This bitfield will be cleared on
	// a successful window post (or at the end of the partition's next
	// deadline). At that time, any still unproven sectors will be added to
	// the faulty sector bitfield.
	Unproven bitfield.BitField
	// Subset of sectors detected/declared faulty and not yet recovered (excl. from PoSt).
	// Faults ∩ Terminated = ∅
	Faults bitfield.BitField
	// Subset of faulty sectors expected to recover on next PoSt
	// Recoveries ∩ Terminated = ∅
	Recoveries bitfield.BitField
	// Subset of sectors terminated but not yet removed from partition (excl. from PoSt)
	Terminated bitfield.BitField
	// Maps epochs sectors that expire in or before that epoch.
	// An expiration may be an "on-time" scheduled expiration, or early "faulty" expiration.
	// Keys are quantized to last-in-deadline epochs.
	ExpirationsEpochs cid.Cid // AMT[ChainEpoch]ExpirationSet
	// Subset of terminated that were before their committed expiration epoch, by termination epoch.
	// Termination fees have not yet been calculated or paid and associated deals have not yet been
	// canceled but effective power has already been adjusted.
	// Not quantized.
	EarlyTerminated cid.Cid // AMT[ChainEpoch]BitField
}

// Bitwidth of AMTs determined empirically from mutation patterns and projections of mainnet data.
const PartitionExpirationAmtBitwidth = 4
const PartitionEarlyTerminationArrayAmtBitwidth = 3

// Value type for a pair of raw and QA power.
type PowerPair struct {
	Raw abi.StoragePower
	QA  abi.StoragePower
}

// A set of sectors associated with a given epoch.
func ConstructPartition(store adt.Store) (*Partition, error) {
	emptyExpirationArrayRoot, err := adt.StoreEmptyArray(store, PartitionExpirationAmtBitwidth)
	if err != nil {
		return nil, err
	}
	emptyEarlyTerminationArrayRoot, err := adt.StoreEmptyArray(store, PartitionEarlyTerminationArrayAmtBitwidth)
	if err != nil {
		return nil, err
	}

	return &Partition{
		Sectors:           bitfield.New(),
		Unproven:          bitfield.New(),
		Faults:            bitfield.New(),
		Recoveries:        bitfield.New(),
		Terminated:        bitfield.New(),
		ExpirationsEpochs: emptyExpirationArrayRoot,
		EarlyTerminated:   emptyEarlyTerminationArrayRoot,
		//LivePower:         NewPowerPairZero(),
		//UnprovenPower:     NewPowerPairZero(),
		//FaultyPower:       NewPowerPairZero(),
		//RecoveringPower:   NewPowerPairZero(),
	}, nil
}

// Live sectors are those that are not terminated (but may be faulty).
func (p *Partition) LiveSectors() (bitfield.BitField, error) {
	live, err := bitfield.SubtractBitField(p.Sectors, p.Terminated)
	if err != nil {
		return bitfield.BitField{}, xerrors.Errorf("failed to compute live sectors: %w", err)
	}
	return live, nil

}

// Active sectors are those that are neither terminated nor faulty nor unproven, i.e. actively contributing power.
func (p *Partition) ActiveSectors() (bitfield.BitField, error) {
	live, err := p.LiveSectors()
	if err != nil {
		return bitfield.BitField{}, err
	}
	nonFaulty, err := bitfield.SubtractBitField(live, p.Faults)
	if err != nil {
		return bitfield.BitField{}, xerrors.Errorf("failed to compute active sectors: %w", err)
	}
	active, err := bitfield.SubtractBitField(nonFaulty, p.Unproven)
	if err != nil {
		return bitfield.BitField{}, xerrors.Errorf("failed to compute active sectors: %w", err)
	}
	return active, err
}

//// Active power is power of non-faulty sectors.
//func (p *Partition) ActivePower() PowerPair {
//	return p.LivePower.Sub(p.FaultyPower).Sub(p.UnprovenPower)
//}

// AddSectors adds new sectors to the partition.
// The sectors are "live", neither faulty, recovering, nor terminated.
// Each new sector's expiration is scheduled shortly after its target expiration epoch.
// If proven is false, the sectors are added to the partition's unproven set.
func (p *Partition) AddSectors(store adt.Store, proven bool, sectors []*SectorOnChainInfo, quant builtin.QuantSpec) error {
	expirations, err := LoadExpirationQueue(store, p.ExpirationsEpochs, quant, PartitionExpirationAmtBitwidth)
	if err != nil {
		return xerrors.Errorf("failed to load sector expirations: %w", err)
	}
	snos, _, err := expirations.AddActiveSectors(sectors)
	if err != nil {
		return xerrors.Errorf("failed to record new sector expirations: %w", err)
	}
	if p.ExpirationsEpochs, err = expirations.Root(); err != nil {
		return xerrors.Errorf("failed to store sector expirations: %w", err)
	}

	if contains, err := util.BitFieldContainsAny(p.Sectors, snos); err != nil {
		return xerrors.Errorf("failed to check if any new sector was already in the partition: %w", err)
	} else if contains {
		return xerrors.Errorf("not all added sectors are new")
	}

	// Update other metadata using the calculated totals.
	if p.Sectors, err = bitfield.MergeBitFields(p.Sectors, snos); err != nil {
		return xerrors.Errorf("failed to record new sector numbers: %w", err)
	}

	if !proven {
		if p.Unproven, err = bitfield.MergeBitFields(p.Unproven, snos); err != nil {
			return xerrors.Errorf("failed to update unproven sectors bitfield: %w", err)
		}
	}

	// check invariants
	if err := p.ValidateState(); err != nil {
		return err
	}

	// No change to faults, recoveries, or terminations.
	// No change to faulty or recovering power.
	return nil
}

// marks a set of sectors faulty
func (p *Partition) addFaults(
	store adt.Store, sectorNos bitfield.BitField, sectors []*SectorOnChainInfo, faultExpiration abi.ChainEpoch,
	ssize abi.SectorSize, quant builtin.QuantSpec,
) (activeCountDelta int64, newFaultCount uint64, err error) {
	// Load expiration queue
	queue, err := LoadExpirationQueue(store, p.ExpirationsEpochs, quant, PartitionExpirationAmtBitwidth)
	if err != nil {
		return 0, 0, xerrors.Errorf("failed to load partition queue: %w", err)
	}

	// Reschedule faults
	if err = queue.RescheduleAsFaults(faultExpiration, sectors); err != nil {
		return 0, 0, xerrors.Errorf("failed to add faults to partition queue: %w", err)
	}

	// Save expiration queue
	if p.ExpirationsEpochs, err = queue.Root(); err != nil {
		return 0, 0, err
	}

	// Update partition metadata
	if p.Faults, err = bitfield.MergeBitFields(p.Faults, sectorNos); err != nil {
		return 0, 0, err
	}

	// The sectors must not have been previously faulty or recovering.
	// No change to recoveries or terminations.
	//p.FaultyPower = p.FaultyPower.Add(newFaultyPower)

	// Once marked faulty, sectors are moved out of the unproven set.
	unproven, err := bitfield.IntersectBitField(sectorNos, p.Unproven)
	if err != nil {
		return 0, 0, xerrors.Errorf("failed to intersect faulty sector IDs with unproven sector IDs: %w", err)
	}
	p.Unproven, err = bitfield.SubtractBitField(p.Unproven, unproven)
	if err != nil {
		return 0, 0, xerrors.Errorf("failed to subtract faulty sectors from unproven sector IDs: %w", err)
	}

	newFaultCount = uint64(len(sectors))
	activeCountDelta = -int64(newFaultCount)
	if unprovenInfos, err := selectSectors(sectors, unproven); err != nil {
		return 0, 0, xerrors.Errorf("failed to select unproven sectors: %w", err)
	} else if len(unprovenInfos) > 0 {
		lostUnprovenCount := int64(len(unprovenInfos))
		activeCountDelta = activeCountDelta + lostUnprovenCount
	}

	// check invariants
	if err := p.ValidateState(); err != nil {
		return 0, 0, err
	}

	// No change to live or recovering power.
	return activeCountDelta, newFaultCount, nil
}

// Declares a set of sectors faulty. Already faulty sectors are ignored,
// terminated sectors are skipped, and recovering sectors are reverted to
// faulty.
//
// - New faults are added to the Faults bitfield and the FaultyPower is increased.
// - The sectors' expirations are rescheduled to the fault expiration epoch, as "early" (if not expiring earlier).
//
// Returns the power of the now-faulty sectors.
func (p *Partition) RecordFaults(
	store adt.Store, sectors Sectors, sectorNos bitfield.BitField, faultExpirationEpoch abi.ChainEpoch,
	ssize abi.SectorSize, quant builtin.QuantSpec,
) (newFaults bitfield.BitField, activeCountDelta int64, newFaultCount uint64, err error) {
	err = validatePartitionContainsSectors(p, sectorNos)
	if err != nil {
		return bitfield.BitField{}, 0, 0, xc.ErrIllegalArgument.Wrapf("failed fault declaration: %w", err)
	}

	// Split declarations into declarations of new faults, and retraction of declared recoveries.
	retractedRecoveries, err := bitfield.IntersectBitField(p.Recoveries, sectorNos)
	if err != nil {
		return bitfield.BitField{}, 0, 0, xerrors.Errorf("failed to intersect sectors with recoveries: %w", err)
	}

	newFaults, err = bitfield.SubtractBitField(sectorNos, retractedRecoveries)
	if err != nil {
		return bitfield.BitField{}, 0, 0, xerrors.Errorf("failed to subtract recoveries from sectors: %w", err)
	}

	// Ignore any terminated sectors and previously declared or detected faults
	newFaults, err = bitfield.SubtractBitField(newFaults, p.Terminated)
	if err != nil {
		return bitfield.BitField{}, 0, 0, xerrors.Errorf("failed to subtract terminations from faults: %w", err)
	}
	newFaults, err = bitfield.SubtractBitField(newFaults, p.Faults)
	if err != nil {
		return bitfield.BitField{}, 0, 0, xerrors.Errorf("failed to subtract existing faults from faults: %w", err)
	}

	// Add new faults to state.
	newFaultCount = 0
	activeCountDelta = 0
	if newFaultSectors, err := sectors.Load(newFaults); err != nil {
		return bitfield.BitField{}, 0, 0, xerrors.Errorf("failed to load fault sectors: %w", err)
	} else if len(newFaultSectors) > 0 {
		activeCountDelta, newFaultCount, err = p.addFaults(store, newFaults, newFaultSectors, faultExpirationEpoch, ssize, quant)
		if err != nil {
			return bitfield.BitField{}, 0, 0, xerrors.Errorf("failed to add faults: %w", err)
		}
	}

	// Remove faulty recoveries from state.
	if retractedRecoverySectors, err := sectors.Load(retractedRecoveries); err != nil {
		return bitfield.BitField{}, 0, 0, xerrors.Errorf("failed to load recovery sectors: %w", err)
	} else if len(retractedRecoverySectors) > 0 {
		err = p.removeRecoveries(retractedRecoveries)
		if err != nil {
			return bitfield.BitField{}, 0, 0, xerrors.Errorf("failed to remove recoveries: %w", err)
		}
	}

	// check invariants
	if err := p.ValidateState(); err != nil {
		return bitfield.BitField{}, 0, 0, err
	}

	return newFaults, activeCountDelta, newFaultCount, nil
}

// Removes sector numbers from faults and thus from recoveries.
// The sectors are removed from the Faults and Recovering bitfields, and FaultyPower and RecoveringPower reduced.
// The sectors are re-scheduled for expiration shortly after their target expiration epoch.
// Returns the count of the now-recovered sectors.
func (p *Partition) RecoverFaults(store adt.Store, sectors Sectors, ssize abi.SectorSize, quant builtin.QuantSpec) (uint64, error) {
	// Process recoveries, assuming the proof will be successful.
	// This similarly updates state.
	recoveredSectors, err := sectors.Load(p.Recoveries)
	if err != nil {
		return 0, xerrors.Errorf("failed to load recovered sectors: %w", err)
	}
	// Load expiration queue
	queue, err := LoadExpirationQueue(store, p.ExpirationsEpochs, quant, PartitionExpirationAmtBitwidth)
	if err != nil {
		return 0, xerrors.Errorf("failed to load partition queue: %w", err)
	}
	// Reschedule recovered
	power, err := queue.RescheduleRecovered(recoveredSectors)
	if err != nil {
		return 0, xerrors.Errorf("failed to reschedule faults in partition queue: %w", err)
	}
	// Save expiration queue
	if p.ExpirationsEpochs, err = queue.Root(); err != nil {
		return 0, err
	}

	// Update partition metadata
	if newFaults, err := bitfield.SubtractBitField(p.Faults, p.Recoveries); err != nil {
		return 0, err
	} else {
		p.Faults = newFaults
	}
	p.Recoveries = bitfield.New()

	// check invariants
	if err := p.ValidateState(); err != nil {
		return 0, err
	}

	return power, err
}

// Activates unproven sectors, returning the activated power.
func (p *Partition) ActivateUnproven() (uint64, error) {
	newCount, err := p.Unproven.Count()
	if err != nil {
		return 0, xerrors.Errorf("failed to count unproven sectors: %w", err)
	}

	//p.UnprovenPower = NewPowerPairZero()
	p.Unproven = bitfield.New()
	return newCount, nil
}

// Declares sectors as recovering. Non-faulty and already recovering sectors will be skipped.
func (p *Partition) DeclareFaultsRecovered(sectorNos bitfield.BitField) (err error) {
	// Check that the declared sectors are actually assigned to the partition.
	err = validatePartitionContainsSectors(p, sectorNos)
	if err != nil {
		return xc.ErrIllegalArgument.Wrapf("failed fault declaration: %w", err)
	}

	// Ignore sectors not faulty or already declared recovered
	recoveries, err := bitfield.IntersectBitField(sectorNos, p.Faults)
	if err != nil {
		return xerrors.Errorf("failed to intersect recoveries with faults: %w", err)
	}
	recoveries, err = bitfield.SubtractBitField(recoveries, p.Recoveries)
	if err != nil {
		return xerrors.Errorf("failed to subtract existing recoveries: %w", err)
	}

	// Record the new recoveries for processing at Window PoSt or deadline cron.
	p.Recoveries, err = bitfield.MergeBitFields(p.Recoveries, recoveries)
	if err != nil {
		return err
	}

	// check invariants
	if err := p.ValidateState(); err != nil {
		return err
	}

	// No change to faults, or terminations.
	// No change to faulty power.
	// No change to unproven power/sectors.
	return nil
}

// Removes sectors from recoveries. Assumes sectors are currently faulty and recovering..
func (p *Partition) removeRecoveries(sectorNos bitfield.BitField) (err error) {
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
	// No change to faults, or terminations.
	// No change to faulty power.
	// No change to unproven or unproven power.
	return nil
}

// Replaces a number of "old" sectors with new ones.
// The old sectors must not be faulty, terminated, or unproven.
// If the same sector is both removed and added, this permits rescheduling *with a change in power*,
// unlike RescheduleExpirations.
// Returns the delta pledge requirement.
func (p *Partition) ReplaceSectors(store adt.Store, oldSectors, newSectors []*SectorOnChainInfo, quant builtin.QuantSpec) (abi.TokenAmount, error) {
	expirations, err := LoadExpirationQueue(store, p.ExpirationsEpochs, quant, PartitionExpirationAmtBitwidth)
	if err != nil {
		return big.Zero(), xerrors.Errorf("failed to load sector expirations: %w", err)
	}
	oldSnos, newSnos, pledgeDelta, err := expirations.ReplaceSectors(oldSectors, newSectors)
	if err != nil {
		return big.Zero(), xerrors.Errorf("failed to replace sector expirations: %w", err)
	}
	if p.ExpirationsEpochs, err = expirations.Root(); err != nil {
		return big.Zero(), xerrors.Errorf("failed to save sector expirations: %w", err)
	}

	// Check the sectors being removed are active (alive, not faulty).
	active, err := p.ActiveSectors()
	if err != nil {
		return big.Zero(), err
	}
	allActive, err := util.BitFieldContainsAll(active, oldSnos)
	if err != nil {
		return big.Zero(), xerrors.Errorf("failed to check for active sectors: %w", err)
	} else if !allActive {
		return big.Zero(), xerrors.Errorf("refusing to replace inactive sectors in %v (active: %v)", oldSnos, active)
	}

	// Update partition metadata.
	if p.Sectors, err = bitfield.SubtractBitField(p.Sectors, oldSnos); err != nil {
		return big.Zero(), xerrors.Errorf("failed to remove replaced sectors: %w", err)
	}
	if p.Sectors, err = bitfield.MergeBitFields(p.Sectors, newSnos); err != nil {
		return big.Zero(), xerrors.Errorf("failed to add replaced sectors: %w", err)
	}

	// check invariants
	if err := p.ValidateState(); err != nil {
		return big.Zero(), err
	}

	// No change to faults, recoveries, or terminations.
	// No change to faulty or recovering power.
	return pledgeDelta, nil
}

// Record the epoch of any sectors expiring early, for termination fee calculation later.
func (p *Partition) recordEarlyTermination(store adt.Store, epoch abi.ChainEpoch, sectors bitfield.BitField) error {
	etQueue, err := LoadBitfieldQueue(store, p.EarlyTerminated, builtin.NoQuantization, PartitionEarlyTerminationArrayAmtBitwidth)
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
func (p *Partition) TerminateSectors(store adt.Store, sectors Sectors, epoch abi.ChainEpoch, sectorNos bitfield.BitField,
	quant builtin.QuantSpec) (*ExpirationSet, error) {
	liveSectors, err := p.LiveSectors()
	if err != nil {
		return nil, err
	}
	if contains, err := util.BitFieldContainsAll(liveSectors, sectorNos); err != nil {
		return nil, xc.ErrIllegalArgument.Wrapf("failed to intersect live sectors with terminating sectors: %w", err)
	} else if !contains {
		return nil, xc.ErrIllegalArgument.Wrapf("can only terminate live sectors")
	}

	sectorInfos, err := sectors.Load(sectorNos)
	if err != nil {
		return nil, err
	}
	expirations, err := LoadExpirationQueue(store, p.ExpirationsEpochs, quant, PartitionExpirationAmtBitwidth)
	if err != nil {
		return nil, xerrors.Errorf("failed to load sector expirations: %w", err)
	}
	removed, err := expirations.RemoveSectors(sectorInfos, p.Faults)
	if err != nil {
		return nil, xerrors.Errorf("failed to remove sector expirations: %w", err)
	}
	if p.ExpirationsEpochs, err = expirations.Root(); err != nil {
		return nil, xerrors.Errorf("failed to save sector expirations: %w", err)
	}

	removedSectors, err := bitfield.MergeBitFields(removed.OnTimeSectors, removed.EarlySectors)
	if err != nil {
		return nil, err
	}

	// Record early termination.
	err = p.recordEarlyTermination(store, epoch, removedSectors)
	if err != nil {
		return nil, xerrors.Errorf("failed to record early sector termination: %w", err)
	}

	unprovenNos, err := bitfield.IntersectBitField(removedSectors, p.Unproven)
	if err != nil {
		return nil, xerrors.Errorf("failed to determine unproven sectors: %w", err)
	}

	// Update partition metadata.
	if p.Faults, err = bitfield.SubtractBitField(p.Faults, removedSectors); err != nil {
		return nil, xerrors.Errorf("failed to remove terminated sectors from faults: %w", err)
	}
	if p.Recoveries, err = bitfield.SubtractBitField(p.Recoveries, removedSectors); err != nil {
		return nil, xerrors.Errorf("failed to remove terminated sectors from recoveries: %w", err)
	}
	if p.Terminated, err = bitfield.MergeBitFields(p.Terminated, removedSectors); err != nil {
		return nil, xerrors.Errorf("failed to add terminated sectors: %w", err)
	}
	if p.Unproven, err = bitfield.SubtractBitField(p.Unproven, unprovenNos); err != nil {
		return nil, xerrors.Errorf("failed to remove unproven sectors: %w", err)
	}

	//p.LivePower = p.LivePower.Sub(removed.ActiveCount).Sub(removed.FaultyCount)
	//p.FaultyPower = p.FaultyPower.Sub(removed.FaultyCount)
	//p.RecoveringPower = p.RecoveringPower.Sub(removedRecovering)
	//if unprovenInfos, err := selectSectors(sectorInfos, unprovenNos); err != nil {
	//	return nil, xerrors.Errorf("failed to select unproven sectors: %w", err)
	//} else {
	//removedUnprovenPower := SectorsPower(ssize, len(unprovenInfos))
	//p.UnprovenPower = p.UnprovenPower.Sub(removedUnprovenPower)
	//removed.ActiveCount = removed.ActiveCount.Sub(removedUnprovenPower)
	//}
	removedUnprovenCount, err := unprovenNos.Count()
	if err != nil {
		return nil, xerrors.Errorf("failed to count removed unproven sectors: %w", err)
	}
	removed.ActiveCount -= removedUnprovenCount

	// check invariants
	if err := p.ValidateState(); err != nil {
		return nil, err
	}
	return removed, nil
}

// PopExpiredSectors traverses the expiration queue up to and including some epoch, and marks all expiring
// sectors as terminated.
//
// This cannot be called while there are unproven sectors.
//
// Returns the expired sector aggregates.
func (p *Partition) PopExpiredSectors(store adt.Store, until abi.ChainEpoch, quant builtin.QuantSpec) (*ExpirationSet, error) {
	// This is a sanity check to make sure we handle proofs _before_
	// handling sector expirations.
	if noUnproven, err := p.Unproven.IsEmpty(); err != nil {
		return nil, xerrors.Errorf("failed to determine if partition has unproven sectors: %w", err)
	} else if !noUnproven {
		return nil, xerrors.Errorf("cannot pop expired sectors from a partition with unproven sectors: %w", err)
	}

	expirations, err := LoadExpirationQueue(store, p.ExpirationsEpochs, quant, PartitionExpirationAmtBitwidth)
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
	// Nothing expiring now should have already terminated.
	alreadyTerminated, err := util.BitFieldContainsAny(p.Terminated, expiredSectors)
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

	// Record the epoch of any sectors expiring early, for termination fee calculation later.
	err = p.recordEarlyTermination(store, until, popped.EarlySectors)
	if err != nil {
		return nil, xerrors.Errorf("failed to record early terminations: %w", err)
	}

	// check invariants
	if err := p.ValidateState(); err != nil {
		return nil, err
	}

	return popped, nil
}

// Marks all non-faulty sectors in the partition as faulty and clears recoveries, updating power memos appropriately.
// All sectors' expirations are rescheduled to the fault expiration, as "early" (if not expiring earlier)
// Returns the power delta, power that should be penalized (new faults + failed recoveries), and newly faulty power.
func (p *Partition) RecordMissedPost(
	store adt.Store, faultExpiration abi.ChainEpoch, quant builtin.QuantSpec,
) (activeCountDelta int64, penalizedCount, newFaultCount uint64, err error) {
	// Collapse tail of queue into the last entry, and mark all power faulty.
	// Load expiration queue
	queue, err := LoadExpirationQueue(store, p.ExpirationsEpochs, quant, PartitionExpirationAmtBitwidth)
	if err != nil {
		return 0, 0, 0, xerrors.Errorf("failed to load partition queue: %w", err)
	}
	if err = queue.RescheduleAllAsFaults(faultExpiration); err != nil {
		return 0, 0, 0, xerrors.Errorf("failed to reschedule all as faults: %w", err)
	}
	// Save expiration queue
	if p.ExpirationsEpochs, err = queue.Root(); err != nil {
		return 0, 0, 0, err
	}

	// Compute sector state changes.
	liveSectors, err := p.LiveSectors()
	if err != nil {
		return 0, 0, 0, xerrors.Errorf("failed to load live sectors: %w", err)
	}
	liveCount, err := liveSectors.Count()
	if err != nil {
		return 0, 0, 0, xerrors.Errorf("failed to count live sectors: %w", err)
	}
	faultCount, err := p.Faults.Count()
	if err != nil {
		return 0, 0, 0, xerrors.Errorf("failed to count faults: %w", err)
	}
	recoveringCount, err := p.Recoveries.Count()
	if err != nil {
		return 0, 0, 0, xerrors.Errorf("failed to count recoveries: %w", err)
	}
	unprovenCount, err := p.Unproven.Count()
	if err != nil {
		return 0, 0, 0, xerrors.Errorf("failed to count unproven: %w", err)
	}
	// New faulty sectors are the live sectors minus already faulty.
	newFaultCount = liveCount - faultCount
	// Penalized sectors are the newly faulty ones, plus the failed recoveries.
	penalizedCount = recoveringCount + newFaultCount
	// The power delta is -(newFaultyPower-unproven), because unproven power
	// was never activated in the first place.
	activeCountDelta = -int64(newFaultCount - unprovenCount)

	// Update partition metadata
	allFaults, err := p.LiveSectors()
	if err != nil {
		return 0, 0, 0, err
	}
	p.Faults = allFaults
	p.Recoveries = bitfield.New()
	p.Unproven = bitfield.New()

	// check invariants
	if err := p.ValidateState(); err != nil {
		return 0, 0, 0, err
	}

	return activeCountDelta, penalizedCount, newFaultCount, nil
}

func (p *Partition) PopEarlyTerminations(store adt.Store, maxSectors uint64) (result TerminationResult, hasMore bool, err error) {
	stopErr := errors.New("stop iter")

	// Load early terminations.
	earlyTerminatedQ, err := LoadBitfieldQueue(store, p.EarlyTerminated, builtin.NoQuantization, PartitionEarlyTerminationArrayAmtBitwidth)
	if err != nil {
		return TerminationResult{}, false, err
	}

	var (
		processed        []uint64
		hasRemaining     bool
		remainingSectors bitfield.BitField
		remainingEpoch   abi.ChainEpoch
	)

	result.PartitionsProcessed = 1
	result.Sectors = make(map[abi.ChainEpoch]bitfield.BitField)

	if err = earlyTerminatedQ.ForEach(func(epoch abi.ChainEpoch, sectors bitfield.BitField) error {
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
			hasRemaining = true
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
	err = earlyTerminatedQ.BatchDelete(processed, true)
	if err != nil {
		return TerminationResult{}, false, xerrors.Errorf("failed to remove entries from early terminations queue: %w", err)
	}

	if hasRemaining {
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

	// check invariants
	if err := p.ValidateState(); err != nil {
		return TerminationResult{}, false, err
	}

	return result, earlyTerminatedQ.Length() > 0, nil
}

// Discovers how skipped faults declared during post intersect with existing faults and recoveries, records the
// new faults in state.
// Returns the amount of power newly faulty, or declared recovered but faulty again.
//
// - Skipped faults that are not in the provided partition triggers an error.
// - Skipped faults that are already declared (but not delcared recovered) are ignored.
func (p *Partition) RecordSkippedFaults(
	store adt.Store, sectors Sectors, ssize abi.SectorSize, quant builtin.QuantSpec, faultExpiration abi.ChainEpoch, skipped bitfield.BitField,
) (activeCountDelta int64, newFaultCount, retractedRecoveryCount uint64, hasNewFaults bool, err error) {
	empty, err := skipped.IsEmpty()
	if err != nil {
		return 0, 0, 0, false, xc.ErrIllegalArgument.Wrapf("failed to check if skipped sectors is empty: %w", err)
	}
	if empty {
		return 0, 0, 0,  false, nil
	}

	// Check that the declared sectors are actually in the partition.
	contains, err := util.BitFieldContainsAll(p.Sectors, skipped)
	if err != nil {
		return 0, 0, 0,  false, xerrors.Errorf("failed to check if skipped faults are in partition: %w", err)
	} else if !contains {
		return 0, 0, 0,  false, xc.ErrIllegalArgument.Wrapf("skipped faults contains sectors outside partition")
	}

	// Find all skipped faults that have been labeled recovered
	retractedRecoveries, err := bitfield.IntersectBitField(p.Recoveries, skipped)
	if err != nil {
		return 0, 0, 0, false, xerrors.Errorf("failed to intersect sectors with recoveries: %w", err)
	}
	retractedRecoverySectors, err := sectors.Load(retractedRecoveries)
	if err != nil {
		return 0, 0, 0, false, xerrors.Errorf("failed to load sectors: %w", err)
	}
	retractedRecoveryCount = uint64(len(retractedRecoverySectors))

	// Ignore skipped faults that are already faults or terminated.
	newFaults, err := bitfield.SubtractBitField(skipped, p.Terminated)
	if err != nil {
		return 0, 0, 0, false, xerrors.Errorf("failed to subtract terminations from skipped: %w", err)
	}
	newFaults, err = bitfield.SubtractBitField(newFaults, p.Faults)
	if err != nil {
		return 0, 0, 0,  false, xerrors.Errorf("failed to subtract existing faults from skipped: %w", err)
	}
	newFaultSectors, err := sectors.Load(newFaults)
	if err != nil {
		return 0, 0, 0,  false, xerrors.Errorf("failed to load sectors: %w", err)
	}

	// Record new faults
	activeCountDelta, newFaultCount, err = p.addFaults(store, newFaults, newFaultSectors, faultExpiration, ssize, quant)
	if err != nil {
		return 0, 0, 0,  false, xerrors.Errorf("failed to add skipped faults: %w", err)
	}

	// Remove faulty recoveries
	err = p.removeRecoveries(retractedRecoveries)
	if err != nil {
		return 0, 0, 0,  false, xerrors.Errorf("failed to remove recoveries: %w", err)
	}

	// check invariants
	if err := p.ValidateState(); err != nil {
		return 0, 0, 0,  false, err
	}

	return activeCountDelta, newFaultCount, retractedRecoveryCount, len(newFaultSectors) > 0, nil
}

// Test all invariants hold
func (p *Partition) ValidateState() error {
	// Merge unproven and faults for checks
	merge, err := bitfield.MultiMerge(p.Unproven, p.Faults)
	if err != nil {
		return err
	}

	// Unproven or faulty sectors should not be in terminated
	if containsAny, err := util.BitFieldContainsAny(p.Terminated, merge); err != nil {
		return err
	} else if containsAny {
		return xerrors.Errorf("Partition left with terminated sectors in multiple states: %v", p)
	}

	// Merge terminated into set for checks
	merge, err = bitfield.MergeBitFields(merge, p.Terminated)
	if err != nil {
		return err
	}

	// All merged sectors should exist in p.Sectors
	if containsAll, err := util.BitFieldContainsAll(p.Sectors, merge); err != nil {
		return err
	} else if !containsAll {
		return xerrors.Errorf("Partition left with invalid sector state: %v", p)
	}

	// All recoveries should exist in p.Faults
	if containsAll, err := util.BitFieldContainsAll(p.Faults, p.Recoveries); err != nil {
		return err
	} else if !containsAll {
		return xerrors.Errorf("Partition left with invalid recovery state: %v", p)
	}

	return nil
}

//
// PowerPair
//

// FIXME delete
func NewPowerPairZero() PowerPair {
	return NewPowerPair(big.Zero(), big.Zero())
}

func NewPowerPair(raw, qa abi.StoragePower) PowerPair {
	return PowerPair{Raw: raw, QA: qa}
}

func (pp PowerPair) IsZero() bool {
	return pp.Raw.IsZero() && pp.QA.IsZero()
}

func (pp PowerPair) Add(other PowerPair) PowerPair {
	return PowerPair{
		Raw: big.Add(pp.Raw, other.Raw),
		QA:  big.Add(pp.QA, other.QA),
	}
}

func (pp PowerPair) Sub(other PowerPair) PowerPair {
	return PowerPair{
		Raw: big.Sub(pp.Raw, other.Raw),
		QA:  big.Sub(pp.QA, other.QA),
	}
}

func (pp PowerPair) Neg() PowerPair {
	return PowerPair{
		Raw: pp.Raw.Neg(),
		QA:  pp.QA.Neg(),
	}
}

func (pp PowerPair) Mul(m big.Int) PowerPair {
	return PowerPair{
		Raw: big.Mul(pp.Raw, m),
		QA:  big.Mul(pp.QA, m),
	}
}

func (pp *PowerPair) Equals(other PowerPair) bool {
	return pp.Raw.Equals(other.Raw) && pp.QA.Equals(other.QA)
}
