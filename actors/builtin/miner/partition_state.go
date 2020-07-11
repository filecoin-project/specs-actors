package miner

import (
	"io"

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
	// Subset of terminated that were before their committed expiration epoch, by termination epoch.
	// Termination fees have not yet been calculated or paid but effective
	// power has already been adjusted.
	EarlyTerminated cid.Cid // AMT[ChainEpoch]BitField
	// Maps epochs sectors that expire in that epoch.
	// The expiration may be an "on-time" scheduled expiration, or early "faulty" expiration
	ExpirationsEpochs cid.Cid // AMT[ChainEpoch]ExpirationSet

	// Power of not-yet-terminated sectors (incl faulty)
	TotalPower PowerPair
	// Power of currently-faulty sectors. FaultyPower <= TotalPower.
	FaultyPower PowerPair
	// Power of expected-to-recover sectors. RecoveringPower <= FaultyPower.
	RecoveringPower PowerPair
	// Sum of initial pledge of sectors
	TotalPledge abi.TokenAmount
}

// Value type for a pair of raw and QA power.
type PowerPair struct {
	Raw abi.StoragePower
	QA  abi.StoragePower
}

func ConstructPartition(emptyArray cid.Cid) *Partition {
	return &Partition{
		Sectors:           abi.NewBitField(),
		Faults:            abi.NewBitField(),
		Recoveries:        abi.NewBitField(),
		Terminated:        abi.NewBitField(),
		EarlyTerminated:   emptyArray,
		ExpirationsEpochs: emptyArray,
		TotalPower:        NewPowerPairZero(),
		FaultyPower:       NewPowerPairZero(),
		RecoveringPower:   NewPowerPairZero(),
		TotalPledge:       big.Zero(),
	}
}

func (p *Partition) AddSectors(store adt.Store, sectors []*SectorOnChainInfo, ssize abi.SectorSize) error {
	// Add the sectors & pledge.
	for _, sector := range sectors {
		p.Sectors.Set(uint64(sector.SectorNumber))
		p.TotalPledge = big.Add(p.TotalPledge, sector.InitialPledge)
	}

	// Update the expirations (and power).
	expirations, err := loadExpirationQueue(store, p.ExpirationsEpochs)
	if err != nil {
		return xerrors.Errorf("failed to load sector expirations: %w", err)
	}

	for _, group := range groupSectorsByExpiration(ssize, sectors) {
		// Update partition total power (saves redundant arithmetic over doing it in the initial loop over sectors).
		p.TotalPower = p.TotalPower.Add(group.power)

		// Update expiration queue.
		if err = expirations.AddNewSectors(group.epoch, bitfield.NewFromSet(group.sectors), group.power); err != nil {
			return xerrors.Errorf("failed to record new sector expirations: %w", err)
		}
	}
	p.ExpirationsEpochs, err = expirations.Root()
	return err
}

// Records a set of sectors as faulty.
// The sectors are added to the Faults bitfield and the FaultyPower is increased.
// The sectors' expirations are rescheduled to the fault expiration epoch, as "early" (if not expiring earlier)
// Returns the power of the now-faulty sectors.
func (p *Partition) AddFaults(store adt.Store, sectorNos *abi.BitField, sectors []*SectorOnChainInfo, faultExpiration abi.ChainEpoch, ssize abi.SectorSize) (PowerPair, error) {
	var err error
	// Load expiration queue
	queue, err := loadExpirationQueue(store, p.ExpirationsEpochs)
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
	p.FaultyPower = p.FaultyPower.Add(power)

	return power, nil
}

// Records all sectors in the partition as faulty.
// The sectors are added to the Faults bitfield and the FaultyPower is set equal to total power.
// All sectors' expirations are rescheduled to the fault expiration, as "early" (if not expiring earlier)
// Returns the power of the newly-faulty sectors.
func (p *Partition) AddAllAsFaults(store adt.Store, faultExpiration abi.ChainEpoch) (PowerPair, error) {
	// Collapse tail of queue into the last entry, and mark all power faulty

	var err error
	// Load expiration queue
	queue, err := loadExpirationQueue(store, p.ExpirationsEpochs)
	if err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to load partition queue: %w", err)
	}

	if err = queue.RescheduleAllAsFaults(faultExpiration); err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to reschedule all as faults: %w", err)
	}

	// Save expiration queue
	if p.ExpirationsEpochs, err = queue.Root(); err != nil {
		return NewPowerPairZero(), err
	}

	// Update partition metadata
	allFaults, err := bitfield.SubtractBitField(p.Sectors, p.Terminated)
	if err != nil {
		return NewPowerPairZero(), err
	}
	p.Faults = allFaults

	newFaultPower := p.TotalPower.Sub(p.FaultyPower)
	p.FaultyPower = p.TotalPower

	return newFaultPower, nil
}

// Removes sector numbers from faults and whichever fault epochs they belong to.
// The sectors are removed from the Faults bitfield and FaultyPower reduced.
// The sectors are removed from the FaultEpochsQueue, reducing the queue power by the sum of sector powers.
// Consistency between the partition totals and queue depend on the reported sectors actually being faulty.
// Returns the power of the now-recovered sectors.
func (p *Partition) RemoveFaults(store adt.Store, sectorNos *abi.BitField, sectors []*SectorOnChainInfo, ssize abi.SectorSize) (PowerPair, error) {
	// Load expiration queue
	queue, err := loadExpirationQueue(store, p.ExpirationsEpochs)
	if err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to load partition queue: %w", err)
	}

	// Reschedule recoveries
	power, err := queue.RescheduleRecovered(sectors, ssize)
	if err != nil {
		return NewPowerPairZero(), xerrors.Errorf("failed to reschedule faults in partition queue: %w", err)
	}

	// Save expiration queue
	if p.ExpirationsEpochs, err = queue.Root(); err != nil {
		return NewPowerPairZero(), err
	}

	// Update partition metadata
	// XXX: sanity check containsAll?
	if newFaults, err := bitfield.SubtractBitField(p.Faults, sectorNos); err != nil {
		return NewPowerPairZero(), err
	} else {
		p.Faults = newFaults
	}
	p.FaultyPower = p.FaultyPower.Sub(power)

	return power, err
}

// Adds sectors to recoveries and recovering power. Assumes not already present.
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
	return nil
}

// Removes sectors from recoveries and recovering power. Assumes sectors are present.
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
	return nil
}

// RescheduleExpirations moves expiring sectors to the target expiration.
func (p *Partition) RescheduleExpirations(store adt.Store, newExpiration abi.ChainEpoch, sectors []*SectorOnChainInfo, ssize abi.SectorSize) error {
	expirations, err := loadExpirationQueue(store, p.ExpirationsEpochs)
	if err != nil {
		return xerrors.Errorf("failed to load sector expirations: %w", err)
	}

	if err = expirations.RescheduleExpirations(newExpiration, sectors, ssize); err != nil {
		return err
	}

	p.ExpirationsEpochs, err = expirations.Root()
	return err
}

func (p *Partition) PopExpiredSectors(store adt.Store, until abi.ChainEpoch) (*ExpirationSet, error) {
	sectorExpirationQ, err := loadExpirationQueue(store, p.ExpirationsEpochs)
	if err != nil {
		return nil, xerrors.Errorf("failed to load expiration queue: %w", err)
	}

	popped, err := sectorExpirationQ.PopUntil(until)
	if err != nil {
		return nil, xerrors.Errorf("failed to pop expiration queue until %d: %w", until, err)
	}

	p.ExpirationsEpochs, err = sectorExpirationQ.Root()
	if err != nil {
		return nil, err
	}

	// TODO: what about faulty sectors?
	// TODO: Update Faulty bitfield?
	// TODO: Update recoveries bitfield?
	// TODO: What if the sector was already terminated? Should we remove it from the set? Error?

	// The expired power is exclusive of any sectors currently marked faulty

	// Mark the sectors as terminated and subtract sector power.
	p.TotalPower = p.TotalPower.Sub(popped.ActivePower)
	p.Terminated, err = bitfield.MergeBitFields(p.Terminated, popped.OnTimeSectors)
	if err != nil {
		return nil, xerrors.Errorf("failed to merge expired sectors: %w", err)
	}
	// Remove from faults, recoveries etc
	// FIXME distinguish removing expired faults from recovering faults
	if err = p.RemoveFaults(store, popped.OnTimeSectors, popped.ActivePower, powers); err != nil {
		return nil, xerrors.Errorf("failed to remove expired sectors from faults: %w", err)
	}

	if err = p.RemoveRecoveries(popped.OnTimeSectors, popped.ActivePower); err != nil {
		return nil, xerrors.Errorf("failed to remove expired sectors from recoveries: %w", err)
	}

	return popped, nil
}

// Marks all non-faulty sectors in the partition as faulty and clears recoveries, updating power memos appropriately.
// Returns the power of the newly faulty and failed recovery sectors.
func (p *Partition) RecordMissedPost(store adt.Store, faultExpiration abi.ChainEpoch) (newFaultPower, failedRecoveryPower PowerPair, err error) {
	// By construction, declared recoveries are currently faulty and thus not in newFaults.
	failedRecoveries, err := bitfield.IntersectBitField(p.Sectors, p.Recoveries)
	if err != nil {
		return newFaultPower, failedRecoveryPower, xerrors.Errorf("bitfield intersect failed: %w", err)
	}

	// Compute faulty power for penalization. New faulty power is the total power minus already faulty.
	newFaultPower = p.TotalPower.Sub(p.FaultyPower)
	failedRecoveryPower = p.RecoveringPower

	// Mark all sectors faulty and not recovering.
	if _, err = p.AddAllAsFaults(store, faultExpiration); err != nil {
		return newFaultPower, failedRecoveryPower, xerrors.Errorf("failed to record new faults: %w", err)
	}

	if err = p.RemoveRecoveries(failedRecoveries, failedRecoveryPower); err != nil {
		return newFaultPower, failedRecoveryPower, xerrors.Errorf("failed to record failed recoveries: %w", err)
	}
	p.RecoveringPower = NewPowerPairZero()
	return newFaultPower, failedRecoveryPower, nil
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

func (p *Partition) MarshalCBOR(w io.Writer) error {
	panic("implement me")
}
func (p *Partition) UnmarshalCBOR(r io.Reader) error {
	panic("implement me")
}
func (p *PowerPair) MarshalCBOR(w io.Writer) error {
	panic("implement me")
}
func (p *PowerPair) UnmarshalCBOR(r io.Reader) error {
	panic("implement me")
}
