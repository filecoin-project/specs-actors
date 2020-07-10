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

type Partition struct {
	// Sector numbers in this partition, including faulty and terminated sectors
	Sectors *abi.BitField
	// Subset of sectors detected/declared faulty and not yet recovered (excl. from PoSt)
	Faults *abi.BitField
	// Subset of faulty sectors expected to recover on next PoSt
	Recoveries *abi.BitField
	// Subset of sectors terminated but not yet removed from partition (excl. from PoSt)
	Terminated *abi.BitField
	// Subset of terminated that were before their committed expiration epoch.
	// Termination fees have not yet been calculated or paid but effective
	// power has already been adjusted.
	EarlyTerminated cid.Cid // AMT[ChainEpoch]BitField

	// Maps epochs to sectors that became faulty during that epoch.
	FaultsEpochs cid.Cid // AMT[ChainEpoch]PowerSet
	// Maps epochs sectors that expire in that epoch.
	ExpirationsEpochs cid.Cid // AMT[ChainEpoch]PowerSet

	// Power of not-yet-terminated sectors (incl faulty)
	TotalPower PowerPair
	// Power of currently-faulty sectors
	FaultyPower PowerPair
	// Power of expected-to-recover sectors
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
		FaultsEpochs:      emptyArray,
		ExpirationsEpochs: emptyArray,
		TotalPower:        PowerPairZero(),
		FaultyPower:       PowerPairZero(),
		RecoveringPower:   PowerPairZero(),
		TotalPledge:       big.Zero(),
	}
}

// Records a set of sectors as faulty at some epoch.
// The sectors are added to the Faults bitfield and the FaultyPower is increased.
// The sectors are also added to the FaultEpochs queue, recording the associated power.
func (p *Partition) AddFaults(store adt.Store, faultEpoch abi.ChainEpoch, sectorNos *abi.BitField, power PowerPair) (err error) {
	p.Faults, err = bitfield.MergeBitFields(p.Faults, sectorNos)
	if err != nil {
		return err
	}
	p.FaultyPower = p.FaultyPower.Add(power)

	queue, err := loadSectorQueue(store, p.FaultsEpochs)
	if err != nil {
		return xerrors.Errorf("failed to load partition fault epochs: %w", err)
	}
	if err := queue.AddToQueue(faultEpoch, sectorNos, power); err != nil {
		return xerrors.Errorf("failed to add faults to partition queue: %w", err)
	}
	p.FaultsEpochs, err = queue.Root()
	return err
}

// Removes sector numbers from faults and whichever fault epochs they belong to.
// The sectors are removed from the Faults bitfield and FaultyPower reduced.
// The sectors are removed from the FaultEpochsQueue, reducing the queue power by the sum of sector powers.
// Consistency between the partition totals and queue depend on the reported sectors actually being faulty.
func (p *Partition) RemoveFaults(store adt.Store, sectorNos *abi.BitField, power PowerPair, powers map[abi.SectorNumber]PowerPair) error {
	if empty, err := sectorNos.IsEmpty(); err != nil {
		return err
	} else if empty {
		return nil
	}

	// XXX: sanity check containsAll?
	if newFaults, err := bitfield.SubtractBitField(p.Faults, sectorNos); err != nil {
		return err
	} else {
		p.Faults = newFaults
	}
	p.FaultyPower = p.FaultyPower.Sub(power)

	queue, err := loadSectorQueue(store, p.FaultsEpochs)
	if err != nil {
		return xerrors.Errorf("failed to load partition fault epochs: %w", err)
	}
	err = queue.RemoveFromQueueAll(sectorNos, powers)
	if err != nil {
		return xerrors.Errorf("failed to remove faults from partition queue: %w", err)
	}

	p.FaultsEpochs, err = queue.Root()
	return err
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

func (p *Partition) AddSectors(store adt.Store, sectorSize abi.SectorSize, sectors []*SectorOnChainInfo) error {
	// Add the sectors & pledge.
	for _, sector := range sectors {
		p.Sectors.Set(uint64(sector.SectorNumber))
		p.TotalPledge = big.Add(p.TotalPledge, sector.InitialPledge)
	}

	// Update the expirations (and power).
	expirations, err := loadSectorQueue(store, p.ExpirationsEpochs)
	if err != nil {
		return xerrors.Errorf("failed to load sector expirations: %w", err)
	}

	for _, group := range groupSectorsByExpiration(sectorSize, sectors) {
		// Update partition total power (saves redundant arithmetic over doing it in the initial loop over sectors).
		p.TotalPower = p.TotalPower.Add(group.totalPower)

		// Update expiration queue.
		if err := expirations.AddToQueue(group.epoch, bitfield.NewFromSet(group.sectors), group.totalPower); err != nil {
			return xerrors.Errorf("failed to record new sector expirations: %w", err)
		}
	}
	p.ExpirationsEpochs, err = expirations.Root()
	return err
}

// RescheduleExpirations moves expiring sectors to the target expiration.
func (p *Partition) RescheduleExpirations(store adt.Store, sectorSize abi.SectorSize, epoch abi.ChainEpoch, sectors []*SectorOnChainInfo) error {
	expirations, err := loadSectorQueue(store, p.ExpirationsEpochs)
	if err != nil {
		return xerrors.Errorf("failed to load sector expirations: %w", err)
	}

	var sectorsTotal []*abi.BitField
	powerTotal := PowerPairZero()

	for _, group := range groupSectorsByExpiration(sectorSize, sectors) {
		sectorsBf := bitfield.NewFromSet(group.sectors)
		if err = expirations.RemoveFromQueue(group.epoch, sectorsBf, group.totalPower); err != nil {
			return err
		}

		sectorsTotal = append(sectorsTotal, sectorsBf)
		powerTotal = powerTotal.Add(group.totalPower)
	}

	if err = expirations.AddToQueue(epoch, sectorsTotal, powerTotal); err != nil {
		return err // XXX ANORTH HERE merge sectorstotal
	}

	p.ExpirationsEpochs, err = expirations.Root()
	return err
}

func (p *Partition) PopExpiredSectors(store adt.Store, until abi.ChainEpoch) (*PowerSet, error) {
	stopErr := fmt.Errorf("stop")

	sectorExpirationQ, err := adt.AsArray(store, p.ExpirationsEpochs)
	if err != nil {
		return nil, err
	}

	expiredSectors := abi.NewBitField()

	totalPower := PowerPairZero()

	var expiredEpochs []uint64
	var expiration PowerSet
	err = sectorExpirationQ.ForEach(&expiration, func(i int64) error {
		if abi.ChainEpoch(i) > until {
			return stopErr
		}
		expiredEpochs = append(expiredEpochs, uint64(i))
		totalPower = totalPower.Add(expiration.TotalPower)

		// TODO: What if this grows too large?
		expiredSectors, err = bitfield.MergeBitFields(expiredSectors, expiration.Values)
		return err
	})
	switch err {
	case nil, stopErr:
	default:
		return nil, err
	}

	err = sectorExpirationQ.BatchDelete(expiredEpochs)
	if err != nil {
		return nil, err
	}

	p.ExpirationsEpochs, err = sectorExpirationQ.Root()
	if err != nil {
		return nil, err
	}

	// TODO: Update power/pledge?

	return &PowerSet{
		Values:     expiredSectors,
		TotalPower: totalPower,
	}, nil
}

// Marks all non-faulty sectors in the partition as faulty and clears recoveries, updating power memos appropriately.
// Returns the power of the newly faulty and failed recovery sectors.
func (p *Partition) RecordMissedPost(store adt.Store, faultEpoch abi.ChainEpoch) (newFaultPower, failedRecoveryPower PowerPair, err error) {
	newFaults, err := bitfield.SubtractBitField(p.Sectors, p.Faults)
	if err != nil {
		return newFaultPower, failedRecoveryPower, xerrors.Errorf("bitfield subtract failed: %w", err)
	}

	// By construction, declared recoveries are currently faulty and thus not in newFaults.
	failedRecoveries, err := bitfield.IntersectBitField(p.Sectors, p.Recoveries)
	if err != nil {
		return newFaultPower, failedRecoveryPower, xerrors.Errorf("bitfield intersect failed: %w", err)
	}

	// Compute faulty power for penalization. New faulty power is the total power minus already faulty.
	newFaultPower = p.TotalPower.Sub(p.FaultyPower)
	failedRecoveryPower = p.RecoveringPower

	// Mark all sectors faulty and not recovering.
	if err := p.AddFaults(store, faultEpoch, newFaults, newFaultPower); err != nil {
		return newFaultPower, failedRecoveryPower, xerrors.Errorf("failed to record new faults: %w", err)
	}

	if err := p.RemoveRecoveries(failedRecoveries, failedRecoveryPower); err != nil {
		return newFaultPower, failedRecoveryPower, xerrors.Errorf("failed to record failed recoveries: %w", err)
	}
	p.RecoveringPower = PowerPairZero()
	return newFaultPower, failedRecoveryPower, nil
}

//
// PowerPair
//

func PowerPairZero() PowerPair {
	return PowerPair{big.Zero(), big.Zero()}
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
