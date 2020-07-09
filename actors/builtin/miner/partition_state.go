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
	FaultsEpochs cid.Cid // AMT[ChainEpoch]BitField
	// Maps epochs sectors that expire in that epoch.
	ExpirationsEpochs cid.Cid // AMT[ChainEpoch]BitField

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

func (p *Partition) AddFaults(store adt.Store, sectorNos *abi.BitField, faultEpoch abi.ChainEpoch) (err error) {
	empty, err := sectorNos.IsEmpty()
	if err != nil {
		return err
	}
	if empty {
		return nil
	}

	p.Faults, err = bitfield.MergeBitFields(p.Faults, sectorNos)
	if err != nil {
		return err
	}

	queue, err := loadEpochQueue(store, p.FaultsEpochs)
	if err != nil {
		return xerrors.Errorf("failed to load partition fault epochs: %w", err)
	}
	if err := queue.AddToQueueBitfield(faultEpoch, sectorNos); err != nil {
		return xerrors.Errorf("failed to add faults to partition: %w", err)
	}
	p.FaultsEpochs, err = queue.Root()
	return err
}

// Removes sector numbers from faults and fault epochs, if present.
func (p *Partition) RemoveFaults(store adt.Store, sectorNos *abi.BitField) error {
	if empty, err := sectorNos.IsEmpty(); err != nil {
		return err
	} else if empty {
		return nil
	}

	if newFaults, err := bitfield.SubtractBitField(p.Faults, sectorNos); err != nil {
		return err
	} else {
		p.Faults = newFaults
	}

	arr, err := adt.AsArray(store, p.FaultsEpochs)
	if err != nil {
		return err
	}

	type change struct {
		index uint64
		value *abi.BitField
	}

	var (
		epochsChanged []change
		epochsDeleted []uint64
	)

	epochFaultsOld := &abi.BitField{}
	err = arr.ForEach(epochFaultsOld, func(i int64) error {
		countOld, err := epochFaultsOld.Count()
		if err != nil {
			return err
		}

		epochFaultsNew, err := bitfield.SubtractBitField(epochFaultsOld, sectorNos)
		if err != nil {
			return err
		}

		countNew, err := epochFaultsNew.Count()
		if err != nil {
			return err
		}

		if countNew == 0 {
			epochsDeleted = append(epochsDeleted, uint64(i))
		} else if countOld != countNew {
			epochsChanged = append(epochsChanged, change{index: uint64(i), value: epochFaultsNew})
		}

		return nil
	})
	if err != nil {
		return err
	}

	err = arr.BatchDelete(epochsDeleted)
	if err != nil {
		return err
	}

	for _, change := range epochsChanged {
		err = arr.Set(change.index, change.value)
		if err != nil {
			return err
		}
	}

	p.FaultsEpochs, err = arr.Root()
	return err
}

// Adds sectors to recoveries.
func (p *Partition) AddRecoveries(sectorNos *abi.BitField) (err error) {
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

	count, err := p.Recoveries.Count()
	if err != nil {
		return err
	}
	if count > SectorsMax {
		return fmt.Errorf("too many recoveries %d, max %d", count, SectorsMax)
	}
	return nil
}

// Removes sectors from recoveries, if present.
func (p *Partition) RemoveRecoveries(sectorNos *abi.BitField) (err error) {
	empty, err := sectorNos.IsEmpty()
	if err != nil {
		return err
	}
	if empty {
		return nil
	}
	p.Recoveries, err = bitfield.SubtractBitField(p.Recoveries, sectorNos)
	return err
}

func (p *Partition) PopExpiredSectors(store adt.Store, until abi.ChainEpoch) (*bitfield.BitField, error) {
	stopErr := fmt.Errorf("stop")

	sectorExpirationQ, err := adt.AsArray(store, p.ExpirationsEpochs)
	if err != nil {
		return nil, err
	}

	expiredSectors := bitfield.NewBitField()

	var expiredEpochs []uint64
	var bf bitfield.BitField
	err = sectorExpirationQ.ForEach(&bf, func(i int64) error {
		if i > until {
			return stopErr
		}
		expiredEpochs = append(expiredEpochs, uint64(i))
		// TODO: What if this grows too large?
		expiredSectors, err = bitfield.MergeBitFields(expiredSectors, bf)
		if err != nil {
			return err
		}
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

	return expiredSectors, nil
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

	// Compute faulty power for penalization.
	newFaultPower = p.TotalPower.Sub(p.FaultyPower)
	failedRecoveryPower = p.RecoveringPower

	// Mark all sectors faulty and not recovering.
	if err := p.AddFaults(store, newFaults, faultEpoch); err != nil {
		return newFaultPower, failedRecoveryPower, xerrors.Errorf("failed to record new faults: %w", err)
	}
	p.FaultyPower = p.TotalPower

	if err := p.RemoveRecoveries(failedRecoveries); err != nil {
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
