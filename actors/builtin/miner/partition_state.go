package miner

import (
	"fmt"
	"io"

	"github.com/filecoin-project/go-bitfield"
	"github.com/ipfs/go-cid"

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
	TotalPower abi.StoragePower
	// Power of currently-faulty sectors
	FaultyPower abi.StoragePower
	// Sum of initial pledge of sectors
	TotalPledge abi.TokenAmount
}

// mergeBitFieldArrays merges two AMTs of bitfields.
func mergeBitFieldArrays(store adt.Store, a, b cid.Cid) (cid.Cid, error) {
	if a == b {
		return a, nil
	}

	bArr, err := adt.AsArray(store, b)
	if err != nil {
		return cid.Undef, err
	}
	if bArr.IsEmpty() {
		return a, nil
	}

	aArr, err := adt.AsArray(store, a)
	if err != nil {
		return cid.Undef, err
	}
	if aArr.IsEmpty() {
		return b, nil
	}

	// Copy from B to A. If B has more elements than A, swap them to copy
	// fewer items.

	if bArr.Length() > aArr.Length() {
		aArr, bArr = bArr, aArr
	}

	var aBf, bBf bitfield.BitField
	bArr.ForEach(&bBf, func(idx uint64) error {
		found, err := aArr.Get(idx, &aBf)
		if err != nil {
			return err
		}
		// Move. No need to copy.
		if !found {
			return aArr.Set(idx, &bBf)
		}
		merged, err := abi.MergeBitFields(&aBf, &bBf)
		if err != nil {
			return err
		}
		return aArr.Set(idx, merged)
	})
	return bArr.Root()
}

// Merge merges the other partition into this partition.
func (p *Partition) Merge(store adt.Store, other *Partition) (err error) {
	p.Sectors, err = bitfield.MergeBitFields(p.Sectors, other.Sectors)
	if err != nil {
		return err
	}
	p.Faults, err = bitfield.MergeBitFields(p.Faults, other.Faults)
	if err != nil {
		return err
	}
	p.Recoveries, err = bitfield.MergeBitFields(p.Recoveries, other.Recoveries)
	if err != nil {
		return err
	}
	p.Terminated, err = bitfield.MergeBitFields(p.Termianted, other.Termianted)
	if err != nil {
		return err
	}
	p.EarlyTermianted, err = mergeBitFieldArrays(p.EarlyTermianted, other.EarlyTerminated)
	if err != nil {
		return err
	}
	p.FaultsEpochs, err = mergeBitFieldArrays(p.FaultsEpochs, other.FaultsEpochs)
	if err != nil {
		return err
	}
	p.ExpirationsEpochs, err = mergeBitFieldArrays(p.ExpirationsEpochs, other.ExpirationsEpochs)
	if err != nil {
		return err
	}
	p.TotalPower = big.Add(p.TotalPower, other.TotalPower)
	p.FaultyPower = big.Add(p.FaultyPower, other.FaultyPower)
	p.TotalPledge = big.Add(p.TotalPledge, other.TotalPledge)
}

func (p *Partition) AddFaults(store adt.Store, sectorNos *abi.BitField, faultEpoch abi.ChainEpoch) (err error) {
	empty, err := sectorNos.IsEmpty()
	if err != nil {
		return err
	}
	if empty {
		return nil
	}

	{
		p.Faults, err = bitfield.MergeBitFields(p.Faults, sectorNos)
		if err != nil {
			return err
		}

		count, err := p.Faults.Count()
		if err != nil {
			return err
		}
		if count > SectorsMax {
			return fmt.Errorf("too many faults %d, max %d", count, SectorsMax)
		}
	}
	{
		epochFaultArr, err := adt.AsArray(store, p.FaultsEpochs)
		if err != nil {
			return err
		}

		bf := abi.NewBitField()
		_, err = epochFaultArr.Get(uint64(faultEpoch), bf)
		if err != nil {
			return err
		}

		bf, err = bitfield.MergeBitFields(bf, sectorNos)
		if err != nil {
			return err
		}

		if err = epochFaultArr.Set(uint64(faultEpoch), bf); err != nil {
			return err
		}

		p.FaultsEpochs, err = epochFaultArr.Root()
		if err != nil {
			return err
		}
	}
	return nil
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
	if err = nil {
		return nil, err
	}

	p.ExpirationsEpochs, err = sectorExpirationQ.Root()
	if err != nil {
		return nil, err
	}

	return expiredSectors, nil
}


func (p *Partition) MarshalCBOR(w io.Writer) error {
	panic("implement me")
}

func (p *Partition) UnmarshalCBOR(r io.Reader) error {
	panic("implement me")
}
