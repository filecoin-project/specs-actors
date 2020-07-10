package miner

import (
	"io"

	"github.com/filecoin-project/go-bitfield"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

// PowerSet represents a set of "values" and their total power + pledge.
// "things" can be either partitions or sectors, depending on the context.
type PowerSet struct {
	Values     *abi.BitField
	TotalPower PowerPair
}

func NewPowerSet() *PowerSet {
	return &PowerSet{
		Values:     abi.NewBitField(),
		TotalPower: PowerPairZero(),
	}
}

// Wrapper for working with an AMT[ChainEpoch]*PowerSet functioning as a queue, bucketed by epoch.
type sectorQueue struct {
	*adt.Array
}

func loadSectorQueue(store adt.Store, root cid.Cid) (sectorQueue, error) {
	arr, err := adt.AsArray(store, root)
	if err != nil {
		return sectorQueue{}, xerrors.Errorf("failed to load epoch queue %v: %w", root, err)
	}
	return sectorQueue{arr}, nil
}

// Adds values to the queue entry for an epoch.
func (q sectorQueue) AddToQueue(epoch abi.ChainEpoch, values *abi.BitField, power PowerPair) error {
	ps := NewPowerSet()
	if _, err := q.Array.Get(uint64(epoch), ps); err != nil {
		return xerrors.Errorf("failed to lookup queue epoch %v: %w", epoch, err)
	}

	var err error
	ps.Values, err = bitfield.MergeBitFields(ps.Values, values)
	if err != nil {
		return xerrors.Errorf("failed to merge bitfields for queue epoch %v: %w", epoch, err)
	}
	ps.TotalPower = ps.TotalPower.Add(power)

	if err = q.Array.Set(uint64(epoch), ps); err != nil {
		return xerrors.Errorf("failed to set queue epoch %v: %w", epoch, err)
	}
	return nil
}

// Adds values to the queue entry for an epoch.
func (q sectorQueue) RemoveFromQueue(epoch abi.ChainEpoch, values *abi.BitField, power PowerPair) error {
	ps := NewPowerSet()
	if _, err := q.Array.Get(uint64(epoch), ps); err != nil {
		return xerrors.Errorf("failed to lookup queue epoch %v: %w", epoch, err)
	}

	var err error
	ps.Values, err = bitfield.SubtractBitField(ps.Values, values)
	if err != nil {
		return xerrors.Errorf("failed to subtract bitfields for queue epoch %v: %w", epoch, err)
	}
	// XXX: check underflow
	ps.TotalPower = ps.TotalPower.Sub(power)

	if empty, err := ps.Values.IsEmpty(); err != nil {
		return err
	} else if empty {
		if err := q.Array.Delete(uint64(epoch)); err != nil {
			return xerrors.Errorf("failed to delete queue epoch %d: %w", epoch, err)
		}
	} else if err := q.Array.Set(uint64(epoch), ps); err != nil {
		return xerrors.Errorf("failed to set queue epoch %v: %w", epoch, err)
	}
	return nil
}

// Removes values from any and all queue entries in which they appear.
func (q sectorQueue) RemoveFromQueueAll(values *abi.BitField, powers map[abi.SectorNumber]PowerPair) error {
	var epochsDeleted []uint64
	var epochFaults PowerSet
	if err := q.Array.ForEach(&epochFaults, func(i int64) error {
		removeThisEpoch, err := bitfield.IntersectBitField(epochFaults.Values, values)
		if err != nil {
			return err
		}
		if empty, err := removeThisEpoch.IsEmpty(); err != nil {
			return err
		} else if empty {
			return nil
		}

		// Remove values from queue entry.
		if epochFaults.Values, err = bitfield.SubtractBitField(epochFaults.Values, removeThisEpoch); err != nil {
			return err
		}
		// Remove power and pledge from queue entry.
		if err = removeThisEpoch.ForEach(func(sno uint64) error {
			if powers != nil {
				pwr, ok := powers[abi.SectorNumber(sno)]
				if !ok {
					return xerrors.Errorf("no power for sector %d", sno)
				}
				epochFaults.TotalPower = epochFaults.TotalPower.Sub(pwr)
			}
			return nil
		}); err != nil {
			return err
		}

		countNew, err := epochFaults.Values.Count()
		if err != nil {
			return err
		}

		if countNew == 0 {
			epochsDeleted = append(epochsDeleted, uint64(i))
		} else if err := q.Array.Set(uint64(i), &epochFaults); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if err := q.Array.BatchDelete(epochsDeleted); err != nil {
		return err
	}
	return nil
}

func (e *PowerSet) MarshalCBOR(w io.Writer) error {
	panic("implement me")
}
func (e *PowerSet) UnmarshalCBOR(r io.Reader) error {
	panic("implement me")
}
