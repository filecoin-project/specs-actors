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

// PowerSet represents a set of "values" and their total power + pledge.
// "things" can be either partitions or sectors, depending on the context.
type PowerSet struct {
	Values      *abi.BitField
	TotalPower  PowerPair
	TotalPledge abi.TokenAmount // XXX: do we need this if releasing pledge is always deferred to miner defrag?
}

func NewPowerSet() *PowerSet {
	return &PowerSet{
		Values:      abi.NewBitField(),
		TotalPower:  PowerPairZero(),
		TotalPledge: big.Zero(),
	}
}

// Wrapper for working with an AMT[ChainEpoch]*PowerSet functioning as a queue, bucketed by epoch.
type epochPowerQueue struct {
	*adt.Array
}

func loadPowerQueue(store adt.Store, root cid.Cid) (epochPowerQueue, error) {
	arr, err := adt.AsArray(store, root)
	if err != nil {
		return epochPowerQueue{}, xerrors.Errorf("failed to load epoch queue %v: %w", root, err)
	}
	return epochPowerQueue{arr}, nil
}

// Adds values to the queue entry for an epoch.
func (q epochPowerQueue) AddToQueue(epoch abi.ChainEpoch, values *abi.BitField, power PowerPair, pledge abi.TokenAmount) error {
	var ps PowerSet
	var err error
	if _, err := q.Array.Get(uint64(epoch), &ps); err != nil {
		return xerrors.Errorf("failed to lookup queue epoch %v: %w", epoch, err)
	}

	ps.Values, err = bitfield.MergeBitFields(ps.Values, values)
	if err != nil {
		return xerrors.Errorf("failed to merge bitfields for queue epoch %v: %w", epoch, err)
	}
	ps.TotalPower = ps.TotalPower.Add(power)
	ps.TotalPledge = big.Add(ps.TotalPledge, pledge)

	if err = q.Array.Set(uint64(epoch), &ps); err != nil {
		return xerrors.Errorf("failed to set queue epoch %v: %w", epoch, err)
	}
	return nil
}

// Removes values from any and all queue entries in which they appear.
func (q epochPowerQueue) RemoveFromQueueAll(values *abi.BitField, powers map[uint64]PowerPair, pledges map[uint64]abi.TokenAmount) error {
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
				pwr, ok := powers[sno]
				if !ok {
					return xerrors.Errorf("no power for sector %d", sno)
				}
				epochFaults.TotalPower = epochFaults.TotalPower.Sub(pwr)
			}
			if pledges != nil {
				pledge, ok := pledges[sno]
				if !ok {
					return xerrors.Errorf("no pledge for sector %d", sno)
				}
				epochFaults.TotalPledge = big.Sub(epochFaults.TotalPledge, pledge)
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
