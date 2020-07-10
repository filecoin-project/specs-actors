package miner

import (
	"github.com/filecoin-project/go-bitfield"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

// Wrapper for working with an AMT[ChainEpoch]*Bitfield functioning as a queue, bucketed by epoch.
type epochQueue struct {
	*adt.Array
}

func loadEpochQueue(store adt.Store, root cid.Cid) (epochQueue, error) {
	arr, err := adt.AsArray(store, root)
	if err != nil {
		return epochQueue{}, xerrors.Errorf("failed to load epoch queue %v: %w", root, err)
	}
	return epochQueue{arr}, nil
}

// Adds values to the queue entry for an epoch.
// The queue AMT root is loaded from a CID, and that CID is overwritten with the new root when this method returns.
func (q epochQueue) AddToQueueBitfield(epoch abi.ChainEpoch, values *abi.BitField) error {
	bf := abi.NewBitField()
	if _, err := q.Array.Get(uint64(epoch), bf); err != nil {
		return xerrors.Errorf("failed to lookup queue epoch %v: %w", epoch, err)
	}

	bf, err := bitfield.MergeBitFields(bf, values)
	if err != nil {
		return xerrors.Errorf("failed to merge bitfields for queue epoch %v: %w", epoch, err)
	}

	if err = q.Array.Set(uint64(epoch), bf); err != nil {
		return xerrors.Errorf("failed to set queue epoch %v: %w", epoch, err)
	}
	return nil
}

func (q epochQueue) AddToQueueValues(epoch abi.ChainEpoch, values ...uint64) error {
	if len(values) == 0 {
		return nil
	}
	return q.AddToQueueBitfield(epoch, bitfield.NewFromSet(values))
}

// Removes values to the queue entry for an epoch.
// The queue AMT root is loaded from a CID, and that CID is overwritten with the new root when this method returns.
func (q epochQueue) RemoveFromQueueBitfield(epoch abi.ChainEpoch, values *abi.BitField) error {
	bf := abi.NewBitField()
	if found, err := q.Array.Get(uint64(epoch), bf); err != nil {
		return xerrors.Errorf("failed to lookup queue epoch %v: %w", epoch, err)
	} if !found {
		// nothing to do.
		// TODO: be paranoid and fail if the values we're trying to remove don't exist?
		return nil
	}

	bf, err := bitfield.SubtractBitField(bf, values)
	if err != nil {
		return xerrors.Errorf("failed to merge bitfields for queue epoch %v: %w", epoch, err)
	}

	if empty, err := bf.IsEmpty(); err != nil {
		return err
	} else if empty {
		if err = q.Array.Delete(uint64(epoch)); err != nil {
			return xerrors.Errorf("failed to delete queue epoch %v: %w", epoch, err)
		}
	} else {
		if err = q.Array.Set(uint64(epoch), bf); err != nil {
			return xerrors.Errorf("failed to set queue epoch %v: %w", epoch, err)
		}
	}

	return nil
}

func (q epochQueue) RemoveFromQueueValues(epoch abi.ChainEpoch, values ...uint64) error {
	if len(values) == 0 {
		return nil
	}
	return q.RemoveFromQueueBitfield(epoch, bitfield.NewFromSet(values))
}
