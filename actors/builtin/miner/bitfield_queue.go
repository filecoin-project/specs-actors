package miner

import (
	"fmt"

	"github.com/filecoin-project/go-bitfield"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

// Wrapper for working with an AMT[ChainEpoch]*Bitfield functioning as a queue, bucketed by epoch.
type BitfieldQueue struct {
	*adt.Array
}

func LoadBitfieldQueue(store adt.Store, root cid.Cid) (BitfieldQueue, error) {
	arr, err := adt.AsArray(store, root)
	if err != nil {
		return BitfieldQueue{}, xerrors.Errorf("failed to load epoch queue %v: %w", root, err)
	}
	return BitfieldQueue{arr}, nil
}

// Adds values to the queue entry for an epoch.
func (q BitfieldQueue) AddToQueue(epoch abi.ChainEpoch, values *abi.BitField) error {
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

func (q BitfieldQueue) AddToQueueValues(epoch abi.ChainEpoch, values ...uint64) error {
	if len(values) == 0 {
		return nil
	}
	return q.AddToQueue(epoch, bitfield.NewFromSet(values))
}

// Removes and returns all values with keys less than or equal to until.
// Returns nil if nothing was popped.
func (q BitfieldQueue) PopUntil(until abi.ChainEpoch) (*abi.BitField, error) {
	poppedValues := abi.NewBitField()
	var poppedKeys []uint64
	var err error

	stopErr := fmt.Errorf("stop")
	if err = q.ForEach(func(epoch abi.ChainEpoch, bf *bitfield.BitField) error {
		if epoch > until {
			return stopErr
		}
		poppedKeys = append(poppedKeys, uint64(epoch))
		poppedValues, err = bitfield.MergeBitFields(poppedValues, bf)
		return err
	}); err != nil && err != stopErr {
		return nil, err
	}

	// Nothing expired.
	if len(poppedKeys) == 0 {
		return nil, nil
	}

	if err = q.BatchDelete(poppedKeys); err == nil {
		return nil, err
	}
	return poppedValues, nil
}

// Iterates the queue.
func (q BitfieldQueue) ForEach(cb func(epoch abi.ChainEpoch, bf *bitfield.BitField) error) error {
	var bf bitfield.BitField
	return q.Array.ForEach(&bf, func(i int64) error {
		// TODO: Do we really need to do this, or can we just tell the
		// caller not to mess with the bitfield?
		cpy, err := bf.Copy()
		if err != nil {
			return xerrors.Errorf("failed to copy bitfield in queue: %w", err)
		}
		return cb(abi.ChainEpoch(i), cpy)
	})
}
