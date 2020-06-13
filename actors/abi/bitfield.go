package abi

import (
	"fmt"

	"github.com/filecoin-project/go-bitfield"
)

type BitField = bitfield.BitField

func NewBitField() *BitField {
	bf, err := bitfield.NewFromBytes([]byte{})
	if err != nil {
		panic(fmt.Sprintf("creating empty rle: %+v", err))
	}
	return &bf
}

// Compute a bitfield that is the union of argument bitfields.
func BitFieldUnion(bfs ...*BitField) (*BitField, error) {
	if len(bfs) == 0 {
		return NewBitField(), nil
	}
	// TODO: optimize me: https://github.com/filecoin-project/specs-actors/issues/460
	for len(bfs) > 1 {
		var next []*BitField
		for i := 0; i < len(bfs); i += 2 {
			if i+1 >= len(bfs) {
				next = append(next, bfs[i])
				break
			}

			merged, err := bitfield.MergeBitFields(bfs[i], bfs[i+1])
			if err != nil {
				return nil, err
			}

			next = append(next, merged)
		}
		bfs = next
	}
	return bfs[0], nil
}

// Checks whether bitfield `a` contains any bit that is set in bitfield `b`.
func BitFieldContainsAny(a, b *BitField) (bool, error) {
	ca, err := a.Count()
	if err != nil {
		return false, err
	}

	cb, err := b.Count()
	if err != nil {
		return false, err
	}

	ab, err := bitfield.MergeBitFields(a, b)
	if err != nil {
		return false, err
	}

	cab, err := ab.Count()
	if err != nil {
		return false, err
	}

	return ca+cb != cab, nil
}

// Checks whether bitfield `a` contains all bits set in bitfield `b`.
func BitFieldContainsAll(a, b *BitField) (bool, error) {
	ca, err := a.Count()
	if err != nil {
		return false, err
	}

	cb, err := b.Count()
	if err != nil {
		return false, err
	}

	if cb > ca {
		return false, nil
	}

	ab, err := bitfield.MergeBitFields(a, b)
	if err != nil {
		return false, err
	}

	cab, err := ab.Count()
	if err != nil {
		return false, err
	}

	return ca == cab, nil
}
