package abi

import (
	"fmt"

	errors "github.com/pkg/errors"

	"github.com/filecoin-project/go-bitfield"
)

var ErrBitFieldTooMany = errors.New("to many items in RLE")

type BitField = bitfield.BitField

func NewBitField() BitField {
	bf, err := bitfield.NewFromBytes([]byte{})
	if err != nil {
		panic(fmt.Sprintf("creating empty rle: %+v", err))
	}
	return bf
}

func NewBitFieldFromBytes(rle []byte) (BitField, error) {
	return bitfield.NewFromBytes(rle)
}

func BitFieldFromSet(setBits []uint64) BitField {
	return bitfield.NewFromSet(setBits)
}

func BitFieldMerge(a, b BitField) (BitField, error) {
	return bitfield.MergeBitFields(a, b)
}

// These methods should be implemented on the bitfield type, not committed here.

// Returns true if a bitfield has no values.
func BitFieldEmpty(bf BitField) bool {
	panic("implement me") // TODO WPOST
}

// Returns the first value from a bitfield, if non-empty.
func BitFieldFirst(bf BitField) (uint64, error) {
	panic("implement me") // TODO WPOST
}

// Extracts a range of entries by index from a bitfield as a new bitfield.
// The bitfield's size must be at least first+count.
func BitFieldSlice(bf BitField, first uint64, count uint64) (BitField, error) {
	panic("implement me") // TODO WPOST
}

// Iterates values in a bitfield.
func BitFieldForEach(bf BitField, f func(i uint64) error) error {
	panic("implement me") // TODO WPOST
}

// Compute a bitfield that is the union of argument bitfields.
func BitFieldUnion(bfs ...BitField) (BitField, error) {
	panic("implement me") // TODO WPOST
}

// Computes the intersection of two bitfields.
func BitFieldIntersection(a BitField, b BitField) (BitField, error) {
	panic("implement me") // TODO WPOST
}

// Computes the difference of two bitfields, `a - b`.
func BitFieldDifference(a BitField, b BitField) (BitField, error) {
	panic("implement me") // TODO WPOST
}

// Checks whether bitfield `a` contains any bit that is set in bitfield `b`.
func BitFieldContainsAny(a, b BitField) (bool, error) {
	ca, err := a.Count()
	if err != nil {
		return false, err
	}

	cb, err := b.Count()
	if err != nil {
		return false, err
	}

	ab, err := BitFieldMerge(a, b)
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
func BitFieldContainsAll(a, b BitField) (bool, error) {
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

	ab, err := BitFieldMerge(a, b)
	if err != nil {
		return false, err
	}

	cab, err := ab.Count()
	if err != nil {
		return false, err
	}

	return ca == cab, nil
}