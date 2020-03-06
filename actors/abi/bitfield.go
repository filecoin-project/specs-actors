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

func MergeBitFields(a, b BitField) (BitField, error) {
	return bitfield.MergeBitFields(a, b)
}
