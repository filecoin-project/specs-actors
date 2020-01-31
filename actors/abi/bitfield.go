package abi

import (
	"fmt"
	"io"

	errors "github.com/pkg/errors"
	cbg "github.com/whyrusleeping/cbor-gen"

	rlepluslazy "github.com/filecoin-project/specs-actors/actors/util/rleplus"
)

var ErrBitFieldTooMany = errors.New("to many items in RLE")

type BitField struct {
	rle rlepluslazy.RLE

	bits map[uint64]struct{}
}

func NewBitField() BitField {
	bf, err := NewBitFieldFromBytes([]byte{})
	if err != nil {
		panic(fmt.Sprintf("creating empty rle: %+v", err))
	}
	return bf
}

func NewBitFieldFromBytes(rle []byte) (BitField, error) {
	bf := BitField{}
	rlep, err := rlepluslazy.FromBuf(rle)
	if err != nil {
		return BitField{}, errors.Wrap(err, "could not decode rle+")
	}
	bf.rle = rlep
	bf.bits = make(map[uint64]struct{})
	return bf, nil

}

func BitFieldFromSet(setBits []uint64) BitField {
	res := BitField{bits: make(map[uint64]struct{})}
	for _, b := range setBits {
		res.bits[b] = struct{}{}
	}
	return res
}

func MergeBitFields(a, b BitField) (BitField, error) {
	ra, err := a.rle.RunIterator()
	if err != nil {
		return BitField{}, err
	}

	rb, err := b.rle.RunIterator()
	if err != nil {
		return BitField{}, err
	}

	merge, err := rlepluslazy.Sum(ra, rb)
	if err != nil {
		return BitField{}, err
	}

	mergebytes, err := rlepluslazy.EncodeRuns(merge, nil)
	if err != nil {
		return BitField{}, err
	}

	rle, err := rlepluslazy.FromBuf(mergebytes)
	if err != nil {
		return BitField{}, err
	}

	return BitField{
		rle:  rle,
		bits: make(map[uint64]struct{}),
	}, nil
}

func (bf BitField) sum() (rlepluslazy.RunIterator, error) {
	if len(bf.bits) == 0 {
		return bf.rle.RunIterator()
	}

	a, err := bf.rle.RunIterator()
	if err != nil {
		return nil, err
	}
	slc := make([]uint64, 0, len(bf.bits))
	for b := range bf.bits {
		slc = append(slc, b)
	}

	b, err := rlepluslazy.RunsFromSlice(slc)
	if err != nil {
		return nil, err
	}

	res, err := rlepluslazy.Sum(a, b)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Set ...s bit in the BitField
func (bf BitField) Set(bit uint64) {
	bf.bits[bit] = struct{}{}
}

// Unset removes bit from the BitField
func (bf BitField) Unset(bit uint64) {
	delete(bf.bits, bit)
}

// Has returns true iff bif is set in the BitField.
func (bf BitField) Has(bit uint64) bool {
	_, ok := bf.bits[bit]
	return ok
}

func (bf BitField) Count() (uint64, error) {
	s, err := bf.sum()
	if err != nil {
		return 0, err
	}
	return rlepluslazy.Count(s)
}

// All returns all set bits
func (bf BitField) All(max uint64) ([]uint64, error) {
	c, err := bf.Count()
	if err != nil {
		return nil, errors.Wrap(err, "count error")
	}
	if c > max {
		return nil, errors.Errorf("expected %d, got %d: %w", max, c, ErrBitFieldTooMany)
	}

	runs, err := bf.sum()
	if err != nil {
		return nil, err
	}

	res, err := rlepluslazy.SliceFromRuns(runs)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (bf BitField) AllMap(max uint64) (map[uint64]bool, error) {
	c, err := bf.Count()
	if err != nil {
		return nil, errors.Wrap(err, "count error")
	}
	if c > max {
		return nil, errors.Errorf("expected %d, got %d: %w", max, c, ErrBitFieldTooMany)
	}

	runs, err := bf.sum()
	if err != nil {
		return nil, err
	}

	res, err := rlepluslazy.SliceFromRuns(runs)
	if err != nil {
		return nil, err
	}

	out := make(map[uint64]bool)
	for _, i := range res {
		out[i] = true
	}
	return out, nil
}

func (bf BitField) MarshalCBOR(w io.Writer) error {
	s, err := bf.sum()
	if err != nil {
		return err
	}

	rle, err := rlepluslazy.EncodeRuns(s, []byte{})
	if err != nil {
		return err
	}

	if len(rle) > 8192 {
		return errors.Errorf("encoded bitfield was too large (%d)", len(rle))
	}

	if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajByteString, uint64(len(rle)))); err != nil {
		return err
	}
	if _, err = w.Write(rle); err != nil {
		return errors.Wrap(err, "writing rle")
	}
	return nil
}

func (bf *BitField) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if extra > 8192 {
		return fmt.Errorf("array too large")
	}

	if maj != cbg.MajByteString {
		return fmt.Errorf("expected byte array")
	}

	buf := make([]byte, extra)
	if _, err := io.ReadFull(br, buf); err != nil {
		return err
	}

	rle, err := rlepluslazy.FromBuf(buf)
	if err != nil {
		return errors.Wrap(err, "could not decode rle+")
	}
	bf.rle = rle
	bf.bits = make(map[uint64]struct{})

	return nil
}
