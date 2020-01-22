package big

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"

	cbg "github.com/whyrusleeping/cbor-gen"
)

// BigIntMaxSerializedLen is the max length of a byte slice representing a CBOR serialized bigint.
const BigIntMaxSerializedLen = 128

type Int struct {
	*big.Int
}

func NewInt(i int64) Int {
	return Int{big.NewInt(0).SetInt64(i)}
}

func Zero() Int {
	return NewInt(0)
}

func FromBytes(b []byte) Int {
	i := big.NewInt(0).SetBytes(b)
	return Int{i}
}

func FromString(s string) (Int, error) {
	v, ok := big.NewInt(0).SetString(s, 10)
	if !ok {
		return Int{}, fmt.Errorf("failed to parse string as a big int")
	}

	return Int{v}, nil
}

func Mul(a, b Int) Int {
	return Int{big.NewInt(0).Mul(a.Int, b.Int)}
}

func Div(a, b Int) Int {
	return Int{big.NewInt(0).Div(a.Int, b.Int)}
}

func Mod(a, b Int) Int {
	return Int{big.NewInt(0).Mod(a.Int, b.Int)}
}

func Add(a, b Int) Int {
	return Int{big.NewInt(0).Add(a.Int, b.Int)}
}

func Sub(a, b Int) Int {
	return Int{big.NewInt(0).Sub(a.Int, b.Int)}
}

func Max(x, y Int) Int {
	// taken from max.Max()
	if x.Equals(Zero()) && x.Equals(y) {
		if x.Sign() != 0 {
			return y
		}
		return x
	}
	if x.GreaterThan(y) {
		return x
	}
	return y
}

func Min(x, y Int) Int {
	// taken from max.Min()
	if x.Equals(Zero()) && x.Equals(y) {
		if x.Sign() != 0 {
			return x
		}
		return y
	}
	if x.LessThan(y) {
		return x
	}
	return y
}

func Cmp(a, b Int) int {
	return a.Int.Cmp(b.Int)
}

// LessThan returns true if bi < o
func (bi Int) LessThan(o Int) bool {
	return Cmp(bi, o) < 0
}

// LessThanEqual returns true if bi <= o
func (bi Int) LessThanEqual(o Int) bool {
	return bi.LessThan(o) || bi.Equals(o)
}

// GreaterThan returns true if bi > o
func (bi Int) GreaterThan(o Int) bool {
	return Cmp(bi, o) > 0
}

// GreaterThanEqual returns true if bi >= o
func (bi Int) GreaterThanEqual(o Int) bool {
	return bi.GreaterThan(o) || bi.Equals(o)
}

// Equals returns true if bi == o
func (bi Int) Equals(o Int) bool {
	return Cmp(bi, o) == 0
}

func (bi *Int) MarshalJSON() ([]byte, error) {
	return json.Marshal(bi.String())
}

func (bi *Int) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	i, ok := big.NewInt(0).SetString(s, 10)
	if !ok {
		return fmt.Errorf("failed to parse bigint string: '%s'", string(b))
	}

	bi.Int = i
	return nil
}

func (bi *Int) CborBytes() ([]byte, error) {
	if bi.Int == nil {
		return []byte{}, fmt.Errorf("failed to convert to bytes, bigint is nil")
	}

	switch {
	case bi.Sign() > 0:
		return append([]byte{0}, bi.Bytes()...), nil
	case bi.Sign() < 0:
		return append([]byte{1}, bi.Bytes()...), nil
	default: //  bi.Sign() == 0:
		return []byte{}
	}
}

func FromCborBytes(buf []byte) (Int, error) {
	if len(buf) == 0 {
		return NewInt(0), nil
	}

	var negative bool
	switch buf[0] {
	case 0:
		negative = false
	case 1:
		negative = true
	default:
		return NewInt(0), fmt.Errorf("big int prefix should be either 0 or 1, got %d", buf[0])
	}

	i := big.NewInt(0).SetBytes(buf[1:])
	if negative {
		i.Neg(i)
	}

	return Int{i}, nil
}

func (bi *Int) MarshalCBOR(w io.Writer) error {
	if bi.Int == nil {
		return fmt.Errorf("failed to Marshal to CBOR, bigint is nil")
	}

	enc, err := bi.CborBytes()
	if err != nil {
		return err
	}

	header := cbg.CborEncodeMajorType(cbg.MajByteString, uint64(len(enc)))
	if _, err := w.Write(header); err != nil {
		return err
	}

	if _, err := w.Write(enc); err != nil {
		return err
	}

	return nil
}

func (bi *Int) UnmarshalCBOR(br io.Reader) error {
	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}

	if maj != cbg.MajByteString {
		return fmt.Errorf("cbor input for fil big int was not a byte string (%x)", maj)
	}

	if extra == 0 {
		bi.Int = big.NewInt(0)
		return nil
	}

	if extra > BigIntMaxSerializedLen {
		return fmt.Errorf("big integer byte array too long")
	}

	buf := make([]byte, extra)
	if _, err := io.ReadFull(br, buf); err != nil {
		return err
	}

	i, err := FromCborBytes(buf)
	if err != nil {
		return err
	}

	*bi = i

	return nil
}

func (bi *Int) IsZero() bool {
	return bi.Int.Sign() == 0
}
