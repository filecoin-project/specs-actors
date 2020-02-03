package abi_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/actors/abi"
)

func TestBitFieldHas(t *testing.T) {
	bf := abi.NewBitField()
	bf.Set(1)
	bf.Set(2)
	bf.Set(3)
	bf.Set(4)
	bf.Set(5)

	found, err := bf.Has(1)
	assert.NoError(t, err)
	assert.True(t, found)

	found, err = bf.Has(6)
	assert.NoError(t, err)
	assert.False(t, found)

	bf2 := roundtripMarshal(t, bf)

	found, err = bf2.Has(1)
	assert.NoError(t, err)
	assert.True(t, found)

	found, err = bf2.Has(6)
	assert.NoError(t, err)
	assert.False(t, found)
}

func TestBitFieldUnset(t *testing.T) {
	bf := abi.NewBitField()
	bf.Set(1)
	bf.Set(2)
	bf.Set(3)
	bf.Set(4)
	bf.Set(5)

	err := bf.Unset(3)
	assert.NoError(t, err)

	found, err := bf.Has(3)
	assert.NoError(t, err)
	assert.False(t, found)

	cnt, err := bf.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), cnt)

	bf2 := roundtripMarshal(t, bf)

	cnt, err = bf2.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), cnt)

	found, err = bf2.Has(3)
	assert.NoError(t, err)
	assert.False(t, found)
}

func roundtripMarshal(t *testing.T, in abi.BitField) abi.BitField {
	buf := new(bytes.Buffer)
	err := in.MarshalCBOR(buf)
	assert.NoError(t, err)

	bf2 := abi.NewBitField()
	err = bf2.UnmarshalCBOR(buf)
	assert.NoError(t, err)
	return bf2
}
