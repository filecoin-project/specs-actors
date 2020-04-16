package abi_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/actors/abi"
)

func TestBitFieldUnset(t *testing.T) {
	bf := abi.NewBitField()
	bf.Set(1)
	bf.Set(2)
	bf.Set(3)
	bf.Set(4)
	bf.Set(5)

	bf.Unset(3)

	m, err := bf.AllMap(100)
	assert.NoError(t, err)
	_, found := m[3]
	assert.False(t, found)

	cnt, err := bf.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), cnt)

	bf2 := roundtripMarshal(t, bf)

	cnt, err = bf2.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), cnt)

	m, err = bf.AllMap(100)
	assert.NoError(t, err)
	_, found = m[3]
	assert.False(t, found)
}

func roundtripMarshal(t *testing.T, in *abi.BitField) *abi.BitField {
	buf := new(bytes.Buffer)
	err := in.MarshalCBOR(buf)
	assert.NoError(t, err)

	bf2 := abi.NewBitField()
	err = bf2.UnmarshalCBOR(buf)
	assert.NoError(t, err)
	return bf2
}
