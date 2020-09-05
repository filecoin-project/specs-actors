package crypto

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func TestEquality(t *testing.T) {
	a := Signature{
		Type: SigTypeSecp256k1,
		Data: []byte{1, 2, 3, 4},
	}

	assert.True(t, a.Equals(&a))
	assert.False(t, a.Equals(&Signature{
		Type: SigTypeBLS,
		Data: a.Data,
	}))
	assert.False(t, a.Equals(&Signature{
		Type: a.Type,
		Data: []byte{1, 1, 1, 1},
	}))
}

func TestCBOR(t *testing.T) {
	a := Signature{
		Type: SigTypeSecp256k1,
		Data: []byte{1, 2, 3, 4},
	}
	b := Signature{
		Type: SigTypeBLS,
		Data: []byte{5, 6, 7, 8},
	}

	t.Run("round trip secp", func(t *testing.T) {
		var buf bytes.Buffer
		var out Signature
		require.NoError(t, a.MarshalCBOR(&buf))
		require.NoError(t, out.UnmarshalCBOR(&buf))
		require.True(t, out.Equals(&a))
	})
	t.Run("round trip bls", func(t *testing.T) {
		var buf bytes.Buffer
		var out Signature
		require.NoError(t, b.MarshalCBOR(&buf))
		require.NoError(t, out.UnmarshalCBOR(&buf))
		require.True(t, out.Equals(&b))
	})

	t.Run("require byte string", func(t *testing.T) {
		var buf bytes.Buffer
		var out Signature
		require.NoError(t, cbg.CborWriteHeader(&buf, cbg.MajArray, 10))
		assert.EqualError(t, out.UnmarshalCBOR(&buf), "not a byte string")
	})
	t.Run("require max length", func(t *testing.T) {
		var buf bytes.Buffer
		var out Signature
		require.NoError(t, cbg.CborWriteHeader(&buf, cbg.MajByteString, SignatureMaxLength+1))
		assert.EqualError(t, out.UnmarshalCBOR(&buf), "string too long")
	})
	t.Run("require non-empty", func(t *testing.T) {
		var buf bytes.Buffer
		var out Signature
		require.NoError(t, cbg.CborWriteHeader(&buf, cbg.MajByteString, 0))
		assert.EqualError(t, out.UnmarshalCBOR(&buf), "string empty")
	})
	t.Run("require correct length", func(t *testing.T) {
		var buf bytes.Buffer
		var out Signature
		require.NoError(t, cbg.CborWriteHeader(&buf, cbg.MajByteString, 10))
		buf.WriteString("7 bytes")
		assert.EqualError(t, out.UnmarshalCBOR(&buf), "unexpected EOF")
	})
	t.Run("require valid type", func(t *testing.T) {
		var buf bytes.Buffer
		var out Signature
		require.NoError(t, cbg.CborWriteHeader(&buf, cbg.MajByteString, 4))
		buf.Write([]byte{byte(SigTypeUnknown)})
		buf.WriteString("sig")
		assert.EqualError(t, out.UnmarshalCBOR(&buf), "invalid signature type in cbor input: 255")
	})
	t.Run("happy decode", func(t *testing.T) {
		var buf bytes.Buffer
		var out Signature
		require.NoError(t, cbg.CborWriteHeader(&buf, cbg.MajByteString, 4))
		buf.Write([]byte{byte(SigTypeSecp256k1)})
		buf.WriteString("sig")
		assert.NoError(t, out.UnmarshalCBOR(&buf))
	})
}
