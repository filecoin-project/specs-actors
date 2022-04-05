package market_test

import (
	"bytes"
	"testing"

	"github.com/filecoin-project/specs-actors/v8/actors/builtin/market"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDealLabel(t *testing.T) {

	// empty label serialize and deserialize to string label of empty string

	label1 := market.EmptyDealLabel
	buf := bytes.Buffer{}
	require.NoError(t, label1.MarshalCBOR(&buf), "failed to marshal empty deal label")
	ser := buf.Bytes()
	assert.Equal(t, 0x60, ser[0]) // cbor empty str (maj type 3)
	label2 := &market.DealLabel{}
	require.NoError(t, label2.UnmarshalCBOR(bytes.NewReader(ser)))
	assert.True(t, label2.IsString())
	assert.False(t, label2.IsEmpty() || label2.IsBytes())

	// bytes label

	// string label

	//

}

func TestDealLabelFromCBOR(t *testing.T) {
	// empty string
	// b011_00000
	emptyCBORText := []byte{0x60}
	var label1 market.DealLabel
	require.NoError(t, label1.UnmarshalCBOR(bytes.NewReader(emptyCBORText)))

	// valid utf8 string
	// b011_00100 "deadbeef"
	cborTestValid := append([]byte{0x64}, []byte("deadbeef")...)
	var label2 market.DealLabel
	require.NoError(t, label2.UnmarshalCBOR(bytes.NewReader(cborTestValid)))

	// invalid utf8 string
	// b011_00100 0xde 0xad 0xbe 0xef
	cborText := []byte{0x64, 0xde, 0xad, 0xbe, 0xef}
	var label3 market.DealLabel
	err := label3.UnmarshalCBOR(bytes.NewReader(cborText))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not valid utf8")

	// empty bytes
	// b010_00000
	emptyCBORBytes := []byte{0x40}
	var label4 market.DealLabel
	require.NoError(t, label4.UnmarshalCBOR(bytes.NewReader(emptyCBORBytes)))

	// bytes
	// b010_00100 0xde 0xad 0xbe 0xef
	cborBytes := []byte{0x44, 0xde, 0xad, 0xbe, 0xef}
	var label5 market.DealLabel
	require.NoError(t, label5.UnmarshalCBOR(bytes.NewReader(cborBytes)))

	// bad major type
	// array of empty array b100_00001 b100_00000
	arrayBytes := []byte{0x81, 0x80}
	var label6 market.DealLabel
	err = label6.UnmarshalCBOR(bytes.NewReader(arrayBytes))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected major tag")
}

func TestSerializeProposal(t *testing.T) {
	// empty label

	// non empty label
}
