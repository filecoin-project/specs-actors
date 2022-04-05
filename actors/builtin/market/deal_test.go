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

	// emtpy bytes

	// bad major type

}

func TestSerializeProposal(t *testing.T) {
	// empty label

	// non empty label
}
