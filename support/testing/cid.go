package testing

import (
	"testing"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var DefaultHashFunction = uint64(mh.BLAKE2B_MIN + 31)
var DefaultCidBuilder = cid.V1Builder{Codec: cid.DagCBOR, MhType: DefaultHashFunction}

func CidFromString(t *testing.T, input string) cid.Cid {
	c, err := DefaultCidBuilder.Sum([]byte(input))
	require.NoError(t, err)
	return c
}
