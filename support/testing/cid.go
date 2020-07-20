package testing

import (
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

var DefaultHashFunction = uint64(mh.BLAKE2B_MIN + 31)
var DefaultCidBuilder = cid.V1Builder{Codec: cid.DagCBOR, MhType: DefaultHashFunction}

func MakeCID(input string, prefix *cid.Prefix) cid.Cid {
	c, err := DefaultCidBuilder.Sum([]byte(input))
	if err != nil {
		panic(err)
	}

	if prefix != nil {
		h, err := mh.Decode(c.Hash())
		if err != nil {
			panic(err)
		}

		ph, err := mh.Encode(h.Digest, prefix.MhType)
		if err != nil {
			panic(err)
		}

		return cid.NewCidV1(prefix.Codec, ph)
	}

	return c
}
