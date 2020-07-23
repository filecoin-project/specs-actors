package abi

import (
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

const (
	HashFunction = uint64(mh.BLAKE2B_MIN + 31)
	HashLength   = 32
)

type cidBuilder struct {
	codec uint64
}

func (cidBuilder) WithCodec(c uint64) cid.Builder {
	return cidBuilder{codec: c}
}

func (b cidBuilder) GetCodec() uint64 {
	return b.codec
}

func (b cidBuilder) Sum(data []byte) (cid.Cid, error) {
	hf := HashFunction
	if len(data) <= HashLength {
		hf = mh.IDENTITY
	}
	return cid.V1Builder{Codec: b.codec, MhType: hf}.Sum(data)
}

// CidBuilder is the default CID builder for Filecoin.
//
// - The default codec is CBOR. This can be changed with CidBuilder.WithCodec.
// - The default hash function is 256bit blake2b when the data is > 32 bytes
//   long and the identity function when the data is <= 32 bytes long.
var CidBuilder cid.Builder = cidBuilder{codec: cid.DagCBOR}
