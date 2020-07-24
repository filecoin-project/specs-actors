package abi

import (
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

const (
	HashFunction = uint64(mh.BLAKE2B_MIN + 31)
	HashLength   = 32
)

// CidBuilder is the default CID builder for Filecoin.
var CidBuilder cid.Builder = cid.V1Builder{
	Codec:    cid.DagCBOR,
	MhLength: HashLength,
	MhType:   HashFunction,
}
