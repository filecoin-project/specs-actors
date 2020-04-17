package testing

import (
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	mh "github.com/multiformats/go-multihash"
)

// NewCidForTestGetter returns a closure that returns a Cid unique to that invocation.
// The Cid is unique wrt the closure returned, not globally. You can use this function
// in tests.
func NewCidForTestGetter() func() cid.Cid {
	i := 31337
	return func() cid.Cid {
		obj, err := cbor.WrapObject([]int{i}, uint64(mh.BLAKE2B_MIN+31), -1)
		if err != nil {
			panic(err)
		}
		i++
		return obj.Cid()
	}
}
