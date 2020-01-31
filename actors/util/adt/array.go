package adt

import (
	"bytes"

	amt "github.com/filecoin-project/go-amt-ipld/v2"
	cid "github.com/ipfs/go-cid"
	errors "github.com/pkg/errors"
	cbg "github.com/whyrusleeping/cbor-gen"

	runtime "github.com/filecoin-project/specs-actors/actors/runtime"
)

// Array stores a contiguous sequence of values in an AMT.
type Array struct {
	root  cid.Cid
	store Store
}

// AsArray interprets a store as an AMT-based array with root `r`.
func AsArray(s Store, r cid.Cid) *Array {
	return &Array{
		root:  r,
		store: s,
	}
}

// Creates a new map backed by an empty HAMT and flushes it to the store.
func MakeEmptyArray(s Store) (*Array, error) {
	root := amt.NewAMT(s)
	newArray := AsArray(s, cid.Undef)
	err := newArray.write(root)
	return newArray, err
}

// Returns the root CID of the underlying AMT.
func (a *Array) Root() cid.Cid {
	return a.root
}

// Appends a value to the end of the array.
func (a *Array) Append(value runtime.CBORMarshaler) error {
	root, err := amt.LoadAMT(a.store.Context(), a.store, a.root)
	if err != nil {
		return errors.Wrapf(err, "array append failed to load root %v", a.root)
	}
	if root.Set(a.store.Context(), root.Count, value) != nil {
		return errors.Wrapf(err, "array append failed to set index %v value %v in root %v, ", root.Count, value, a.root)
	}
	return a.write(root)
}

// Iterates all entries in the array, deserializing each value in turn into `out` and then calling a function.
// Iteration halts if the function returns an error.
// If the output parameter is nil, deserialization is skipped.
func (a *Array) ForEach(out runtime.CBORUnmarshaler, fn func(i int64) error) error {
	root, err := amt.LoadAMT(a.store.Context(), a.store, a.root)
	if err != nil {
		return errors.Wrapf(err, "array foreach failed to load root %v", a.root)
	}
	return root.ForEach(a.store.Context(), func(k uint64, val *cbg.Deferred) error {
		if out != nil {
			// Why doesn't amt.ForEach() just return the value as bytes?
			err = out.UnmarshalCBOR(bytes.NewReader(val.Raw))
			if err != nil {
				return err
			}
		}
		return fn(int64(k))
	})
}

// Writes the root node to storage and sets the new root CID.
func (a *Array) write(root *amt.Root) error {
	newCid, err := root.Flush(a.store.Context()) // this does the store.Put() too, differing from HAMT
	if err != nil {
		return errors.Wrapf(err, "failed to write AMT root %v", root)
	}
	a.root = newCid
	return nil
}
