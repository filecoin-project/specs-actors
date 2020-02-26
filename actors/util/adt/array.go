package adt

import (
	"bytes"

	amt "github.com/filecoin-project/go-amt-ipld/v2"
	cid "github.com/ipfs/go-cid"
	errors "github.com/pkg/errors"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"

	runtime "github.com/filecoin-project/specs-actors/actors/runtime"
)

// Array stores a sparse sequence of values in an AMT.
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

// Appends a value to the end of the array. Assumes continuous array.
// If the array isn't continuous use Set and a separate counter
func (a *Array) AppendContinuous(value runtime.CBORMarshaler) error {
	root, err := amt.LoadAMT(a.store.Context(), a.store, a.root)
	if err != nil {
		return errors.Wrapf(err, "array append failed to load root %v", a.root)
	}
	if root.Set(a.store.Context(), root.Count, value) != nil {
		return errors.Wrapf(err, "array append failed to set index %v value %v in root %v, ", root.Count, value, a.root)
	}
	return a.write(root)
}

func (a *Array) Set(i uint64, value runtime.CBORMarshaler) error {
	root, err := amt.LoadAMT(a.store.Context(), a.store, a.root)
	if err != nil {
		return errors.Wrapf(err, "array set failed to load root %v", a.root)
	}
	if root.Set(a.store.Context(), i, value) != nil {
		return errors.Wrapf(err, "array set failed to set index %v value %v in root %v, ", i, value, a.root)
	}
	return a.write(root)
}

func (a *Array) Delete(i uint64) error {
	root, err := amt.LoadAMT(a.store.Context(), a.store, a.root)
	if err != nil {
		return errors.Wrapf(err, "array delete failed to load root %v", a.root)
	}
	if root.Delete(a.store.Context(), i) != nil {
		return errors.Wrapf(err, "array delete failed to delete index %v in root %v, ", i, a.root)
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

func (a *Array) Length() (uint64, error) {
	root, err := amt.LoadAMT(a.store.Context(), a.store, a.root)
	if err != nil {
		return 0, err
	}
	return root.Count, nil
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

// Get retrieves array element into the 'out' unmarshaler, returning a boolean
//  indicating whether the element was found in the array
func (a *Array) Get(k uint64, out runtime.CBORUnmarshaler) (bool, error) {
	root, err := amt.LoadAMT(a.store.Context(), a.store, a.root)
	if err != nil {
		return false, xerrors.Errorf("array get failed to load root %v: %w", a.root, err)
	}

	err = root.Get(a.store.Context(), k, out)
	if err == nil {
		return true, nil
	}
	if _, nf := err.(*amt.ErrNotFound); nf {
		return false, nil
	}

	return false, err
}
