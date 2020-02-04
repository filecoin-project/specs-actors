package adt

import (
	"bytes"

	amt "github.com/filecoin-project/go-amt-ipld/v2"
	cid "github.com/ipfs/go-cid"
	errors "github.com/pkg/errors"
	cbg "github.com/whyrusleeping/cbor-gen"

	runtime "github.com/filecoin-project/specs-actors/actors/runtime"
)

// IntMap stores key-value pairs, with integer keys, in a AMT.
type IntMap struct {
	root  cid.Cid
	store Store
}

// Interprets a store as an AMT-based map with root `r`.
func AsIntMap(s Store, r cid.Cid) *IntMap {
	return &IntMap{
		root:  r,
		store: s,
	}
}

// Creates a new map backed by an empty AMT and flushes it to the store.
func MakeEmptyIntMap(s Store) (*IntMap, error) {
	nd := amt.NewAMT(s)
	newMap := AsIntMap(s, cid.Undef)
	err := newMap.write(nd)
	return newMap, err
}

// Returns the root cid of underlying AMT.
func (m *IntMap) Root() cid.Cid {
	return m.root
}

// Put adds value `v` with key `k` to the AMT store.
func (m *IntMap) Put(i uint64, v runtime.CBORMarshaler) error {
	root, err := amt.LoadAMT(m.store.Context(), m.store, m.root)
	if err != nil {
		return errors.Wrapf(err, "intmap put failed to load node %v", m.root)
	}
	if err = root.Set(m.store.Context(), i, v); err != nil {
		return errors.Wrapf(err, "intmap put failed set in node %v with key %v value %v", m.root, i, v)
	}
	return m.write(root)
}

// Get puts the value at `k` into `out`.
func (m *IntMap) Get(i uint64, out runtime.CBORUnmarshaler) (bool, error) {
	root, err := amt.LoadAMT(m.store.Context(), m.store, m.root)
	if err != nil {
		return false, errors.Wrapf(err, "intmap get failed to load node %v", m.root)
	}
	if err = root.Get(m.store.Context(), i, out); err != nil {
		if _, ok := err.(*amt.ErrNotFound); ok {
			return false, nil
		}
		return false, errors.Wrapf(err, "intmap get failed find in node %v with key %v", m.root, i)
	}
	return true, nil
}

// Delete removes the value at `k` from the hamt store.
func (m *IntMap) Delete(k uint64) error {
	root, err := amt.LoadAMT(m.store.Context(), m.store, m.root)
	if err != nil {
		return errors.Wrapf(err, "intmap delete failed to load node %v", m.root)
	}
	if err = root.Delete(m.store.Context(), k); err != nil {
		return errors.Wrapf(err, "intmap delete failed in node %v key %v", m.root, k)
	}

	return m.write(root)
}

// Iterates all entries in the map, deserializing each value in turn into `out` and then
// calling a function with the corresponding key.
// Iteration halts if the function returns an error.
// If the output parameter is nil, deserialization is skipped.
func (m *IntMap) ForEach(out runtime.CBORUnmarshaler, fn func(idx uint64) error) error {
	root, err := amt.LoadAMT(m.store.Context(), m.store, m.root)
	if err != nil {
		return errors.Wrapf(err, "intmap foreach failed to load root %s", m.root)
	}
	return root.ForEach(m.store.Context(), func(idx uint64, val *cbg.Deferred) error {
		if out != nil {
			// Why doesn't hamt.ForEach() just return the value as bytes?
			err = out.UnmarshalCBOR(bytes.NewReader(val.Raw))
			if err != nil {
				return err
			}
		}
		return fn(idx)
	})
}

// Writes the root node to storage and sets the new root CID.
func (m *IntMap) write(root *amt.Root) error {
	newCid, err := root.Flush(m.store.Context()) // this does the store.Put() too, differing from HAMT
	if err != nil {
		return errors.Wrapf(err, "failed to write AMT root %v", root)
	}
	m.root = newCid
	return nil
}
