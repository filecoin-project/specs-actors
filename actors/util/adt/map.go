package adt

import (
	"bytes"

	cid "github.com/ipfs/go-cid"
	hamt "github.com/ipfs/go-hamt-ipld"
	errors "github.com/pkg/errors"
	cbg "github.com/whyrusleeping/cbor-gen"

	runtime "github.com/filecoin-project/specs-actors/actors/runtime"
)

// Map stores key-value pairs in a HAMT.
type Map struct {
	root  cid.Cid
	store Store
}

// AsMap interprets a store as a HAMT-based map with root `r`.
func AsMap(s Store, r cid.Cid) *Map {
	return &Map{
		root:  r,
		store: s,
	}
}

// Creates a new map backed by an empty HAMT and flushes it to the store.
func MakeEmptyMap(s Store) (*Map, error) {
	nd := hamt.NewNode(s)
	newMap := AsMap(s, cid.Undef)
	err := newMap.write(nd)
	return newMap, err
}

// Returns the root cid of underlying HAMT.
func (m *Map) Root() cid.Cid {
	return m.root
}

// Put adds value `v` with key `k` to the hamt store.
func (m *Map) Put(k Keyer, v runtime.CBORMarshaler) error {
	root, err := hamt.LoadNode(m.store.Context(), m.store, m.root)
	if err != nil {
		return errors.Wrapf(err, "map put failed to load node %v", m.root)
	}
	if err = root.Set(m.store.Context(), k.Key(), v); err != nil {
		return errors.Wrapf(err, "map put failed set in node %v with key %v value %v", m.root, k.Key(), v)
	}
	if err = root.Flush(m.store.Context()); err != nil {
		return errors.Wrapf(err, "map put failed to flush node %v : %v", m.root, err)
	}

	return m.write(root)
}

// Get puts the value at `k` into `out`.
func (m *Map) Get(k Keyer, out runtime.CBORUnmarshaler) (bool, error) {
	root, err := hamt.LoadNode(m.store.Context(), m.store, m.root)
	if err != nil {
		return false, errors.Wrapf(err, "map get failed to load node %v", m.root)
	}
	if err = root.Find(m.store.Context(), k.Key(), out); err != nil {
		if err == hamt.ErrNotFound {
			return false, nil
		}
		return false, errors.Wrapf(err, "map get failed find in node %v with key %v", m.root, k.Key())
	}
	return true, nil
}

// Delete removes the value at `k` from the hamt store.
func (m *Map) Delete(k Keyer) error {
	root, err := hamt.LoadNode(m.store.Context(), m.store, m.root)
	if err != nil {
		return errors.Wrapf(err, "map delete failed to load node %v", m.root)
	}
	if err = root.Delete(m.store.Context(), k.Key()); err != nil {
		return errors.Wrapf(err, "map delete failed in node %v key %v", m.root, k.Key())
	}
	if err = root.Flush(m.store.Context()); err != nil {
		return errors.Wrapf(err, "map delete failed to flush node %v : %v", m.root, err)
	}

	return m.write(root)
}

// Iterates all entries in the map, deserializing each value in turn into `out` and then
// calling a function with the corresponding key.
// Iteration halts if the function returns an error.
// If the output parameter is nil, deserialization is skipped.
func (m *Map) ForEach(out runtime.CBORUnmarshaler, fn func(key string) error) error {
	root, err := hamt.LoadNode(m.store.Context(), m.store, m.root)
	if err != nil {
		return errors.Wrapf(err, "map foreach failed to load root %s", m.root)
	}
	return root.ForEach(m.store.Context(), func(k string, val interface{}) error {
		if out != nil {
			// Why doesn't hamt.ForEach() just return the value as bytes?
			err = out.UnmarshalCBOR(bytes.NewReader(val.(*cbg.Deferred).Raw))
			if err != nil {
				return err
			}
		}
		return fn(k)
	})
}

// Collects all the keys from the map into a slice of strings.
func (m *Map) CollectKeys() (out []string, err error) {
	err = m.ForEach(nil, func(key string) error {
		out = append(out, key)
		return nil
	})
	return
}

// Writes the root node to storage and sets the new root CID.
func (m *Map) write(root *hamt.Node) error {
	newCid, err := m.store.Put(m.store.Context(), root)
	if err != nil {
		return errors.Wrapf(err, "map failed to write node %v", root)
	}
	m.root = newCid
	return nil
}
