package adt

import (
	"bytes"
	"context"

	cid "github.com/ipfs/go-cid"
	hamt "github.com/ipfs/go-hamt-ipld"
	errors "github.com/pkg/errors"
	cbg "github.com/whyrusleeping/cbor-gen"

	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
)

// Store defines an interface required to back Map.
type Store interface {
	Context() context.Context
	hamt.CborIpldStore
}

// Keyer defines an interface required to put values in Map.
type Keyer interface {
	Key() string
}

// Map stores data in a HAMT.
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

// Root return the root cid of HAMT.
func (h *Map) Root() cid.Cid {
	return h.root
}

// Put adds value `v` with key `k` to the hamt store.
func (h *Map) Put(k Keyer, v vmr.CBORMarshaler) error {
	root, err := hamt.LoadNode(h.store.Context(), h.store, h.root)
	if err != nil {
		return errors.Wrapf(err, "Map Put failed to load node %v", h.root)
	}
	if err = root.Set(h.store.Context(), k.Key(), v); err != nil {
		return errors.Wrapf(err, "Map Put failed set in node %v with key %v value %v", h.root, k.Key(), v)
	}
	if err = root.Flush(h.store.Context()); err != nil {
		return errors.Wrapf(err, "Map Put failed to flush node %v : %v", h.root, err)
	}

	return h.write(root)
}

// Get puts the value at `k` into `out`.
func (h *Map) Get(k Keyer, out vmr.CBORUnmarshaler) (bool, error) {
	root, err := hamt.LoadNode(h.store.Context(), h.store, h.root)
	if err != nil {
		return false, errors.Wrapf(err, "Map Get failed to load node %v", h.root)
	}
	if err := root.Find(h.store.Context(), k.Key(), out); err != nil {
		if err == hamt.ErrNotFound {
			return false, nil
		}
		return false, errors.Wrapf(err, "Map Get failed find in node %v with key %v", h.root, k.Key())
	}
	return true, nil
}

// Delete removes the value at `k` from the hamt store.
func (h *Map) Delete(k Keyer) error {
	root, err := hamt.LoadNode(h.store.Context(), h.store, h.root)
	if err != nil {
		return errors.Wrapf(err, "Map Delete failed to load node %v", h.root)
	}
	if err = root.Delete(h.store.Context(), k.Key()); err != nil {
		return errors.Wrapf(err, "Map Delete failed in node %v key %v", h.root, k.Key())
	}
	if err = root.Flush(h.store.Context()); err != nil {
		return errors.Wrapf(err, "Map Delete failed to flush node %v : %v", h.root, err)
	}

	return h.write(root)
}

// ForEach iterates all entries in the map, deserializing each value in turn into `out` and then
// calling a function with the corresponding key.
// If the output parameter is nil, deserialization is skipped.
func (h *Map) ForEach(out vmr.CBORUnmarshaler, fn func(key string) error) error {
	oldRoot, err := hamt.LoadNode(h.store.Context(), h.store, h.root)
	if err != nil {
		return errors.Wrapf(err, "Map Delete failed to persist changes to store %s", h.root)
	}
	return oldRoot.ForEach(h.store.Context(), func(k string, val interface{}) error {
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
func (h *Map) CollectKeys() (out []string, err error) {
	err = h.ForEach(nil, func(key string) error {
		out = append(out, key)
		return nil
	})
	return
}

// Writes the root node to storage and sets the new root CID.
func (h *Map) write(root *hamt.Node) error {
	newCid, err := h.store.Put(h.store.Context(), root)
	if err != nil {
		return errors.Wrapf(err, "Map ForEach failed to load node %v", h.root)
	}
	h.root = newCid
	return nil
}

// AsStore allows Runtime to satisfy the adt.Store interface.
func AsStore(rt vmr.Runtime) Store {
	return rtStore{rt}
}

var _ Store = &rtStore{}

type rtStore struct {
	vmr.Runtime
}

func (r rtStore) Context() context.Context {
	return r.Runtime.Context()
}

func (r rtStore) Get(ctx context.Context, c cid.Cid, out interface{}) error {
	if !r.IpldGet(c, out.(vmr.CBORUnmarshaler)) {
		r.AbortStateMsg("not found")
	}
	return nil
}

func (r rtStore) Put(ctx context.Context, v interface{}) (cid.Cid, error) {
	return r.IpldPut(v.(vmr.CBORMarshaler)), nil
}
