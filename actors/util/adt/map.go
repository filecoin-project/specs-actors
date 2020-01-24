package adt

import (
	"context"

	cid "github.com/ipfs/go-cid"
	hamt "github.com/ipfs/go-hamt-ipld"

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

// NewMap creates a new HAMT with root `r` and store `s`.
func NewMap(s Store, r cid.Cid) *Map {
	return &Map{
		root:  r,
		store: s,
	}
}

// Root return the root cid of HAMT.
func (h *Map) Root() cid.Cid {
	return h.root
}

// Put adds value `v` with key `k` to the hamt store.
func (h *Map) Put(k Keyer, v vmr.CBORMarshalable) error {
	oldRoot, err := hamt.LoadNode(h.store.Context(), h.store, h.root)
	if err != nil {
		return err
	}
	if err := oldRoot.Set(h.store.Context(), k.Key(), v); err != nil {
		return err
	}
	if err := oldRoot.Flush(h.store.Context()); err != nil {
		return err
	}

	// update the root
	newRoot, err := h.store.Put(h.store.Context(), oldRoot)
	if err != nil {
		return err
	}
	h.root = newRoot
	return nil
}

// Get puts the value at `k` into `out`.
func (h *Map) Get(k Keyer, out vmr.CBORUnmarshalable) error {
	oldRoot, err := hamt.LoadNode(h.store.Context(), h.store, h.root)
	if err != nil {
		return err
	}
	return oldRoot.Find(h.store.Context(), k.Key(), out)
}

// Delete removes the value at `k` from the hamt store.
func (h *Map) Delete(k Keyer) error {
	oldRoot, err := hamt.LoadNode(h.store.Context(), h.store, h.root)
	if err != nil {
		return err
	}
	if err := oldRoot.Delete(h.store.Context(), k.Key()); err != nil {
		return err
	}
	if err := oldRoot.Flush(h.store.Context()); err != nil {
		return err
	}

	// update the root
	newRoot, err := h.store.Put(h.store.Context(), oldRoot)
	if err != nil {
		return err
	}
	h.root = newRoot
	return nil
}

// ForEach applies fn to each key value in hamt.
func (h *Map) ForEach(fn func(key string, v interface{}) error) error {
	oldRoot, err := hamt.LoadNode(h.store.Context(), h.store, h.root)
	if err != nil {
		return err
	}
	if err := oldRoot.ForEach(h.store.Context(), fn); err != nil {
		return err
	}
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
	if !r.IpldGet(c, out.(vmr.CBORUnmarshalable)) {
		r.AbortStateMsg("not found")
	}
	return nil
}

func (r rtStore) Put(ctx context.Context, v interface{}) (cid.Cid, error) {
	return r.IpldPut(v.(vmr.CBORMarshalable)), nil
}
