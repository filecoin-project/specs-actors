package adt

import (
	"context"

	cid "github.com/ipfs/go-cid"
	hamt "github.com/ipfs/go-hamt-ipld"
	cbg "github.com/whyrusleeping/cbor-gen"
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
func (h *Map) Put(k Keyer, v cbg.CBORMarshaler) error {
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
func (h *Map) Get(k Keyer, out cbg.CBORUnmarshaler) error {
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
