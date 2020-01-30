package adt

import (
	cid "github.com/ipfs/go-cid"
)

// Set interprets a Map as a set, storing keys (with empty values) in a HAMT.
type Set struct {
	m *Map
}

// AsSet interprets a store as a HAMT-based set with root `r`.
func AsSet(s Store, r cid.Cid) *Set {
	return &Set{
		m: AsMap(s, r),
	}
}

// NewSet creates a new HAMT with root `r` and store `s`.
func MakeEmptySet(s Store) (*Set, error) {
	m, err := MakeEmptyMap(s)
	if err != nil {
		return nil, err
	}
	return &Set{m}, nil
}

// Root return the root cid of HAMT.
func (h *Set) Root() cid.Cid {
	return h.m.root
}

// Put adds `k` to the set.
func (h *Set) Put(k Keyer) error {
	return h.m.Put(k, EmptyValue{})
}

// Has returns true iff `k` is in the set.
func (h *Set) Has(k Keyer) (bool, error) {
	return h.m.Get(k, nil)
}

// Delete removes `k` from the set.
func (h *Set) Delete(k Keyer) error {
	return h.m.Delete(k)
}

// Collects all the keys from the set into a slice of strings.
func (h *Set) CollectKeys() (out []string, err error) {
	return h.m.CollectKeys()
}
