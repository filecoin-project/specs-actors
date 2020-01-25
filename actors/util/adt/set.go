package adt

import (
	"io"

	cid "github.com/ipfs/go-cid"
	hamt "github.com/ipfs/go-hamt-ipld"
	cbg "github.com/whyrusleeping/cbor-gen"
)

var setValue hamtSetValue

func init() {
	setValue = hamtSetValue(1)
}

type hamtSetValue uint64

func (s hamtSetValue) MarshalCBOR(w io.Writer) error {
	if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(s))); err != nil {
		return err
	}
	return nil
}

// Set stores data in a HAMT.
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
	nd := hamt.NewNode(s)
	newSet := AsSet(s, cid.Undef)
	err := newSet.write(nd)
	return newSet, err
}

// Root return the root cid of HAMT.
func (h *Set) Root() cid.Cid {
	return h.m.root
}

// Put adds `k` to the set.
func (h *Set) Put(k Keyer) error {
	return h.m.Put(k, setValue)
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

// Writes the root node to storage and sets the new root CID.
func (h *Set) write(root *hamt.Node) error {
	return h.m.write(root)
}
