package adt

import (
	"fmt"
	"io"

	cid "github.com/ipfs/go-cid"
	hamt "github.com/ipfs/go-hamt-ipld"

	"github.com/filecoin-project/specs-actors/actors/runtime"
)

type EmptyValue struct{}

var _ runtime.CBORMarshaler = (*EmptyValue)(nil)
var _ runtime.CBORUnmarshaler = (*EmptyValue)(nil)

// 0x80 is empty list (major type 4 with zero length)
// 0xa0 is empty map (major type 5 with zero length)
// This is encoded with empty-list since we use tuple-encoding for everything.
const emptyListEncoded = 0x80

func (EmptyValue) MarshalCBOR(w io.Writer) error {
	_, err := w.Write([]byte{emptyListEncoded})
	return err
}

func (EmptyValue) UnmarshalCBOR(r io.Reader) error {
	buf := make([]byte, 1)
	_, err := r.Read(buf)
	if err != nil {
		return err
	}
	if buf[0] != emptyListEncoded {
		return fmt.Errorf("invalid empty return %x", buf[0])
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

// Writes the root node to storage and sets the new root CID.
func (h *Set) write(root *hamt.Node) error {
	return h.m.write(root)
}
