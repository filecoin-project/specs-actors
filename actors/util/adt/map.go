package adt

import (
	"bytes"
	"crypto/sha256"

	hamt "github.com/filecoin-project/go-hamt-ipld/v3"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/specs-actors/v3/actors/builtin"
	cid "github.com/ipfs/go-cid"
	errors "github.com/pkg/errors"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
)

// DefaultHamtOptions specifies default options used to construct Filecoin HAMTs.
// Specific HAMT instances may specify additional options, especially the bitwidth.
var DefaultHamtOptions = []hamt.Option{
	hamt.UseHashFunction(func(input []byte) []byte {
		res := sha256.Sum256(input)
		return res[:]
	}),
}

// DefaultHamtOptionsWithDefaultBitwidth specifies DefaultHamtOptions plus
// a bitwidh of the default hamt bitwidth (5)
var DefaultHamtOptionsWithDefaultBitwidth = []hamt.Option{
	hamt.UseHashFunction(func(input []byte) []byte {
		res := sha256.Sum256(input)
		return res[:]
	}),
	hamt.UseTreeBitWidth(builtin.DefaultHamtBitwidth),
}

// Map stores key-value pairs in a HAMT.
type Map struct {
	lastCid cid.Cid
	root    *hamt.Node
	store   Store
}

// AsMap interprets a store as a HAMT-based map with root `r`.
// The HAMT is interpreted with branching factor 2^bitwidth.
// We could drop this parameter if https://github.com/filecoin-project/go-hamt-ipld/issues/79 is implemented.
func AsMap(s Store, root cid.Cid, bitwidth int) (*Map, error) {
	options := append(DefaultHamtOptions, hamt.UseTreeBitWidth(bitwidth))
	nd, err := hamt.LoadNode(s.Context(), s, root, options...)
	if err != nil {
		return nil, xerrors.Errorf("failed to load hamt node: %w", err)
	}

	return &Map{
		lastCid: root,
		root:    nd,
		store:   s,
	}, nil
}

// Creates a new map backed by an empty HAMT and flushes it to the store.
// The hamt
func MakeEmptyMap(s Store, bitwidth int) *Map {
	options := append(DefaultHamtOptions, hamt.UseTreeBitWidth(bitwidth))
	nd := hamt.NewNode(s, options...)
	return &Map{
		lastCid: cid.Undef,
		root:    nd,
		store:   s,
	}
}

// Returns the root cid of underlying HAMT.
func (m *Map) Root() (cid.Cid, error) {
	if err := m.root.Flush(m.store.Context()); err != nil {
		return cid.Undef, xerrors.Errorf("failed to flush map root: %w", err)
	}

	c, err := m.store.Put(m.store.Context(), m.root)
	if err != nil {
		return cid.Undef, xerrors.Errorf("writing map root object: %w", err)
	}
	m.lastCid = c

	return c, nil
}

// Put adds value `v` with key `k` to the hamt store.
func (m *Map) Put(k abi.Keyer, v cbor.Marshaler) error {
	if err := m.root.Set(m.store.Context(), k.Key(), v); err != nil {
		return errors.Wrapf(err, "map put failed set in node %v with key %v value %v", m.lastCid, k.Key(), v)
	}
	return nil
}

// Get puts the value at `k` into `out`.
func (m *Map) Get(k abi.Keyer, out cbor.Unmarshaler) (bool, error) {
	if err := m.root.Find(m.store.Context(), k.Key(), out); err != nil {
		if err == hamt.ErrNotFound {
			return false, nil
		}
		return false, errors.Wrapf(err, "map get failed find in node %v with key %v", m.lastCid, k.Key())
	}
	return true, nil
}

// Has checks for the existance of a key without deserializing its value.
func (m *Map) Has(k abi.Keyer) (bool, error) {
	if _, err := m.root.FindRaw(m.store.Context(), k.Key()); err != nil {
		if err == hamt.ErrNotFound {
			return false, nil
		}
		return false, errors.Wrapf(err, "map get failed find in node %v with key %v", m.lastCid, k.Key())
	}
	return true, nil
}

// Delete removes the value at `k` from the hamt store.
func (m *Map) Delete(k abi.Keyer) error {
	if err := m.root.Delete(m.store.Context(), k.Key()); err != nil {
		return errors.Wrapf(err, "map delete failed in node %v key %v", m.root, k.Key())
	}

	return nil
}

// Iterates all entries in the map, deserializing each value in turn into `out` and then
// calling a function with the corresponding key.
// Iteration halts if the function returns an error.
// If the output parameter is nil, deserialization is skipped.
func (m *Map) ForEach(out cbor.Unmarshaler, fn func(key string) error) error {
	return m.root.ForEach(m.store.Context(), func(k string, val interface{}) error {
		if out != nil {
			// Why doesn't hamt.ForEach() just return the value as bytes?
			err := out.UnmarshalCBOR(bytes.NewReader(val.(*cbg.Deferred).Raw))
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
