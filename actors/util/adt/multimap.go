package adt

import (
	cid "github.com/ipfs/go-cid"
	errors "github.com/pkg/errors"
	cbg "github.com/whyrusleeping/cbor-gen"

	runtime "github.com/filecoin-project/specs-actors/actors/runtime"
)

// Multimap stores multiple values per key in a HAMT of AMTs.
// The order of insertion of values for each key is retained.
type Multimap struct {
	mp *Map
}

// Interprets a store as a HAMT-based map of AMTs with root `r`.
func AsMultimap(s Store, r cid.Cid) *Multimap {
	return &Multimap{AsMap(s, r)}
}

// Creates a new map backed by an empty HAMT and flushes it to the store.
func MakeEmptyMultiap(s Store) (*Multimap, error) {
	m, err := MakeEmptyMap(s)
	return &Multimap{m}, err
}

// Returns the root cid of the underlying HAMT.
func (mm *Multimap) Root() cid.Cid {
	return mm.mp.Root()
}

// Adds a value for a key.
func (mm *Multimap) Add(key Keyer, value runtime.CBORMarshaler) error {
	// Load the array under key, or initialize a new empty one if not found.
	array, found, err := mm.get(key)
	if err != nil {
		return err
	}
	if !found {
		array, err = MakeEmptyArray(mm.mp.store)
		if err != nil {
			return errors.Wrapf(err, "failed to initialize multimap array value under root %v", mm.mp.root)
		}
	}

	// Append to the array.
	if err = array.Append(value); err != nil {
		return errors.Wrapf(err, "failed to add multimap key %v value %v", key, value)
	}

	// Store the new array root under key.
	newArrayRoot := cbg.CborCid(array.root)
	err = mm.mp.Put(key, &newArrayRoot)
	if err != nil {
		return errors.Wrapf(err, "failed to store multimap values")
	}
	return nil
}

// Removes all values for a key.
func (mm *Multimap) RemoveAll(key Keyer) error {
	err := mm.mp.Delete(key)
	if err != nil {
		return errors.Wrapf(err, "failed to delete multimap key %v root %v", key, mm.mp.root)
	}
	return nil
}

// Iterates all entries for a key in the order they were inserted, deserializing each value in turn into `out` and then
// calling a function.
// Iteration halts if the function returns an error.
// If the output parameter is nil, deserialization is skipped.
func (mm *Multimap) ForEach(key Keyer, out runtime.CBORUnmarshaler, fn func(i int64) error) error {
	array, found, err := mm.get(key)
	if err != nil {
		return err
	}
	if found {
		return array.ForEach(out, fn)
	}
	return nil
}

func (mm *Multimap) get(key Keyer) (*Array, bool, error) {
	var arrayRoot cbg.CborCid
	found, err := mm.mp.Get(key, &arrayRoot)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to load multimap key %v", key)
	}
	var array *Array
	if found {
		array = AsArray(mm.mp.store, cid.Cid(arrayRoot))
	}
	return array, found, nil
}
