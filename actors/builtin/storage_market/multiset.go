package storage_market

import (
	cid "github.com/ipfs/go-cid"
	errors "github.com/pkg/errors"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

type Multiset struct {
	mp *adt.Map
	store adt.Store
}

// Interprets a store as a HAMT-based map of AMTs with root `r`.
func AsMultiset(s adt.Store, r cid.Cid) *Multiset {
	return &Multiset{mp: adt.AsMap(s, r), store: s}
}

// Creates a new map backed by an empty HAMT and flushes it to the store.
func MakeEmptyMultiset(s adt.Store) (*Multiset, error) {
	m, err := adt.MakeEmptyMap(s)
	return &Multiset{m, s}, err
}

// Returns the root cid of the underlying HAMT.
func (mm *Multiset) Root() cid.Cid {
	return mm.mp.Root()
}

// Adds a value for a key.
func (mm *Multiset) Add(key adt.Keyer, idx uint64) error {
	// Load the array under key, or initialize a new empty one if not found.
	arr, found, err := mm.get(key)
	if err != nil {
		return err
	}
	if !found {
		arr, err = adt.MakeEmptyArray(mm.store)
		if err != nil {
			return errors.Wrapf(err, "failed to initialize multiset set value under root %v", mm.mp.Root())
		}
	}

	// Append to the array.
	if err = arr.Set(idx, adt.EmptyValue{}); err != nil {
		return errors.Wrapf(err, "failed to add multiset key %v", key)
	}

	// Store the new array root under key.
	newArrayRoot := cbg.CborCid(arr.Root())
	err = mm.mp.Put(key, &newArrayRoot)
	if err != nil {
		return errors.Wrapf(err, "failed to store multiset array")
	}
	return nil
}

// Removes a value for a key.
func (mm *Multiset) Remove(key adt.Keyer, idx uint64) error {
	// Load the array under key, or initialize a new empty one if not found.
	arr, found, err := mm.get(key)
	if err != nil {
		return err
	}
	if !found {
		return adt.ErrNotFound{
			Root: arr.Root(),
			Key:  []interface{}{key, idx},
		}
	}

	// Append to the array.
	if err = arr.Delete(idx); err != nil {
		return errors.Wrapf(err, "failed to remove multiset key %v", key)
	}

	// Store the new array root under key.
	newArrayRoot := cbg.CborCid(arr.Root())
	err = mm.mp.Put(key, &newArrayRoot)
	if err != nil {
		return errors.Wrapf(err, "failed to store multiset array")
	}
	return nil
}

// Removes all values for a key.
func (mm *Multiset) RemoveAll(key adt.Keyer) error {
	err := mm.mp.Delete(key)
	if err != nil {
		return errors.Wrapf(err, "failed to delete multiset key %v root %v", key, mm.mp.Root())
	}
	return nil
}

// Iterates all entries for a key in the order they were inserted, deserializing each value in turn into `out` and then
// calling a function.
// Iteration halts if the function returns an error.
// If the output parameter is nil, deserialization is skipped.
func (mm *Multiset) ForEach(key adt.Keyer, fn func(i int64) error) error {
	array, found, err := mm.get(key)
	if err != nil {
		return err
	}
	if found {
		return array.ForEach(adt.EmptyValue{}, fn)
	}
	return nil
}

func (mm *Multiset) get(key adt.Keyer) (*adt.Array, bool, error) {
	var arrayRoot cbg.CborCid
	found, err := mm.mp.Get(key, &arrayRoot)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to load multiset key %v", key)
	}
	var set *adt.Array
	if found {
		set = adt.AsArray(mm.store, cid.Cid(arrayRoot))
	}
	return set, found, nil
}
