package market

import (
	"reflect"

	"github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	errors "github.com/pkg/errors"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

type SetMultimap struct {
	mp    *adt.Map
	store adt.Store
}

// Interprets a store as a HAMT-based map of HAMT-based sets with root `r`.
func AsSetMultimap(s adt.Store, r cid.Cid) (*SetMultimap, error) {
	m, err := adt.AsMap(s, r)
	if err != nil {
		return nil, err
	}
	return &SetMultimap{mp: m, store: s}, nil
}

// Creates a new map backed by an empty HAMT and flushes it to the store.
func MakeEmptySetMultimap(s adt.Store) *SetMultimap {
	m := adt.MakeEmptyMap(s)
	return &SetMultimap{m, s}
}

// Returns the root cid of the underlying HAMT.
func (mm *SetMultimap) Root() (cid.Cid, error) {
	return mm.mp.Root()
}

func (mm *SetMultimap) Put(key address.Address, v abi.DealID) error {
	// Load the hamt under key, or initialize a new empty one if not found.
	k := adt.AddrKey(key)
	set, found, err := mm.get(k)
	if err != nil {
		return err
	}
	if !found {
		set = adt.MakeEmptySet(mm.store)
	}

	// Add to the set.
	if err = set.Put(dealKey(v)); err != nil {
		return errors.Wrapf(err, "failed to add key to set %v", key)
	}

	src, err := set.Root()
	if err != nil {
		return xerrors.Errorf("failed to flush set root: %w", err)
	}
	// Store the new set root under key.
	newSetRoot := cbg.CborCid(src)
	err = mm.mp.Put(k, &newSetRoot)
	if err != nil {
		return errors.Wrapf(err, "failed to store set")
	}
	return nil
}

// Removes a value for a key.
func (mm *SetMultimap) Remove(key address.Address, v abi.DealID) error {
	k := adt.AddrKey(key)
	// Load the set under key, or initialize a new empty one if not found.
	set, found, err := mm.get(k)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	// Append to the set.
	if err = set.Delete(dealKey(v)); err != nil {
		return errors.Wrapf(err, "failed to remove set key %v", key)
	}

	// Store the new set root under key.
	src, err := set.Root()
	if err != nil {
		return xerrors.Errorf("failed to flush set root: %w", err)
	}

	newSetRoot := cbg.CborCid(src)
	err = mm.mp.Put(k, &newSetRoot)
	if err != nil {
		return errors.Wrapf(err, "failed to store set root")
	}
	return nil
}

// Removes all values for a key.
func (mm *SetMultimap) RemoveAll(key address.Address) error {
	err := mm.mp.Delete(adt.AddrKey(key))
	if err != nil {
		return xerrors.Errorf("failed to delete set key %v: %w", key, err)
	}
	return nil
}

// Iterates all entries for a key, iteration halts if the function returns an error.
func (mm *SetMultimap) ForEach(key address.Address, fn func(id abi.DealID) error) error {
	set, found, err := mm.get(adt.AddrKey(key))
	if err != nil {
		return err
	}
	if found {
		return set.ForEach(func(k string) error {
			v, err := parseDealKey(k)
			if err != nil {
				return err
			}
			return fn(v)
		})
	}
	return nil
}

func (mm *SetMultimap) get(key adt.Keyer) (*adt.Set, bool, error) {
	var setRoot cbg.CborCid
	found, err := mm.mp.Get(key, &setRoot)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to load set key %v", key)
	}
	var set *adt.Set
	if found {
		set, err = adt.AsSet(mm.store, cid.Cid(setRoot))
		if err != nil {
			return nil, false, err
		}
	}
	return set, found, nil
}

func dealKey(e abi.DealID) adt.Keyer {
	return adt.UIntKey(uint64(e))
}

func parseDealKey(s string) (abi.DealID, error) {
	key, err := adt.ParseUIntKey(s)
	return abi.DealID(key), err
}

func init() {
	// Check that DealID is indeed an unsigned integer to confirm that dealKey is making the right interpretation.
	var e abi.DealID
	if reflect.TypeOf(e).Kind() != reflect.Uint64 {
		panic("incorrect sector number encoding")
	}
}
