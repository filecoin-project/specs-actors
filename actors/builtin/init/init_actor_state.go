package init

import (
	"context"
	"fmt"
	"io"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	cid "github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/v8/actors/builtin"
	"github.com/filecoin-project/specs-actors/v8/actors/util/adt"
)

type State struct {
	AddressMap      cid.Cid // HAMT[addr.Address]abi.ActorID
	NextID          abi.ActorID
	NetworkName     string
	InstalledActors cid.Cid
}

func ConstructState(store adt.Store, networkName string) (*State, error) {
	emptyAddressMapCid, err := adt.StoreEmptyMap(store, builtin.DefaultHamtBitwidth)
	if err != nil {
		return nil, xerrors.Errorf("failed to create empty map: %w", err)
	}

	emptyInstalledActors, err := store.Put(context.TODO(), &InstalledActors{})
	if err != nil {
		return nil, xerrors.Errorf("failed to create empty manifest: %w", err)
	}

	return &State{
		AddressMap:      emptyAddressMapCid,
		NextID:          abi.ActorID(builtin.FirstNonSingletonActorId),
		NetworkName:     networkName,
		InstalledActors: emptyInstalledActors,
	}, nil
}

// ResolveAddress resolves an address to an ID-address, if possible.
// If the provided address is an ID address, it is returned as-is.
// This means that mapped ID-addresses (which should only appear as values, not keys) and
// singleton actor addresses (which are not in the map) pass through unchanged.
//
// Returns an ID-address and `true` if the address was already an ID-address or was resolved in the mapping.
// Returns an undefined address and `false` if the address was not an ID-address and not found in the mapping.
// Returns an error only if state was inconsistent.
func (s *State) ResolveAddress(store adt.Store, address addr.Address) (addr.Address, bool, error) {
	// Short-circuit ID address resolution.
	if address.Protocol() == addr.ID {
		return address, true, nil
	}

	// Lookup address.
	m, err := adt.AsMap(store, s.AddressMap, builtin.DefaultHamtBitwidth)
	if err != nil {
		return addr.Undef, false, xerrors.Errorf("failed to load address map: %w", err)
	}

	var actorID cbg.CborInt
	if found, err := m.Get(abi.AddrKey(address), &actorID); err != nil {
		return addr.Undef, false, xerrors.Errorf("failed to get from address map: %w", err)
	} else if found {
		// Reconstruct address from the ActorID.
		idAddr, err := addr.NewIDAddress(uint64(actorID))
		return idAddr, true, err
	} else {
		return addr.Undef, false, nil
	}
}

// Allocates a new ID address and stores a mapping of the argument address to it.
// Returns the newly-allocated address.
func (s *State) MapAddressToNewID(store adt.Store, address addr.Address) (addr.Address, error) {
	actorID := cbg.CborInt(s.NextID)
	s.NextID++

	m, err := adt.AsMap(store, s.AddressMap, builtin.DefaultHamtBitwidth)
	if err != nil {
		return addr.Undef, xerrors.Errorf("failed to load address map: %w", err)
	}
	err = m.Put(abi.AddrKey(address), &actorID)
	if err != nil {
		return addr.Undef, xerrors.Errorf("map address failed to store entry: %w", err)
	}
	amr, err := m.Root()
	if err != nil {
		return addr.Undef, xerrors.Errorf("failed to get address map root: %w", err)
	}
	s.AddressMap = amr

	idAddr, err := addr.NewIDAddress(uint64(actorID))
	return idAddr, err
}

type InstalledActors struct {
	Entries []cid.Cid
}

// this is a flat tuple, so we need to write these by hand
func (d *InstalledActors) UnmarshalCBOR(r io.Reader) error {
	*d = InstalledActors{}

	br := cbg.GetPeeker(r)
	scratch := make([]byte, 8)

	maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	entries := int(extra)
	d.Entries = make([]cid.Cid, 0, entries)

	for i := 0; i < entries; i++ {
		entry, err := cbg.ReadCid(br)
		if err != nil {
			return fmt.Errorf("error unmarsnalling manifest entry: %w", err)
		}

		d.Entries = append(d.Entries, entry)
	}

	return nil
}

func (d *InstalledActors) MarshalCBOR(w io.Writer) error {
	if d == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	scratch := make([]byte, 9)

	if len(d.Entries) > cbg.MaxLength {
		return fmt.Errorf("too many installed actor entries")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajArray, uint64(len(d.Entries))); err != nil {
		return err
	}

	for _, v := range d.Entries {
		if err := cbg.WriteCidBuf(scratch, w, v); err != nil {
			return err
		}
	}

	return nil
}
