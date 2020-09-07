package migration

import (
	"context"

	addr "github.com/filecoin-project/go-address"
	verifreg0 "github.com/filecoin-project/specs-actors/actors/builtin/verifreg"
	adt0 "github.com/filecoin-project/specs-actors/actors/util/adt"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	verifreg2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/verifreg"
	adt2 "github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type verifregMigrator struct {
}

func (m *verifregMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid) (cid.Cid, error) {
	var inState verifreg0.State
	if err := store.Get(ctx, head, &inState); err != nil {
		return cid.Undef, err
	}

	verifiersRoot, err := m.migrateCapTable(ctx, store, inState.Verifiers)
	if err != nil {
		return cid.Undef, err
	}

	clientsRoot, err := m.migrateCapTable(ctx, store, inState.VerifiedClients)
	if err != nil {
		return cid.Undef, err
	}

	if inState.RootKey.Protocol() != addr.ID {
		return cid.Undef, xerrors.Errorf("unexpected non-ID root key address %v", inState.RootKey)
	}

	outState := verifreg2.State{
		RootKey:         inState.RootKey,
		Verifiers:       verifiersRoot,
		VerifiedClients: clientsRoot,
	}
	return store.Put(ctx, &outState)
}

func (m *verifregMigrator) migrateCapTable(ctx context.Context, store cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// The HAMT has changed, but the value type (big.Int) is identical.
	// The keys must be normalized to ID-addresses.

	inMap, err := adt0.AsMap(adt0.WrapStore(ctx, store), root)
	if err != nil {
		return cid.Undef, err
	}
	outMap := adt2.MakeEmptyMap(adt2.WrapStore(ctx, store))

	var inCap verifreg0.DataCap
	if err = inMap.ForEach(&inCap, func(key string) error {
		inAddr, err := addr.NewFromBytes([]byte(key))
		if err != nil {
			return err
		}
		if inAddr.Protocol() != addr.ID {
			// TODO: this is expected, we need to resolve them
			return xerrors.Errorf("unexpected non-ID cap table address %v", inAddr)
		}
		outAddr := inAddr
		outCap := verifreg2.DataCap(inCap) // Identical
		return outMap.Put(verifreg2.AddrKey(outAddr), &outCap)
	}); err != nil {
		return cid.Undef, err
	}

	return outMap.Root()
}
