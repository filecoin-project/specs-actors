package migration

import (
	"context"

	init0 "github.com/filecoin-project/specs-actors/actors/builtin/init"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	init2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/init"
)

type initMigrator struct {
}

func (m *initMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid) (cid.Cid, error) {
	var inState init0.State
	if err := store.Get(ctx, head, &inState); err != nil {
		return cid.Undef, err
	}

	// Migrate address resolution map
	addrMapRoot, err := m.migrateAddrs(ctx, store, inState.AddressMap)
	if err != nil {
		return cid.Undef, xerrors.Errorf("migrate addrs: %w", err)
	}

	outState := init2.State{
		AddressMap:  addrMapRoot,
		NextID:      inState.NextID,
		NetworkName: inState.NetworkName,
	}
	return store.Put(ctx, &outState)
}

func (m *initMigrator) migrateAddrs(ctx context.Context, store cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// The HAMT has changed, but the value type (Address) is identical.
	return migrateHAMTRaw(ctx, store, root)
}
