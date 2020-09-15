package migration

import (
	"context"

	account0 "github.com/filecoin-project/specs-actors/actors/builtin/account"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	account2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/account"
)

type accountMigrator struct {
}

func (m *accountMigrator) MigrateState(ctx context.Context, storeIn, storeOut cbor.IpldStore, head cid.Cid) (cid.Cid, error) {
	var inState account0.State
	if err := storeIn.Get(ctx, head, &inState); err != nil {
		return cid.Undef, err
	}

	outState := account2.State(inState) // Identical
	return storeOut.Put(ctx, &outState)
}
