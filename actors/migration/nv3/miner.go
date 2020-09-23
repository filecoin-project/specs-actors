package nv3

import (
	"context"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	miner "github.com/filecoin-project/specs-actors/actors/builtin/miner"
)

type minerMigrator struct {
}

func (m *minerMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid) (cid.Cid, error) {
	var st miner.State
	if err := store.Get(ctx, head, &st); err != nil {
		return cid.Undef, err
	}

	// TODO: https://github.com/filecoin-project/specs-actors/issues/1177
	//  - repair broken partitions, deadline info:
	//  - fix power actor claim with any power delta

	newHead, err := store.Put(ctx, &st)
	return newHead, err
}
