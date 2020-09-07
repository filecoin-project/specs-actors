package migration

import (
	"context"

	system0 "github.com/filecoin-project/specs-actors/actors/builtin/system"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	system2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/system"
)

type systemMigrator struct {
}

func (m *systemMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid) (cid.Cid, error) {
	// No change
	var _ = system2.State(system0.State{})
	return head, nil
}
