package migration

import (
	"context"

	addr "github.com/filecoin-project/go-address"
	paych0 "github.com/filecoin-project/specs-actors/actors/builtin/paych"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	paych2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/paych"
)

type paychMigrator struct {
}

func (m *paychMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid) (cid.Cid, error) {
	var inState paych0.State
	if err := store.Get(ctx, head, &inState); err != nil {
		return cid.Undef, err
	}

	// Migrate lane states map
	laneStatesRoot, err := m.migrateLaneStates(ctx, store, inState.LaneStates)
	if err != nil {
		return cid.Undef, xerrors.Errorf("lane state: %w", err)
	}

	// Verify parties are all ID addrs
	if inState.From.Protocol() != addr.ID {
		return cid.Undef, xerrors.Errorf("unexpected non-ID from address %s", inState.From)
	}
	if inState.To.Protocol() != addr.ID {
		return cid.Undef, xerrors.Errorf("unexpected non-ID to address %s", inState.To)
	}

	outState := paych2.State{
		From:            inState.From,
		To:              inState.To,
		ToSend:          inState.ToSend,
		SettlingAt:      inState.SettlingAt,
		MinSettleHeight: inState.MinSettleHeight,
		LaneStates:      laneStatesRoot,
	}
	return store.Put(ctx, &outState)
}

func (m *paychMigrator) migrateLaneStates(_ context.Context, _ cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// AMT and both the key and value type unchanged between v0 and v2.
	// Verify that the value type is identical.
	var _ = paych2.LaneState(paych0.LaneState{})

	return root, nil
}
