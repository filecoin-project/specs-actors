package nv9

import (
	"context"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	adt2 "github.com/filecoin-project/specs-actors/v2/actors/util/adt"

	market3 "github.com/filecoin-project/specs-actors/v3/actors/builtin/market"
	adt3 "github.com/filecoin-project/specs-actors/v3/actors/util/adt"
)

type MarketMigrator struct{}

func (m MarketMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, info MigrationInfo) (*StateMigrationResult, error) {
	var inState market2.State
	if err := store.Get(ctx, head, &inState); err != nil {
		return nil, err
	}

	pendingProposalsCid, err := m.MapPendingProposals(ctx, store, inState.PendingProposals)
	if err != nil {
		return nil, err
	}

	outState := market3.State{
		Proposals:                     inState.Proposals,
		States:                        inState.States,
		PendingProposals:              pendingProposalsCid,
		EscrowTable:                   inState.EscrowTable,
		LockedTable:                   inState.LockedTable,
		NextID:                        inState.NextID,
		DealOpsByEpoch:                inState.DealOpsByEpoch,
		LastCron:                      inState.LastCron,
		TotalClientLockedCollateral:   inState.TotalClientLockedCollateral,
		TotalProviderLockedCollateral: inState.TotalProviderLockedCollateral,
		TotalClientStorageFee:         inState.TotalClientStorageFee,
	}

	newHead, err := store.Put(ctx, &outState)
	return &StateMigrationResult{
		NewHead: newHead,
	}, err
}

func (a MarketMigrator) MapPendingProposals(ctx context.Context, store cbor.IpldStore, pendingProposalsRoot cid.Cid) (cid.Cid, error) {
	oldPendingProposals, err := adt2.AsMap(adt2.WrapStore(ctx, store), pendingProposalsRoot)
	if err != nil {
		return cid.Undef, err
	}

	newPendingProposals := adt3.MakeEmptySet(adt3.WrapStore(ctx, store))

	err = oldPendingProposals.ForEach(nil, func(key string) error {
		return newPendingProposals.Put(StringKey(key))
	})
	if err != nil {
		return cid.Undef, err
	}

	newPendingProposalsCid, err := newPendingProposals.Root()
	if err != nil {
		return cid.Undef, err
	}

	return newPendingProposalsCid, nil
}

// An adt.Map key that just preserves the underlying string.
type StringKey string

func (k StringKey) Key() string {
	return string(k)
}
