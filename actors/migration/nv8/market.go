package nv7

import (
	"context"

	"github.com/filecoin-project/go-state-types/big"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	market "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	adt2 "github.com/filecoin-project/specs-actors/v3/actors/util/adt"

	adt3 "github.com/filecoin-project/specs-actors/v3/actors/util/adt"
)

type MarketMigrator struct{}

func (m MarketMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, info MigrationInfo) (*StateMigrationResult, error) {
	var inState market.State
	if err := store.Get(ctx, head, &inState); err != nil {
		return nil, err
	}

	pendingProposalsCid, err := m.MapPendingProposals(ctx, store, inState.PendingProposals)
	if err != nil {
		return nil, err
	}

	outState := market.State{
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
		NewHead:  newHead,
		Transfer: big.Zero(),
	}, err
}

func (a MarketMigrator) MapPendingProposals(ctx context.Context, store cbor.IpldStore, pendingProposalsRoot cid.Cid) (cid.Cid, error) {
	oldPendingProposals, err := adt2.AsMap(adt2.WrapStore(ctx, store), pendingProposalsRoot)
	if err != nil {
		return cid.Cid{}, err
	}

	newPendingProposals := adt3.MakeEmptyMap(adt3.WrapStore(ctx, store))

	err = oldPendingProposals.ForEach(nil, func(key string) error {
		return newPendingProposals.Put(StringKey(key), nil)
	})
	if err != nil {
		return cid.Cid{}, err
	}

	newPendingProposalsCid, err := newPendingProposals.Root()
	if err != nil {
		return cid.Cid{}, err
	}

	return newPendingProposalsCid, nil
}

// An adt.Map key that just preserves the underlying string.
type StringKey string

func (k StringKey) Key() string {
	return string(k)
}
