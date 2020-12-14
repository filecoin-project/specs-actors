package nv9

import (
	"context"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	amt3 "github.com/filecoin-project/go-amt-ipld/v3"
	hamt3 "github.com/filecoin-project/go-hamt-ipld/v3"
	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	adt2 "github.com/filecoin-project/specs-actors/v2/actors/util/adt"

	builtin3 "github.com/filecoin-project/specs-actors/v3/actors/builtin"
	market3 "github.com/filecoin-project/specs-actors/v3/actors/builtin/market"
	adt3 "github.com/filecoin-project/specs-actors/v3/actors/util/adt"
)

type marketMigrator struct{}

func (m marketMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, in StateMigrationInput) (*StateMigrationResult, error) {
	var inState market2.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}

	pendingProposalsCidOut, err := m.MapPendingProposals(ctx, store, inState.PendingProposals)
	if err != nil {
		return nil, err
	}
	proposalsCidOut, err := migrateAMTRaw(ctx, store, inState.Proposals, []amt3.Option{amt3.UseTreeBitWidth(market3.ProposalsAmtBitwidth)})
	if err != nil {
		return nil, err
	}
	statesCidOut, err := migrateAMTRaw(ctx, store, inState.States, []amt3.Option{amt3.UseTreeBitWidth(market3.StatesAmtBitwidth)})
	if err != nil {
		return nil, err
	}
	escrowTableCidOut, err := migrateHAMTRaw(ctx, store, inState.EscrowTable, append(adt3.DefaultHamtOptions, hamt3.UseTreeBitWidth(adt3.BalanceTableBitwidth)))
	if err != nil {
		return nil, err
	}
	lockedTableCidOut, err := migrateHAMTRaw(ctx, store, inState.LockedTable, append(adt3.DefaultHamtOptions, hamt3.UseTreeBitWidth(adt3.BalanceTableBitwidth)))
	if err != nil {
		return nil, err
	}
	dobeCidOut, err := migrateHAMTHAMTRaw(ctx, store, inState.DealOpsByEpoch, adt3.DefaultHamtOptionsWithDefaultBitwidth, adt3.DefaultHamtOptionsWithDefaultBitwidth)
	if err != nil {
		return nil, err
	}

	outState := market3.State{
		Proposals:                     proposalsCidOut,
		States:                        statesCidOut,
		PendingProposals:              pendingProposalsCidOut,
		EscrowTable:                   escrowTableCidOut,
		LockedTable:                   lockedTableCidOut,
		NextID:                        inState.NextID,
		DealOpsByEpoch:                dobeCidOut,
		LastCron:                      inState.LastCron,
		TotalClientLockedCollateral:   inState.TotalClientLockedCollateral,
		TotalProviderLockedCollateral: inState.TotalProviderLockedCollateral,
		TotalClientStorageFee:         inState.TotalClientStorageFee,
	}

	newHead, err := store.Put(ctx, &outState)
	return &StateMigrationResult{
		NewCodeCID: builtin3.StorageMarketActorCodeID,
		NewHead:    newHead,
	}, err
}

func (a marketMigrator) MapPendingProposals(ctx context.Context, store cbor.IpldStore, pendingProposalsRoot cid.Cid) (cid.Cid, error) {
	oldPendingProposals, err := adt2.AsMap(adt2.WrapStore(ctx, store), pendingProposalsRoot)
	if err != nil {
		return cid.Undef, err
	}

	newPendingProposals := adt3.MakeEmptySet(adt3.WrapStore(ctx, store), builtin3.DefaultHamtBitwidth)

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
