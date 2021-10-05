package nv14

import (
	"context"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	"github.com/filecoin-project/go-state-types/abi"
	market5 "github.com/filecoin-project/specs-actors/v5/actors/builtin/market"

	"github.com/filecoin-project/specs-actors/v6/actors/builtin"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v6/actors/util/adt"
)

type marketMigrator struct{}

func (m marketMigrator) migratedCodeCID() cid.Cid {
	return builtin.StorageMarketActorCodeID
}

func (m marketMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	var inState market5.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}

	proposalsCidOut, err := MapProposals(ctx, store, inState.Proposals)
	if err != nil {
		return nil, err
	}

	pendingProposalsCidOut, err := CreateNewPendingProposals(ctx, store, inState.Proposals, inState.States)
	if err != nil {
		return nil, err
	}

	outState := market.State{
		Proposals:                     proposalsCidOut,
		States:                        inState.States,
		PendingProposals:              pendingProposalsCidOut,
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
	return &actorMigrationResult{
		newCodeCID: m.migratedCodeCID(),
		newHead:    newHead,
	}, err
}

func MapProposals(ctx context.Context, store cbor.IpldStore, proposalsRoot cid.Cid) (cid.Cid, error) {
	oldProposals, err := adt.AsMap(adt.WrapStore(ctx, store), proposalsRoot, builtin.DefaultHamtBitwidth)
	if err != nil {
		return cid.Undef, err
	}

	newProposals, err := adt.MakeEmptyMap(adt.WrapStore(ctx, store), builtin.DefaultHamtBitwidth)
	if err != nil {
		return cid.Undef, err
	}

	err = oldProposals.ForEach(nil, func(key string) error {
		var dealprop5 market5.DealProposal
		oldProposals.Get(StringKey(key), &dealprop5)
		dealprop6 := market.DealProposal{
			PieceCID:             dealprop5.PieceCID,
			PieceSize:            dealprop5.PieceSize,
			VerifiedDeal:         dealprop5.VerifiedDeal,
			Client:               dealprop5.Client,
			Provider:             dealprop5.Provider,
			Label:                []byte(dealprop5.Label),
			StartEpoch:           dealprop5.StartEpoch,
			EndEpoch:             dealprop5.EndEpoch,
			StoragePricePerEpoch: dealprop5.StoragePricePerEpoch,
			ProviderCollateral:   dealprop5.ProviderCollateral,
			ClientCollateral:     dealprop5.ClientCollateral,
		}
		return newProposals.Put(StringKey(key), &dealprop6)
	})
	if err != nil {
		return cid.Undef, err
	}

	newProposalsCid, err := newProposals.Root()
	if err != nil {
		return cid.Undef, err
	}

	return newProposalsCid, nil
}

// This functions with the assumption that PendingProposals and States (aka active deals) form a disjoint union of Proposals
// Iterates over Proposals, checks for membership in States, and then if it doesn't find it, adds the hash to PendingProposals
func CreateNewPendingProposals(ctx context.Context, store cbor.IpldStore, proposalsRoot cid.Cid, statesRoot cid.Cid) (cid.Cid, error) {
	proposals, err := adt.AsMap(adt.WrapStore(ctx, store), proposalsRoot, builtin.DefaultHamtBitwidth)
	if err != nil {
		return cid.Undef, err
	}

	states, err := adt.AsMap(adt.WrapStore(ctx, store), statesRoot, builtin.DefaultHamtBitwidth)
	if err != nil {
		return cid.Undef, err
	}

	pendingProposals, err := adt.MakeEmptySet(adt.WrapStore(ctx, store), builtin.DefaultHamtBitwidth)
	if err != nil {
		return cid.Undef, err
	}

	err = proposals.ForEach(nil, func(key string) error {
		has, err := states.Has(StringKey(key))
		if err != nil {
			return err
		}
		if !has {
			var dealprop market5.DealProposal
			proposals.Get(StringKey(key), &dealprop)
			dealpropCid, err := dealprop.Cid()
			if err != nil {
				return err
			}
			return pendingProposals.Put(abi.CidKey(dealpropCid))
		}
		return nil
	})
	if err != nil {
		return cid.Undef, err
	}

	pendingProposalsCid, err := pendingProposals.Root()
	if err != nil {
		return cid.Undef, err
	}

	return pendingProposalsCid, nil
}

// An adt.Map key that just preserves the underlying string.
type StringKey string

func (k StringKey) Key() string {
	return string(k)
}
