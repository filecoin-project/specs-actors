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

	pendingProposalsCidOut, err := CreateNewPendingProposals(ctx, store, proposalsCidOut, inState.States)
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
	oldProposals, err := adt.AsArray(adt.WrapStore(ctx, store), proposalsRoot, market5.ProposalsAmtBitwidth)
	if err != nil {
		return cid.Undef, err
	}

	newProposals, err := adt.MakeEmptyArray(adt.WrapStore(ctx, store), market.ProposalsAmtBitwidth)
	if err != nil {
		return cid.Undef, err
	}

	var dealprop5 market5.DealProposal

	err = oldProposals.ForEach(&dealprop5, func(key int64) error {
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
		return newProposals.Set(uint64(key), &dealprop6)
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

// This rebuilds pendingproposals after all the CIDs have changed because the labels are of a different type in dealProposal
// a proposal in Proposals is pending if its dealID is not a member of States, or if the LastUpdatedEpoch field is market.EpochUndefined.
// Precondition proposalsRoot is new proposals as computed in MapProposals
func CreateNewPendingProposals(ctx context.Context, store cbor.IpldStore, proposalsRoot cid.Cid, statesRoot cid.Cid) (cid.Cid, error) {
	proposals, err := adt.AsArray(adt.WrapStore(ctx, store), proposalsRoot, market5.ProposalsAmtBitwidth)
	if err != nil {
		return cid.Undef, err
	}

	states, err := adt.AsArray(adt.WrapStore(ctx, store), statesRoot, market5.StatesAmtBitwidth)
	if err != nil {
		return cid.Undef, err
	}

	pendingProposals, err := adt.MakeEmptySet(adt.WrapStore(ctx, store), builtin.DefaultHamtBitwidth)
	if err != nil {
		return cid.Undef, err
	}

	var dealprop market.DealProposal
	err = proposals.ForEach(&dealprop, func(key int64) error {
		var dealstate market.DealState
		has, err := states.Get(uint64(key), &dealstate)
		if err != nil {
			return err
		}
		if (has && dealstate.LastUpdatedEpoch == market.EpochUndefined) || !has {
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
