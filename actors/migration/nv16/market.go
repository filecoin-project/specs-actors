package nv16

import (
	"context"
	"unicode/utf8"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	"github.com/filecoin-project/go-state-types/abi"

	market7 "github.com/filecoin-project/specs-actors/v7/actors/builtin/market"

	"github.com/filecoin-project/specs-actors/v8/actors/builtin"
	"github.com/filecoin-project/specs-actors/v8/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v8/actors/util/adt"
)

type marketMigrator struct {
	OutCodeCID cid.Cid
}

func (m marketMigrator) migratedCodeCID() cid.Cid {
	return m.OutCodeCID
}

func (m marketMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	var inState market7.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}
	wrappedStore := adt.WrapStore(ctx, store)

	proposalsCidOut, changedProposalCIDs, err := MapProposals(ctx, wrappedStore, inState.Proposals)
	if err != nil {
		return nil, err
	}

	pendingProposalsCidOut, err := CreateNewPendingProposals(ctx, wrappedStore, changedProposalCIDs, proposalsCidOut, inState.States, inState.PendingProposals)
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

type cidSwap struct {
	old cid.Cid
	new cid.Cid
}

// MapProposals converts proposals with invalid i.e. non-utf8 string label serializations into proposals with
// byte label serializations.  It returns a map from old proposal cid to new proposal cid for those proposals
// whose serialization has changed
func MapProposals(ctx context.Context, store adt.Store, proposalsRoot cid.Cid) (cid.Cid, map[int64]cidSwap, error) {
	changedProposalCIDs := make(map[int64]cidSwap)
	oldProposals, err := adt.AsArray(store, proposalsRoot, market7.ProposalsAmtBitwidth)
	if err != nil {
		return cid.Undef, nil, err
	}

	newProposals, err := adt.MakeEmptyArray(store, market.ProposalsAmtBitwidth)
	if err != nil {
		return cid.Undef, nil, err
	}

	var dealprop7 market7.DealProposal

	err = oldProposals.ForEach(&dealprop7, func(key int64) error {
		var newLabel market.DealLabel
		if utf8.ValidString(dealprop7.Label) {
			newLabel, err = market.NewLabelFromString(dealprop7.Label)
			if err != nil {
				return err
			}
		} else {
			// serialization changes
			if err != nil {
				return err
			}
			newLabel, err = market.NewLabelFromBytes([]byte(dealprop7.Label))
			if err != nil {
				return err
			}
		}

		dealprop8 := market.DealProposal{
			PieceCID:             dealprop7.PieceCID,
			PieceSize:            dealprop7.PieceSize,
			VerifiedDeal:         dealprop7.VerifiedDeal,
			Client:               dealprop7.Client,
			Provider:             dealprop7.Provider,
			Label:                newLabel,
			StartEpoch:           dealprop7.StartEpoch,
			EndEpoch:             dealprop7.EndEpoch,
			StoragePricePerEpoch: dealprop7.StoragePricePerEpoch,
			ProviderCollateral:   dealprop7.ProviderCollateral,
			ClientCollateral:     dealprop7.ClientCollateral,
		}
		if newLabel.IsBytes() {
			old, err := dealprop7.Cid()
			if err != nil {
				return err
			}
			new, err := dealprop8.Cid()
			if err != nil {
				return err
			}
			changedProposalCIDs[key] = cidSwap{old: old, new: new}
		}
		return newProposals.Set(uint64(key), &dealprop8)
	})
	if err != nil {
		return cid.Undef, nil, err
	}

	newProposalsCid, err := newProposals.Root()
	if err != nil {
		return cid.Undef, nil, err
	}

	return newProposalsCid, changedProposalCIDs, nil
}

// This rebuilds pendingproposals after all the CIDs have changed when the labels are of a different type in dealProposal.
// A proposal in Proposals is pending if its dealID is not a member of States, or if the LastUpdatedEpoch field is market.EpochUndefined.
// Precondition: proposalsRoot is new proposals as computed in MapProposals
func CreateNewPendingProposals(ctx context.Context, store adt.Store, changedProposalCIDs map[int64]cidSwap, proposalsRoot cid.Cid, statesRoot cid.Cid, pendingProposalsRoot cid.Cid) (cid.Cid, error) {
	proposals, err := adt.AsArray(store, proposalsRoot, market7.ProposalsAmtBitwidth)
	if err != nil {
		return cid.Undef, err
	}

	states, err := adt.AsArray(store, statesRoot, market7.StatesAmtBitwidth)
	if err != nil {
		return cid.Undef, err
	}

	pendingProposals, err := adt.AsSet(store, pendingProposalsRoot, builtin.DefaultHamtBitwidth)
	if err != nil {
		return cid.Undef, err
	}

	err = proposals.ForEach(nil, func(key int64) error {
		// only process deals whose serialization has changed
		swap, ok := changedProposalCIDs[key]
		if !ok {
			return nil
		}
		var dealstate market.DealState
		has, err := states.Get(uint64(key), &dealstate)
		if err != nil {
			return err
		}

		// Only process deals that have a cid in pending proposals
		if (has && dealstate.LastUpdatedEpoch == market.EpochUndefined) || !has {
			if err := pendingProposals.Delete(abi.CidKey(swap.old)); err != nil {
				return err
			}
			return pendingProposals.Put(abi.CidKey(swap.new))
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
