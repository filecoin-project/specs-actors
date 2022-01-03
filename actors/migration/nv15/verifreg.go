package nv15

import (
	"context"
	miner7 "github.com/filecoin-project/specs-actors/v7/actors/builtin/miner"

	builtin7 "github.com/filecoin-project/specs-actors/v7/actors/builtin"
	"github.com/filecoin-project/specs-actors/v7/actors/util/adt"
	"golang.org/x/xerrors"

	verifreg6 "github.com/filecoin-project/specs-actors/v6/actors/builtin/verifreg"
	verifreg7 "github.com/filecoin-project/specs-actors/v7/actors/builtin/verifreg"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
)

type verifregMigrator struct{}

func (m verifregMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	var inState verifreg6.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}

	proposalId, err := migrateProposalIds(ctx, store)
	if err != nil {
		return nil, err
	}


	outState := verifreg7.State{
		RootKey: inState.RootKey,
		Verifiers: inState.Verifiers,
		VerifiedClients: inState.VerifiedClients,
		RemoveDataCapProposalIDs: proposalId,
	}

	newHead, err := store.Put(ctx, &outState)
	return &actorMigrationResult{
		newCodeCID: m.migratedCodeCID(),
		newHead:    newHead,
	}, err
}

func (m verifregMigrator) migratedCodeCID() cid.Cid {
	return builtin7.VerifiedRegistryActorCodeID
}

func migrateProposalIds(ctx context.Context, store cbor.IpldStore) (cid.Cid, error) {
	ctxStore := adt.WrapStore(ctx, store)

	outArray, err := adt.MakeEmptyMap(ctxStore, miner7.SectorsAmtBitwidth)
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to construct new remove datacap proposal id map %w", err)
	}

	return outArray.Root()
}