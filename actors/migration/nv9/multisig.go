package nv9

import (
	"context"

	multisig2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/multisig"
	cbor "github.com/ipfs/go-ipld-cbor"

	builtin3 "github.com/filecoin-project/specs-actors/v3/actors/builtin"
	multisig3 "github.com/filecoin-project/specs-actors/v3/actors/builtin/multisig"
	adt3 "github.com/filecoin-project/specs-actors/v3/actors/util/adt"
)

type multisigMigrator struct{}

func (m multisigMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, in StateMigrationInput) (*StateMigrationResult, error) {
	var inState multisig2.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}

	pendingTxnsOut, err := migrateHAMTRaw(ctx, store, inState.PendingTxns, adt3.DefaultHamtOptionsWithDefaultBitwidth)
	if err != nil {
		return nil, err
	}

	outState := multisig3.State{
		Signers:               inState.Signers,
		NumApprovalsThreshold: inState.NumApprovalsThreshold,
		NextTxnID:             inState.NextTxnID,
		InitialBalance:        inState.InitialBalance,
		StartEpoch:            inState.StartEpoch,
		UnlockDuration:        inState.UnlockDuration,
		PendingTxns:           pendingTxnsOut,
	}
	newHead, err := store.Put(ctx, &outState)
	return &StateMigrationResult{
		NewCodeCID: builtin3.MultisigActorCodeID,
		NewHead:    newHead,
	}, err
}
