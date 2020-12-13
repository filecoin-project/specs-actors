package nv9

import (
	"context"

	init2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/init"
	cbor "github.com/ipfs/go-ipld-cbor"

	builtin3 "github.com/filecoin-project/specs-actors/v3/actors/builtin"
	init3 "github.com/filecoin-project/specs-actors/v3/actors/builtin/init"
	adt3 "github.com/filecoin-project/specs-actors/v3/actors/util/adt"
)

type initMigrator struct{}

func (m initMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, in StateMigrationInput) (*StateMigrationResult, error) {
	var inState init2.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}

	addressMapOut, err := migrateHAMTRaw(ctx, store, inState.AddressMap, adt3.DefaultHamtOptionsWithDefaultBitwidth)
	if err != nil {
		return nil, err
	}

	outState := init3.State{
		AddressMap:  addressMapOut,
		NextID:      inState.NextID,
		NetworkName: inState.NetworkName,
	}
	newHead, err := store.Put(ctx, &outState)
	return &StateMigrationResult{
		NewCodeCID: builtin3.InitActorCodeID,
		NewHead:    newHead,
	}, err
}
