package nv7

import (
	"context"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
)

type MinerMigrator struct{}

func (m MinerMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, info MigrationInfo) (*StateMigrationResult, error) {
	var state miner.State
	if err := store.Get(ctx, head, &state); err != nil {
		return nil, err
	}
	adtStore := adt.WrapStore(ctx, store)

	mInfo, err := state.GetInfo(adtStore)
	if err != nil {
		return nil, err
	}
	if mInfo.SealProofType == abi.RegisteredSealProof_StackedDrg32GiBV1 {
		mInfo.SealProofType = abi.RegisteredSealProof_StackedDrg32GiBV1_1
	} else if mInfo.SealProofType == abi.RegisteredSealProof_StackedDrg64GiBV1 {
		mInfo.SealProofType = abi.RegisteredSealProof_StackedDrg64GiBV1_1
	}
	if err := state.SaveInfo(adtStore, mInfo); err != nil {
		return nil, err
	}

	newHead, err := store.Put(ctx, &state)
	return &StateMigrationResult{
		NewHead:  newHead,
		Transfer: big.Zero(),
	}, err
}
