package nv3

import (
	"context"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	"github.com/filecoin-project/specs-actors/actors/builtin"
)

type StateMigration interface {
	// Loads an actor's state from an input store and writes new state to an output store.
	// Returns the new state head CID.
	MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid) (newHead cid.Cid, err error)
}

var migrators = map[cid.Cid]StateMigration{ // nolint:varcheck,deadcode,unused
	builtin.RewardActorCodeID: &rewardMigrator{},
	builtin.StorageMinerActorCodeID: &minerMigrator{},
	//builtin.AccountActorCodeID: nil,
	//builtin.CronActorCodeID: nil,
	//builtin.InitActorCodeID: nil,
	//builtin.MultisigActorCodeID:nil,
	//builtin.PaymentChannelActorCodeID: nil,
	//builtin.StorageMarketActorCodeID: nil,
	//builtin.StoragePowerActorCodeID: nil,
	//builtin.SystemActorCodeID: nil,
	//builtin.VerifiedRegistryActorCodeID: nil,
}

