package actors

import (
	cid "github.com/ipfs/go-cid"

	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/account"
	"github.com/filecoin-project/specs-actors/actors/builtin/cron"
	init_ "github.com/filecoin-project/specs-actors/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/builtin/multisig"
	"github.com/filecoin-project/specs-actors/actors/builtin/paych"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/actors/builtin/system"
	"github.com/filecoin-project/specs-actors/actors/builtin/verifreg"
)

type BuiltinActor struct {
	exports []interface{}
	code    cid.Cid
}

// Exports has a list of method available on the actor.
func (b BuiltinActor) Exports() []interface{} {
	return b.exports
}

// Code is the CodeID (cid) of the actor.
func (b BuiltinActor) Code() cid.Cid {
	return b.code
}

func BuiltinActors() []BuiltinActor {
	return []BuiltinActor{
		{
			exports: account.Actor{}.Exports(),
			code:    builtin.AccountActorCodeID,
		},
		{
			exports: cron.Actor{}.Exports(),
			code:    builtin.CronActorCodeID,
		},
		{
			exports: init_.Actor{}.Exports(),
			code:    builtin.InitActorCodeID,
		},
		{
			exports: market.Actor{}.Exports(),
			code:    builtin.StorageMarketActorCodeID,
		},
		{
			exports: miner.Actor{}.Exports(),
			code:    builtin.StorageMinerActorCodeID,
		},
		{
			exports: multisig.Actor{}.Exports(),
			code:    builtin.MultisigActorCodeID,
		},
		{
			exports: paych.Actor{}.Exports(),
			code:    builtin.PaymentChannelActorCodeID,
		},
		{
			exports: power.Actor{}.Exports(),
			code:    builtin.StoragePowerActorCodeID,
		},
		{
			exports: reward.Actor{}.Exports(),
			code:    builtin.RewardActorCodeID,
		},
		{
			exports: system.Actor{}.Exports(),
			code:    builtin.SystemActorCodeID,
		},
		{
			exports: verifreg.Actor{}.Exports(),
			code:    builtin.VerifiedRegistryActorCodeID,
		},
	}
}
