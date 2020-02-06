package builtin

import (
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// The built-in actor code IDs
var SystemActorCodeID cid.Cid // todo: drop id?
var InitActorCodeID cid.Cid
var CronActorCodeID cid.Cid
var AccountActorCodeID cid.Cid
var StoragePowerActorCodeID cid.Cid
var StorageMinerActorCodeID cid.Cid
var StorageMarketActorCodeID cid.Cid
var PaymentChannelActorCodeID cid.Cid
var MultisigActorCodeID cid.Cid
var RewardActorCodeID cid.Cid
var CallerTypesSignable []cid.Cid

func init() {
	builder := cid.V1Builder{Codec: cid.Raw, MhType: mh.IDENTITY}
	makeBuiltin := func(s string) cid.Cid {
		c, err := builder.Sum([]byte(s))
		if err != nil {
			panic(err)
		}
		return c
	}

	SystemActorCodeID = makeBuiltin("fil/1/system")
	InitActorCodeID = makeBuiltin("fil/1/init")
	CronActorCodeID = makeBuiltin("fil/1/cron")
	AccountActorCodeID = makeBuiltin("fil/1/account")
	StoragePowerActorCodeID = makeBuiltin("fil/1/storagepower")
	StorageMinerActorCodeID = makeBuiltin("fil/1/storageminer")
	StorageMarketActorCodeID = makeBuiltin("fil/1/storagemarket")
	PaymentChannelActorCodeID = makeBuiltin("fil/1/paymentchannel")
	MultisigActorCodeID = makeBuiltin("fil/1/multisig")
	RewardActorCodeID = makeBuiltin("fil/1/reward")

	// Set of actor code types that can represent external signing parties.
	CallerTypesSignable = []cid.Cid{AccountActorCodeID, MultisigActorCodeID}
}
