package main

import (
	multisig "github.com/filecoin-project/specs-actors/actors/builtin/multisig"
	reward "github.com/filecoin-project/specs-actors/actors/builtin/reward"
	gen "github.com/whyrusleeping/cbor-gen"
)

func main() {

	// General Types
	if err := gen.WriteTupleEncodersToFile("./actors/builtin/multisig/cbor_gen.go", "multisig",
		// actor state
		multisig.MultiSigActorState{},
		multisig.MultiSigTransaction{},
		// method params
		multisig.ConstructorParams{},
		multisig.ProposeParams{},
		multisig.TxnIDParams{},
		multisig.ChangeNumApprovalsThresholdParams{},
		multisig.SwapAuthorizedPartyParams{},
		// method returns
		multisig.ProposeReturn{},
	); err != nil {
		panic(err)
	}

	if err := gen.WriteTupleEncodersToFile("./actors/builtin/reward/cbor_gen.go", "reward",
		// actor state
		reward.RewardActorState{},
		// method params
		reward.AwardBlockRewardParams{},
	); err != nil {
		panic(err)
	}
}
