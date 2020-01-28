package main

import (
	multisig "github.com/filecoin-project/specs-actors/actors/builtin/multisig"
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
		multisig.SwapSignerParams{},
	); err != nil {
		panic(err)
	}
}
