package main

import (
	multisig "github.com/filecoin-project/specs-actors/actors/builtin/multisig"
	storage_miner "github.com/filecoin-project/specs-actors/actors/builtin/storage_miner"
	storage_power "github.com/filecoin-project/specs-actors/actors/builtin/storage_power"

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
		multisig.AddSignerParams{},
		multisig.RemoveSignerParams{},
		multisig.TxnIDParams{},
		multisig.ChangeNumApprovalsThresholdParams{},
		multisig.SwapSignerParams{},
	); err != nil {
		panic(err)
	}

	if err := gen.WriteTupleEncodersToFile("./actors/builtin/storage_power/cbor_gen.go", "storage_power",
		// method params
		storage_power.AddBalanceParams{},
		storage_power.CreateMinerParams{},
		storage_power.DeleteMinerParams{},
		storage_power.WithdrawBalanceParams{},
		storage_power.OnMinerEnrollCronEvent{},
		storage_power.OnSectorTerminateParams{},
		storage_power.OnSectorModifyWeightDesc{},
		storage_power.OnSectorProveCommitParams{},
		storage_power.ReportConsensusFaultParams{},
		storage_power.OnMinerSurprisePoStFailure{},
		storage_power.OnSectorTemporaryFaultEffectiveEnd{},
		storage_power.OnSectorTemporaryFaultEffectiveBegin{},
		// method returns
		storage_power.CreateMinerReturn{},
	); err != nil {
		panic(err)
	}

	if err := gen.WriteTupleEncodersToFile("./actors/builtin/storage_miner/cbor_gen.go", "storage_miner",
		storage_miner.ConstructorParams{},
		storage_miner.PreCommitSectorParams{},
		storage_miner.TerminateSectorsParams{},
		storage_miner.ProveCommitSectorParams{},
		storage_miner.OnDeferredCronEventParams{},
		storage_miner.StageWorkerKeyChangeParams{},
		storage_miner.ExtendSectorExpirationParams{},
		storage_miner.SubmitSurprisePoStResponseParams{},
		storage_miner.DeclareTemporaryFaultsParams{},
	); err != nil {
		panic(err)
	}

}
