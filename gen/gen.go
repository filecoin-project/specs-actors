package main

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	init_ "github.com/filecoin-project/specs-actors/actors/builtin/init"
	multisig "github.com/filecoin-project/specs-actors/actors/builtin/multisig"
	storage_market "github.com/filecoin-project/specs-actors/actors/builtin/storage_market"
	storage_miner "github.com/filecoin-project/specs-actors/actors/builtin/storage_miner"
	storage_power "github.com/filecoin-project/specs-actors/actors/builtin/storage_power"
	crypto "github.com/filecoin-project/specs-actors/actors/crypto"

	gen "github.com/whyrusleeping/cbor-gen"
)

func main() {
	// Common types
	if err := gen.WriteTupleEncodersToFile("./actors/abi/cbor_gen.go", "abi",
		abi.PieceSize{},
		abi.PieceInfo{},
	); err != nil {
		panic(err)
	}

	if err := gen.WriteTupleEncodersToFile("./actors/crypto/cbor_gen.go", "crypto",
		crypto.Signature{},
	); err != nil {
		panic(err)
	}

	// Actors
	if err := gen.WriteTupleEncodersToFile("./actors/builtin/init/cbor_gen.go", "init",
		// actor state
		init_.InitActorState{},
		// method params
		init_.ExecReturn{},
	); err != nil {
		panic(err)
	}

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
		storage_power.EnrollCronEventParams{},
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

	if err := gen.WriteTupleEncodersToFile("./actors/builtin/storage_market/cbor_gen.go", "storage_market",
		// method params
		storage_market.WithdrawBalanceParams{},
		storage_market.PublishStorageDealsParams{},
		storage_market.VerifyDealsOnSectorProveCommitParams{},
		storage_market.GetPieceInfosForDealIDsParams{},
		storage_market.OnMinerSectorsTerminateParams{},
		// method returns
		storage_market.GetPieceInfosForDealIDsReturn{},
		// other types
		storage_market.StorageDealProposal{},
		storage_market.StorageDeal{},
		storage_market.OnChainDeal{},
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
		storage_miner.CronEventPayload{},
	); err != nil {
		panic(err)
	}

}
