package main

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	account "github.com/filecoin-project/specs-actors/actors/builtin/account"
	cron "github.com/filecoin-project/specs-actors/actors/builtin/cron"
	init_ "github.com/filecoin-project/specs-actors/actors/builtin/init"
	multisig "github.com/filecoin-project/specs-actors/actors/builtin/multisig"
	reward "github.com/filecoin-project/specs-actors/actors/builtin/reward"
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
		abi.SectorID{},
		abi.SealProof{},
		abi.SealVerifyInfo{},
		abi.OnChainSealVerifyInfo{},
	); err != nil {
		panic(err)
	}

	if err := gen.WriteTupleEncodersToFile("./actors/crypto/cbor_gen.go", "crypto",
		crypto.Signature{},
	); err != nil {
		panic(err)
	}

	// Actors
	if err := gen.WriteTupleEncodersToFile("./actors/builtin/account/cbor_gen.go", "account",
		// actor state
		account.AccountActorState{},
	); err != nil {
		panic(err)
	}

	if err := gen.WriteTupleEncodersToFile("./actors/builtin/init/cbor_gen.go", "init",
		// actor state
		init_.InitActorState{},
		// method params
		init_.ExecParams{},
		init_.ExecReturn{},
	); err != nil {
		panic(err)
	}

	if err := gen.WriteTupleEncodersToFile("./actors/builtin/cron/cbor_gen.go", "cron",
		// actor state
		cron.CronActorState{},
		cron.CronTableEntry{},
		// method params
		cron.ConstructorParams{},
	); err != nil {
		panic(err)
	}

	if err := gen.WriteTupleEncodersToFile("./actors/builtin/reward/cbor_gen.go", "reward",
		// actor state
		reward.RewardActorState{},
		// method params
		reward.AwardBlockRewardParams{},
		// other types
		reward.Reward{},
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
		// actors state
		storage_power.StoragePowerActorState{},
		storage_power.CronEvent{},
		// method params
		storage_power.AddBalanceParams{},
		storage_power.CreateMinerParams{},
		storage_power.DeleteMinerParams{},
		storage_power.WithdrawBalanceParams{},
		storage_power.EnrollCronEventParams{},
		storage_power.OnSectorTerminateParams{},
		storage_power.OnSectorModifyWeightDescParams{},
		storage_power.OnSectorProveCommitParams{},
		storage_power.ReportConsensusFaultParams{},
		storage_power.OnMinerSurprisePoStFailureParams{},
		storage_power.OnSectorTemporaryFaultEffectiveEndParams{},
		storage_power.OnSectorTemporaryFaultEffectiveBeginParams{},
		// method returns
		storage_power.CreateMinerReturn{},
		// other types
		storage_power.MinerConstructorParams{},
		storage_power.SectorStorageWeightDesc{},
	); err != nil {
		panic(err)
	}

	if err := gen.WriteTupleEncodersToFile("./actors/builtin/storage_market/cbor_gen.go", "storage_market",
		// actor state
		storage_market.StorageMarketActorState{},

		storage_market.PartyDeals{}, // Temp hacks until we cleanup the state
		storage_market.DealSet{},
		// method params
		storage_market.WithdrawBalanceParams{},
		storage_market.PublishStorageDealsParams{},
		storage_market.VerifyDealsOnSectorProveCommitParams{},
		storage_market.GetPieceInfosForDealIDsParams{},
		storage_market.OnMinerSectorsTerminateParams{},
		// method returns
		storage_market.GetPieceInfosForDealIDsReturn{},
		storage_market.PublishStorageDealsReturn{},
		// other types
		storage_market.StorageDealProposal{},
		storage_market.OnChainDeal{},
	); err != nil {
		panic(err)
	}

	if err := gen.WriteTupleEncodersToFile("./actors/builtin/storage_miner/cbor_gen.go", "storage_miner",
		// actor state
		storage_miner.StorageMinerActorState{},
		storage_miner.MinerInfo{},
		storage_miner.MinerPoStState{},
		storage_miner.SectorPreCommitOnChainInfo{},
		storage_miner.SectorPreCommitInfo{},
		storage_miner.SectorOnChainInfo{},
		storage_miner.WorkerKeyChange{},
		// method params
		//storage_miner.ConstructorParams{},
		storage_miner.PreCommitSectorParams{},
		storage_miner.TerminateSectorsParams{},
		storage_miner.ProveCommitSectorParams{},
		storage_miner.OnDeferredCronEventParams{},
		storage_miner.StageWorkerKeyChangeParams{},
		storage_miner.ExtendSectorExpirationParams{},
		storage_miner.SubmitSurprisePoStResponseParams{},
		storage_miner.DeclareTemporaryFaultsParams{},
		// other types
		storage_miner.CronEventPayload{},
	); err != nil {
		panic(err)
	}

}
