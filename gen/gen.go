package main

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	account "github.com/filecoin-project/specs-actors/actors/builtin/account"
	cron "github.com/filecoin-project/specs-actors/actors/builtin/cron"
	init_ "github.com/filecoin-project/specs-actors/actors/builtin/init"
	multisig "github.com/filecoin-project/specs-actors/actors/builtin/multisig"
	payment_channel "github.com/filecoin-project/specs-actors/actors/builtin/paych"
	reward "github.com/filecoin-project/specs-actors/actors/builtin/reward"
	storage_market "github.com/filecoin-project/specs-actors/actors/builtin/market"
	storage_miner "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	storage_power "github.com/filecoin-project/specs-actors/actors/builtin/power"
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
		account.State{},
	); err != nil {
		panic(err)
	}

	if err := gen.WriteTupleEncodersToFile("./actors/builtin/init/cbor_gen.go", "init",
		// actor state
		init_.State{},
		// method params
		init_.ExecParams{},
		init_.ExecReturn{},
	); err != nil {
		panic(err)
	}

	if err := gen.WriteTupleEncodersToFile("./actors/builtin/cron/cbor_gen.go", "cron",
		// actor state
		cron.State{},
		cron.Entry{},
		// method params
		cron.ConstructorParams{},
	); err != nil {
		panic(err)
	}

	if err := gen.WriteTupleEncodersToFile("./actors/builtin/reward/cbor_gen.go", "reward",
		// actor state
		reward.State{},
		// method params
		reward.AwardBlockRewardParams{},
		// other types
		reward.Reward{},
	); err != nil {
		panic(err)
	}

	if err := gen.WriteTupleEncodersToFile("./actors/builtin/multisig/cbor_gen.go", "multisig",
		// actor state
		multisig.State{},
		multisig.Transaction{},
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

	if err := gen.WriteTupleEncodersToFile("./actors/builtin/payment_channel/cbor_gen.go", "payment_channel",
		// actor state
		payment_channel.State{},
		payment_channel.LaneState{},
		payment_channel.Merge{},
		// method params
		payment_channel.ConstructorParams{},
		payment_channel.UpdateChannelStateParams{},
		payment_channel.SignedVoucher{},
		payment_channel.ModVerifyParams{},
		payment_channel.PaymentVerifyParams{},
	); err != nil {
		panic(err)
	}

	if err := gen.WriteTupleEncodersToFile("./actors/builtin/storage_power/cbor_gen.go", "storage_power",
		// actors state
		storage_power.State{},
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
		storage_market.State{},

		// method params
		storage_market.WithdrawBalanceParams{},
		storage_market.PublishStorageDealsParams{},
		storage_market.VerifyDealsOnSectorProveCommitParams{},
		storage_market.ComputeDataCommitmentParams{},
		storage_market.OnMinerSectorsTerminateParams{},
		// method returns
		storage_market.PublishStorageDealsReturn{},
		// other types
		storage_market.DealProposal{},
		storage_market.DealState{},
	); err != nil {
		panic(err)
	}

	if err := gen.WriteTupleEncodersToFile("./actors/builtin/storage_miner/cbor_gen.go", "storage_miner",
		// actor state
		storage_miner.State{},
		storage_miner.MinerInfo{},
		storage_miner.PoStState{},
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
