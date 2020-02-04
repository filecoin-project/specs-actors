package storage_miner

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	storage_power "github.com/filecoin-project/specs-actors/actors/builtin/storage_power"
)

func temporaryFaultFee(weights []storage_power.SectorStorageWeightDesc, duration abi.ChainEpoch) abi.TokenAmount {
	// PARAM_FINISH
	return abi.NewTokenAmount(0)
}
