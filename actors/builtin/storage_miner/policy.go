package storage_miner

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	storage_power "github.com/filecoin-project/specs-actors/actors/builtin/storage_power"
	autil "github.com/filecoin-project/specs-actors/actors/util"
)

func temporaryFaultFee(weights []storage_power.SectorStorageWeightDesc, duration abi.ChainEpoch) abi.TokenAmount {
	autil.PARAM_FINISH()
	panic("")
}
