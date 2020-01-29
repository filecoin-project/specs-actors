package storage_power

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	autil "github.com/filecoin-project/specs-actors/actors/util"
)

// TerminationFault
func pledgePenaltyForSectorTermination(weight autil.SectorStorageWeightDesc, termType builtin.SectorTermination) abi.TokenAmount {
	autil.PARAM_FINISH()
	return abi.NewTokenAmount(0)
}

// DetectedFault
func pledgePenaltyForSurprisePoStFailure(claimedPower abi.StoragePower, failures int64) abi.TokenAmount {
	autil.PARAM_FINISH()
	return abi.NewTokenAmount(0)
}

func pledgePenaltyForConsensusFault(currPledge abi.TokenAmount, faultType ConsensusFaultType) abi.TokenAmount {
	// default is to slash all pledge collateral for all consensus fault
	autil.TODO()
	switch faultType {
	case DoubleForkMiningFault:
		return currPledge
	case ParentGrindingFault:
		return currPledge
	case TimeOffsetMiningFault:
		return currPledge
	default:
		panic("Unsupported case for pledge collateral consensus fault slashing")
	}
}

func rewardForConsensusSlashReport(elapsedEpoch abi.ChainEpoch, collateralToSlash abi.TokenAmount) abi.TokenAmount {
	autil.TODO()
	// BigInt Operation
	// var growthRate = builtin.SLASHER_SHARE_GROWTH_RATE_NUM / builtin.SLASHER_SHARE_GROWTH_RATE_DENOM
	// var multiplier = growthRate^elapsedEpoch
	// var slasherProportion = min(INITIAL_SLASHER_SHARE * multiplier, 1.0)
	// return collateralToSlash * slasherProportion
	return abi.NewTokenAmount(0)
}
