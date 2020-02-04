package storage_power

import (
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	indices "github.com/filecoin-project/specs-actors/actors/runtime/indices"
)

// TerminationFault
func pledgePenaltyForSectorTermination(weight SectorStorageWeightDesc, termType builtin.SectorTermination) abi.TokenAmount {
	// autil.PARAM_FINISH()
	return abi.NewTokenAmount(0)
}

// DetectedFault
func pledgePenaltyForSurprisePoStFailure(claimedPower abi.StoragePower, failures int64) abi.TokenAmount {
	// autil.PARAM_FINISH()
	return abi.NewTokenAmount(0)
}

func pledgePenaltyForConsensusFault(currPledge abi.TokenAmount, faultType ConsensusFaultType) abi.TokenAmount {
	// default is to slash all pledge collateral for all consensus fault
	// autil.PARAM_FINISH()
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
	// autil.PARAM_FINISH()
	growthRate := big.Div(indices.ConsensusFault_SlasherShareGrowthRateNum(), indices.ConsensusFault_SlasherShareGrowthRateDenom())
	multiplier := big.NewInt(1)
	for i := int64(0); i < int64(elapsedEpoch); i++ {
		multiplier = big.Mul(multiplier, growthRate)
	}
	initial_slasher_share := big.Div(indices.ConsensusFault_SlasherInitialShareNum(), indices.ConsensusFault_SlasherInitialShareDenom())
	slasher_proportion := big.Min(big.Mul(initial_slasher_share, multiplier), big.NewInt(1))
	return big.Mul(collateralToSlash, slasher_proportion)
}

func consensusPowerForWeight(weight SectorStorageWeightDesc) abi.StoragePower {
	// autil.PARAM_FINISH()
	return big.Mul(big.NewInt(int64(weight.SectorSize)), big.NewInt(int64(weight.Duration)))
}
