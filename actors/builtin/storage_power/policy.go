package storage_power

import (
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	indices "github.com/filecoin-project/specs-actors/actors/runtime/indices"
)

// TerminationFault
func pledgePenaltyForSectorTermination(weight SectorStorageWeightDesc, termType builtin.SectorTermination) abi.TokenAmount {
	// PARAM_FINISH
	return abi.NewTokenAmount(0)
}

// DetectedFault
func pledgePenaltyForSurprisePoStFailure(claimedPower abi.StoragePower, failures int64) abi.TokenAmount {
	// PARAM_FINISH
	return abi.NewTokenAmount(0)
}

func pledgePenaltyForConsensusFault(currPledge abi.TokenAmount, faultType ConsensusFaultType) abi.TokenAmount {
	// default is to slash all pledge collateral for all consensus fault
	// PARAM_FINISH
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
	// PARAM_FINISH
	// var growthRate = SLASHER_SHARE_GROWTH_RATE_NUM / SLASHER_SHARE_GROWTH_RATE_DENOM
	// var multiplier = growthRate^elapsedEpoch
	// var slasherProportion = min(INITIAL_SLASHER_SHARE * multiplier, 1.0)
	// return collateralToSlash * slasherProportion

	// BigInt Operation
	// NUM = SLASHER_SHARE_GROWTH_RATE_NUM^elapsedEpoch * INITIAL_SLASHER_SHARE_NUM * collateralToSlash
	// DENOM = SLASHER_SHARE_GROWTH_RATE_DENOM^elapsedEpoch * INITIAL_SLASHER_SHARE_DENOM
	// slasher_amount = min(NUM/DENOM, collateralToSlash)
	slasherShareNumMultiplier := big.NewInt(1)
	for i := int64(0); i < int64(elapsedEpoch); i++ {
		slasherShareNumMultiplier = big.Mul(slasherShareNumMultiplier, indices.ConsensusFault_SlasherShareGrowthRateNum())
	}

	slasherShareDenomMultiplier := big.NewInt(1)
	for i := int64(0); i < int64(elapsedEpoch); i++ {
		slasherShareDenomMultiplier = big.Mul(slasherShareDenomMultiplier, indices.ConsensusFault_SlasherShareGrowthRateDenom())
	}

	num := big.Mul(big.Mul(slasherShareNumMultiplier, indices.ConsensusFault_SlasherInitialShareNum()), collateralToSlash)
	denom := big.Mul(slasherShareDenomMultiplier, indices.ConsensusFault_SlasherInitialShareDenom())
	return big.Min(big.Div(num, denom), collateralToSlash)
}

func consensusPowerForWeight(weight SectorStorageWeightDesc) abi.StoragePower {
	// PARAM_FINISH
	return abi.StoragePower(big.NewInt(int64(weight.SectorSize)))
}
