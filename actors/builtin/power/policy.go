package power

import (
	"fmt"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	reward "github.com/filecoin-project/specs-actors/actors/builtin/reward"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
)

// The time a miner has to respond to a surprise PoSt challenge.
const WindowedPostChallengeDuration = abi.ChainEpoch(240) // ~2 hours @ 30 second epochs. PARAM_FINISH

// The number of consecutive failures to meet a surprise PoSt challenge before a miner is terminated.
const WindowedPostFailureLimit = int64(3) // PARAM_FINISH

// Minimum number of registered miners for the minimum miner size limit to effectively limit consensus power.
const ConsensusMinerMinMiners = 3

// Multiplier on sector pledge requirement.
var PledgeFactor = big.NewInt(3) // PARAM_FINISH

// Total expected block reward per epoch (per-winner reward * expected winners), as input to pledge requirement.
var EpochTotalExpectedReward = big.Mul(reward.BlockRewardTarget, big.NewInt(5)) // PARAM_FINISH

// Minimum power of an individual miner to meet the threshold for leader election.
var ConsensusMinerMinPower = abi.NewStoragePower(2 << 30) // placeholder

// Maximum age of a block header used as proof of a consensus fault to appear in the chain.
var ConsensusFaultReportingWindow = abi.ChainEpoch(2880) // 1 day @ 30 second epochs.

type BigFrac struct {
	numerator   big.Int
	denominator big.Int
}

// Penalty to pledge collateral for the termination of an individual sector.
func pledgePenaltyForSectorTermination(pledge abi.TokenAmount, termType SectorTermination) abi.TokenAmount {
	return big.Zero() // PARAM_FINISH
}

// Penalty to pledge collateral for repeated failure to prove storage.
func pledgePenaltyForWindowedPoStFailure(pledge abi.TokenAmount, failures int64) abi.TokenAmount {
	return big.Zero() // PARAM_FINISH
}

// Penalty to pledge collateral for a consensus fault.
func pledgePenaltyForConsensusFault(pledge abi.TokenAmount, faultType vmr.ConsensusFaultType) abi.TokenAmount {
	// PARAM_FINISH: always penalise the entire pledge.
	switch faultType {
	case vmr.ConsensusFaultDoubleForkMining:
		return pledge
	case vmr.ConsensusFaultParentGrinding:
		return pledge
	case vmr.ConsensusFaultTimeOffsetMining:
		return pledge
	default:
		panic(fmt.Sprintf("unknown fault type %d", faultType))
	}
}

var consensusFaultReporterInitialShare = BigFrac{
	// PARAM_FINISH
	numerator:   big.NewInt(1),
	denominator: big.NewInt(1000),
}
var consensusFaultReporterShareGrowthRate = BigFrac{
	// PARAM_FINISH
	numerator:   big.NewInt(102813),
	denominator: big.NewInt(100000),
}

func rewardForConsensusSlashReport(elapsedEpoch abi.ChainEpoch, collateral abi.TokenAmount) abi.TokenAmount {
	// PARAM_FINISH
	// var growthRate = SLASHER_SHARE_GROWTH_RATE_NUM / SLASHER_SHARE_GROWTH_RATE_DENOM
	// var multiplier = growthRate^elapsedEpoch
	// var slasherProportion = min(INITIAL_SLASHER_SHARE * multiplier, 1.0)
	// return collateral * slasherProportion

	// BigInt Operation
	// NUM = SLASHER_SHARE_GROWTH_RATE_NUM^elapsedEpoch * INITIAL_SLASHER_SHARE_NUM * collateral
	// DENOM = SLASHER_SHARE_GROWTH_RATE_DENOM^elapsedEpoch * INITIAL_SLASHER_SHARE_DENOM
	// slasher_amount = min(NUM/DENOM, collateral)
	elapsed := big.NewInt(int64(elapsedEpoch))
	slasherShareNumerator := big.Exp(consensusFaultReporterShareGrowthRate.numerator, elapsed)
	slasherShareDenominator := big.Exp(consensusFaultReporterShareGrowthRate.denominator, elapsed)

	num := big.Mul(big.Mul(slasherShareNumerator, consensusFaultReporterInitialShare.numerator), collateral)
	denom := big.Mul(slasherShareDenominator, consensusFaultReporterInitialShare.denominator)
	return big.Min(big.Div(num, denom), collateral)
}

func SectorQualityFromWeight(weight *SectorStorageWeightDesc) abi.SectorQuality {
	return 1_000_000
}

func QAPowerForWeight(weight *SectorStorageWeightDesc) abi.StoragePower {
	qual := SectorQualityFromWeight(weight)
	qap := (uint64(qual) * uint64(weight.SectorSize)) / 1_000_000
	return big.NewIntUnsigned(qap) // PARAM_FINISH
}

func InitialPledgeForWeight(qapower abi.StoragePower, totqapower abi.StoragePower, circSupply abi.TokenAmount, totalPledge abi.TokenAmount, perEpochReward abi.TokenAmount) abi.TokenAmount {
	// Details here are still subject to change.
	// PARAM_FINISH
	_ = circSupply  // TODO: ce use this
	_ = totalPledge // TODO: ce use this

	return big.Div(big.Mul(qapower, perEpochReward), totqapower)
}
