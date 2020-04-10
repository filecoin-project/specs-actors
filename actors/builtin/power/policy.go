package power

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	reward "github.com/filecoin-project/specs-actors/actors/builtin/reward"
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
