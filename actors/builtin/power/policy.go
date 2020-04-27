package power

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"

	. "github.com/filecoin-project/specs-actors/actors/util"
)

// Minimum number of registered miners for the minimum miner size limit to effectively limit consensus power.
const ConsensusMinerMinMiners = 3

// Minimum power of an individual miner to meet the threshold for leader election.
var ConsensusMinerMinPower = abi.NewStoragePower(2 << 30) // PARAM_FINISH

var BaseMultiplier = big.NewInt(10)                // PARAM_FINISH
var DealWeightMultiplier = big.NewInt(11)          // PARAM_FINISH
var VerifiedDealWeightMultiplier = big.NewInt(100) // PARAM_FINISH
const SectorQualityPrecision = 20

// DealWeight and VerifiedDealWeight are spacetime occupied by regular deals and verified deals in a sector.
// Sum of DealWeight and VerifiedDealWeight should be less than or equal to total SpaceTime of a sector.
// Sectors full of VerifiedDeals will have a SectorQuality of VerifiedDealWeightMultiplier/BaseMultiplier.
// Sectors full of Deals will have a SectorQuality of DealWeightMultiplier/BaseMultiplier.
// Sectors with neither will have a SectorQuality of BaseMultiplier/BaseMultiplier.
// SectorQuality of a sector is a weighted average of multipliers based on their propotions.
func SectorQualityFromWeight(weight *SectorStorageWeightDesc) abi.SectorQuality {
	sectorSpaceTime := big.Mul(big.NewInt(int64(weight.SectorSize)), big.NewInt(int64(weight.Duration)))
	totalDealSpaceTime := big.Add(weight.DealWeight, weight.VerifiedDealWeight)
	Assert(sectorSpaceTime.GreaterThanEqual(totalDealSpaceTime))

	weightedBaseSpaceTime := big.Mul(big.Sub(sectorSpaceTime, totalDealSpaceTime), BaseMultiplier)
	weightedDealSpaceTime := big.Mul(weight.DealWeight, DealWeightMultiplier)
	weightedVerifiedSpaceTime := big.Mul(weight.VerifiedDealWeight, VerifiedDealWeightMultiplier)
	weightedSumSpaceTime := big.Add(weightedBaseSpaceTime, big.Add(weightedDealSpaceTime, weightedVerifiedSpaceTime))
	scaledUpWeightedSumSpaceTime := big.Lsh(weightedSumSpaceTime, SectorQualityPrecision)

	return big.Div(big.Div(scaledUpWeightedSumSpaceTime, sectorSpaceTime), BaseMultiplier)
}

func QAPowerForWeight(weight *SectorStorageWeightDesc) abi.StoragePower {
	qual := SectorQualityFromWeight(weight)
	return big.Rsh(big.Mul(big.NewInt(int64(weight.SectorSize)), qual), SectorQualityPrecision)
}

func InitialPledgeForWeight(qapower abi.StoragePower, totqapower abi.StoragePower, circSupply abi.TokenAmount, totalPledge abi.TokenAmount, perEpochReward abi.TokenAmount) abi.TokenAmount {
	// Details here are still subject to change.
	// PARAM_FINISH
	_ = circSupply  // TODO: ce use this
	_ = totalPledge // TODO: ce use this

	return big.Div(big.Mul(qapower, perEpochReward), totqapower)
}
