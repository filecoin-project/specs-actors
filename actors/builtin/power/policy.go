package power

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
)

// Minimum number of registered miners for the minimum miner size limit to effectively limit consensus power.
const ConsensusMinerMinMiners = 3

// Minimum power of an individual miner to meet the threshold for leader election.
var ConsensusMinerMinPower = abi.NewStoragePower(2 << 30) // PARAM_FINISH

const VerifiedClientQualityMultiplier = 10 // PARAM_FINISH

// Sector quality is a scalar with 6 decimal points.
// Sectors with verified deal weight of 0 have a sector quality of 1.
// Sectors with verified deal weight of 1 should have a sector quality of VerifedClientQualityMutliplier.
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
