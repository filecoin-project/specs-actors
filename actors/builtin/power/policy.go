package power

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
)

// Minimum number of registered miners for the minimum miner size limit to effectively limit consensus power.
const ConsensusMinerMinMiners = 3

// Minimum power of an individual miner to meet the threshold for leader election.
var ConsensusMinerMinPower = abi.NewStoragePower(1 << 40) // PARAM_FINISH

func InitialPledgeForWeight(qapower abi.StoragePower, totqapower abi.StoragePower, circSupply abi.TokenAmount, totalPledge abi.TokenAmount, perEpochReward abi.TokenAmount) abi.TokenAmount {
	// Details here are still subject to change.
	// PARAM_FINISH
	// https://github.com/filecoin-project/specs-actors/issues/468
	_ = circSupply  // TODO: ce use this
	_ = totalPledge // TODO: ce use this

	return big.Div(big.Mul(qapower, perEpochReward), totqapower)
}
