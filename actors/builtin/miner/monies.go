package miner

import (
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/util"
)

// This is the BR(t) value of the given sector for the current epoch.
// It is the expected reward this sector would pay out over a one day period.
// BR(t) = CurrEpochReward(t) * SectorQualityAdjustedPower * EpochsInDay / TotalNetworkQualityAdjustedPower(t)
func ExpectedDayRewardForSector(epochTargetReward abi.TokenAmount, networkQAPower abi.StoragePower, qaSectorPower abi.StoragePower) abi.TokenAmount {
	util.Assert(!networkQAPower.IsZero())
	expectedRewardForProvingPeriod := big.Mul(big.NewInt(builtin.EpochsInDay), epochTargetReward)
	return big.Div(big.Mul(qaSectorPower, expectedRewardForProvingPeriod), networkQAPower)
}

// Computes the pledge requirement for committing new quality-adjusted power to the network, given the current
// total power, total pledge commitment, epoch block reward, and circulating token supply.
// In plain language, the pledge requirement is a multiple of the block reward expected to be earned by the
// newly-committed power, holding the per-epoch block reward constant (though in reality it will change over time).
// The network total pledge and circulating supply parameters are currently unused, but may be included in a
// future calculation.
func InitialPledgeForPower(qaSectorPower abi.StoragePower, networkQAPower abi.StoragePower, networkTotalPledge abi.TokenAmount, epochTargetReward abi.TokenAmount, networkCirculatingSupply abi.TokenAmount) abi.TokenAmount {
	// Details here are still subject to change.
	// PARAM_FINISH
	// https://github.com/filecoin-project/specs-actors/issues/468
	_ = networkCirculatingSupply // TODO: ce use this
	_ = networkTotalPledge       // TODO: ce use this

	if networkQAPower.IsZero() {
		return epochTargetReward
	}
	return big.Mul(big.NewInt(20), ExpectedDayRewardForSector(epochTargetReward, networkQAPower, qaSectorPower))
}
