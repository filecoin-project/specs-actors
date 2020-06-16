package miner

import (
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
)

const InitialPledgeFactor = int64(20)
const ExpectedFaultFactorNum = int64(214)
const ExpectedFaultFactorDenom = int64(1000)
const UnexpectedFaultFactor = int64(5)

// This is the BR(t) value of the given sector for the current epoch.
// It is the expected reward this sector would pay out over a one day period.
// BR(t) = CurrEpochReward(t) * SectorQualityAdjustedPower * EpochsInDay / TotalNetworkQualityAdjustedPower(t)
func ExpectedDayRewardForSector(epochTargetReward abi.TokenAmount, networkQAPower abi.StoragePower, qaSectorPower abi.StoragePower) abi.TokenAmount {
	expectedRewardForProvingPeriod := big.Mul(big.NewInt(builtin.EpochsInDay), epochTargetReward)
	return big.Div(big.Mul(qaSectorPower, expectedRewardForProvingPeriod), networkQAPower)
}

// This is the FF(t) penalty for a sector expected to be in the fault state either because the fault was declared or because
// it has been previously detected by the network.
// FF(t) = ExpectedFaultFactor * BR(t)
func pledgePenaltyForSectorDeclaredFault(epochTargetReward abi.TokenAmount, networkQAPower abi.StoragePower, qaSectorPower abi.StoragePower) abi.TokenAmount {
	return big.Div(
		big.Mul(big.NewInt(ExpectedFaultFactorNum), ExpectedDayRewardForSector(epochTargetReward, networkQAPower, qaSectorPower)),
		big.NewInt(ExpectedFaultFactorDenom))
}

// This is the SP(t) penalty for a newly faulty sector that has not been declared.
// SP(t) = UnexpectedFaultFactor * BR(t)
func pledgePenaltyForSectorUndeclaredFault(epochTargetReward abi.TokenAmount, networkQAPower abi.StoragePower, qaSectorPower abi.StoragePower) abi.TokenAmount {
	return big.Mul(big.NewInt(UnexpectedFaultFactor), ExpectedDayRewardForSector(epochTargetReward, networkQAPower, qaSectorPower))
}

// Penalty to locked pledge collateral for the termination of a sector before scheduled expiry.
func pledgePenaltyForSectorTermination(size abi.SectorSize, s *SectorOnChainInfo) abi.TokenAmount {
	return big.Zero() // PARAM_FINISH
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
	return big.Mul(big.NewInt(InitialPledgeFactor), ExpectedDayRewardForSector(epochTargetReward, networkQAPower, qaSectorPower))
}
