package miner

import (
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
)

// IP = InitialPledgeFactor * BR(precommit time)
var InitialPledgeFactor = big.NewInt(20)

// FF = (DeclaredFaultFactorNum / DeclaredFaultFactorDenom) * BR(t)
var DeclaredFaultFactorNum = big.NewInt(214)

// FF = (DeclaredFaultFactorNum / DeclaredFaultFactorDenom) * BR(t)
var DeclaredFaultFactorDenom = big.NewInt(100)

// SP = UndeclaredFaultFactor * BR(t)
var UndeclaredFaultFactor = big.NewInt(5)

// This is the BR(t) value of the given sector for the current epoch.
// It is the expected reward this sector would pay out over a one day period.
// BR(t) = CurrEpochReward(t) * SectorQualityAdjustedPower * EpochsInDay / TotalNetworkQualityAdjustedPower(t)
func ExpectedDayRewardForPower(epochTargetReward abi.TokenAmount, networkQAPower abi.StoragePower, qaSectorPower abi.StoragePower) abi.TokenAmount {
	expectedRewardForProvingPeriod := big.Mul(big.NewInt(builtin.EpochsInDay), epochTargetReward)
	return big.Div(big.Mul(qaSectorPower, expectedRewardForProvingPeriod), networkQAPower)
}

// This is the FF(t) penalty for a sector expected to be in the fault state either because the fault was declared or because
// it has been previously detected by the network.
// FF(t) = DeclaredFaultFactor * BR(t)
func PledgePenaltyForDeclaredFault(epochTargetReward abi.TokenAmount, networkQAPower abi.StoragePower, qaSectorPower abi.StoragePower) abi.TokenAmount {
	return big.Div(
		big.Mul(DeclaredFaultFactorNum, ExpectedDayRewardForPower(epochTargetReward, networkQAPower, qaSectorPower)),
		DeclaredFaultFactorDenom)
}

// This is the SP(t) penalty for a newly faulty sector that has not been declared.
// SP(t) = UndeclaredFaultFactor * BR(t)
func PledgePenaltyForUndeclaredFault(epochTargetReward abi.TokenAmount, networkQAPower abi.StoragePower, qaSectorPower abi.StoragePower) abi.TokenAmount {
	return big.Div(
		big.Mul(UndeclaredFaultFactor, ExpectedDayRewardForPower(epochTargetReward, networkQAPower, qaSectorPower)),
		big.NewInt(1))
}

// Penalty to locked pledge collateral for the termination of a sector before scheduled expiry.
func PledgePenaltyForTermination(s *SectorOnChainInfo, currEpoch abi.ChainEpoch, epochTargetReward abi.TokenAmount, networkQAPower abi.StoragePower, qaSectorPower abi.StoragePower) abi.TokenAmount {
	// max(SP(t), InitialPledge + BR(StartEpoch)*min(SectorAgeInDays, 180)) where BF(StartEpoch)=InitialPledge/InitialPledgeFactor
	sectorAge := big.NewInt(int64(minEpoch(currEpoch-s.Activation, 180*builtin.EpochsInDay)))
	return big.Max(
		PledgePenaltyForUndeclaredFault(epochTargetReward, networkQAPower, qaSectorPower),
		big.Add(
			InitialPledgeFactor,
			big.Div(
				big.Mul(s.InitialPledge, sectorAge),
				InitialPledgeFactor)))
}

// Computes the pledge requirement for committing new quality-adjusted power to the network, given the current
// total power, total pledge commitment, epoch block reward, and circulating token supply.
// In plain language, the pledge requirement is a multiple of the block reward expected to be earned by the
// newly-committed power, holding the per-epoch block reward constant (though in reality it will change over time).
// The network total pledge and circulating supply parameters are currently unused, but may be included in a
// future calculation.
func InitialPledgeForPower(qaPower abi.StoragePower, networkQAPower abi.StoragePower, networkTotalPledge abi.TokenAmount, epochTargetReward abi.TokenAmount, networkCirculatingSupply abi.TokenAmount) abi.TokenAmount {
	// Details here are still subject to change.
	// PARAM_FINISH
	// https://github.com/filecoin-project/specs-actors/issues/468
	_ = networkCirculatingSupply // TODO: ce use this
	_ = networkTotalPledge       // TODO: ce use this

	if networkQAPower.IsZero() {
		return epochTargetReward
	}
	return big.Mul(InitialPledgeFactor, ExpectedDayRewardForPower(epochTargetReward, networkQAPower, qaPower))
}
