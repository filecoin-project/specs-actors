package miner

import (
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/util/math"
	"github.com/filecoin-project/specs-actors/actors/util/smoothing"
)

// IP = IPBase(precommit time) + AdditionalIP(precommit time)
// IPBase(t) = InitialPledgeFactor * BR(t)
// AdditionalIP(t) = LockTarget(t)*PledgeShare(t)
// LockTarget = (LockTargetFactorNum / LockTargetFactorDenom) * FILCirculatingSupply(t)
// PledgeShare(t) = sectorQAPower / max(BaselinePower(t), NetworkQAPower(t))
// PARAM_FINISH
var PreCommitDepositFactor = big.NewInt(20)
var InitialPledgeFactor = big.NewInt(20)
var LockTargetFactorNum = big.NewInt(3)
var LockTargetFactorDenom = big.NewInt(10)

// FF = (DeclaredFaultFactorNum / DeclaredFaultFactorDenom) * BR(t)
var DeclaredFaultFactorNum = big.NewInt(214)
var DeclaredFaultFactorDenom = big.NewInt(100)

// SP = (UndeclaredFaultFactor / DeclaredFaultFactorDenom) * BR(t)
var UndeclaredFaultFactorNum = big.NewInt(5)
var UndeclaredFaultFactorDenom = big.NewInt(1)

// This is the BR(t) value of the given sector for the current epoch.
// It is the expected reward this sector would pay out over a one day period.
// BR(t) = CurrEpochReward(t) * SectorQualityAdjustedPower * EpochsInDay / TotalNetworkQualityAdjustedPower(t)
func ExpectedDayRewardForPower(rewardEstimate, networkQAPowerEstimate *smoothing.FilterEstimate, qaSectorPower abi.StoragePower) abi.TokenAmount {
	networkQAPowerSmoothed := networkQAPowerEstimate.Estimate()
	if networkQAPowerSmoothed.IsZero() {
		return rewardEstimate.Estimate()
	}
	expectedRewardForProvingPeriod := smoothing.ExtrapolatedCumSumOfRatio(abi.ChainEpoch(builtin.EpochsInDay), 0, rewardEstimate, networkQAPowerEstimate)
	br := big.Mul(qaSectorPower, expectedRewardForProvingPeriod) // Q.0 * Q.128 => Q.128
	return big.Rsh(br, math.Precision)
}

// This is the FF(t) penalty for a sector expected to be in the fault state either because the fault was declared or because
// it has been previously detected by the network.
// FF(t) = DeclaredFaultFactor * BR(t)
func PledgePenaltyForDeclaredFault(rewardEstimate, networkQAPowerEstimate *smoothing.FilterEstimate, qaSectorPower abi.StoragePower) abi.TokenAmount {
	return big.Div(
		big.Mul(DeclaredFaultFactorNum, ExpectedDayRewardForPower(rewardEstimate, networkQAPowerEstimate, qaSectorPower)),
		DeclaredFaultFactorDenom)
}

// This is the SP(t) penalty for a newly faulty sector that has not been declared.
// SP(t) = UndeclaredFaultFactor * BR(t)
func PledgePenaltyForUndeclaredFault(rewardEstimate, networkQAPowerEstimate *smoothing.FilterEstimate, qaSectorPower abi.StoragePower) abi.TokenAmount {
	return big.Div(
		big.Mul(UndeclaredFaultFactorNum, ExpectedDayRewardForPower(rewardEstimate, networkQAPowerEstimate, qaSectorPower)),
		UndeclaredFaultFactorDenom)
}

// Penalty to locked pledge collateral for the termination of a sector before scheduled expiry.
// SectorAge is the time between the sector's activation and termination.
func PledgePenaltyForTermination(initialPledge abi.TokenAmount, sectorAge abi.ChainEpoch, rewardEstimate, networkQAPowerEstimate *smoothing.FilterEstimate, qaSectorPower abi.StoragePower) abi.TokenAmount {
	// max(SP(t), IP + BR(StartEpoch)*min(SectorAgeInDays, 180))
	// where BR(StartEpoch)=IP/InitialPledgeFactor
	// and sectorAgeInDays = sectorAge / EpochsInDay
	cappedSectorAge := big.NewInt(int64(minEpoch(sectorAge, 180*builtin.EpochsInDay)))
	return big.Max(
		PledgePenaltyForUndeclaredFault(rewardEstimate, networkQAPowerEstimate, qaSectorPower),
		big.Add(
			initialPledge,
			big.Div(
				big.Mul(initialPledge, cappedSectorAge),
				big.Mul(InitialPledgeFactor, big.NewInt(builtin.EpochsInDay)))))
}

// Computes the PreCommit Deposit given sector qa weight and current network conditions.
// PreCommit Deposit = 20 * BR(t)
func PreCommitDepositForPower(rewardEstimate, networkQAPowerEstimate *smoothing.FilterEstimate, qaSectorPower abi.StoragePower) abi.TokenAmount {
	return big.Mul(PreCommitDepositFactor, ExpectedDayRewardForPower(rewardEstimate, networkQAPowerEstimate, qaSectorPower))
}

// Computes the pledge requirement for committing new quality-adjusted power to the network, given the current
// total power, total pledge commitment, epoch block reward, and circulating token supply.
// In plain language, the pledge requirement is a multiple of the block reward expected to be earned by the
// newly-committed power, holding the per-epoch block reward constant (though in reality it will change over time).
func InitialPledgeForPower(qaPower abi.StoragePower, baselinePower abi.StoragePower, networkTotalPledge abi.TokenAmount, rewardEstimate, networkQAPowerEstimate *smoothing.FilterEstimate, networkCirculatingSupplySmoothed abi.TokenAmount) abi.TokenAmount {
	networkQAPower := networkQAPowerEstimate.Estimate()
	ipBase := big.Mul(InitialPledgeFactor, ExpectedDayRewardForPower(rewardEstimate, networkQAPowerEstimate, qaPower))

	lockTargetNum := big.Mul(LockTargetFactorNum, networkCirculatingSupplySmoothed)
	lockTargetDenom := LockTargetFactorDenom
	pledgeShareNum := qaPower
	pledgeShareDenom := big.Max(big.Max(networkQAPower, baselinePower), qaPower) // use qaPower in case others are 0
	additionalIPNum := big.Mul(lockTargetNum, pledgeShareNum)
	additionalIPDenom := big.Mul(lockTargetDenom, pledgeShareDenom)
	additionalIP := big.Div(additionalIPNum, additionalIPDenom)

	return big.Add(ipBase, additionalIP)
}
