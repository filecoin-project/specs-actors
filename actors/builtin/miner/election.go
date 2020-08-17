package miner

import (
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

const ElectionPledgeRequirementFactor = 5

func MinerEligibleForElection(store adt.Store, mSt *State, rSt *reward.State, minerActorBalance abi.TokenAmount, currEpoch abi.ChainEpoch) (bool, error) {
	// IP requirements are met.  This includes zero fee debt
	if !mSt.MeetsInitialPledgeCondition(minerActorBalance) {
		return false, nil
	}

	// No active consensus faults
	mInfo, err := mSt.GetInfo(store)
	if err != nil {
		return false, err
	}
	if ConsensusFaultActive(mInfo, currEpoch) {
		return false, nil
	}

	// IP requirement exceeds minimum for election
	electionRequirement := big.Div(
		big.Mul(
			big.NewInt(ElectionPledgeRequirementFactor),
			rSt.ThisEpochReward,
		),
		big.NewInt(builtin.ExpectedLeadersPerEpoch),
	)
	if mSt.InitialPledgeRequirement.LessThan(electionRequirement) {
		return false, nil
	}

	return true, nil
}
