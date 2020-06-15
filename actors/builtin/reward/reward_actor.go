package reward

import (
	"github.com/filecoin-project/go-address"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	. "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type Actor struct{}

func (a Actor) Exports() []interface{} {
	return []interface{}{
		builtin.MethodConstructor: a.Constructor,
		2:                         a.AwardBlockReward,
		3:                         a.LastPerEpochReward,
		4:                         a.UpdateNetworkKPI,
	}
}

var _ abi.Invokee = Actor{}

func (a Actor) Constructor(rt vmr.Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	// TODO: the initial epoch reward should be set here based on the genesis storage power KPI.
	// https://github.com/filecoin-project/specs-actors/issues/317
	st := ConstructState()
	rt.State().Create(st)
	return nil
}

type AwardBlockRewardParams struct {
	Miner     address.Address
	Penalty   abi.TokenAmount // penalty for including bad messages in a block
	GasReward abi.TokenAmount // gas reward from all gas fees in a block
}

// Awards a reward to a block producer.
// This method is called only by the system actor, implicitly, as the last message in the evaluation of a block.
// The system actor thus computes the parameters and attached value.
//
// The reward includes two components:
// - the epoch block reward, computed and paid from the reward actor's balance,
// - the block gas reward, expected to be transferred to the reward actor with this invocation.
//
// The reward is reduced before the residual is credited to the block producer, by:
// - a penalty amount, provided as a parameter, which is burnt,
func (a Actor) AwardBlockReward(rt vmr.Runtime, params *AwardBlockRewardParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	AssertMsg(rt.CurrentBalance().GreaterThanEqual(params.GasReward),
		"actor current balance %v insufficient to pay gas reward %v", rt.CurrentBalance(), params.GasReward)

	minerAddr, ok := rt.ResolveAddress(params.Miner)
	if !ok {
		rt.Abortf(exitcode.ErrIllegalState, "failed to resolve given owner address")
	}

	priorBalance := rt.CurrentBalance()

	penalty := abi.NewTokenAmount(0)
	var st State
	rt.State().Readonly(&st)
	blockReward := big.Div(st.LastPerEpochReward, big.NewInt(builtin.ExpectedLeadersPerEpoch))
	totalReward := big.Add(blockReward, params.GasReward)

	// Cap the penalty at the total reward value.
	penalty = big.Min(params.Penalty, totalReward)

	// Reduce the payable reward by the penalty.
	rewardPayable := big.Sub(totalReward, penalty)

	AssertMsg(big.Add(rewardPayable, penalty).LessThanEqual(priorBalance),
		"reward payable %v + penalty %v exceeds balance %v", rewardPayable, penalty, priorBalance)

	_, code := rt.Send(minerAddr, builtin.MethodsMiner.AddLockedFund, &rewardPayable, rewardPayable)
	builtin.RequireSuccess(rt, code, "failed to send reward to miner: %s", minerAddr)

	// Burn the penalty amount.
	_, code = rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, penalty)
	builtin.RequireSuccess(rt, code, "failed to send penalty to burnt funds actor")

	return nil
}

func (a Actor) LastPerEpochReward(rt vmr.Runtime, _ *adt.EmptyValue) *abi.TokenAmount {
	rt.ValidateImmediateCallerAcceptAny()

	var st State
	rt.State().Readonly(&st)
	return &st.LastPerEpochReward
}

// Updates the simple/baseline supply state and last epoch reward with computation for for a single epoch.
func (a Actor) computePerEpochReward(st *State, clockTime abi.ChainEpoch, networkTime NetworkTime) abi.TokenAmount {
	// TODO: PARAM_FINISH
	newSimpleSupply := mintingFunction(SimpleTotal, big.Lsh(big.NewInt(int64(clockTime)), MintingInputFixedPoint))
	newBaselineSupply := mintingFunction(BaselineTotal, networkTime)

	newSimpleMinted := big.Max(big.Sub(newSimpleSupply, st.SimpleSupply), big.Zero())
	newBaselineMinted := big.Max(big.Sub(newBaselineSupply, st.BaselineSupply), big.Zero())

	// TODO: this isn't actually counting emitted reward, but expected reward (which will generally over-estimate).
	// It's difficult to extract this from the minting function in its current form.
	// https://github.com/filecoin-project/specs-actors/issues/317
	st.SimpleSupply = newSimpleSupply
	st.BaselineSupply = newBaselineSupply

	perEpochReward := big.Add(newSimpleMinted, newBaselineMinted)
	st.LastPerEpochReward = perEpochReward

	return perEpochReward
}

const baselinePower = 1 << 50 // 1PiB for testnet, PARAM_FINISH
func (a Actor) newBaselinePower(st *State, rewardEpochsPaid abi.ChainEpoch) abi.StoragePower {
	// TODO: this is not the final baseline function or value, PARAM_FINISH
	return big.NewInt(baselinePower)
}

func (a Actor) getEffectiveNetworkTime(st *State, cumsumBaseline abi.Spacetime, cumsumRealized abi.Spacetime) NetworkTime {
	// TODO: this function depends on the final baseline
	// EffectiveNetworkTime is a fractional input with an implicit denominator of (2^MintingInputFixedPoint).
	// realizedCumsum is thus left shifted by MintingInputFixedPoint before converted into a FixedPoint fraction
	// through division (which is an inverse function for the integral of the baseline).
	return big.Div(big.Lsh(cumsumRealized, MintingInputFixedPoint), big.NewInt(baselinePower))
}

// Called at the end of each epoch by the power actor (in turn by its cron hook).
// This is only invoked for non-empty tipsets. The impact of this is that block rewards are paid out over
// a schedule defined by non-empty tipsets, not by elapsed time/epochs.
// This is not necessarily what we want, and may change.
func (a Actor) UpdateNetworkKPI(rt vmr.Runtime, currRealizedPower *abi.StoragePower) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	var st State
	rt.State().Transaction(&st, func() interface{} {
		// By the time this is called, the rewards for this epoch have been paid to miners.
		st.RewardEpochsPaid++
		st.RealizedPower = *currRealizedPower

		st.BaselinePower = a.newBaselinePower(&st, st.RewardEpochsPaid)
		st.CumsumBaseline = big.Add(st.CumsumBaseline, st.BaselinePower)

		// Cap realized power in computing CumsumRealized so that progress is only relative to the current epoch.
		cappedRealizedPower := big.Min(st.BaselinePower, st.RealizedPower)
		st.CumsumRealized = big.Add(st.CumsumRealized, cappedRealizedPower)

		st.EffectiveNetworkTime = a.getEffectiveNetworkTime(&st, st.CumsumBaseline, st.CumsumRealized)

		a.computePerEpochReward(&st, st.RewardEpochsPaid, st.EffectiveNetworkTime)

		return nil
	})
	return nil
}
