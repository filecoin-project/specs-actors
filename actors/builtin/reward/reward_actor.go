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

	rewards, err := adt.MakeEmptyMultimap(adt.AsStore(rt)).Root()
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to construct state: %v", err)
	}

	st := ConstructState()
	rt.State().Create(st)
	return nil
}

type AwardBlockRewardParams struct {
	Miner       address.Address
	Penalty     abi.TokenAmount // penalty for including bad messages in a block
	GasReward   abi.TokenAmount // gas reward from all gas fees in a block
	TicketCount int64
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

	AssertMsg(params.TicketCount > 0, "cannot give block reward for zero tickets")

	minerAddr, ok := rt.ResolveAddress(params.Miner)
	if !ok {
		rt.Abortf(exitcode.ErrIllegalState, "failed to resolve given owner address")
	}

	priorBalance := rt.CurrentBalance()

	var penalty abi.TokenAmount
	var st State
	rt.State().Readonly(&st)
	blockReward := big.Div(st.LastPerEpochReward, big.NewInt(ExpectedLeadersPerRound))
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
func (a Actor) computePerEpochReward(st *State, clockTime abi.ChainEpoch, networkTime abi.ChainEpoch, ticketCount int64) abi.TokenAmount {
	// TODO: PARAM_FINISH
	newSimpleSupply := mintingFunction(SimpleTotal, big.Lsh(big.NewInt(int64(clockTime)), MintingInputFixedPoint))

	// TODO: when network time calculation produces a fixed-point representation
	// with a fractional part of MintingInputFixedPoint, remove this Lsh
	newBaselineSupply := mintingFunction(BaselineTotal, big.Lsh(big.NewInt(int64(networkTime)), MintingInputFixedPoint))

	newSimpleMinted := big.Max(big.Sub(newSimpleSupply, st.SimpleSupply), big.Zero())
	newBaselineMinted := big.Max(big.Sub(newBaselineSupply, st.BaselineSupply), big.Zero())

	st.SimpleSupply = newSimpleSupply
	st.BaselineSupply = newBaselineSupply

	perEpochReward := big.Add(newSimpleMinted, newBaselineMinted)
	st.LastPerEpochReward = perEpochReward

	return perEpochReward
}

const baselinePower = 1 << 50 // 1PiB for testnet, PARAM_FINISH
func (a Actor) newBaselinePower(st *State, epochEnd abi.ChainEpoch) abi.StoragePower {
	// TODO: this is not the final baseline function or value, PARAM_FINISH
	return big.NewInt(baselinePower)
}

func (a Actor) getEffectiveNetworkTime(st *State, cumsumBaseline abi.Spacetime, cumsumRealized abi.Spacetime) abi.ChainEpoch {
	// TODO: this function depends on the final baseline
	realizedCumsum := big.Min(cumsumBaseline, cumsumRealized)
	return abi.ChainEpoch(big.Div(realizedCumsum, big.NewInt(baselinePower)).Int64())
}

// Called at the end of each epoch by the power actor (in turn by it's cron hook).
func (a Actor) UpdateNetworkKPI(rt vmr.Runtime, currRealizedPower *abi.StoragePower) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	var st State
	rt.State().Transaction(&st, func() interface{} {
		// Loop to compute intermediates for any missed epochs (empty tipsets), and then finally the current epoch.
		for i := st.LastKPIUpdate + 1; i <= rt.CurrEpoch(); i++ {
			st.BaselinePower = a.newBaselinePower(&st, i)
			st.CumsumBaseline = big.Add(st.CumsumBaseline, st.BaselinePower)

			// Add the prior realized power for each empty tipset, then the new realized power only for this non-empty one.
			if i == rt.CurrEpoch() {
				st.CumsumRealized = big.Add(st.CumsumRealized, *currRealizedPower)
			} else {
				st.CumsumRealized = big.Add(st.CumsumRealized, st.RealizedPower)

			}

			st.EffectiveNetworkTime = a.getEffectiveNetworkTime(&st, st.CumsumBaseline, st.CumsumRealized)

			// TODO: in case of an empty tipset, this will incorrectly update the supply state to suggest that reward
			// was emitted in epoch, when none was. It's better than the alternative of paying double reward in the
			// subsequent non-null round, though, because that would incentivize bad behaviour.
			// A real fix will require teasing that logic apart somewhat.
			// https://github.com/filecoin-project/specs-actors/issues/317
			a.computePerEpochReward(&st, i, st.EffectiveNetworkTime, 1)
		}

		st.RealizedPower = *currRealizedPower
		st.LastKPIUpdate = rt.CurrEpoch()
		return nil
	})

	return nil
}
