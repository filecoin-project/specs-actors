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

// Number of token units in an abstract "FIL" token.
// The network works purely in the indivisible token amounts. This constant converts to a fixed decimal with more
// human-friendly scale.
const TokenPrecision = int64(1_000_000_000_000_000_000)

// Target reward released to each block winner.
var BlockRewardTarget = big.Mul(big.NewInt(100), big.NewInt(TokenPrecision))

const rewardVestingFunction = None            // PARAM_FINISH
const rewardVestingPeriod = abi.ChainEpoch(0) // PARAM_FINISH

type Actor struct{}

func (a Actor) Exports() []interface{} {
	return []interface{}{
		builtin.MethodConstructor: a.Constructor,
		2:                         a.AwardBlockReward,
		3:                         a.WithdrawReward,
	}
}

var _ abi.Invokee = Actor{}

func (a Actor) Constructor(rt vmr.Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)

	rewards, err := adt.MakeEmptyMultimap(adt.AsStore(rt))
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to construct state: %v", err)
	}

	st := ConstructState(rewards.Root())
	rt.State().Create(st)
	return &adt.EmptyValue{}
}

type AwardBlockRewardParams struct {
	Miner     address.Address
	Penalty   abi.TokenAmount // penalty for including bad messages in a block
	GasReward abi.TokenAmount // gas reward from all gas fees in a block
}

// Awards a reward to a block producer, by accounting for it internally to be withdrawn later.
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

	miner, ok := rt.ResolveAddress(params.Miner)
	if !ok {
		rt.Abortf(exitcode.ErrIllegalState, "failed to resolve given owner address")
	}

	priorBalance := rt.CurrentBalance()

	var penalty abi.TokenAmount
	var st State
	rt.State().Transaction(&st, func() interface{} {
		blockReward := a.computeBlockReward(&st, big.Sub(priorBalance, params.GasReward))
		totalReward := big.Add(blockReward, params.GasReward)

		// Cap the penalty at the total reward value.
		penalty = big.Min(params.Penalty, totalReward)

		// Reduce the payable reward by the penalty.
		rewardPayable := big.Sub(totalReward, penalty)

		AssertMsg(big.Add(rewardPayable, penalty).LessThanEqual(priorBalance),
			"reward payable %v + penalty %v exceeds balance %v", rewardPayable, penalty, priorBalance)

		// Record new reward into reward map.
		if rewardPayable.GreaterThan(abi.NewTokenAmount(0)) {
			newReward := Reward{
				StartEpoch:      rt.CurrEpoch(),
				EndEpoch:        rt.CurrEpoch() + rewardVestingPeriod,
				Value:           rewardPayable,
				AmountWithdrawn: abi.NewTokenAmount(0),
				VestingFunction: rewardVestingFunction,
			}
			return st.addReward(adt.AsStore(rt), miner, &newReward)
		}
		return nil
	})

	// Burn the penalty amount.
	_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, penalty)
	builtin.RequireSuccess(rt, code, "failed to send penalty to BurntFundsActor")

	return &adt.EmptyValue{}
}

func (a Actor) WithdrawReward(rt vmr.Runtime, minerin *address.Address) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)

	maddr, ok := rt.ResolveAddress(*minerin)
	if !ok {
		rt.Abortf(exitcode.ErrIllegalArgument, "failed to resolve input address")
	}

	owner, _ := builtin.RequestMinerControlAddrs(rt, maddr)

	var st State
	withdrawableReward := rt.State().Transaction(&st, func() interface{} {
		// Withdraw all available funds
		withdrawn, err := st.withdrawReward(adt.AsStore(rt), maddr, rt.CurrEpoch())
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to withdraw reward: %v", err)
		}
		return withdrawn
	}).(abi.TokenAmount)

	_, code := rt.Send(owner, builtin.MethodSend, nil, withdrawableReward)
	builtin.RequireSuccess(rt, code, "failed to send funds %v to owner %v", withdrawableReward, owner)
	return &adt.EmptyValue{}
}

func (a Actor) computeBlockReward(st *State, balance abi.TokenAmount) abi.TokenAmount {
	// TODO: this is definitely not the final form of the block reward function.
	// The eventual form will be some kind of exponential decay.
	treasury := big.Sub(balance, st.RewardTotal)
	targetReward := BlockRewardTarget
	return big.Min(targetReward, treasury)
}
