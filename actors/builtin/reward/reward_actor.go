package reward

import (
	addr "github.com/filecoin-project/go-address"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	power "github.com/filecoin-project/specs-actors/actors/builtin/power"
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
		3:                         a.WithdrawReward,
	}
}

var _ abi.Invokee = Actor{}

func (a Actor) Constructor(rt vmr.Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	rt.State().Construct(func() vmr.CBORMarshaler {
		state, err := ConstructState(adt.AsStore(rt))
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to construct state: %v", err)
		}
		return state
	})
	return &adt.EmptyValue{}
}

func (a Actor) WithdrawReward(rt vmr.Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	owner := rt.ImmediateCaller()

	var st State
	withdrawableReward := rt.State().Transaction(&st, func() interface{} {
		// Withdraw all available funds
		withdrawn, err := st.withdrawReward(adt.AsStore(rt), owner, rt.CurrEpoch())
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to withdraw reward: %v", err)
		}
		return withdrawn
	}).(abi.TokenAmount)

	_, code := rt.Send(owner, builtin.MethodSend, nil, withdrawableReward)
	builtin.RequireSuccess(rt, code, "failed to send funds %v to owner %v", withdrawableReward, owner)
	return &adt.EmptyValue{}
}

type AwardBlockRewardParams struct {
	Miner            addr.Address
	Penalty          abi.TokenAmount // penalty for including bad messages in a block
	GasReward        abi.TokenAmount // gas reward from all gas fees in a block
	NominalPower     abi.StoragePower
	PledgeCollateral abi.TokenAmount
}

// gasReward is expected to be transferred to this actor by the runtime before invocation
func (a Actor) AwardBlockReward(rt vmr.Runtime, params *AwardBlockRewardParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	AssertMsg(params.GasReward.Equals(rt.ValueReceived()),
		"expected value received %v to match gas reward %v", rt.ValueReceived(), params.GasReward)

	inds := rt.CurrIndices()
	pledgeReq := inds.PledgeCollateralReq(params.NominalPower)
	currReward := inds.GetCurrBlockRewardForMiner(params.NominalPower, params.PledgeCollateral)
	totalReward := big.Add(currReward, params.GasReward)

	// BlockReward + GasReward <= penalty
	penalty := params.Penalty
	if totalReward.LessThanEqual(params.Penalty) {
		penalty = totalReward
	}

	// Burn the penalty amount
	_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, penalty)
	builtin.RequireSuccess(rt, code, "failed to send penalty to BurntFundsActor")

	// 0 if totalReward <= penalty
	rewardAfterPenalty := big.Sub(totalReward, penalty)

	// 0 if over collateralized
	underPledge := big.Max(big.Zero(), big.Sub(pledgeReq, params.PledgeCollateral))
	rewardToGarnish := big.Min(rewardAfterPenalty, underPledge)

	actualReward := big.Sub(rewardAfterPenalty, rewardToGarnish)
	if rewardToGarnish.GreaterThan(big.Zero()) {
		// Send fund to SPA to top up collateral
		_, code = rt.Send(
			builtin.StoragePowerActorAddr,
			builtin.Method_StoragePowerActor_AddBalance,
			&power.AddBalanceParams{Miner: params.Miner},
			abi.TokenAmount(rewardToGarnish),
		)
		builtin.RequireSuccess(rt, code, "failed to add balance to power actor")
	}

	// Record new reward into reward map.
	var st State
	if actualReward.GreaterThan(abi.NewTokenAmount(0)) {
		rt.State().Transaction(&st, func() interface{} {
			newReward := Reward{
				StartEpoch:      rt.CurrEpoch(),
				EndEpoch:        rt.CurrEpoch(),
				Value:           actualReward,
				AmountWithdrawn: abi.NewTokenAmount(0),
				VestingFunction: None,
			}
			return st.addReward(adt.AsStore(rt), params.Miner, &newReward)
		})
	}
	return &adt.EmptyValue{}
}
