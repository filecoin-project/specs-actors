package reward

import (
	"io"
	"sort"

	addr "github.com/filecoin-project/go-address"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	serde "github.com/filecoin-project/specs-actors/actors/serde"
	autil "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

var TODO = autil.TODO

type VestingFunction int64

const (
	None VestingFunction = iota
	Linear
)

type Reward struct {
	VestingFunction
	StartEpoch      abi.ChainEpoch
	EndEpoch        abi.ChainEpoch
	Value           abi.TokenAmount
	AmountWithdrawn abi.TokenAmount
}

func (r *Reward) AmountVested(elapsedEpoch abi.ChainEpoch) abi.TokenAmount {
	switch r.VestingFunction {
	case None:
		return r.Value
	case Linear:
		vestDuration := big.Sub(big.NewInt(int64(r.EndEpoch)), big.NewInt(int64(r.StartEpoch)))
		if big.NewInt(int64(elapsedEpoch)).GreaterThanEqual(vestDuration) {
			return r.Value
		}

		// totalReward * elapsedEpoch / vestDuration
		return big.Div(big.Mul(r.Value, big.NewInt(int64(elapsedEpoch))), vestDuration)
	default:
		return abi.NewTokenAmount(0)
	}
}

// ownerAddr to a collection of Reward
// TODO: AMT
type RewardBalanceAMT map[addr.Address][]Reward

type RewardActorState struct {
	RewardMap RewardBalanceAMT
}

func (st *RewardActorState) _withdrawReward(rt vmr.Runtime, ownerAddr addr.Address) abi.TokenAmount {
	rewards, found := st.RewardMap[ownerAddr]
	if !found {
		rt.Abort(exitcode.ErrNotFound, "ra._withdrawReward: ownerAddr not found in RewardMap.")
	}

	rewardToWithdrawTotal := abi.NewTokenAmount(0)
	indicesToRemove := make([]int, len(rewards))

	for i, r := range rewards {
		elapsedEpoch := rt.CurrEpoch() - r.StartEpoch
		unlockedReward := r.AmountVested(elapsedEpoch)
		withdrawableReward := big.Sub(unlockedReward, r.AmountWithdrawn)

		if withdrawableReward.LessThan(big.Zero()) {
			rt.Abort(exitcode.ErrIllegalState, "ra._withdrawReward: negative withdrawableReward.")
		}

		r.AmountWithdrawn = unlockedReward // modify rewards in place
		rewardToWithdrawTotal = big.Add(rewardToWithdrawTotal, withdrawableReward)

		if r.AmountWithdrawn == r.Value {
			indicesToRemove = append(indicesToRemove, i)
		}
	}

	updatedRewards := removeIndices(rewards, indicesToRemove)
	st.RewardMap[ownerAddr] = updatedRewards

	return rewardToWithdrawTotal
}

type RewardActor struct{}

func (a *RewardActor) Constructor(rt vmr.Runtime) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	return &adt.EmptyValue{}
}

func (a *RewardActor) WithdrawReward(rt vmr.Runtime) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	ownerAddr := rt.ImmediateCaller()

	var st RewardActorState
	withdrawableReward := rt.State().Transaction(&st, func() interface{} {
		// withdraw available funds from RewardMap
		return st._withdrawReward(rt, ownerAddr)
	}).(abi.TokenAmount)

	_, code := rt.Send(ownerAddr, builtin.MethodSend, nil, withdrawableReward)
	builtin.RequireSuccess(rt, code, "failed to send funds to owner")
	return &adt.EmptyValue{}
}

// gasReward is expected to be transferred to this actor by the runtime before invocation
func (a *RewardActor) AwardBlockReward(
	rt vmr.Runtime,
	miner addr.Address,
	penalty abi.TokenAmount, // gas penalty for including bad messages
	gasReward abi.TokenAmount, // gas reward from all gas fees in a block
	minerNominalPower abi.StoragePower,
	currPledge abi.TokenAmount,
) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)

	inds := rt.CurrIndices()
	pledgeReq := inds.PledgeCollateralReq(minerNominalPower)
	currReward := inds.GetCurrBlockRewardForMiner(minerNominalPower, currPledge)

	totalReward := big.Add(currReward, gasReward)

	// BlockReward + GasReward <= penalty
	if totalReward.LessThanEqual(penalty) {
		penalty = totalReward
	}
	_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, penalty)
	builtin.RequireSuccess(rt, code, "failed to send penalty to BurntFundsActor")

	// 0 if totalReward <= penalty
	rewardAfterPenalty := big.Sub(totalReward, penalty)

	// 0 if over collateralized
	underPledge := big.Max(big.Zero(), big.Sub(pledgeReq, currPledge))
	rewardToGarnish := big.Min(rewardAfterPenalty, underPledge)

	actualReward := big.Sub(rewardAfterPenalty, rewardToGarnish)
	if rewardToGarnish.GreaterThan(big.Zero()) {
		// Send fund to SPA for collateral
		_, code := rt.Send(
			builtin.StoragePowerActorAddr,
			builtin.Method_StoragePowerActor_AddBalance,
			serde.MustSerializeParams(miner),
			abi.TokenAmount(rewardToGarnish),
		)
		builtin.RequireSuccess(rt, code, "failed to add balance to power actor")
	}

	var st RewardActorState
	rt.State().Transaction(&st, func() interface{} {
		if actualReward.GreaterThan(abi.NewTokenAmount(0)) {
			// put Reward into RewardMap
			newReward := Reward{
				StartEpoch:      rt.CurrEpoch(),
				EndEpoch:        rt.CurrEpoch(),
				Value:           actualReward,
				AmountWithdrawn: abi.NewTokenAmount(0),
				VestingFunction: None,
			}
			rewards, found := st.RewardMap[miner]
			if !found {
				rewards = make([]Reward, 0)
			}
			rewards = append(rewards, newReward)
			st.RewardMap[miner] = rewards
		}
		return nil
	})
	return &adt.EmptyValue{}
}

func removeIndices(rewards []Reward, indices []int) []Reward {
	// remove fully paid out Rewards by indices
	var newRewards []Reward
	var lastIndex int = 0
	sort.Ints(indices)
	for _, index := range indices {
		newRewards = append(newRewards, rewards[lastIndex:index]...)
		lastIndex = index + 1
	}
	return newRewards
}

func (s *RewardActorState) MarshalCBOR(w io.Writer) error {
	panic("replace with cbor-gen")
}

func (s *RewardActorState) UnmarshalCBOR(r io.Reader) error {
	panic("replace with cbor-gen")
}
