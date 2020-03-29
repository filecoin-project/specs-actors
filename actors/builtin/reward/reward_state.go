package reward

import (
	"fmt"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	errors "github.com/pkg/errors"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	. "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type State struct {
	RewardMap   cid.Cid         // HAMT[Address]AMT[Reward]
	RewardTotal abi.TokenAmount // Sum of un-withdrawn rewards.
}

type Reward struct {
	VestingFunction
	StartEpoch      abi.ChainEpoch
	EndEpoch        abi.ChainEpoch
	Value           abi.TokenAmount
	AmountWithdrawn abi.TokenAmount
}

type VestingFunction int64

const (
	None VestingFunction = iota
	Linear
)

type AddrKey = adt.AddrKey

func ConstructState(emptyMultiMapCid cid.Cid) *State {
	return &State{
		RewardMap:   emptyMultiMapCid,
		RewardTotal: big.Zero(),
	}
}

func (st *State) addReward(store adt.Store, owner addr.Address, reward *Reward) error {
	rewards := adt.AsMultimap(store, st.RewardMap)
	key := AddrKey(owner)
	err := rewards.Add(key, reward)
	if err != nil {
		return errors.Wrap(err, "failed to add reward")
	}
	st.RewardMap = rewards.Root()
	st.RewardTotal = big.Add(st.RewardTotal, reward.Value)
	return nil
}

// Calculates and subtracts the total withdrawable reward for an owner.
func (st *State) withdrawReward(store adt.Store, owner addr.Address, currEpoch abi.ChainEpoch) (abi.TokenAmount, error) {
	rewards := adt.AsMultimap(store, st.RewardMap)
	key := AddrKey(owner)

	// Iterate rewards, accumulating withdrawable total and updated reward state (for those rewards that are
	// not yet exhausted).
	withdrawableSum := abi.NewTokenAmount(0)
	remainingRewards := []*Reward{}
	var reward Reward
	if err := rewards.ForEach(key, &reward, func(i int64) error {
		unlocked := reward.AmountVested(currEpoch)
		withdrawable := big.Sub(unlocked, reward.AmountWithdrawn)

		if withdrawable.LessThan(big.Zero()) {
			return errors.Errorf("negative withdrawable %v from reward %v at epoch %v", withdrawable, reward, currEpoch)
		}

		withdrawableSum = big.Add(withdrawableSum, withdrawable)
		if unlocked.LessThan(reward.Value) {
			newReward := Reward{
				VestingFunction: reward.VestingFunction,
				StartEpoch:      reward.StartEpoch,
				EndEpoch:        reward.EndEpoch,
				Value:           reward.Value,
				AmountWithdrawn: unlocked,
			}
			remainingRewards = append(remainingRewards, &newReward)
		}
		return nil
	}); err != nil {
		return big.Zero(), err
	}

	AssertMsg(withdrawableSum.LessThan(st.RewardTotal), "withdrawable %v exceeds recorded prior total %v",
		withdrawableSum, st.RewardTotal)

	// Replace old reward list for this key with the updated list.
	if err := rewards.RemoveAll(key); err != nil {
		return big.Zero(), errors.Wrapf(err, "failed to remove rewards")
	}
	for _, r := range remainingRewards {
		if err := rewards.Add(key, r); err != nil {
			return big.Zero(), errors.Wrapf(err, "failed to store updated rewards")
		}
	}
	st.RewardMap = rewards.Root()
	st.RewardTotal = big.Sub(st.RewardTotal, withdrawableSum)
	return withdrawableSum, nil
}

// AmountVested returns the `TokenAmount` value of funds vested in the reward at an epoch
func (r *Reward) AmountVested(currEpoch abi.ChainEpoch) abi.TokenAmount {
	switch r.VestingFunction {
	case None:
		return r.Value
	case Linear:
		elapsed := currEpoch - r.StartEpoch

		vestDuration := r.EndEpoch - r.StartEpoch
		if elapsed >= vestDuration {
			return r.Value
		}

		// totalReward * elapsedEpoch / vestDuration
		return big.Mul(r.Value, big.NewInt(int64(elapsed/vestDuration)))
	default:
		panic(fmt.Sprintf("invalid vesting function %v", r.VestingFunction))

	}
}
