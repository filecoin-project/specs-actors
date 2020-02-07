package reward

import (
	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	errors "github.com/pkg/errors"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type State struct {
	RewardMap cid.Cid // HAMT[Address]AMT[Reward]
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

func ConstructState(store adt.Store) (*State, error) {
	rewards, err := adt.MakeEmptyMultiap(store)
	if err != nil {
		return nil, err
	}

	return &State{
		RewardMap: rewards.Root(),
	}, nil
}

func (st *State) addReward(store adt.Store, owner addr.Address, reward *Reward) error {
	rewards := adt.AsMultimap(store, st.RewardMap)
	key := AddrKey(owner)
	err := rewards.Add(key, reward)
	if err != nil {
		return errors.Wrap(err, "failed to add reward")
	}
	st.RewardMap = rewards.Root()
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
	return withdrawableSum, nil
}

func (r *Reward) AmountVested(currEpoch abi.ChainEpoch) abi.TokenAmount {
	elapsed := currEpoch - r.StartEpoch
	switch r.VestingFunction {
	case None:
		return r.Value
	case Linear:
		vestDuration := big.Sub(big.NewInt(int64(r.EndEpoch)), big.NewInt(int64(r.StartEpoch)))
		if big.NewInt(int64(elapsed)).GreaterThanEqual(vestDuration) {
			return r.Value
		}

		// totalReward * elapsedEpoch / vestDuration
		return big.Div(big.Mul(r.Value, big.NewInt(int64(elapsed))), vestDuration)
	default:
		return abi.NewTokenAmount(0)
	}
}
