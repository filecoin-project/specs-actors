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
	BaselinePower abi.StoragePower
	RealizedPower abi.StoragePower
	CumsumBaseline abi.Spacetime
	CumsumRealized abi.Spacetime
	EffectiveNetworkTime abi.ChainEpoch

	RewardMap   cid.Cid         // HAMT[Address]AMT[Reward]

	LambdaNum big.Int
	LambdaDen	big.Int
	SimpleTotal abi.TokenAmount // constant total
	SimpleSupply abi.TokenAmount // current supply
	BaselineTotal abi.TokenAmount // constant total
	BaselineSupply abi.TokenAmount // current supply

	LastPerEpochReward abi.TokenAmount
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
		BaselinePower: big.Zero(),
		RealizedPower: big.Zero(),
		CumsumBaseline: big.Zero(),
		CumsumRealized: big.Zero(),
		EffectiveNetworkTime: abi.ChainEpoch(int64(0)),

		// MintRate: ln(0.5)/half_life_in_epoch
		LambdaNum: big.NewInt(-69314718056),
		LambdaDen: big.NewInt(6 * 365 * 24 * 60 * 2 * 100000000000),

		SimpleTotal: big.NewInt(1000000),
		SimpleSupply: big.Zero(),
		BaselineTotal: big.NewInt(1000000),
		BaselineSupply: big.Zero(),
	}
}

func factorial(x int64) int64 {
	if x == 0 {
		return 1
	}
	return x * factorial(x-1)
}

// Return taylor series expansion of e^(-lambda*t)
func (st *State) tsExpansion(t abi.ChainEpoch) big.Int {
	ret := big.Zero()
	for n := int64(0); n < int64(5); n++ {
		exponent := big.NewInt(n)
		numerator := big.Mul(big.Exp(st.LambdaNum.Neg(), exponent), big.Exp(big.NewInt(int64(t)), exponent))
		denominator := big.Mul(big.Exp(st.LambdaDen, exponent), big.NewInt(factorial(int64(n))))
		ret = big.Add(ret, big.Div(numerator, denominator))
	}

	return ret
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

		// (totalReward * elapsedEpoch) / vestDuration
		// Division must be done last to avoid precision loss with integer values
		return big.Div(big.Mul(r.Value, big.NewInt(int64(elapsed))), big.NewInt(int64(vestDuration)))
	default:
		return abi.NewTokenAmount(0)
	}
}
