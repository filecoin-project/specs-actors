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
	BaselinePower        abi.StoragePower
	RealizedPower        abi.StoragePower
	CumsumBaseline       abi.Spacetime
	CumsumRealized       abi.Spacetime
	EffectiveNetworkTime abi.ChainEpoch

	RewardMap cid.Cid // HAMT[Address]AMT[Reward]

	SimpleSupply   abi.TokenAmount // current supply
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

const BlockTimeSeconds = 30
const SecondsInYear = 31556925
const FixedPoint = 97 // bit of precision for the minting function

// TODO: load in the number here
var LambdaNum = big.NewInt(-69314718056 * BlockTimeSeconds)
var LambdaDen = big.Mul(big.NewInt(6 * SecondsInYear), big.NewInt(100000000000))

// TODO: placeholder but unit should be attoFIL
var SimpleTotal = big.NewInt(1000000)
var BaselineTotal = big.NewInt(1000000)

func ConstructState(emptyMultiMapCid cid.Cid) *State {
	return &State{
		RewardMap:            emptyMultiMapCid,
		BaselinePower:        big.Zero(),
		RealizedPower:        big.Zero(),
		CumsumBaseline:       big.Zero(),
		CumsumRealized:       big.Zero(),
		EffectiveNetworkTime: abi.ChainEpoch(int64(0)),

		// MintRate: ln(0.5)/half_life_in_epoch

		SimpleSupply:   big.Zero(),
		BaselineSupply: big.Zero(),
	}
}

// Return taylor series expansion of 1-e^(-lambda*t)
// except that it is multiplied by 2^FixedPoint
// divide by 2^FixedPoint before using the return value
func (st *State) taylorSeriesExpansion(t abi.ChainEpoch) big.Int {
	numeratorBase := big.Mul(LambdaNum.Neg(), big.NewInt(int64(t)))
	numerator := numeratorBase.Neg()
	denominator := LambdaDen
	ret := big.Zero()

	for n := int64(1); n < int64(25); n++ {
		denominator = big.Mul(denominator, big.NewInt(n))
		term := big.Div(big.Lsh(numerator, FixedPoint), denominator)
		ret = big.Add(ret, term)

		denominator = big.Mul(denominator, LambdaDen)
		numerator = big.Mul(numerator, numeratorBase)

		denominatorLen := big.BitLen(denominator)
		unnecessaryBits := denominatorLen - FixedPoint
		if unnecessaryBits < 0 {
			unnecessaryBits = 0
		}

		numerator = big.Rsh(numerator, unnecessaryBits)
		denominator = big.Rsh(denominator, unnecessaryBits)
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
