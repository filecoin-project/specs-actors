package reward

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/util/math"
)

// Round(e^(ln[1 + 200%] / epochsInYear) * 2^128
// Q.128 formatted number such that f(epoch) = baseExponent^epoch grows 200% in one year of epochs
// Calculation here: https://www.wolframalpha.com/input/?i=Round%5BExp%5BLog%5B1%2B200%25%5D%2F%28%281+tropical+year%29%2F%2825+seconds%29%29%5D*2%5E128%5D
const BaselineExponentString = "340282663082994238536867392845056089438"

// Baseline function = BaselineInitialValue * (BaselineExponent) ^(t), t in epochs
// Note: we compute exponential iteratively using recurrence e(n) = e * e(n-1).
// Caller of baseline power function is responsible for keeping track of intermediate,
// state e(n-1), the baseline power function just does the next multiplication
var BaselineExponent big.Int     // Q.128
var BaselineInitialValue big.Int // Q.0

func init() {
	BaselineExponent = big.MustFromString(BaselineExponentString)
	BaselineInitialValue = big.Lsh(big.NewInt(1), 60) // 1 EiB
}

// Initialize baseline power for epoch -1 so that baseline power at epoch 0 is
// BaselineInitialValue.
func InitBaselinePower() abi.StoragePower {
	baselineInitialValue256 := big.Lsh(BaselineInitialValue, 2*math.Precision) // Q.0 => Q.256
	baselineAtMinusOne := big.Div(baselineInitialValue256, BaselineExponent)   // Q.256 / Q.128 => Q.128
	return big.Rsh(baselineAtMinusOne, math.Precision)                         // Q.128 => Q.0
}

// Compute BaselinePower(t) from BaselinePower(t-1) with an additional multiplication
// of the base exponent.
func BaselinePowerFromPrev(prevEpochBaselinePower abi.StoragePower) abi.StoragePower {
	thisEpochBaselinePower := big.Mul(prevEpochBaselinePower, BaselineExponent) // Q.0 * Q.128 => Q.128
	return big.Rsh(thisEpochBaselinePower, math.Precision)                      // Q.128 => Q.0
}

// These numbers are placeholders, but should be in units of attoFIL, 10^-18 FIL
var SimpleTotal = big.Mul(big.NewInt(300e6), big.NewInt(1e18))   // 300M for testnet, PARAM_FINISH
var BaselineTotal = big.Mul(big.NewInt(700e6), big.NewInt(1e18)) // 700M for testnet, PARAM_FINISH

// Computes RewardTheta which is is precise fractional value of effectiveNetworkTime.
// The effectiveNetworkTime is defined by CumsumBaselinePower(theta) == CumsumRealizedPower
// As baseline power is defined over integers and the RewardTheta is required to be fractional,
// we perform linear interpolation between CumsumBaseline(⌊theta⌋) and CumsumBaseline(⌈theta⌉).
// The effectiveNetworkTime argument is ceiling of theta.
// The result is a fractional effectiveNetworkTime (theta) in Q.128 format.
func computeRTheta(effectiveNetworkTime abi.ChainEpoch, baselinePowerAtEffectiveNetworkTime, cumsumRealized, cumsumBaseline big.Int) big.Int {
	var rewardTheta big.Int
	if effectiveNetworkTime != 0 {
		rewardTheta = big.NewInt(int64(effectiveNetworkTime)) // Q.0
		rewardTheta = big.Lsh(rewardTheta, math.Precision)    // Q.0 => Q.128
		diff := big.Sub(cumsumBaseline, cumsumRealized)
		diff = big.Lsh(diff, math.Precision)                      // Q.0 => Q.128
		diff = big.Div(diff, baselinePowerAtEffectiveNetworkTime) // Q.128 / Q.0 => Q.128
		rewardTheta = big.Sub(rewardTheta, diff)                  // Q.128
	} else {
		// special case for initialization
		rewardTheta = big.Zero()
	}
	return rewardTheta
}

var (
	// parameters in Q.128 format
	// lambda = tropicalYearInSeconds/blockDelay*ln(2)
	// Precise calculation:
	// lambda = ln(2) / (6 * 365.24219 * 24 * 60 * 60 / blockDelay(25))
	// for Q.128: lambdaQ128 = floor(lambda * 2^128)
	lambda = big.MustFromString("31142895155747063090497695472430")
	// expLamSubOne = e^lambda - 1
	expLamSubOne = big.MustFromString("31142896580857563299345000661898")
)

// Computes a reward for all expected leaders when effective network time changes from prevTheta to currTheta
// Inputs are in Q.128 format
func computeReward(epoch abi.ChainEpoch, prevTheta, currTheta big.Int) abi.TokenAmount {
	simpleReward := big.Mul(SimpleTotal, expLamSubOne)    //Q.0 * Q.128 =>  Q.128
	epochLam := big.Mul(big.NewInt(int64(epoch)), lambda) // Q.0 * Q.128 => Q.128

	simpleReward = big.Mul(simpleReward, big.Int{Int: expneg(epochLam.Int)}) // Q.128 * Q.128 => Q.256
	simpleReward = big.Rsh(simpleReward, math.Precision)                     // Q.256 >> 128 => Q.128

	baselineReward := big.Sub(computeBaselineSupply(currTheta), computeBaselineSupply(prevTheta)) // Q.128

	reward := big.Add(simpleReward, baselineReward) // Q.128

	return big.Rsh(reward, math.Precision) // Q.128 => Q.0
}

// Computes baseline supply based on theta in Q.128 format.
// Return is in Q.128 format
func computeBaselineSupply(theta big.Int) big.Int {
	thetaLam := big.Mul(theta, lambda)           // Q.128 * Q.128 => Q.256
	thetaLam = big.Rsh(thetaLam, math.Precision) // Q.256 >> 128 => Q.128

	eTL := big.Int{Int: expneg(thetaLam.Int)} // Q.128

	one := big.NewInt(1)
	one = big.Lsh(one, math.Precision) // Q.0 => Q.128
	oneSub := big.Sub(one, eTL)        // Q.128

	return big.Mul(BaselineTotal, oneSub) // Q.0 * Q.128 => Q.128
}

// SlowConvenientBaselineForEpoch computes baseline power for use in epoch t
// by calculating the value of ThisEpochBaselinePower that shows up in block at t - 1
// It multiplies ~t times so it should not be used in actor code directly.  It is exported as
// convenience for consuming node.
func SlowConvenientBaselineForEpoch(targetEpoch abi.ChainEpoch) abi.StoragePower {
	baseline := InitBaselinePower()
	baseline = BaselinePowerFromPrev(baseline) // value in genesis block (for epoch 1)
	for i := abi.ChainEpoch(1); i < targetEpoch; i++ {
		baseline = BaselinePowerFromPrev(baseline) // value in block i (for epoch i+1)
	}
	return baseline
}
