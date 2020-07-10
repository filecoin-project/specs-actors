package reward

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
)

var BaselinePowerAt = func(epoch abi.ChainEpoch) abi.StoragePower {
	return big.NewInt(1 << 40) // PARAM_FINISH
}

// These numbers are placeholders, but should be in units of attoFIL, 10^-18 FIL
var SimpleTotal = big.Mul(big.NewInt(100e6), big.NewInt(1e18))   // 100M for testnet, PARAM_FINISH
var BaselineTotal = big.Mul(big.NewInt(900e6), big.NewInt(1e18)) // 900M for testnet, PARAM_FINISH

// Computes RewardTheta which is is precise fractional value of effectiveNetworkTime.
// The effectiveNetworkTime is defined by CumsumBaselinePower(theta) == CumsumRealizedPower
// As baseline power is defined over integers and the RewardTheta is required to be fractional,
// we perform linear interpolation between CumsumBaseline(⌊theta⌋) and CumsumBaseline(⌈theta⌉).
// The effectiveNetworkTime argument is ceiling of theta.
// The result is a fractional effectiveNetworkTime (theta) in Q.128 format.
func computeRTheta(effectiveNetworkTime abi.ChainEpoch, cumsumRealized, cumsumBaseline big.Int) big.Int {
	var rewardTheta big.Int
	if effectiveNetworkTime != 0 {
		rewardTheta = big.NewInt(int64(effectiveNetworkTime)) // Q.0
		rewardTheta = big.Lsh(rewardTheta, precision)         // Q.0 => Q.128
		diff := big.Sub(cumsumBaseline, cumsumRealized)
		diff = big.Lsh(diff, precision)                             // Q.0 => Q.128
		diff = big.Div(diff, BaselinePowerAt(effectiveNetworkTime)) // Q.128 / Q.0 => Q.128
		rewardTheta = big.Sub(rewardTheta, diff)                    // Q.128
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
	simpleReward = big.Rsh(simpleReward, precision)                          // Q.256 >> 128 => Q.128

	baselineReward := big.Sub(computeBaselineSupply(currTheta), computeBaselineSupply(prevTheta)) // Q.128

	reward := big.Add(simpleReward, baselineReward) // Q.128

	return big.Rsh(reward, precision) // Q.128 => Q.0
}

// Computes baseline supply based on theta in Q.128 format.
// Return is in Q.128 format
func computeBaselineSupply(theta big.Int) big.Int {
	thetaLam := big.Mul(theta, lambda)      // Q.128 * Q.128 => Q.256
	thetaLam = big.Rsh(thetaLam, precision) // Q.256 >> 128 => Q.128

	eTL := big.Int{Int: expneg(thetaLam.Int)} // Q.128

	one := big.NewInt(1)
	one = big.Lsh(one, precision) // Q.0 => Q.128
	oneSub := big.Sub(one, eTL)   // Q.128

	return big.Mul(BaselineTotal, oneSub) // Q.0 * Q.128 => Q.128
}
