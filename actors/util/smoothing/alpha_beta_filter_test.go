package smoothing_test

import (
	"fmt"
	"testing"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/util/math"
	"github.com/filecoin-project/specs-actors/actors/util/smoothing"
	"github.com/stretchr/testify/assert"
)

// project of cumsum ratio is equal to cumsum of ratio of projections
func TestCumSumRatioProjection(t *testing.T) {
	t.Run("constant estimate", func(t *testing.T) {
		numEstimate := smoothing.TestingConstantEstimate(big.NewInt(4e6))
		denomEstimate := smoothing.TestingConstantEstimate(big.NewInt(1))
		// 4e6/1 over 1000 epochs should give us 4e9
		csr := smoothing.ExtrapolatedCumSumOfRatio(abi.ChainEpoch(1000), abi.ChainEpoch(0), numEstimate, denomEstimate)
		csr = big.Rsh(csr, math.Precision)
		assert.Equal(t, big.NewInt(4e9), csr)

		// if we change t0 nothing should change because velocity is 0
		csr2 := smoothing.ExtrapolatedCumSumOfRatio(abi.ChainEpoch(1000), abi.ChainEpoch(1e15), numEstimate, denomEstimate)
		csr2 = big.Rsh(csr2, math.Precision)
		assert.Equal(t, csr, csr2)

		// 1e12 / 200e12 for 100 epochs should give ratio of 1/2
		numEstimate = smoothing.TestingConstantEstimate(big.NewInt(1e12))
		denomEstimate = smoothing.TestingConstantEstimate(big.NewInt(200e12))
		csrFrac := smoothing.ExtrapolatedCumSumOfRatio(abi.ChainEpoch(100), abi.ChainEpoch(0), numEstimate, denomEstimate)
		// If we didn't return Q.128 we'd just get zero
		assert.Equal(t, big.Zero(), big.Rsh(csrFrac, math.Precision))
		// multiply by 10k and we'll get 5k
		// note: this is a bit sensative to input, lots of numbers approach from below
		// (...99999) and so truncating division takes us off by one
		product := big.Mul(csrFrac, big.Lsh(big.NewInt(10000), math.Precision)) // Q.256
		assert.Equal(t, big.NewInt(5000), big.Rsh(product, 2*math.Precision))
	})

	// Q.128 cumsum of ratio
	iterativeCumSumOfRatio := func(num, denom *smoothing.FilterEstimate, t0, delta abi.ChainEpoch) big.Int {
		ratio := big.Zero() // Q.128
		for i := abi.ChainEpoch(0); i < delta; i++ {
			numEpsilon := num.Extrapolate(t0 + i)                // Q.256
			denomEpsilon := denom.Extrapolate(t0 + i)            // Q.256
			denomEpsilon = big.Rsh(denomEpsilon, math.Precision) // Q.256 => Q.128
			epsilon := big.Div(numEpsilon, denomEpsilon)         // Q.256 / Q.128 => Q.128
			ratio = big.Sum(ratio, epsilon)
		}
		return ratio
	}

	// Millionths of difference between val1 and val2
	// (val1 - val2) / val1 * 1e6
	// all inputs Q.128, output Q.0
	perMillionError := func(val1, val2 big.Int) big.Int {
		diff := big.Sub(val1, val2)
		if diff.LessThan(big.Zero()) {
			diff = diff.Neg()
		}
		diff = big.Lsh(diff, math.Precision)                // Q.128 => Q.256
		perMillion := big.Div(diff, val1)                   // Q.256 / Q.128 => Q.128
		million := big.Lsh(big.NewInt(1e6), math.Precision) // Q.0 => Q.128

		perMillion = big.Mul(perMillion, million) // Q.128 * Q.128 => Q.256
		return big.Rsh(perMillion, 2*math.Precision)
	}

	// millionths of error difference
	// set this error value after empirically seeing 56 millionths
	errBound := big.NewInt(100)
	t.Run("both positive velocity", func(t *testing.T) {
		numEstimate := smoothing.TestingEstimate(big.NewInt(111), big.NewInt(33))
		denomEstimate := smoothing.TestingEstimate(big.NewInt(3456), big.NewInt(8))
		delta := abi.ChainEpoch(10000)
		t0 := abi.ChainEpoch(0)
		analytic := smoothing.ExtrapolatedCumSumOfRatio(delta, t0, numEstimate, denomEstimate)
		iterative := iterativeCumSumOfRatio(numEstimate, denomEstimate, t0, delta)

		pme := perMillionError(analytic, iterative)
		fmt.Printf("pme: %v\n", pme)

		assert.True(t, pme.LessThan(errBound))
	})

	t.Run("flipped signs", func(t *testing.T) {
		numEstimate := smoothing.TestingEstimate(big.NewInt(1e6), big.NewInt(-100))
		denomEstimate := smoothing.TestingEstimate(big.NewInt(7e4), big.NewInt(1000))
		delta := abi.ChainEpoch(100000)
		t0 := abi.ChainEpoch(0)
		analytic := smoothing.ExtrapolatedCumSumOfRatio(delta, t0, numEstimate, denomEstimate)
		iterative := iterativeCumSumOfRatio(numEstimate, denomEstimate, t0, delta)

		pme := perMillionError(analytic, iterative)
		fmt.Printf("pme: %v\n", pme)
		assert.True(t, pme.LessThan(errBound))
	})

}

func TestNaturalLog(t *testing.T) {
	// TODO compare some standard natural log values to our computed values
}
