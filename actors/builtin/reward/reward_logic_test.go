package reward

import (
	"bytes"
	"fmt"
	gbig "math/big"
	"testing"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/stretchr/testify/assert"
	"github.com/xorcare/golden"
)

func q128ToF(x big.Int) float64 {
	q128 := new(gbig.Int).SetInt64(1)
	q128 = q128.Lsh(q128, precision)
	res, _ := new(gbig.Rat).SetFrac(x.Int, q128).Float64()
	return res
}

func TestComputeRTeta(t *testing.T) {
	baselinePowerAt := func(epoch abi.ChainEpoch) abi.StoragePower {
		return big.Mul(big.NewInt(int64(epoch+1)), big.NewInt(2048))
	}

	assert.Equal(t, 0.5, q128ToF(computeRTheta(1, baselinePowerAt(1), big.NewInt(2048+2*2048*0.5), big.NewInt(2048+2*2048))))
	assert.Equal(t, 0.25, q128ToF(computeRTheta(1, baselinePowerAt(1), big.NewInt(2048+2*2048*0.25), big.NewInt(2048+2*2048))))

	cumsum15 := big.NewInt(0)
	for i := abi.ChainEpoch(0); i < 16; i++ {
		cumsum15 = big.Add(cumsum15, baselinePowerAt(i))
	}
	assert.Equal(t, 15.25, q128ToF(computeRTheta(16,
		baselinePowerAt(16),
		big.Add(cumsum15, big.Div(baselinePowerAt(16), big.NewInt(4))),
		big.Add(cumsum15, baselinePowerAt(16)))))
}

func TestBaselineReward(t *testing.T) {
	step := gbig.NewInt(5000)
	step = step.Lsh(step, precision)
	step = step.Sub(step, gbig.NewInt(77777777777)) // offset from full integers

	delta := gbig.NewInt(1)
	delta = delta.Lsh(delta, precision)
	delta = delta.Sub(delta, gbig.NewInt(33333333333)) // offset from full integers

	prevTheta := new(gbig.Int)
	theta := new(gbig.Int).Set(delta)

	b := &bytes.Buffer{}
	b.WriteString("t0, t1, y\n")
	simple := computeReward(0, big.Zero(), big.Zero())

	for i := 0; i < 512; i++ {
		reward := computeReward(0, big.Int{Int: prevTheta}, big.Int{Int: theta})
		reward = big.Sub(reward, simple)
		fmt.Fprintf(b, "%s,%s,%s\n", prevTheta, theta, reward.Int)
		prevTheta = prevTheta.Add(prevTheta, step)
		theta = theta.Add(theta, step)
	}

	golden.Assert(t, b.Bytes())
}

func TestSimpleRewrad(t *testing.T) {
	b := &bytes.Buffer{}
	b.WriteString("x, y\n")
	for i := int64(0); i < 512; i++ {
		x := i * 5000
		reward := computeReward(abi.ChainEpoch(x), big.Zero(), big.Zero())
		fmt.Fprintf(b, "%d,%s\n", x, reward.Int)
	}

	golden.Assert(t, b.Bytes())
}

func TestBaselineRewardGrowth(t *testing.T) {
	// Baseline reward should have 200% growth rate in one year of epochs
	baselineInYears := func(start abi.StoragePower, x abi.ChainEpoch) abi.StoragePower {
		baseline := start
		for i := abi.ChainEpoch(0); i < x*builtin.EpochsInYear; i++ {
			baseline = BaselinePowerFromPrev(baseline)
		}
		return baseline
	}

	baselineQ128InYears := func(start abi.StoragePower, x abi.ChainEpoch) abi.StoragePower {
		baseline := big.Lsh(start, precision)
		for i := abi.ChainEpoch(0); i < x*builtin.EpochsInYear; i++ {
			baseline = BaselineQ128(baseline)
		}
		return big.Rsh(baseline, precision)
	}
	// starts := []abi.StoragePower{
	// abi.NewStoragePower(1),

	// abi.NewStoragePower(1 << 30), // GiB
	// abi.NewStoragePower(1 << 40), // TiB
	// abi.NewStoragePower(1 << 50), // PiB
	// BaselineInitialValue, // EiB
	// big.Lsh(big.NewInt(1), 70),   // ZiB

	// abi.NewStoragePower(513633559722596517), // non power of 2 ~ 1 EiB

	//	}
	// for _, start := range starts {
	// 	end := baselineInYears(start)
	// 	fmt.Printf("start: %v, end: %v\n", start, end)
	// 	assert.Equal(t, big.Mul(big.NewInt(3), start), end)
	// }

	for x := abi.ChainEpoch(1); x < 10; x++ {
		end := baselineInYears(BaselineInitialValue, x)
		multiplier := big.Exp(big.NewInt(3), big.NewInt(int64(x)))
		expected := big.Mul(BaselineInitialValue, multiplier)
		diff := big.Sub(expected, end)
		perrDenom := gbig.NewFloat(0).Mul(gbig.NewFloat(float64(BaselineInitialValue.Int64())), gbig.NewFloat(float64(multiplier.Int64())))
		perrNum := gbig.NewFloat(float64(diff.Int64()))
		perr := gbig.NewFloat(0).Quo(perrNum, perrDenom)

		fmt.Printf("1 EiB, %d years, diff in bytes: %v, diff percent err: %v\n", x, diff, perr)
	}
	fmt.Printf("\ntrack q128 precision in between\n\n")
	for x := abi.ChainEpoch(1); x < 10; x++ {
		end := baselineQ128InYears(BaselineInitialValue, x)
		multiplier := big.Exp(big.NewInt(3), big.NewInt(int64(x)))
		expected := big.Mul(BaselineInitialValue, multiplier)
		diff := big.Sub(expected, end)
		perrDenom := gbig.NewFloat(0).Mul(gbig.NewFloat(float64(BaselineInitialValue.Int64())), gbig.NewFloat(float64(multiplier.Int64())))
		perrNum := gbig.NewFloat(float64(diff.Int64()))
		perr := gbig.NewFloat(0).Quo(perrNum, perrDenom)
		fmt.Printf("1 EiB, %d years, diff: %v, diff percent err:%v \n", x, diff, perr)
	}

}
