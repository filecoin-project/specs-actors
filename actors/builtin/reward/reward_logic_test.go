package reward

import (
	"bytes"
	"fmt"
	gbig "math/big"
	"testing"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
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
	var oldBaselinePowerAt func(abi.ChainEpoch) abi.StoragePower

	BaselinePowerAt, oldBaselinePowerAt = func(epoch abi.ChainEpoch) abi.StoragePower {
		return big.Mul(big.NewInt(int64(epoch+1)), big.NewInt(2048))
	}, BaselinePowerAt
	defer func() {
		BaselinePowerAt = oldBaselinePowerAt
	}()

	assert.Equal(t, 0.5, q128ToF(computeRTheta(1, big.NewInt(2048+2*2048*0.5), big.NewInt(2048+2*2048))))
	assert.Equal(t, 0.25, q128ToF(computeRTheta(1, big.NewInt(2048+2*2048*0.25), big.NewInt(2048+2*2048))))

	cumsum15 := big.NewInt(0)
	for i := abi.ChainEpoch(0); i < 16; i++ {
		cumsum15 = big.Add(cumsum15, BaselinePowerAt(i))
	}
	assert.Equal(t, 15.25, q128ToF(computeRTheta(16,
		big.Add(cumsum15, big.Div(BaselinePowerAt(16), big.NewInt(4))),
		big.Add(cumsum15, BaselinePowerAt(16)))))
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
