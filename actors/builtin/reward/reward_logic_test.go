package reward

import (
	gbig "math/big"
	"testing"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/stretchr/testify/assert"
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
