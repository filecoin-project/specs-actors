package reward

import (
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/util/math"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type StateSummary struct{}

var FIL = big.NewInt(1e18)
var StorageMiningAllocationCheck = big.Mul(big.NewInt(1_100_000_000), FIL)

func CheckStateInvariants(st *State, store adt.Store, priorEpoch abi.ChainEpoch, balance abi.TokenAmount) (*StateSummary, *builtin.MessageAccumulator, error) {
	acc := &builtin.MessageAccumulator{}

	acc.Require(big.Add(st.TotalStoragePowerReward, balance).Equals(StorageMiningAllocationCheck), "reward given + reward left != storage mining allocation")

	acc.Require(st.Epoch == priorEpoch, "reward state epoch %d does not match priorEpoch %d", st.Epoch, priorEpoch)
	acc.Require(st.EffectiveNetworkTime <= st.Epoch, "effective network time greater than state epoch")

	acc.Require(st.CumsumRealized.LessThanEqual(st.CumsumBaseline), "cumsum realized > cumsum baseline")
	acc.Require(st.CumsumRealized.GreaterThanEqual(big.Zero()), "cumsum realized < 0")
	acc.Require(st.EffectiveBaselinePower.LessThanEqual(st.ThisEpochBaselinePower), "effective baseline power > baseline power")

	computedBaseline := math.ExpBySquaring(BaselineExponent, int64(st.Epoch))
	acc.Require(st.ThisEpochBaselinePower.Equals(computedBaseline), "state baseline power does not match computed")

	return &StateSummary{}, acc, nil
}
