package reward

import (
	"fmt"
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

	acc.Require(big.Add(st.TotalStoragePowerReward, balance).Equals(StorageMiningAllocationCheck), "reward given %v + reward left %v != storage mining allocation %v", st.TotalStoragePowerReward, balance, StorageMiningAllocationCheck)

	acc.Require(st.Epoch == priorEpoch+1, "reward state epoch %d does not match priorEpoch+1 %d", st.Epoch, priorEpoch+1)
	acc.Require(st.EffectiveNetworkTime <= st.Epoch, "effective network time greater than state epoch")

	acc.Require(st.CumsumRealized.LessThanEqual(st.CumsumBaseline), "cumsum realized > cumsum baseline")
	acc.Require(st.CumsumRealized.GreaterThanEqual(big.Zero()), "cumsum realized < 0")
	acc.Require(st.EffectiveBaselinePower.LessThanEqual(st.ThisEpochBaselinePower), "effective baseline power > baseline power")

	computedBaseline := big.Mul(BaselineInitialValue, math.ExpBySquaring(BaselineExponent, int64(st.Epoch)))
	computedBaseline = big.Rsh(computedBaseline, math.Precision128)
	acc.Require(st.ThisEpochBaselinePower.Equals(computedBaseline), "state baseline power %v does not match computed %v", st.ThisEpochBaselinePower, computedBaseline)

	fmt.Printf(`{
		"CumsumBaseline": %v,
		"CumsumRealized": %v,
		"EffectiveBaselinePower": %v,
		"EffectiveNetworkTime": %d,
		"Epoch": %d,
		"ThisEpochBaselinePower": %v,
		"ThisEpochReward": %v,
		"ThisEpochRewardSmoothed": {
		  "PositionEstimate": %v,
		  "VelocityEstimate": %v
		},
		"TotalMined": %v
	}
	`, st.CumsumBaseline, st.CumsumRealized, st.EffectiveBaselinePower, st.EffectiveNetworkTime,
	st.Epoch, st.ThisEpochBaselinePower, st.ThisEpochReward, st.ThisEpochRewardSmoothed.PositionEstimate,
	st.ThisEpochRewardSmoothed.VelocityEstimate, st.TotalStoragePowerReward)

	return &StateSummary{}, acc, nil
}
