package smoothing

import (
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
)

// Returns an estimate between position 0, velocity 0, and val over delta epochs
func TestingEstimate(val big.Int, delta abi.ChainEpoch) *FilterEstimate {
	estimate := InitialEstimate()
	filter := LoadFilter(estimate, DefaultAlpha, DefaultBeta)
	return filter.NextEstimate(val, delta)
}
