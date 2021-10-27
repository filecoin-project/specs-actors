package paych

import (
	"github.com/filecoin-project/go-state-types/abi"
	"math"

	"github.com/filecoin-project/specs-actors/v7/actors/builtin"
)

// Maximum number of lanes in a channel.
const MaxLane = math.MaxInt64

func SettleDelay() abi.ChainEpoch {
	return builtin.EpochsInHour() * 12
}

// Maximum size of a secret that can be submitted with a payment channel update (in bytes).
const MaxSecretSize = 256
