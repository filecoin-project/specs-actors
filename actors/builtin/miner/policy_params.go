// +build !testground

package miner

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
)

// The period over which all a miner's active sectors will be challenged.
const WPoStProvingPeriod = abi.ChainEpoch(builtin.EpochsInDay) // 24 hours

// The duration of a deadline's challenge window, the period before a deadline when the challenge is available.
const WPoStChallengeWindow = abi.ChainEpoch(40 * 60 / builtin.EpochDurationSeconds) // 40 minutes (36 per day)

// The number of non-overlapping PoSt deadlines in each proving period.
const WPoStPeriodDeadlines = uint64(WPoStProvingPeriod / WPoStChallengeWindow)

// The maximum age of a fault before the sector is terminated.
const FaultMaxAge = WPoStProvingPeriod*14 - 1
