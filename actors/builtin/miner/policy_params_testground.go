// +build testground

package miner

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
)

// The period over which all a miner's active sectors will be challenged.
const WPoStProvingPeriod = abi.ChainEpoch(360) // proving period set to 360 epochs ~ 6min. instead of 24 hours (assuming block time delay of 1sec.)

// The duration of a deadline's challenge window, the period before a deadline when the challenge is available.
const WPoStChallengeWindow = abi.ChainEpoch(10) // challenge window set to 10 epochs ~ 10sec. instead of 40 minutes (assuming block time delay of 1sec.)

// The number of non-overlapping PoSt deadlines in each proving period.
const WPoStPeriodDeadlines = uint64(WPoStProvingPeriod / WPoStChallengeWindow) // Again 36 non-overlapping windows, same as production.

// The maximum age of a fault before the sector is terminated.
const FaultMaxAge = WPoStProvingPeriod*14 - 1
