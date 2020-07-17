package builtin

import "fmt"

// Filecoin Parameter: The duration of a chain epoch.
// Motivation: It guarantees that a block is propagated and WinningPoSt can be successfully done in time all supported miners.
// Usage: It is used for deriving epoch-denominated periods that are more naturally expressed in clock time.
// TODO: In lieu of a real configuration mechanism for this value, we'd like to make it a var so that implementations
// can override it at runtime. Doing so requires changing all the static references to it in this repo to go through
// late-binding function calls, or they'll see the "wrong" value.
// https://github.com/filecoin-project/specs-actors/issues/353
// If EpochDurationSeconds is changed, update `lambda` and `expLamSubOne` in ./reward/reward_logic.go
const EpochDurationSeconds = 45
const SecondsInHour = 3600
const SecondsInDay = 86400
const SecondsInYear = 31556925
const EpochsInHour = SecondsInHour / EpochDurationSeconds
const EpochsInDay = SecondsInDay / EpochDurationSeconds
const EpochsInYear = SecondsInYear / EpochDurationSeconds

// Filecoin Parameter: Expected number of block quality in an epoch (e.g. 1 block with block quality 5, or 5 blocks with quality 1)
// Motivation: It ensures that there is enough on-chain throughput
// Usage: It is used to calculate the block reward.
var ExpectedLeadersPerEpoch = int64(5)

func init() {
	//noinspection GoBoolExpressions
	if SecondsInHour%EpochDurationSeconds != 0 {
		// This even division is an assumption that other code might unwittingly make.
		// Don't rely on it on purpose, though.
		// While we're pretty sure everything will still work fine, we're safer maintaining this invariant anyway.
		panic(fmt.Sprintf("epoch duration %d does not evenly divide one hour (%d)", EpochDurationSeconds, SecondsInHour))
	}
}
