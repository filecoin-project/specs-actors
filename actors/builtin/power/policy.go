package power

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
)

// PARAM_SPEC
// This is a switch for turning on the minimum miner size requirement.
// When this number of miners of minimum miner size is reached, minimum miner size is enforce.
const ConsensusMinerMinMiners = 4

// Minimum power of an individual miner to meet the threshold for leader election.
var ConsensusMinerMinPower = abi.NewStoragePower(1 << 40) // PARAM_FINISH

// Maximum number of prove commits a miner can submit in one epoch
const MaxMinerProveCommitsPerEpoch = 8000
