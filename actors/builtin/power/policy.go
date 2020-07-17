package power

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
)

// PARAM_SPEC
// This is a switch for turning on the minimum miner size requirement.
// When this number of miners of minimum miner size is reached, minimum miner size is enforce.
const ConsensusMinerMinMiners = 4

// PARAM_SPEC
// Minimum miner size, the minimum power of an individual miner to meet the threshold for leader election (in bytes).
// Motivation:
// - Limits sybil generation
// - Improves consensus fault detection
// - Guarantees a minimum fee for consensus faults
// - Ensures that a specific soundness for the power table
// Future: we can consensus fault fee and sybil generation with crypto econ mechanic and we can mantain the target soundness by increasing the challenges for small miners.
var ConsensusMinerMinPower = abi.NewStoragePower(100 << 40) // PARAM_FINISH

// PARAM_SPEC
// Maximum number of prove commits a miner can submit in one epoch
//
// We bound this to 200 to limit the number of prove partitions we may need to update in a given epoch to 200.
//
// To support onboarding 1EiB/year, we need to allow at least 32 prove commits per epoch.
const MaxMinerProveCommitsPerEpoch = 200
