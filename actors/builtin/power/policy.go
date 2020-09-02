package power

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
)

// The number of miners that must meet the consensus minimum miner power before that minimum power is enforced
// as a condition of leader election.
// This ensures a network still functions before any miners reach that threshold.
const ConsensusMinerMinMiners = 4 // PARAM_SPEC

// The minimum power of an individual miner to meet the threshold for leader election (in bytes).
// Motivation:
// - Limits sybil generation
// - Improves consensus fault detection
// - Guarantees a minimum fee for consensus faults
// - Ensures that a specific soundness for the power table
// Note: We may be able to reduce this in the future, addressing consensus faults with more complicated penalties,
// sybil generation with crypto-economic mechanism, and PoSt soundness by increasing the challenges for small miners.
var ConsensusMinerMinPower = abi.NewStoragePower(100 << 40) // PARAM_SPEC PARAM_FINISH

// Maximum number of prove-commits each miner can submit in one epoch.
//
// This limits the number of proof partitions we may need to load in the cron call path.
// Onboarding 1EiB/year requires at least 32 prove-commits per epoch.
const MaxMinerProveCommitsPerEpoch = 200 // PARAM_SPEC
