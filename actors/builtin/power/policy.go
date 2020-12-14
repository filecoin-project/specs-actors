package power

// The number of miners that must meet the consensus minimum miner power before that minimum power is enforced
// as a condition of leader election.
// This ensures a network still functions before any miners reach that threshold.
const ConsensusMinerMinMiners = 4 // PARAM_SPEC

// Maximum number of prove-commits each miner can submit in one epoch.
//
// This limits the number of proof partitions we may need to load in the cron call path.
// Onboarding 1EiB/year requires at least 32 prove-commits per epoch.
const MaxMinerProveCommitsPerEpoch = 200 // PARAM_SPEC

// Bitwidth of CronEventQueue HAMT determined empirically from mutation
// patterns and projections of mainnet data.
const CronQueueHamtBitwidth = 6

// Bitwidth of CronEventQueue AMT determined empirically from mutation
// patterns and projections of mainnet data.
const CronQueueAmtBitwidth = 6

// Bitwidth of ProofValidationBatch AMT determined empirically from mutation
// pattersn and projections of mainnet data.
const ProofValidationBatchAmtBitwidth = 4
