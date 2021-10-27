package power

type Policy struct {
	// The number of miners that must meet the consensus minimum miner power before that minimum power is enforced
	// as a condition of leader election.
	// This ensures a network still functions before any miners reach that threshold.
	ConsensusMinerMinMiners int64

	// Maximum number of prove-commits each miner can submit in one epoch.
	//
	// This limits the number of proof partitions we may need to load in the cron call path.
	// Onboarding 1EiB/year requires at least 32 prove-commits per epoch.
	MaxMinerProveCommitsPerEpoch int64
}

var DefaultPowerPolicy = Policy{
	4,
	200,
}

var CurrentPowerPolicy = DefaultPowerPolicy

func ConsensusMinerMinMiners() int64 {
	return CurrentPowerPolicy.ConsensusMinerMinMiners
}

func MaxMinerProveCommitsPerEpoch() int64 {
	return CurrentPowerPolicy.MaxMinerProveCommitsPerEpoch
}
