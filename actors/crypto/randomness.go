package crypto

// Specifies a domain for randomness generation.
type DomainSeparationTag int

const (
	DomainSeparationTag_TicketProduction DomainSeparationTag = 1 + iota
	DomainSeparationTag_ElectionPoStChallengeSeed
	DomainSeparationTag_WindowedPoStChallengeSeed
	DomainSeparationTag_SealRandomness
	DomainSeparationTag_InteractiveSealChallengeSeed
)
