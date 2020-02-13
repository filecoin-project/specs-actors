package crypto

import (
	addr "github.com/filecoin-project/go-address"
	abi "github.com/filecoin-project/specs-actors/actors/abi"
)

// Specifies a domain for randomness generation.
type DomainSeparationTag int

const (
	DomainSeparationTag_TicketProduction DomainSeparationTag = 1 + iota
	DomainSeparationTag_ElectionPoStChallengeSeed
	DomainSeparationTag_WindowedPoStChallengeSeed
)

type AddressEpochEntropy struct {
	MinerAddress addr.Address // Must be an ID-addr
	Epoch        abi.ChainEpoch
}
