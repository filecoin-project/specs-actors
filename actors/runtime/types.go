package runtime

import (
	runtime0 "github.com/filecoin-project/specs-actors/actors/runtime"
)

// Concrete types associated with the runtime interface.

// Enumeration of network upgrades where actor behaviour can change (without necessarily
// vendoring and versioning the whole actor codebase).
type NetworkVersion uint // FIXME move to types

const (
	NetworkVersion0 = NetworkVersion(iota) // specs-actors v0.9.3
	NetworkVersion1                        // specs-actors v0.9.7
	NetworkVersion2                        // specs-actors v2.0.?

	NetworkVersionLatest = NetworkVersion2
)

// Specifies importance of message, LogLevel numbering is consistent with the uber-go/zap package.
type LogLevel = runtime0.LogLevel

const (
	// DebugLevel logs are typically voluminous, and are usually disabled in production.
	DEBUG = runtime0.DEBUG
	// InfoLevel is the default logging priority.
	INFO = runtime0.INFO
	// WarnLevel logs are more important than Info, but don't need individual
	// human review.
	WARN = runtime0.WARN
	// ErrorLevel logs are high-priority. If an application is running smoothly,
	// it shouldn't generate any error-level logs.
	ERROR = runtime0.WARN
)

// Result of checking two headers for a consensus fault.
type ConsensusFault = runtime0.ConsensusFault

//type ConsensusFault struct {
//	// Address of the miner at fault (always an ID address).
//	Target addr.Address
//	// Epoch of the fault, which is the higher epoch of the two blocks causing it.
//	Epoch abi.ChainEpoch
//	// Type of fault.
//	Type ConsensusFaultType
//}

type ConsensusFaultType = runtime0.ConsensusFaultType

const (
	ConsensusFaultDoubleForkMining = runtime0.ConsensusFaultDoubleForkMining
	ConsensusFaultParentGrinding   = runtime0.ConsensusFaultParentGrinding
	ConsensusFaultTimeOffsetMining = runtime0.ConsensusFaultTimeOffsetMining
)
