package runtime

import (
	"github.com/filecoin-project/go-state-types/rt"
	runtime2 "github.com/filecoin-project/specs-actors/v2/actors/runtime"
)

// Concrete types associated with the runtime interface.

// Result of checking two headers for a consensus fault.
type ConsensusFault = runtime2.ConsensusFault

//type ConsensusFault struct {
//	// Address of the miner at fault (always an ID address).
//	Target addr.Address
//	// Epoch of the fault, which is the higher epoch of the two blocks causing it.
//	Epoch abi.ChainEpoch
//	// Type of fault.
//	Type ConsensusFaultType
//}

type ConsensusFaultType = runtime2.ConsensusFaultType

const (
	ConsensusFaultDoubleForkMining = runtime2.ConsensusFaultDoubleForkMining
	ConsensusFaultParentGrinding   = runtime2.ConsensusFaultParentGrinding
	ConsensusFaultTimeOffsetMining = runtime2.ConsensusFaultTimeOffsetMining
)

type VMActor = rt.VMActor
