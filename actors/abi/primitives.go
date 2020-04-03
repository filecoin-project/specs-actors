package abi

import (
	"strconv"

	"github.com/filecoin-project/specs-actors/actors/abi/big"
)

// The abi package contains definitions of all types that cross the VM boundary and are used
// within actor code.
//
// Primitive types include numerics and opaque array types.

// Epoch number of the chain state, which acts as a proxy for time within the VM.
type ChainEpoch int64

func (e ChainEpoch) String() string {
	return strconv.FormatInt(int64(e), 10)
}

// A sequential number assigned to an actor when created by the InitActor.
// This ID is embedded in ID-type addresses.
type ActorID uint64

func (e ActorID) String() string {
	return strconv.FormatInt(int64(e), 10)
}

// MethodNum is an integer that represents a particular method
// in an actor's function table. These numbers are used to compress
// invocation of actor code, and to decouple human language concerns
// about method names from the ability to uniquely refer to a particular
// method.
//
// Consider MethodNum numbers to be similar in concerns as for
// offsets in function tables (in programming languages), and for
// tags in ProtocolBuffer fields. Tags in ProtocolBuffers recommend
// assigning a unique tag to a field and never reusing that tag.
// If a field is no longer used, the field name may change but should
// still remain defined in the code to ensure the tag number is not
// reused accidentally. The same should apply to the MethodNum
// associated with methods in Filecoin VM Actors.
type MethodNum uint64

func (e MethodNum) String() string {
	return strconv.FormatInt(int64(e), 10)
}

// TokenAmount is an amount of Filecoin tokens. This type is used within
// the VM in message execution, to account movement of tokens, payment
// of VM gas, and more.
//
// BigInt types are aliases rather than new types because the latter introduce incredible amounts of noise converting to
// and from types in order to manipulate values. We give up some type safety for ergonomics.
type TokenAmount = big.Int

func NewTokenAmount(t int64) TokenAmount {
	return big.NewInt(t)
}

// Randomness is a string of random bytes
type Randomness []byte

type LockedFund struct {
	VestingFunction
	CliffStartEpoch      ChainEpoch // StartEpoch of the Un
	FullyVestedEpoch        ChainEpoch
	Value           TokenAmount
	AmountWithdrawn TokenAmount
	AmountSlashed TokenAmount
}

type VestingFunction int64

const (
	Linear VestingFunction = iota
	Other
)

// AmountVested returns the `TokenAmount` value of funds vested in the reward at an epoch
func (r *LockedFund) AmountVested(currEpoch abi.ChainEpoch) abi.TokenAmount {
	switch r.VestingFunction {
	case None:
		return r.Value
	case Linear:
		elapsed := currEpoch - r.StartEpoch
		vestDuration := r.EndEpoch - r.StartEpoch
		if elapsed >= vestDuration {
			return r.Value
		}

		// (totalReward * elapsedEpoch) / vestDuration
		// Division must be done last to avoid precision loss with integer values
		return big.Div(big.Mul(r.Value, big.NewInt(int64(elapsed))), big.NewInt(int64(vestDuration)))
	default:
		return abi.NewTokenAmount(0)
	}
}


