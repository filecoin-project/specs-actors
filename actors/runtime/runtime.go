package runtime

import (
	"context"
	"io"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	crypto "github.com/filecoin-project/specs-actors/actors/crypto"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	indices "github.com/filecoin-project/specs-actors/actors/runtime/indices"
)

// Runtime is the VM's internal runtime object.
// this is everything that is accessible to actors, beyond parameters.
type Runtime interface {
	// The current chain epoch number. The genesis block has epoch zero.
	CurrEpoch() abi.ChainEpoch

	// The address of the actor receiving the message. Always an ID-address.
	CurrReceiver() addr.Address

	// The address of the immediate calling actor. Always an ID-address.
	ImmediateCaller() addr.Address

	// Validates the caller against some predicate.
	// Exported actor methods must invoke at least one caller validation before returning.
	ValidateImmediateCallerAcceptAny()
	ValidateImmediateCallerIs(addrs ...addr.Address)
	ValidateImmediateCallerType(types ...cid.Cid)

	// The actor who mined the block in which the current invocation is being evaluated.
	// Always an ID-address.
	ToplevelBlockWinner() addr.Address

	// The balance of the receiver.
	CurrentBalance() abi.TokenAmount

	// The value attached to the message being processed, implicitly added to CurrentBalance() before method invocation.
	ValueReceived() abi.TokenAmount

	// Look up the code ID at an actor address.
	GetActorCodeID(addr addr.Address) (ret cid.Cid, ok bool)

	// Randomness returns a (pseudo)random string for the given epoch and tag.
	GetRandomness(epoch abi.ChainEpoch) abi.RandomnessSeed

	// Provides a handle for the actor's state object.
	State() StateHandle

	// Retrieves and deserializes an object from the store into o. Returns whether successful.
	IpldGet(c cid.Cid, o CBORUnmarshalable) bool
	// Serializes and stores an object, returning its CID.
	IpldPut(x CBORMarshalable) cid.Cid

	// Sends a message to another actor, returning the exit code and return value envelope.
	// If the invoked method does not return successfully, this caller will be aborted too.
	Send(toAddr addr.Address, methodNum abi.MethodNum, params abi.MethodParams, value abi.TokenAmount) (SendReturn, exitcode.ExitCode)

	// Halts execution upon an error from which the actor cannot recover. This method does not return.
	// State changes will be rolled back, including any made by the caller.
	// The message and args are for diagnostic purposes and do not persist on chain. They should be suitable for
	// passing to fmt.Errorf(msg, args...).
	Abort(errExitCode exitcode.ExitCode, msg string, args ...interface{})

	// Calls Abort with InconsistentState_User.
	// TODO: replace all call sites with Abort(exitcode, msg, ...)
	AbortStateMsg(msg string)

	// Computes an address for a new actor. The returned address is intended to uniquely refer to
	// the actor even in the event of a chain re-org (whereas an ID-address might refer to a
	// different actor after messages are re-ordered).
	// Always an ActorExec address.
	NewActorAddress() addr.Address

	// Creates an actor with code `codeID` and address `address`, with empty state. May only be called by InitActor.
	CreateActor(codeId cid.Cid, address addr.Address)

	// Deletes an actor in the state tree. May only be called by the actor itself,
	// or by StoragePowerActor in the case of StorageMinerActors.
	DeleteActor(address addr.Address)

	// Look up the current values of several system-wide economic indices.
	CurrIndices() indices.Indices

	// Provides the system call interface.
	Syscalls() Syscalls

	// Provides a Go context for use by HAMT, etc.
	// The VM is intended to provide an idealised machine abstraction, with infinite storage etc, so this context
	// should not be used by actor code directly.
	Context() context.Context

	// Starts a new tracing span. The span must be End()ed explicitly, typically with a deferred invocation.
	StartSpan(name string) TraceSpan
}

// Pure functions implemented as primitives by the runtime.
type Syscalls interface {
	// Verifies that a signature is valid for an address and plaintext.
	VerifySignature(signature crypto.Signature, signer addr.Address, plaintext []byte) bool
	// Computes an unsealed sector CID (CommD) from its constituent piece CIDs (CommPs) and sizes.
	ComputeUnsealedSectorCID(sectorSize abi.SectorSize, pieces []abi.PieceInfo) (abi.UnsealedSectorCID, error)
	// Verifies a sector seal proof.
	VerifySeal(sectorSize abi.SectorSize, vi abi.SealVerifyInfo) bool
	// Verifies a proof of spacetime.
	VerifyPoSt(sectorSize abi.SectorSize, vi abi.PoStVerifyInfo) bool
	// Verifies valid consensus fault committed with two block headers
	VerifyConsensusFault(h1, h2 []byte) bool
}

// The return type from a message send from one actor to another. This abstracts over the internal representation of
// the return, in particular whether it has been serialized to bytes or just passed through.
// Production code is expected to de/serialize, but test and other code may pass the value straight through.
type SendReturn interface {
	Into(CBORUnmarshalable) error
}

// Provides (minimal) tracing facilities to actor code.
type TraceSpan interface {
	// Ends the span
	End()
}

// ReadonlyStateHandle provides read-only access to an actor's state object.
type ReadonlyStateHandle interface {
	// Readonly loads a readonly copy of the state into the argument.
	//
	// Any modification to the state is illegal and will result in an abort.
	Readonly(obj CBORMarshalable)
}

// StateHandle provides mutable, exclusive access to actor state.
type StateHandle interface {
	ReadonlyStateHandle

	// Transaction loads a mutable version of the state into the `obj` argument and protects
	// the execution from side effects (including message send).
	//
	// The second argument is a function which allows the caller to mutate the state.
	// The return value from that function will be returned from the call to Transaction().
	//
	// If the state is modified after this function returns, execution will abort.
	//
	// The gas cost of this method is that of an IpldPut of the mutated state object.
	//
	// Note: the Go signature is not ideal due to lack of type system power.
	//
	// # Usage
	// ```go
	// var state SomeState
	// ret := rt.State().Transaction(&state, func() (interface{}) {
	//   // make some changes
	//	 st.ImLoaded = True
	//   return st.Thing, nil
	// })
	// // state.ImLoaded = False // BAD!! state is readonly outside the lambda, it will panic
	// ```
	Transaction(obj CBORMarshalable, f func() interface{}) interface{}
}

// These interfaces are intended to match those from whyrusleeping/cbor-gen, such that code generated from that
// system is automatically usable here (but not mandatory).
type CBORMarshalable interface {
	MarshalCBOR(w io.Writer) error
}

type CBORUnmarshalable interface {
	UnmarshalCBOR(r io.Reader) error
}
