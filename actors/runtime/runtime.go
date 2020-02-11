package runtime

import (
	"context"
	"io"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	crypto "github.com/filecoin-project/specs-actors/actors/crypto"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
)

// Runtime is the VM's internal runtime object.
// this is everything that is accessible to actors, beyond parameters.
type Runtime interface {
	// Information related to the current message being executed.
	Message() Message

	// The network name, e.g. "mainnet".
	NetworkName() string

	// The current chain epoch number. The genesis block has epoch zero.
	CurrEpoch() abi.ChainEpoch

	// Validates the caller against some predicate.
	// Exported actor methods must invoke at least one caller validation before returning.
	ValidateImmediateCallerAcceptAny()
	ValidateImmediateCallerIs(addrs ...addr.Address)
	ValidateImmediateCallerType(types ...cid.Cid)

	// The balance of the receiver.
	CurrentBalance() abi.TokenAmount

	// Look up the code ID at an actor address.
	GetActorCodeCID(addr addr.Address) (ret cid.Cid, ok bool)

	// Randomness returns a (pseudo)random string for the given epoch and tag.
	GetRandomness(epoch abi.ChainEpoch) abi.RandomnessSeed

	// Provides a handle for the actor's state object.
	State() StateHandle

	Store() Store

	// Sends a message to another actor, returning the exit code and return value envelope.
	// If the invoked method does not return successfully, its state changes (and that of any messages it sent in turn)
	// will be rolled back.
	Send(toAddr addr.Address, methodNum abi.MethodNum, params CBORMarshaler, value abi.TokenAmount) (SendReturn, exitcode.ExitCode)

	// Halts execution upon an error from which the receiver cannot recover. The caller will receive the exitcode and
	// an empty return value. State changes made within this call will be rolled back.
	// This method does not return.
	// The message and args are for diagnostic purposes and do not persist on chain. They should be suitable for
	// passing to fmt.Errorf(msg, args...).
	Abortf(errExitCode exitcode.ExitCode, msg string, args ...interface{})

	// Computes an address for a new actor. The returned address is intended to uniquely refer to
	// the actor even in the event of a chain re-org (whereas an ID-address might refer to a
	// different actor after messages are re-ordered).
	// Always an ActorExec address.
	NewActorAddress() addr.Address

	// Creates an actor with code `codeID` and address `address`, with empty state. May only be called by InitActor.
	CreateActor(codeId cid.Cid, address addr.Address)

	// Deletes the executing actor from the state tree. May only be called by the actor itself.
	DeleteActor()

	// Provides the system call interface.
	Syscalls() Syscalls

	// Provides a Go context for use by HAMT, etc.
	// The VM is intended to provide an idealised machine abstraction, with infinite storage etc, so this context
	// should not be used by actor code directly.
	Context() context.Context

	// Starts a new tracing span. The span must be End()ed explicitly, typically with a deferred invocation.
	StartSpan(name string) TraceSpan
}

// Store defines the storage module exposed to actors.
type Store interface {
	// Retrieves and deserializes an object from the store into `o`. Returns whether successful.
	Get(c cid.Cid, o CBORUnmarshaler) bool
	// Serializes and stores an object, returning its CID.
	Put(x CBORMarshaler) cid.Cid
}

// Message contains information available to the actor about the executing message.
type Message interface {
	// The actor who mined the block in which the current method was included.
	// Always an ID-address.
	BlockMiner() addr.Address

	// The address of the immediate calling actor. Always an ID-address.
	Caller() addr.Address

	// The address of the actor receiving the message. Always an ID-address.
	Receiver() addr.Address

	// The value attached to the message being processed, implicitly added to CurrentBalance() before method invocation.
	ValueReceived() abi.TokenAmount
}

// Pure functions implemented as primitives by the runtime.
type Syscalls interface {
	// Verifies that a signature is valid for an address and plaintext.
	VerifySignature(signature crypto.Signature, signer addr.Address, plaintext []byte) bool
	// Hashes input data using SHA256.
	Hash_SHA256(data []byte) []byte
	// Computes an unsealed sector CID (CommD) from its constituent piece CIDs (CommPs) and sizes.
	ComputeUnsealedSectorCID(sectorSize abi.SectorSize, pieces []abi.PieceInfo) (cid.Cid, error)
	// Verifies a sector seal proof.
	VerifySeal(sectorSize abi.SectorSize, vi abi.SealVerifyInfo) bool
	// Verifies a proof of spacetime.
	VerifyPoSt(sectorSize abi.SectorSize, vi abi.PoStVerifyInfo) bool
	// Verifies valid consensus fault committed with two block headers:
	// - both headers mined by the same actor
	// - headers are different
	// - first header is of the same or lower epoch as the second
	// - at least one of the headers appears in the current chain.
	VerifyConsensusFault(h1, h2 []byte) bool
}

// The return type from a message send from one actor to another. This abstracts over the internal representation of
// the return, in particular whether it has been serialized to bytes or just passed through.
// Production code is expected to de/serialize, but test and other code may pass the value straight through.
type SendReturn interface {
	Into(CBORUnmarshaler) error
}

// Provides (minimal) tracing facilities to actor code.
type TraceSpan interface {
	// Ends the span
	End()
}

// StateHandle provides mutable, exclusive access to actor state.
type StateHandle interface {
	// Create initializes the state object.
	// This is only valid in a constructor function and when the state has not yet been initialized.
	Create(obj CBORMarshaler)

	// Readonly loads a readonly copy of the state into the argument.
	//
	// Any modification to the state is illegal and will result in an abort.
	Readonly(obj CBORUnmarshaler)

	// Transaction loads a mutable version of the state into the `obj` argument and protects
	// the execution from side effects (including message send).
	//
	// The second argument is a function which allows the caller to mutate the state.
	// The return value from that function will be returned from the call to Transaction().
	//
	// If the state is modified after this function returns, execution will abort.
	//
	// The gas cost of this method is that of a Store.Put of the mutated state object.
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
	Transaction(obj CBORer, f func() interface{}) interface{}
}

// These interfaces are intended to match those from whyrusleeping/cbor-gen, such that code generated from that
// system is automatically usable here (but not mandatory).
type CBORMarshaler interface {
	MarshalCBOR(w io.Writer) error
}

type CBORUnmarshaler interface {
	UnmarshalCBOR(r io.Reader) error
}

type CBORer interface {
	CBORMarshaler
	CBORUnmarshaler
}

// Wraps already-serialized bytes as CBOR-marshalable.
type CBORBytes []byte

func (b CBORBytes) MarshalCBOR(w io.Writer) error {
	_, err := w.Write(b)
	return err
}
