package init

import (
	"io"

	addr "github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	autil "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type Runtime = vmr.Runtime

var AssertMsg = autil.AssertMsg

type InitActorState struct {
	// responsible for create new actors
	AddressMap  map[addr.Address]abi.ActorID
	NextID      abi.ActorID
	NetworkName string
}

func (s *InitActorState) ResolveAddress(address addr.Address) addr.Address {
	actorID, ok := s.AddressMap[address]
	if ok {
		idAddr, err := addr.NewIDAddress(uint64(actorID))
		autil.Assert(err == nil)
		return idAddr
	}
	return address
}

func (s *InitActorState) MapAddressToNewID(address addr.Address) addr.Address {
	actorID := s.NextID
	s.NextID++
	s.AddressMap[address] = actorID
	idAddr, err := addr.NewIDAddress(uint64(actorID))
	autil.Assert(err == nil)
	return idAddr
}

type InitActor struct{}

func (a *InitActor) Constructor(rt Runtime) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	var st InitActorState
	rt.State().Transaction(&st, func() interface{} {
		st.AddressMap = map[addr.Address]abi.ActorID{} // TODO HAMT
		st.NextID = abi.ActorID(builtin.FirstNonSingletonActorId)
		st.NetworkName = rt.NetworkName()
		return nil
	})
	return &adt.EmptyValue{}
}

type ExecReturn struct {
	IDAddress     addr.Address // The canonical ID-based address for the actor.
	RobustAddress addr.Address // A more expensive but re-org-safe address for the newly created actor.
}

func (e ExecReturn) UnmarshalCBOR(r io.Reader) error {
	panic("replace with cbor-gen")
}

func (a *InitActor) Exec(rt Runtime, execCodeID cid.Cid, constructorParams abi.MethodParams) *ExecReturn {
	rt.ValidateImmediateCallerAcceptAny()
	callerCodeID, ok := rt.GetActorCodeID(rt.ImmediateCaller())
	AssertMsg(ok, "no code for actor at %s", rt.ImmediateCaller())
	if !_codeIDSupportsExec(callerCodeID, execCodeID) {
		rt.Abort(exitcode.ErrForbidden, "Caller type %v cannot create an actor of type %v", callerCodeID, execCodeID)
	}

	// Compute a re-org-stable address.
	// This address exists for use by messages coming from outside the system, in order to
	// stably address the newly created actor even if a chain re-org causes it to end up with
	// a different ID.
	uniqueAddress := rt.NewActorAddress()

	// Allocate an ID for this actor.
	// Store mapping of pubkey or actor address to actor ID
	var idAddr addr.Address
	var st InitActorState
	rt.State().Transaction(&st, func() interface{} {
		idAddr = st.MapAddressToNewID(uniqueAddress)
		return nil
	})

	// Create an empty actor.
	rt.CreateActor(execCodeID, idAddr)

	// Invoke constructor.
	_, code := rt.Send(idAddr, builtin.MethodConstructor, constructorParams, rt.ValueReceived())
	builtin.RequireSuccess(rt, code, "constructor failed")

	return &ExecReturn{idAddr, uniqueAddress}
}

func _codeIDSupportsExec(callerCodeID cid.Cid, execCodeID cid.Cid) bool {
	if execCodeID == builtin.AccountActorCodeID {
		// Special case: account actors must be created implicitly by sending value;
		// cannot be created via exec.
		return false
	}

	if execCodeID == builtin.PaymentChannelActorCodeID {
		return true
	}

	if execCodeID == builtin.StorageMinerActorCodeID {
		if callerCodeID == builtin.StoragePowerActorCodeID {
			return true
		}
	}

	return false
}

func (s *InitActorState) MarshalCBOR(w io.Writer) error {
	panic("replace with cbor-gen")
}

func (s *InitActorState) UnmarshalCBOR(r io.Reader) error {
	panic("replace with cbor-gen")
}
