package account

import (
	addr "github.com/filecoin-project/go-address"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type AccountActor struct{}

func (a AccountActor) Exports() []interface{} {
	return []interface{}{
		1: a.Constructor,
		2: a.PubkeyAddress,
	}
}

var _ abi.Invokee = AccountActor{}

type AccountActorState struct {
	Address addr.Address
}

func (a AccountActor) Constructor(rt vmr.Runtime, address *addr.Address) *adt.EmptyValue {
	// Account actors are created implicitly by sending a message to a pubkey-style address.
	// This constructor is not invoked by the InitActor, but by the system.
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	switch address.Protocol() {
	case addr.SECP256K1:
	case addr.BLS:
		break // ok
	default:
		rt.Abort(exitcode.ErrIllegalArgument, "address must use BLS or SECP protocol, got %v", address.Protocol())
	}
	rt.State().Construct(func() vmr.CBORMarshaler {
		st := AccountActorState{Address: *address}
		return &st
	})
	return &adt.EmptyValue{}
}

// Fetches the pubkey-type address from this actor.
func (a AccountActor) PubkeyAddress(rt vmr.Runtime, _ *adt.EmptyValue) addr.Address {
	var st AccountActorState
	rt.State().Readonly(&st)
	return st.Address
}
