package account

import (
	addr "github.com/filecoin-project/go-address"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type AccountActor struct{}

func (a *AccountActor) Constructor(rt vmr.Runtime) *adt.EmptyValue {
	// TODO anorth/hs: set the pubkey address here from parameters
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	return &adt.EmptyValue{}
}

type AccountActorState struct {
	Address addr.Address
}
