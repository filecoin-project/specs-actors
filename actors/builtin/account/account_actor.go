package account

import (
	addr "github.com/filecoin-project/go-address"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
)

type AccountActor struct{}

func (a *AccountActor) Constructor(rt vmr.Runtime) *vmr.EmptyReturn {
	// TODO: set the pubkey address here from parameters
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	return &vmr.EmptyReturn{}
}

type AccountActorState struct {
	Address addr.Address
}
