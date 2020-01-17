package account

import (
	addr "github.com/filecoin-project/go-address"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
)

type InvocOutput = vmr.InvocOutput

type AccountActor struct{}

func (a *AccountActor) Constructor(rt vmr.Runtime) InvocOutput {
	// TODO: set the pubkey address here from parameters
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	return InvocOutput{}
}

type AccountActorState struct {
	Address addr.Address
}