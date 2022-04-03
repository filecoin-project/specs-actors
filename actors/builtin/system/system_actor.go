package system

import (
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/specs-actors/v8/actors/builtin"
	"github.com/filecoin-project/specs-actors/v8/actors/runtime"
	"github.com/filecoin-project/specs-actors/v8/actors/util/adt"
)

type Actor struct{}

func (a Actor) Exports() []interface{} {
	return []interface{}{
		builtin.MethodConstructor: a.Constructor,
	}
}

func (a Actor) Code() cid.Cid {
	return builtin.SystemActorCodeID
}

func (a Actor) IsSingleton() bool {
	return true
}

func (a Actor) State() cbor.Er {
	return new(State)
}

var _ runtime.VMActor = Actor{}

func (a Actor) Constructor(rt runtime.Runtime, _ *abi.EmptyValue) *abi.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	st, err := ConstructState(adt.AsStore(rt))
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to construct state")
	rt.StateCreate(st)
	return nil
}
