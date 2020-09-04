package cron

import (
	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/runtime"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

// The cron actor is a built-in singleton that sends messages to other registered actors at the end of each epoch.
type Actor struct{}

func (a Actor) Exports() []interface{} {
	return []interface{}{
		builtin.MethodConstructor: a.Constructor,
		2:                         a.EpochTick,
	}
}

var _ runtime.Invokee = Actor{}

type ConstructorParams struct {
	Entries []Entry
}

func (a Actor) Constructor(rt runtime.Runtime, params *ConstructorParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	rt.State().Create(ConstructState(params.Entries))
	return nil
}

// Invoked by the system after all other messages in the epoch have been processed.
func (a Actor) EpochTick(rt runtime.Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)

	var st State
	rt.State().Readonly(&st)
	for _, entry := range st.Entries {
		_, _ = rt.Send(entry.Receiver, entry.MethodNum, nil, abi.NewTokenAmount(0))
		// Any error and return value are ignored.
	}

	return nil
}
