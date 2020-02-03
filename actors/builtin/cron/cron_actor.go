package cron

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

// The cron actor is a built-in singleton that sends messages to other registered actors at the end of each epoch.
type CronActor struct {
}

func (a CronActor) Exports() []interface{} {
	return []interface{}{
		builtin.MethodConstructor: a.Constructor,
		2: a.EpochTick,
	}
}

var _ abi.Invokee = CronActor{}

type ConstructorParams struct {
	Entries []CronTableEntry
}

func (a CronActor) Constructor(rt vmr.Runtime, params *ConstructorParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	rt.State().Construct(func() vmr.CBORMarshaler {
		return ConstructState(params.Entries)
	})
	return &adt.EmptyValue{}
}

// Invoked by the system after all other messages in the epoch have been processed.
func (a CronActor) EpochTick(rt vmr.Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)

	var st CronActorState
	rt.State().Readonly(&st)
	for _, entry := range st.Entries {
		_, _ = rt.Send(entry.Receiver, entry.MethodNum, adt.EmptyValue{}, abi.NewTokenAmount(0))
		// Any error and return value are ignored.
	}

	return &adt.EmptyValue{}
}
