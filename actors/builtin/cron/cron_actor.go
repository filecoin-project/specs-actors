package cron

import (
	addr "github.com/filecoin-project/go-address"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

type CronActorState struct{}

type CronActor struct {
	// TODO move Entries into the CronActorState struct
	Entries []CronTableEntry
}

type CronTableEntry struct {
	ToAddr    addr.Address
	MethodNum abi.MethodNum
}

func (a *CronActor) Constructor(rt vmr.Runtime) *adt.EmptyValue {
	// Nothing. intentionally left blank.
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	return &adt.EmptyValue{}
}

func (a *CronActor) EpochTick(rt vmr.Runtime) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)

	// a.Entries is basically a static registry for now, loaded
	// in the interpreter static registry.
	for _, entry := range a.Entries {
		_, _ = rt.Send(entry.ToAddr, entry.MethodNum, nil, abi.NewTokenAmount(0))
		// Any error and return value are ignored.
	}

	return &adt.EmptyValue{}
}
