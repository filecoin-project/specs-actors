package cron

import (
	"io"

	addr "github.com/filecoin-project/go-address"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

type CronActorState struct {
	Entries []CronTableEntry
}

type CronActor struct {
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

	var st CronActorState
	rt.State().Readonly(&st)
	// st.Entries is basically a static registry for now, loaded
	// in the interpreter static registry.
	for _, entry := range st.Entries {
		_, _ = rt.Send(entry.ToAddr, entry.MethodNum, nil, abi.NewTokenAmount(0))
		// Any error and return value are ignored.
	}

	return &adt.EmptyValue{}
}

func (st *CronActorState) MarshalCBOR(w io.Writer) error {
	panic("replace with cbor-gen")
}

func (st *CronActorState) UnmarshalCBOR(r io.Reader) error {
	panic("replace with cbor-gen")
}
