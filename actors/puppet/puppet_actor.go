package puppet

import (
	addr "github.com/filecoin-project/go-address"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	runtime "github.com/filecoin-project/specs-actors/actors/runtime"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type Actor struct{}

func (a Actor) Exports() []interface{} {
	return []interface{}{
		builtin.MethodConstructor: a.Constructor,
		2:                         a.Send,
	}
}

var _ abi.Invokee = Actor{}

func (a Actor) Constructor(rt runtime.Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)

	rt.State().Create(&State{})
	return nil
}

type SendParams struct {
	To     addr.Address
	Value  abi.TokenAmount
	Method abi.MethodNum
	Params []byte
}

func (a Actor) Send(rt runtime.Runtime, params *SendParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerAcceptAny()
	_, _ = rt.Send(
		params.To,
		params.Method,
		runtime.CBORBytes(params.Params),
		params.Value,
	)
	return nil
}

type State struct{}
