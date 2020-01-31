package mock

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/runtime"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/actors/runtime/indices"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

// A mock runtime for unit testing of actors in isolation.
// The mock allows direct specification of the runtime context as observable by an actor, supports
// the storage interface, and mocks out side-effect-inducing calls.
type Runtime struct {
	// Execution context
	ctx           context.Context
	epoch         abi.ChainEpoch
	receiver      addr.Address
	caller        addr.Address
	callerType    cid.Cid
	miner         addr.Address
	valueReceived abi.TokenAmount

	// Actor state
	state   cid.Cid
	balance abi.TokenAmount

	// VM implementation
	inCall        bool
	store         map[cid.Cid][]byte
	inTransaction bool

	// Expectations
	t                        testing.TB
	expectValidateCallerAny  bool
	expectValidateCallerAddr []addr.Address
	expectValidateCallerType []cid.Cid
	expectSends              []*expectedMessage
}

var _ runtime.Runtime = &Runtime{}
var _ runtime.StateHandle = &Runtime{}
var typeOfRuntimeInterface = reflect.TypeOf((*runtime.Runtime)(nil)).Elem()
var typeOfCborUnmarshaler = reflect.TypeOf((*runtime.CBORUnmarshaler)(nil)).Elem()
var typeOfCborMarshaler = reflect.TypeOf((*runtime.CBORMarshaler)(nil)).Elem()

var cidBuilder = cid.V1Builder{
	Codec:    cid.DagCBOR,
	MhType:   mh.SHA2_256,
	MhLength: 0, // default
}

///// Implementation of the runtime API /////

func (rt *Runtime) NetworkName() string {
	rt.requireInCall()
	return "mock"
}

func (rt *Runtime) CurrEpoch() abi.ChainEpoch {
	rt.requireInCall()
	return rt.epoch
}

func (rt *Runtime) CurrReceiver() addr.Address {
	rt.requireInCall()
	return rt.receiver
}

func (rt *Runtime) ImmediateCaller() addr.Address {
	rt.requireInCall()
	return rt.caller
}

func (rt *Runtime) ValidateImmediateCallerAcceptAny() {
	rt.requireInCall()
	if !rt.expectValidateCallerAny {
		rt.t.Fatalf("unexpected validate-caller-any")
	}
	rt.expectValidateCallerAny = false
}

func (rt *Runtime) ValidateImmediateCallerIs(addrs ...addr.Address) {
	rt.requireInCall()
	rt.checkArgument(len(addrs) > 0, "addrs must be non-empty")
	// Check and clear expectations.
	if len(rt.expectValidateCallerAddr) == 0 {
		rt.t.Errorf("unexpected validate caller addrs")
		return
	}
	if !reflect.DeepEqual(rt.expectValidateCallerAddr, addrs) {
		rt.t.Errorf("unexpected validate caller addrs %v, expected %v", addrs, rt.expectValidateCallerAddr)
		return
	}
	defer func() {
		rt.expectValidateCallerAddr = nil
	}()

	// Implement method.
	for _, expected := range addrs {
		if rt.caller == expected {
			return
		}
	}
	rt.Abort(exitcode.ErrForbidden, "caller address %v forbidden, allowed: %v", rt.caller, addrs)
}

func (rt *Runtime) ValidateImmediateCallerType(types ...cid.Cid) {
	rt.requireInCall()
	rt.checkArgument(len(types) > 0, "types must be non-empty")

	// Check and clear expectations.
	if len(rt.expectValidateCallerType) == 0 {
		rt.t.Errorf("unexpected validate caller code")
	}
	if !reflect.DeepEqual(rt.expectValidateCallerType, types) {
		rt.t.Errorf("unexpected validate caller code %v, expected %v", types, rt.expectValidateCallerType)
	}
	defer func() {
		rt.expectValidateCallerType = nil
	}()

	// Implement method.
	for _, expected := range types {
		if rt.callerType.Equals(expected) {
			return
		}
	}
	rt.Abort(exitcode.ErrForbidden, "caller type %v forbidden, allowed: %v", rt.callerType, types)
}

func (rt *Runtime) ToplevelBlockWinner() addr.Address {
	rt.requireInCall()
	return rt.miner
}

func (rt *Runtime) CurrentBalance() abi.TokenAmount {
	rt.requireInCall()
	return rt.balance
}

func (rt *Runtime) ValueReceived() abi.TokenAmount {
	rt.requireInCall()
	return rt.valueReceived
}

func (rt *Runtime) GetActorCodeID(addr addr.Address) (ret cid.Cid, ok bool) {
	rt.requireInCall()
	panic("implement me")
}

func (rt *Runtime) GetRandomness(epoch abi.ChainEpoch) abi.RandomnessSeed {
	rt.requireInCall()
	panic("implement me")
}

func (rt *Runtime) State() runtime.StateHandle {
	rt.requireInCall()
	return rt
}

func (rt *Runtime) IpldGet(c cid.Cid, o runtime.CBORUnmarshaler) bool {
	// requireInCall omitted because it makes using this mock runtime as a store awkward.
	data, found := rt.store[c]
	if found {
		err := o.UnmarshalCBOR(bytes.NewReader(data))
		if err != nil {
			rt.Abort(exitcode.SysErrSerialization, err.Error())
		}
	}
	return found
}

func (rt *Runtime) IpldPut(o runtime.CBORMarshaler) cid.Cid {
	// requireInCall omitted because it makes using this mock runtime as a store awkward.
	if !rt.inTransaction {
		rt.Abort(exitcode.SysErrorIllegalActor, "store put outside transaction")
	}

	r := bytes.Buffer{}
	err := o.MarshalCBOR(&r)
	if err != nil {
		rt.Abort(exitcode.SysErrSerialization, err.Error())
	}
	data := r.Bytes()
	key, err := cidBuilder.Sum(data)
	if err != nil {
		rt.Abort(exitcode.SysErrSerialization, err.Error())
	}
	rt.store[key] = data
	return key
}

func (rt *Runtime) Send(toAddr addr.Address, methodNum abi.MethodNum, params runtime.CBORMarshaler, value abi.TokenAmount) (runtime.SendReturn, exitcode.ExitCode) {
	rt.requireInCall()
	if rt.inTransaction {
		rt.Abort(exitcode.SysErrorIllegalActor, "side-effect within transaction")
	}
	if len(rt.expectSends) == 0 {
		rt.t.Fatalf("unexpected expectedMessage to: %v method: %v, value: %v, params: %v", toAddr, methodNum, value, params)
	}

	expectedMsg := rt.expectSends[0]

	if expectedMsg.Equal(toAddr, methodNum, params, value) {
		rt.t.Errorf("expectedMessage being sent does not match expectation. Message: to %v method: %v value: %v params: %v, Expected: %v", toAddr, methodNum, value, params, rt.expectSends[0].String())
	}
	// pop the expectedMessage from the queue
	defer func() {
		rt.expectSends = rt.expectSends[1:]
	}()
	return expectedMsg.sendReturn, expectedMsg.exitCode
}

func (rt *Runtime) NewActorAddress() addr.Address {
	rt.requireInCall()
	panic("implement me")
}

func (rt *Runtime) CreateActor(codeId cid.Cid, address addr.Address) {
	rt.requireInCall()
	if rt.inTransaction {
		rt.Abort(exitcode.SysErrorIllegalActor, "side-effect within transaction")
	}
	panic("implement me")
}

func (rt *Runtime) DeleteActor(address addr.Address) {
	rt.requireInCall()
	if rt.inTransaction {
		rt.Abort(exitcode.SysErrorIllegalActor, "side-effect within transaction")
	}
	panic("implement me")
}

func (rt *Runtime) CurrIndices() indices.Indices {
	rt.requireInCall()
	panic("implement me")
}

func (rt *Runtime) Abort(errExitCode exitcode.ExitCode, msg string, args ...interface{}) {
	rt.requireInCall()
	rt.t.Logf("Mock Runtime Abort ExitCode: %v Reason: %s", errExitCode, fmt.Sprintf(msg, args...))
	panic(abort{errExitCode, fmt.Sprintf(msg, args...)})
}

func (rt *Runtime) AbortStateMsg(msg string) {
	rt.requireInCall()
	rt.Abort(exitcode.ErrPlaceholder, msg)
}

func (rt *Runtime) Syscalls() runtime.Syscalls {
	rt.requireInCall()
	panic("implement me")
}

func (rt *Runtime) Context() context.Context {
	// requireInCall omitted because it makes using this mock runtime as a store awkward.
	return rt.ctx
}

func (rt *Runtime) StartSpan(name string) runtime.TraceSpan {
	rt.requireInCall()
	return &TraceSpan{}
}

func (rt *Runtime) checkArgument(predicate bool, msg string, args ...interface{}) {
	if !predicate {
		rt.Abort(exitcode.SysErrorIllegalArgument, msg, args...)
	}
}

///// State handle implementation /////

func (rt *Runtime) Construct(f func() runtime.CBORMarshaler) {
	if rt.state.Defined() {
		rt.Abort(exitcode.SysErrorIllegalActor, "state already constructed")
	}
	rt.inTransaction = true
	st := f()
	rt.state = rt.IpldPut(st)
	rt.inTransaction = false
}

func (rt *Runtime) Readonly(st runtime.CBORUnmarshaler) {
	found := rt.IpldGet(rt.state, st)
	if !found {
		rt.Abort(exitcode.SysErrInternal, "actor state not found: %v", rt.state)
	}
}

func (rt *Runtime) Transaction(st runtime.CBORer, f func() interface{}) interface{} {
	if rt.inTransaction {
		rt.Abort(exitcode.SysErrorIllegalActor, "nested transaction")
	}
	rt.Readonly(st)
	rt.inTransaction = true
	ret := f()
	rt.state = rt.IpldPut(st)
	rt.inTransaction = false
	return ret
}

///// Trace span implementation /////

type TraceSpan struct {
}

func (t TraceSpan) End() {
	// no-op
}

type abort struct {
	code exitcode.ExitCode
	msg  string
}

func (a abort) String() string {
	return fmt.Sprintf("abort(%v): %s", a.code, a.msg)
}

///// Inspection facilities /////

func (rt *Runtime) StateRoot() cid.Cid {
	return rt.state
}

func (rt *Runtime) GetState(o runtime.CBORUnmarshaler) {
	data, found := rt.store[rt.state]
	if !found {
		rt.t.Fatalf("can't find state at root %v", rt.state) // something internal is messed up
	}
	err := o.UnmarshalCBOR(bytes.NewReader(data))
	if err != nil {
		rt.t.Fatalf("error loading state: %v", err)
	}
}

func (rt *Runtime) Store() adt.Store {
	return adt.AsStore(rt)
}

///// Mocking facilities /////

type expectedMessage struct {
	// expectedMessage values
	to     addr.Address
	method abi.MethodNum
	params runtime.CBORMarshaler
	value  abi.TokenAmount

	// returns from applying expectedMessage
	sendReturn runtime.SendReturn
	exitCode   exitcode.ExitCode
}

func (m *expectedMessage) Equal(to addr.Address, method abi.MethodNum, params runtime.CBORMarshaler, value abi.TokenAmount) bool {
	return m.to == to && m.method == method && m.value == value && reflect.DeepEqual(m.params, params)
}

func (m *expectedMessage) String() string {
	return fmt.Sprintf("to: %v from: %v params: %v value: %v sendReturn: %v exitCode: %v", m.to.String(), m.method, m.params, m.value, m.sendReturn, m.exitCode)
}

func (rt *Runtime) SetCaller(address addr.Address, actorType cid.Cid) {
	rt.caller = address
	rt.callerType = actorType
}

func (rt *Runtime) SetBalance(amt abi.TokenAmount) {
	rt.balance = amt
}

func (rt *Runtime) SetReceived(amt abi.TokenAmount) {
	rt.valueReceived = amt
}

func (rt *Runtime) SetEpoch(epoch abi.ChainEpoch) {
	rt.epoch = epoch
}

func (rt *Runtime) ExpectValidateCallerAny() {
	rt.expectValidateCallerAny = true
}

func (rt *Runtime) ExpectValidateCallerAddr(addrs ...addr.Address) {
	rt.require(len(addrs) > 0, "addrs must be non-empty")
	rt.expectValidateCallerAddr = addrs[:]
}

func (rt *Runtime) ExpectValidateCallerType(types ...cid.Cid) {
	rt.require(len(types) > 0, "types must be non-empty")
	rt.expectValidateCallerType = types[:]
}

func (rt *Runtime) ExpectSend(toAddr addr.Address, methodNum abi.MethodNum, params runtime.CBORMarshaler, value abi.TokenAmount, sendReturn runtime.SendReturn, exitCode exitcode.ExitCode) {
	// append to the send queue
	rt.expectSends = append(rt.expectSends, &expectedMessage{
		to:         toAddr,
		method:     methodNum,
		params:     params,
		value:      value,
		sendReturn: sendReturn,
		exitCode:   exitCode,
	})
}

// Verifies that expected calls were received, and resets all expectations.
func (rt *Runtime) Verify() {
	if rt.expectValidateCallerAny {
		rt.t.Error("expected ValidateCallerAny, not received")
	}
	if len(rt.expectValidateCallerAddr) > 0 {
		rt.t.Errorf("expected ValidateCallerAddr %v, not received", rt.expectValidateCallerAddr)
	}
	if len(rt.expectValidateCallerType) > 0 {
		rt.t.Errorf("expected ValidateCallerType %v, not received", rt.expectValidateCallerType)
	}
	if len(rt.expectSends) > 0 {
		rt.t.Errorf("expected all message to be send, unsent messages %v", rt.expectSends)
	}

	rt.Reset()
}

// Resets expectations
func (rt *Runtime) Reset() {
	rt.expectValidateCallerAny = false
	rt.expectValidateCallerAddr = nil
	rt.expectValidateCallerType = nil
}

// Calls f() expecting it to invoke Runtime.Abort() with a specified exit code.
func (rt *Runtime) ExpectAbort(expected exitcode.ExitCode, f func()) {
	prevState := rt.state

	defer func() {
		r := recover()
		if r == nil {
			rt.t.Errorf("expected abort with code %v but call succeeded", expected)
			return
		}
		a, ok := r.(abort)
		if !ok {
			panic(r)
		}
		if a.code != expected {
			rt.t.Errorf("abort expected code %v, got %v %s", expected, a.code, a.msg)
		}
		// Roll back state change.
		rt.state = prevState
	}()
	f()
}

func (rt *Runtime) Call(method interface{}, params interface{}) interface{} {
	meth := reflect.ValueOf(method)
	rt.verifyExportedMethodType(meth)

	// There's no panic recovery here. If an abort is expected, this call will be inside an ExpectAbort block.
	// If not expected, the panic will escape and cause the test to fail.

	rt.inCall = true
	ret := meth.Call([]reflect.Value{reflect.ValueOf(rt), reflect.ValueOf(params)})
	rt.inCall = false
	return ret[0].Interface()
}

func (rt *Runtime) verifyExportedMethodType(meth reflect.Value) {
	t := meth.Type()
	rt.require(t.Kind() == reflect.Func, "%v is not a function", meth)
	rt.require(t.NumIn() == 2, "exported method %v must have two parameters, got %v", meth, t.NumIn())
	rt.require(t.In(0) == typeOfRuntimeInterface, "exported method first parameter must be runtime, got %v", t.In(0))
	rt.require(t.In(1).Kind() == reflect.Ptr, "exported method second parameter must be pointer to params, got %v", t.In(1))
	rt.require(t.In(1).Implements(typeOfCborUnmarshaler), "exported method second parameter must be CBOR-unmarshalable params, got %v", t.In(1))
	rt.require(t.NumOut() == 1, "exported method must return a single value")
	rt.require(t.Out(0).Implements(typeOfCborMarshaler), "exported method must return CBOR-marshalable value")
}

func (rt *Runtime) requireInCall() {
	rt.require(rt.inCall, "invalid runtime invocation outside of method call")
}

func (rt *Runtime) require(predicate bool, msg string, args ...interface{}) {
	if !predicate {
		rt.t.Fatalf(msg, args...)
	}
}
