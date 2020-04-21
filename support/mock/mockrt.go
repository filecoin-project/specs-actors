package mock

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"runtime/debug"
	"testing"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/crypto"
	runtime "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
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
	idAddresses   map[addr.Address]addr.Address
	actorCodeCIDs map[addr.Address]cid.Cid
	newActorAddr  addr.Address

	syscalls syscaller

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
	expectRandomness         []*expectRandomness
	expectSends              []*expectedMessage
	expectCreateActor        *expectCreateActor
}

type expectRandomness struct {
	// Expected parameters.
	tag     crypto.DomainSeparationTag
	epoch   abi.ChainEpoch
	entropy []byte
	// Result.
	out abi.Randomness
}

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
	return m.to == to && m.method == method && m.value.Equals(value) && reflect.DeepEqual(m.params, params)
}

func (m *expectedMessage) String() string {
	return fmt.Sprintf("to: %v method: %v value: %v params: %v sendReturn: %v exitCode: %v", m.to, m.method, m.value, m.params, m.sendReturn, m.exitCode)
}

type expectCreateActor struct {
	// Expected code CID.
	codeId cid.Cid
	// Returned address.
	address addr.Address
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

func (rt *Runtime) Message() runtime.Message {
	rt.requireInCall()
	return rt
}

func (rt *Runtime) CurrEpoch() abi.ChainEpoch {
	rt.requireInCall()
	return rt.epoch
}

func (rt *Runtime) ValidateImmediateCallerAcceptAny() {
	rt.requireInCall()
	if !rt.expectValidateCallerAny {
		rt.failTest("unexpected validate-caller-any")
	}
	rt.expectValidateCallerAny = false
}

func (rt *Runtime) ValidateImmediateCallerIs(addrs ...addr.Address) {
	rt.requireInCall()
	rt.checkArgument(len(addrs) > 0, "addrs must be non-empty")
	// Check and clear expectations.
	if len(rt.expectValidateCallerAddr) == 0 {
		rt.failTest("unexpected validate caller addrs")
		return
	}
	if !reflect.DeepEqual(rt.expectValidateCallerAddr, addrs) {
		rt.failTest("unexpected validate caller addrs %v, expected %v", addrs, rt.expectValidateCallerAddr)
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
	rt.Abortf(exitcode.ErrForbidden, "caller address %v forbidden, allowed: %v", rt.caller, addrs)
}

func (rt *Runtime) ValidateImmediateCallerType(types ...cid.Cid) {
	rt.requireInCall()
	rt.checkArgument(len(types) > 0, "types must be non-empty")

	// Check and clear expectations.
	if len(rt.expectValidateCallerType) == 0 {
		rt.failTest("unexpected validate caller code")
	}
	if !reflect.DeepEqual(rt.expectValidateCallerType, types) {
		rt.failTest("unexpected validate caller code %v, expected %v", types, rt.expectValidateCallerType)
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
	rt.Abortf(exitcode.ErrForbidden, "caller type %v forbidden, allowed: %v", rt.callerType, types)
}

func (rt *Runtime) CurrentBalance() abi.TokenAmount {
	rt.requireInCall()
	return rt.balance
}

func (rt *Runtime) ResolveAddress(address addr.Address) (ret addr.Address, ok bool) {
	rt.requireInCall()
	if address.Protocol() == addr.ID {
		return address, true
	}
	resolved, ok := rt.idAddresses[address]
	return resolved, ok
}

func (rt *Runtime) GetActorCodeCID(addr addr.Address) (ret cid.Cid, ok bool) {
	rt.requireInCall()
	ret, ok = rt.actorCodeCIDs[addr]
	return
}

func (rt *Runtime) GetRandomness(tag crypto.DomainSeparationTag, epoch abi.ChainEpoch, entropy []byte) abi.Randomness {
	rt.requireInCall()
	if len(rt.expectRandomness) == 0 {
		rt.failTestNow("unexpected call to get randomness for tag %v, epoch %v", tag, epoch)
	}
	expectation := rt.expectRandomness[0]
	if tag != expectation.tag || epoch != expectation.epoch || !bytes.Equal(entropy, expectation.entropy) {
		rt.failTest("get randomness does not match expectation.\n"+
			"Call     - tag: %d, epoch: %d, entropy: %v\n"+
			"Expected - tag: %d, epoch: %d, entropy: %v", tag, epoch, entropy, expectation.tag, expectation.epoch, expectation.entropy)
	}
	defer func() {
		rt.expectRandomness = rt.expectRandomness[1:]
	}()
	return expectation.out
}

func (rt *Runtime) State() runtime.StateHandle {
	rt.requireInCall()
	return rt
}

func (rt *Runtime) Store() runtime.Store {
	// requireInCall omitted because it makes using this mock runtime as a store awkward.
	return rt
}

func (rt *Runtime) Send(toAddr addr.Address, methodNum abi.MethodNum, params runtime.CBORMarshaler, value abi.TokenAmount) (runtime.SendReturn, exitcode.ExitCode) {
	rt.requireInCall()
	if rt.inTransaction {
		rt.Abortf(exitcode.SysErrorIllegalActor, "side-effect within transaction")
	}
	if len(rt.expectSends) == 0 {
		rt.failTestNow("unexpected send to: %v method: %v, value: %v, params: %v", toAddr, methodNum, value, params)
	}
	expectedMsg := rt.expectSends[0]

	if !expectedMsg.Equal(toAddr, methodNum, params, value) {
		rt.failTest("send does not match expectation.\n" +
			"Call     - to: %v method: %v value: %v params: %v\n" +
			"Expected - %v", toAddr, methodNum, value, params, rt.expectSends[0])
	}

	if value.GreaterThan(rt.balance) {
		rt.Abortf(exitcode.SysErrSenderStateInvalid, "cannot send value: %v exceeds balance: %v", value, rt.balance)
	}

	// pop the expectedMessage from the queue and modify the mockrt balance to reflect the send.
	defer func() {
		rt.expectSends = rt.expectSends[1:]
		rt.balance = big.Sub(rt.balance, value)
	}()
	return expectedMsg.sendReturn, expectedMsg.exitCode
}

func (rt *Runtime) NewActorAddress() addr.Address {
	rt.requireInCall()
	if rt.newActorAddr == addr.Undef {
		rt.failTestNow("unexpected call to new actor address")
	}
	defer func() { rt.newActorAddr = addr.Undef }()
	return rt.newActorAddr
}

func (rt *Runtime) CreateActor(codeId cid.Cid, address addr.Address) {
	rt.requireInCall()
	if rt.inTransaction {
		rt.Abortf(exitcode.SysErrorIllegalActor, "side-effect within transaction")
	}
	if rt.expectCreateActor == nil {
		rt.failTestNow("unexpected call to create actor")
	}
	if !rt.expectCreateActor.codeId.Equals(codeId) || rt.expectCreateActor.address != address {
		rt.failTest("unexpected actor being created, expected code: %s address: %s, actual code: %s address: %s",
			rt.expectCreateActor.codeId, rt.expectCreateActor.address, codeId, address)
	}
	defer func() {
		rt.expectCreateActor = nil
	}()
}

func (rt *Runtime) DeleteActor(_ addr.Address) {
	rt.requireInCall()
	if rt.inTransaction {
		rt.Abortf(exitcode.SysErrorIllegalActor, "side-effect within transaction")
	}
	panic("implement me")
}

func (rt *Runtime) Abortf(errExitCode exitcode.ExitCode, msg string, args ...interface{}) {
	rt.requireInCall()
	rt.t.Logf("Mock Runtime Abort ExitCode: %v Reason: %s", errExitCode, fmt.Sprintf(msg, args...))
	panic(abort{errExitCode, fmt.Sprintf(msg, args...)})
}

func (rt *Runtime) AbortStateMsg(msg string) {
	rt.requireInCall()
	rt.Abortf(exitcode.ErrPlaceholder, msg)
}

func (rt *Runtime) Syscalls() runtime.Syscalls {
	rt.requireInCall()
	return &rt.syscalls
}

func (rt *Runtime) Context() context.Context {
	// requireInCall omitted because it makes using this mock runtime as a store awkward.
	return rt.ctx
}

func (rt *Runtime) StartSpan(_ string) runtime.TraceSpan {
	rt.requireInCall()
	return &TraceSpan{}
}

func (rt *Runtime) checkArgument(predicate bool, msg string, args ...interface{}) {
	if !predicate {
		rt.Abortf(exitcode.SysErrorIllegalArgument, msg, args...)
	}
}

///// Store implementation /////

func (rt *Runtime) Get(c cid.Cid, o runtime.CBORUnmarshaler) bool {
	// requireInCall omitted because it makes using this mock runtime as a store awkward.
	data, found := rt.store[c]
	if found {
		err := o.UnmarshalCBOR(bytes.NewReader(data))
		if err != nil {
			rt.Abortf(exitcode.SysErrSerialization, err.Error())
		}
	}
	return found
}

func (rt *Runtime) Put(o runtime.CBORMarshaler) cid.Cid {
	// requireInCall omitted because it makes using this mock runtime as a store awkward.
	r := bytes.Buffer{}
	err := o.MarshalCBOR(&r)
	if err != nil {
		rt.Abortf(exitcode.SysErrSerialization, err.Error())
	}
	data := r.Bytes()
	key, err := cidBuilder.Sum(data)
	if err != nil {
		rt.Abortf(exitcode.SysErrSerialization, err.Error())
	}
	rt.store[key] = data
	return key
}

///// Message implementation /////

func (rt *Runtime) BlockMiner() addr.Address {
	return rt.miner
}

func (rt *Runtime) Caller() addr.Address {
	return rt.caller
}

func (rt *Runtime) Receiver() addr.Address {
	return rt.receiver
}

func (rt *Runtime) ValueReceived() abi.TokenAmount {
	return rt.valueReceived
}

///// State handle implementation /////

func (rt *Runtime) Create(obj runtime.CBORMarshaler) {
	if rt.state.Defined() {
		rt.Abortf(exitcode.SysErrorIllegalActor, "state already constructed")
	}
	rt.state = rt.Store().Put(obj)
}

func (rt *Runtime) Readonly(st runtime.CBORUnmarshaler) {
	found := rt.Store().Get(rt.state, st)
	if !found {
		rt.Abortf(exitcode.SysErrInternal, "actor state not found: %v", rt.state)
	}
}

func (rt *Runtime) Transaction(st runtime.CBORer, f func() interface{}) interface{} {
	if rt.inTransaction {
		rt.Abortf(exitcode.SysErrorIllegalActor, "nested transaction")
	}
	rt.Readonly(st)
	rt.inTransaction = true
	defer func() { rt.inTransaction = false }()
	ret := f()
	rt.state = rt.Put(st)
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

func (rt *Runtime) GetReceiver() addr.Address {
	return rt.receiver
}

func (rt *Runtime) StateRoot() cid.Cid {
	return rt.state
}

func (rt *Runtime) GetState(o runtime.CBORUnmarshaler) {
	data, found := rt.store[rt.state]
	if !found {
		rt.failTestNow("can't find state at root %v", rt.state) // something internal is messed up
	}
	err := o.UnmarshalCBOR(bytes.NewReader(data))
	if err != nil {
		rt.failTestNow("error loading state: %v", err)
	}
}

func (rt *Runtime) GetBalance() abi.TokenAmount {
	return rt.balance
}

func (rt *Runtime) GetEpoch() abi.ChainEpoch {
	return rt.epoch
}

///// Mocking facilities /////

func (rt *Runtime) SetCaller(address addr.Address, actorType cid.Cid) {
	rt.caller = address
	rt.callerType = actorType
	rt.actorCodeCIDs[address] = actorType
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

func (rt *Runtime) AddIDAddress(src addr.Address, target addr.Address) {
	rt.require(target.Protocol() == addr.ID, "target must use ID address protocol")
	rt.idAddresses[src] = target
}

func (rt *Runtime) SetNewActorAddress(actAddr addr.Address) {
	rt.require(actAddr.Protocol() == addr.Actor, "new actor address must be protocol: Actor, got protocol: %v", actAddr.Protocol())
	rt.newActorAddr = actAddr
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

func (rt *Runtime) ExpectGetRandomness(tag crypto.DomainSeparationTag, epoch abi.ChainEpoch, entropy []byte, out abi.Randomness) {
	rt.expectRandomness = append(rt.expectRandomness, &expectRandomness{
		tag:     tag,
		epoch:   epoch,
		entropy: entropy,
		out:     out,
	})
}

func (rt *Runtime) ExpectSend(toAddr addr.Address, methodNum abi.MethodNum, params runtime.CBORMarshaler, value abi.TokenAmount, ret runtime.CBORMarshaler, exitCode exitcode.ExitCode) {
	// append to the send queue
	rt.expectSends = append(rt.expectSends, &expectedMessage{
		to:         toAddr,
		method:     methodNum,
		params:     params,
		value:      value,
		sendReturn: ReturnWrapper{ret},
		exitCode:   exitCode,
	})
}

func (rt *Runtime) ExpectCreateActor(codeId cid.Cid, address addr.Address) {
	rt.expectCreateActor = &expectCreateActor{
		codeId:  codeId,
		address: address,
	}
}

// Verifies that expected calls were received, and resets all expectations.
func (rt *Runtime) Verify() {
	if rt.expectValidateCallerAny {
		rt.failTest("expected ValidateCallerAny, not received")
	}
	if len(rt.expectValidateCallerAddr) > 0 {
		rt.failTest("expected ValidateCallerAddr %v, not received", rt.expectValidateCallerAddr)
	}
	if len(rt.expectValidateCallerType) > 0 {
		rt.failTest("expected ValidateCallerType %v, not received", rt.expectValidateCallerType)
	}
	if len(rt.expectSends) > 0 {
		rt.failTest("expected all message to be send, unsent messages %v", rt.expectSends)
	}
	if rt.expectCreateActor != nil {
		rt.failTest("expected actor to be created, uncreated actor code: %v, address %v",
			rt.expectCreateActor.codeId, rt.expectCreateActor.address)
	}

	rt.Reset()
}

// Resets expectations
func (rt *Runtime) Reset() {
	rt.expectValidateCallerAny = false
	rt.expectValidateCallerAddr = nil
	rt.expectValidateCallerType = nil
	rt.expectCreateActor = nil
}

// Calls f() expecting it to invoke Runtime.Abortf() with a specified exit code.
func (rt *Runtime) ExpectAbort(expected exitcode.ExitCode, f func()) {
	prevState := rt.state

	defer func() {
		r := recover()
		if r == nil {
			rt.failTest("expected abort with code %v but call succeeded", expected)
			return
		}
		a, ok := r.(abort)
		if !ok {
			panic(r)
		}
		if a.code != expected {
			rt.failTest("abort expected code %v, got %v %s", expected, a.code, a.msg)
		}
		// Roll back state change.
		rt.state = prevState
	}()
	f()
}

func (rt *Runtime) ExpectAssertionFailure(expected string, f func()) {
	prevState := rt.state

	defer func() {
		r := recover()
		if r == nil {
			rt.failTest("expected panic with message %v but call succeeded", expected)
			return
		}
		a, ok := r.(abort)
		if ok {
			rt.failTest("expected panic with message %v but got abort %v", expected, a)
			return
		}
		p, ok := r.(string)
		if !ok {
			panic(r)
		}
		if p != expected {
			rt.failTest("expected panic with message \"%v\" but got message \"%v\"", expected, p)
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
	defer func() { rt.inCall = false }()
	var arg reflect.Value
	if params != nil {
		arg = reflect.ValueOf(params)
	} else {
		arg = reflect.ValueOf(adt.Empty)
	}
	ret := meth.Call([]reflect.Value{reflect.ValueOf(rt), arg})
	return ret[0].Interface()
}

func (rt *Runtime) SetVerifier(f VerifyFunc) {
	rt.syscalls.SignatureVerifier = f
}

func (rt *Runtime) SetHasher(f HasherFunc) {
	rt.syscalls.Hasher = f
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
		rt.failTestNow(msg, args...)
	}
}

func (rt *Runtime) failTest(msg string, args ...interface{}) {
	rt.t.Logf(msg, args...)
	rt.t.Logf("%s", debug.Stack())
	rt.t.Fail()
}

func (rt *Runtime) failTestNow(msg string, args ...interface{}) {
	rt.t.Logf(msg, args...)
	rt.t.Logf("%s", debug.Stack())
	rt.t.FailNow()
}

func (rt *Runtime) TotalFilCircSupply() abi.TokenAmount {
	panic("todo crypto econ")
}

type ReturnWrapper struct {
	V runtime.CBORMarshaler
}

func (r ReturnWrapper) Into(o runtime.CBORUnmarshaler) error {
	b := bytes.Buffer{}
	err := r.V.MarshalCBOR(&b)
	if err != nil {
		return err
	}
	err = o.UnmarshalCBOR(&b)
	return err
}
