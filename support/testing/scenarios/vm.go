package scenarios

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/account"
	"github.com/filecoin-project/specs-actors/actors/builtin/exported"
	init_ "github.com/filecoin-project/specs-actors/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/actors/runtime"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/actors/util/adt"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-hamt-ipld"
	"github.com/pkg/errors"
)

// VM holds the state and executes messages over the state.
type VM struct {
	ctx   context.Context
	store adt.Store

	currentEpoch abi.ChainEpoch

	actorImpls  ActorImplLookup
	actorRoot   cid.Cid  // The last committed root.
	actors      *adt.Map // The current (not necessarily committed) root node.
	actorsDirty bool

	emptyObject cid.Cid

	logs []string
}

// VM types

type TestActor struct {
	Head    cid.Cid
	Code    cid.Cid
	Balance abi.TokenAmount
}

type ActorImplLookup map[cid.Cid]exported.BuiltinActor

type internalMessage struct {
	from   address.Address
	to     address.Address
	value  abi.TokenAmount
	method abi.MethodNum
	params interface{}
}

// NewVM creates a new runtime for executing messages.
func NewVM(ctx context.Context, actorImpls ActorImplLookup, store adt.Store) *VM {
	actors := adt.MakeEmptyMap(store)
	actorRoot, err := actors.Root()
	if err != nil {
		panic(err)
	}

	emptyObject, err := store.Put(context.TODO(), []struct{}{})
	if err != nil {
		panic(err)
	}

	if err != nil {
		panic("could not create empty actor root")
	}
	return &VM{
		ctx:         ctx,
		actorImpls:  actorImpls,
		store:       store,
		actors:      actors,
		actorRoot:   actorRoot,
		actorsDirty: false,
		emptyObject: emptyObject,
	}
}

func (vm *VM) rollback(root cid.Cid) error {
	var err error
	vm.actors, err = adt.AsMap(vm.store, root)
	if err != nil {
		return errors.Wrapf(err, "failed to load node for %s", root)
	}

	// reset the root node
	vm.actorRoot = root
	vm.actorsDirty = false
	return nil
}

func (vm *VM) GetActor(a address.Address) (*TestActor, bool, error) {
	var act TestActor
	found, err := vm.actors.Get(adt.AddrKey(a), &act)
	return &act, found, err
}

// SetActor sets the the actor to the given value whether it previously existed or not.
//
// This method will not check if the actor previously existed, it will blindly overwrite it.
func (vm *VM) setActor(ctx context.Context, key address.Address, a *TestActor) error {
	if err := vm.actors.Put(adt.AddrKey(key), a); err != nil {
		return errors.Wrap(err, "setting actor in state tree failed")
	}
	vm.actorsDirty = true
	return nil
}

// deleteActor remove the actor from the storage.
//
// This method will NOT return an error if the actor was not found.
// This behaviour is based on a principle that some store implementations might not be able to determine
// whether something exists before deleting it.
func (vm *VM) deleteActor(ctx context.Context, key address.Address) error {
	err := vm.actors.Delete(adt.AddrKey(key))
	vm.actorsDirty = true
	if err == hamt.ErrNotFound {
		return nil
	}
	return err
}

func (vm *VM) checkpoint() (cid.Cid, error) {
	return vm.commit()
}

func (vm *VM) commit() (cid.Cid, error) {
	// commit the vm state
	root, err := vm.actors.Root()
	if err != nil {
		return cid.Undef, err
	}
	vm.actorRoot = root
	vm.actorsDirty = false

	return root, nil
}

func (vm *VM) normalizeAddress(addr address.Address) (address.Address, bool) {
	// short-circuit if the address is already an ID address
	if addr.Protocol() == address.ID {
		return addr, true
	}

	// resolve the target address via the InitActor, and attempt to load state.
	initActorEntry, found, err := vm.GetActor(builtin.InitActorAddr)
	if err != nil {
		panic(errors.Wrapf(err, "failed to load init actor"))
	}
	if !found {
		panic(errors.Wrapf(err, "no init actor"))
	}

	// get a view into the actor state
	var state init_.State
	if err := vm.store.Get(vm.ctx, initActorEntry.Head, &state); err != nil {
		panic(err)
	}

	idAddr, err := state.ResolveAddress(vm.store, addr)
	if err == init_.ErrAddressNotFound {
		return address.Undef, false
	} else if err != nil {
		panic(err)
	}
	return idAddr, true
}

// ApplyMessage applies the message to the current state.
func (vm *VM) ApplyMessage(from, to address.Address, value abi.TokenAmount, method abi.MethodNum, params interface{}) (runtime.CBORMarshaler, exitcode.ExitCode) {
	// This method does not actually execute the message itself,
	// but rather deals with the pre/post processing of a message.
	// (see: `invocationContext.invoke()` for the dispatch and execution)

	// load actor from global state
	var ok bool
	if from, ok = vm.normalizeAddress(from); !ok {
		return nil, exitcode.SysErrSenderInvalid
	}

	fromActor, found, err := vm.GetActor(from)
	if err != nil {
		panic(err)
	}
	if !found {
		// Execution error; sender does not exist at time of message execution.
		return nil, exitcode.SysErrSenderInvalid
	}

	if !fromActor.Code.Equals(builtin.AccountActorCodeID) {
		// Execution error; sender is not an account.
		return nil, exitcode.SysErrSenderInvalid
	}

	// Load sender account state to obtain stable pubkey address.
	var senderState account.State
	err = vm.store.Get(vm.ctx, fromActor.Head, &senderState)
	if err != nil {
		panic(err)
	}

	// checkpoint state
	// Even if the message fails, the following accumulated changes will be applied:
	// - CallSeqNumber increment
	// - sender balance withheld
	priorRoot, err := vm.checkpoint()
	if err != nil {
		panic(err)
	}

	// send
	// 1. build internal message
	// 2. build invocation context
	// 3. process the msg

	topLevel := topLevelContext{
		originatorStableAddress: senderState.Address,
		newActorAddressCount:    0,
	}

	// build internal msg
	imsg := internalMessage{
		from:   from,
		to:     to,
		value:  value,
		method: method,
		params: params,
	}

	// build invocation context
	ctx := newInvocationContext(vm, &topLevel, imsg, fromActor, vm.emptyObject)

	// 3. invoke
	ret, exitCode := ctx.invoke()

	// Roll back all state if the receipt's exit code is not ok.
	// This is required in addition to rollback within the invocation context since top level messages can fail for
	// more reasons than internal ones. Invocation context still needs its own rollback so actors can recover and
	// proceed from a nested call failure.
	if exitCode != exitcode.Ok {
		if err := vm.rollback(priorRoot); err != nil {
			panic(err)
		}
	}

	return ret.inner, exitCode
}

func (vm *VM) GetState(addr address.Address, out runtime.CBORUnmarshaler) error {
	act, found, err := vm.GetActor(addr)
	if err != nil {
		return err
	}
	if !found {
		return errors.Errorf("actor %v not found", addr)
	}
	return vm.store.Get(vm.ctx, act.Head, out)
}

func (vm *VM) Store() adt.Store {
	return vm.store
}

// transfer debits money from one account and credits it to another.
// avoid calling this method with a zero amount else it will perform unnecessary actor loading.
//
// WARNING: this method will panic if the the amount is negative, accounts dont exist, or have inssuficient funds.
//
// Note: this is not idiomatic, it follows the Spec expectations for this method.
func (vm *VM) transfer(debitFrom address.Address, creditTo address.Address, amount abi.TokenAmount) (*TestActor, *TestActor) {
	// allow only for positive amounts
	if amount.LessThan(abi.NewTokenAmount(0)) {
		panic("unreachable: negative funds transfer not allowed")
	}

	ctx := context.Background()

	// retrieve debit account
	fromActor, found, err := vm.GetActor(debitFrom)
	if err != nil {
		panic(err)
	}
	if !found {
		panic(fmt.Errorf("unreachable: debit account not found. %s", err))
	}

	// check that account has enough balance for transfer
	if fromActor.Balance.LessThan(amount) {
		panic("unreachable: insufficient balance on debit account")
	}

	// debit funds
	fromActor.Balance = big.Sub(fromActor.Balance, amount)
	if err := vm.setActor(ctx, debitFrom, fromActor); err != nil {
		panic(err)
	}

	// retrieve credit account
	toActor, found, err := vm.GetActor(creditTo)
	if err != nil {
		panic(err)
	}
	if !found {
		panic(fmt.Errorf("unreachable: credit account not found. %s", err))
	}

	// credit funds
	toActor.Balance = big.Add(toActor.Balance, amount)
	if err := vm.setActor(ctx, creditTo, toActor); err != nil {
		panic(err)
	}
	return toActor, fromActor
}

func (vm *VM) getActorImpl(code cid.Cid) exported.BuiltinActor {
	actorImpl, ok := vm.actorImpls[code]
	if !ok {
		vm.Abortf(exitcode.SysErrInvalidReceiver, "actor implementation not found for code %v", code)
	}
	return actorImpl
}

//
// implement runtime.Runtime for VM
//

func (vm *VM) Log(level runtime.LogLevel, msg string, args ...interface{}) {
	vm.logs = append(vm.logs, fmt.Sprintf(msg, args...))
}

type abort struct {
	code exitcode.ExitCode
	msg  string
}

func (vm *VM) Abortf(errExitCode exitcode.ExitCode, msg string, args ...interface{}) {
	panic(abort{errExitCode, fmt.Sprintf(msg, args...)})
}

//
// implement runtime.MessageInfo for internalMessage
//

var _ runtime.Message = (*internalMessage)(nil)

// ValueReceived implements runtime.MessageInfo.
func (msg internalMessage) ValueReceived() abi.TokenAmount {
	return msg.value
}

// Caller implements runtime.MessageInfo.
func (msg internalMessage) Caller() address.Address {
	return msg.from
}

// Receiver implements runtime.MessageInfo.
func (msg internalMessage) Receiver() address.Address {
	return msg.to
}
