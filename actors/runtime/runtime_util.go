package runtime

import (
	"context"
	cid "github.com/ipfs/go-cid"
	hamt "github.com/ipfs/go-hamt-ipld"

	addr "github.com/filecoin-project/go-address"
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	autil "github.com/filecoin-project/specs-actors/actors/util"
)

// TODO: most of this file doesn't need to be part of runtime, just generic actor shared code.

// Name should be set per unique filecoin network
var Name = "mainnet"

func NetworkName() string {
	return Name
}

// Propagates a failed send by aborting the current method with the same exit code.
func RequireSuccess(rt Runtime, e exitcode.ExitCode, msg string, args ...interface{}) {
	if !e.IsSuccess() {
		rt.Abort(e, msg, args)
	}
}

type MinerEntrySpec int64

const (
	MinerEntrySpec_MinerOnly = iota
	MinerEntrySpec_MinerOrSignable
)

// ActorCode is the interface that all actor code types should satisfy.
// It is merely a method dispatch interface.
type ActorCode interface {
	//InvokeMethod(rt Runtime, method actor.MethodNum, params actor.MethodParams) InvocOutput
	// TODO: method dispatch mechanism is deferred to implementations.
	// When the executable actor spec is complete we can re-instantiate something here.
}

func RT_Address_Is_StorageMiner(rt Runtime, minerAddr addr.Address) bool {
	codeID, ok := rt.GetActorCodeID(minerAddr)
	autil.Assert(ok)
	return codeID == builtin.StorageMinerActorCodeID
}

func RT_GetMinerAccountsAssert(rt Runtime, minerAddr addr.Address) (ownerAddr addr.Address, workerAddr addr.Address) {
	ret, code := rt.Send(minerAddr, builtin.Method_StorageMinerActor_GetOwnerAddr, nil, abi.NewTokenAmount(0))
	RequireSuccess(rt, code, "failed fetching owner addr")
	autil.AssertNoError(ret.Into(&ownerAddr))

	ret, code = rt.Send(minerAddr, builtin.Method_StorageMinerActor_GetWorkerAddr, nil, abi.NewTokenAmount(0))
	RequireSuccess(rt, code, "failed fetching worker addr")
	autil.AssertNoError(ret.Into(&workerAddr))
	return
}

func RT_MinerEntry_ValidateCaller_DetermineFundsLocation(rt Runtime, entryAddr addr.Address, entrySpec MinerEntrySpec) addr.Address {
	if RT_Address_Is_StorageMiner(rt, entryAddr) {
		// Storage miner actor entry; implied funds recipient is the associated owner address.
		ownerAddr, workerAddr := RT_GetMinerAccountsAssert(rt, entryAddr)
		rt.ValidateImmediateCallerIs(ownerAddr, workerAddr)
		return ownerAddr
	} else {
		if entrySpec == MinerEntrySpec_MinerOnly {
			rt.Abort(exitcode.ErrPlaceholder, "Only miner entries valid in current context")
		}
		// Ordinary account-style actor entry; funds recipient is just the entry address itself.
		rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
		return entryAddr
	}
}

func RT_ConfirmFundsReceiptOrAbort_RefundRemainder(rt Runtime, fundsRequired abi.TokenAmount) {
	if rt.ValueReceived().LessThan(fundsRequired) {
		rt.Abort(exitcode.ErrInsufficientFunds, "Insufficient funds received accompanying message")
	}

	if rt.ValueReceived().GreaterThan(fundsRequired) {
		_, code := rt.Send(rt.ImmediateCaller(), builtin.MethodSend, nil, big.Sub(rt.ValueReceived(), fundsRequired))
		RequireSuccess(rt, code, "failed to transfer refund")
	}
}

// AsStore allows Runtime to satisfy the hamt.CborIpldStore interface.
func AsStore(rt Runtime) hamt.CborIpldStore {
	return cborStore{rt}
}

var _ hamt.CborIpldStore = &cborStore{}

type cborStore struct {
	Runtime
}

func (r cborStore) Get(ctx context.Context, c cid.Cid, out interface{}) error {
	if !r.IpldGet(c, out.(CBORUnmarshalable)) {
		r.AbortStateMsg("not found")
	}
	return nil
}

func (r cborStore) Put(ctx context.Context, v interface{}) (cid.Cid, error) {
	return r.IpldPut(v.(CBORMarshalable)), nil
}
