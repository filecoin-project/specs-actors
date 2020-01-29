package builtin

import (
	addr "github.com/filecoin-project/go-address"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	autil "github.com/filecoin-project/specs-actors/actors/util"
)

///// Code shared by multiple built-in actors. /////

// Propagates a failed send by aborting the current method with the same exit code.
func RequireSuccess(rt runtime.Runtime, e exitcode.ExitCode, msg string, args ...interface{}) {
	if !e.IsSuccess() {
		rt.Abort(e, msg, args)
	}
}

type SectorTermination int64

// Note: Detected fault termination (due to exceeding the limit of consecutive
// SurprisePoSt failures) is not listed here, since this does not terminate all
// sectors individually, but rather the miner as a whole.
const (
	NormalExpiration SectorTermination = iota
	UserTermination
)

type MinerEntrySpec int64

const (
	MinerEntrySpec_MinerOnly = iota
	MinerEntrySpec_MinerOrSignable
)

// ActorCode is the interface that all actor code types should satisfy.
// It is merely a method dispatch interface.
type ActorCode interface {
	//InvokeMethod(rt Runtime, method actor.MethodNum, params actor.MethodParams) InvocOutput
	// Method dispatch mechanism is deferred to implementations.
	// When the executable actor spec is complete we can re-instantiate something here.
}

func RT_Address_Is_StorageMiner(rt runtime.Runtime, minerAddr addr.Address) bool {
	codeID, ok := rt.GetActorCodeID(minerAddr)
	autil.Assert(ok)
	return codeID == StorageMinerActorCodeID
}

func RT_GetMinerAccountsAssert(rt runtime.Runtime, minerAddr addr.Address) (ownerAddr addr.Address, workerAddr addr.Address) {
	ret, code := rt.Send(minerAddr, Method_StorageMinerActor_GetOwnerAddr, nil, abi.NewTokenAmount(0))
	RequireSuccess(rt, code, "failed fetching owner addr")
	autil.AssertNoError(ret.Into(&ownerAddr))

	ret, code = rt.Send(minerAddr, Method_StorageMinerActor_GetWorkerAddr, nil, abi.NewTokenAmount(0))
	RequireSuccess(rt, code, "failed fetching worker addr")
	autil.AssertNoError(ret.Into(&workerAddr))
	return
}

func RT_MinerEntry_ValidateCaller_DetermineFundsLocation(rt runtime.Runtime, entryAddr addr.Address, entrySpec MinerEntrySpec) addr.Address {
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
		rt.ValidateImmediateCallerType(CallerTypesSignable...)
		return entryAddr
	}
}

func RT_ConfirmFundsReceiptOrAbort_RefundRemainder(rt runtime.Runtime, fundsRequired abi.TokenAmount) {
	if rt.ValueReceived().LessThan(fundsRequired) {
		rt.Abort(exitcode.ErrInsufficientFunds, "Insufficient funds received accompanying message")
	}

	if rt.ValueReceived().GreaterThan(fundsRequired) {
		_, code := rt.Send(rt.ImmediateCaller(), MethodSend, nil, big.Sub(rt.ValueReceived(), fundsRequired))
		RequireSuccess(rt, code, "failed to transfer refund")
	}
}
