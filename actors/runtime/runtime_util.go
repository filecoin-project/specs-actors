package runtime

import (
	"bytes"

	addr "github.com/filecoin-project/go-address"
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	autil "github.com/filecoin-project/specs-actors/actors/util"
)

// TODO: most of this file doesn't need to be part of runtime, just generic actor shared code.

// Name should be set per unique filecoin network
var Name = "mainnet"

func NetworkName() string {
	return Name
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
	raw := rt.SendQuery(minerAddr, builtin.Method_StorageMinerActor_GetOwnerAddr, nil)
	r := bytes.NewReader(raw)
	err := ownerAddr.UnmarshalCBOR(r)
	autil.AssertNoError(err)

	raw = rt.SendQuery(minerAddr, builtin.Method_StorageMinerActor_GetWorkerAddr, nil)
	r = bytes.NewReader(raw)
	err = workerAddr.UnmarshalCBOR(r)
	autil.AssertNoError(err)

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
			rt.AbortArgMsg("Only miner entries valid in current context")
		}
		// Ordinary account-style actor entry; funds recipient is just the entry address itself.
		rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
		return entryAddr
	}
}

func RT_ConfirmFundsReceiptOrAbort_RefundRemainder(rt Runtime, fundsRequired abi.TokenAmount) {
	if rt.ValueReceived() < fundsRequired {
		rt.AbortFundsMsg("Insufficient funds received accompanying message")
	}

	if rt.ValueReceived() > fundsRequired {
		rt.SendFunds(rt.ImmediateCaller(), rt.ValueReceived()-fundsRequired)
	}
}
