package multisig

import (
	"math/big"

	addr "github.com/filecoin-project/go-address"
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	autil "github.com/filecoin-project/specs-actors/actors/util"
	cid "github.com/ipfs/go-cid"
)

type InvocOutput = vmr.InvocOutput
type Runtime = vmr.Runtime

var AssertMsg = autil.AssertMsg
var IMPL_FINISH = autil.IMPL_FINISH

type TxnID int64

type MultiSigTransaction struct {
	Proposer   addr.Address
	Expiration abi.ChainEpoch

	To     addr.Address
	Method abi.MethodNum
	Params abi.MethodParams
	Value  abi.TokenAmount
}

func (txn *MultiSigTransaction) Equals(MultiSigTransaction) bool {
	IMPL_FINISH()
	panic("")
}

type MultiSigTransactionHAMT map[TxnID]MultiSigTransaction
type MultiSigApprovalSetHAMT map[TxnID]autil.ActorIDSetHAMT

func MultiSigTransactionHAMT_Empty() MultiSigTransactionHAMT {
	IMPL_FINISH()
	panic("")
}

func MultiSigApprovalSetHAMT_Empty() MultiSigApprovalSetHAMT {
	IMPL_FINISH()
	panic("")
}

type MultiSigActor struct{}

func (a *MultiSigActor) State(rt Runtime) (vmr.ActorStateHandle, MultiSigActorState) {
	h := rt.AcquireState()
	stateCID := cid.Cid(h.Take())
	var state MultiSigActorState
	if !rt.IpldGet(stateCID, &state) {
		rt.AbortAPI("state not found")
	}
	return h, state
}

type ConstructorParams struct {
	AuthorizedParties     autil.ActorIDSetHAMT
	NumApprovalsThreshold int64
}

func (a *MultiSigActor) Constructor(rt vmr.Runtime, params *ConstructorParams) {

	rt.ValidateImmediateCallerIs(builtin.InitActorAddr)
	h := rt.AcquireState()

	st := MultiSigActorState{
		AuthorizedParties:     params.AuthorizedParties,
		NumApprovalsThreshold: params.NumApprovalsThreshold,
		PendingTxns:           MultiSigTransactionHAMT_Empty(),
		PendingApprovals:      MultiSigApprovalSetHAMT_Empty(),
	}

	UpdateRelease_MultiSig(rt, h, st)
}

type ProposeParams struct {
	To     addr.Address
	Value  big.Int
	Method abi.MethodNum
	Params abi.MethodParams
}

func (a *MultiSigActor) Propose(rt vmr.Runtime, params ProposeParams) TxnID {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	callerAddr := rt.ImmediateCaller()
	a._rtValidateAuthorizedPartyOrAbort(rt, callerAddr)

	h, st := a.State(rt)
	txnID := st.NextTxnID
	st.NextTxnID += 1

	txn := MultiSigTransaction{
		Proposer:   callerAddr,
		Expiration: 0, // TODO lotus does not specify this.
		To:         params.To,
		Method:     params.Method,
		Params:     params.Params,
		Value:      0, // TODO lotus does not specify this.
	}

	st.PendingTxns[txnID] = txn
	st.PendingApprovals[txnID] = autil.ActorIDSetHAMT_Empty()
	UpdateRelease_MultiSig(rt, h, st)

	// Proposal implicitly includes approval of a transaction.
	a._rtApproveTransactionOrAbort(rt, callerAddr, txnID)

	// Note: this ID may not be stable across chain re-orgs.
	// https://github.com/filecoin-project/specs-actors/issues/7
	return txnID
}

type TxnIDParams struct {
	ID TxnID
}

func (a *MultiSigActor) Approve(rt vmr.Runtime, params TxnIDParams) {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	callerAddr := rt.ImmediateCaller()
	a._rtValidateAuthorizedPartyOrAbort(rt, callerAddr)
	a._rtApproveTransactionOrAbort(rt, callerAddr, params.ID)
}

func (a *MultiSigActor) Cancel(rt vmr.Runtime, params TxnIDParams) {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	callerAddr := rt.ImmediateCaller()
	a._rtValidateAuthorizedPartyOrAbort(rt, callerAddr)
	// TODO implement cancel logic
}

type ModifyAuthorizedPartyParams struct {
	AuthorizedParty addr.Address // must be an ID protocol address.
	ModifyPartySize bool
}

func (a *MultiSigActor) AddAuthorizedParty(rt vmr.Runtime, params ModifyAuthorizedPartyParams) {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.CurrReceiver())

	actorID, err := addr.IDFromAddress(params.AuthorizedParty)
	if err != nil {
		// TODO runtime handles this input failure
		panic(err)
	}

	h, st := a.State(rt)
	st.AuthorizedParties[abi.ActorID(actorID)] = true
	UpdateRelease_MultiSig(rt, h, st)
}

func (a *MultiSigActor) RemoveAuthorizedParty(rt vmr.Runtime, params ModifyAuthorizedPartyParams) {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.CurrReceiver())

	actorID, err := addr.IDFromAddress(params.AuthorizedParty)
	if err != nil {
		// TODO runtime handles this input failure
		panic(err)
	}

	h, st := a.State(rt)

	if _, found := st.AuthorizedParties[abi.ActorID(actorID)]; !found {
		rt.AbortStateMsg("Party not found")
	}

	delete(st.AuthorizedParties, abi.ActorID(actorID))

	if int64(len(st.AuthorizedParties)) < st.NumApprovalsThreshold {
		rt.AbortStateMsg("Cannot decrease authorized parties below threshold")
	}

	UpdateRelease_MultiSig(rt, h, st)
}

type SwapAuthorizedPartyParams struct {
	From addr.Address
	To   addr.Address
}

func (a *MultiSigActor) SwapAuthorizedParty(rt vmr.Runtime, params SwapAuthorizedPartyParams) {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.CurrReceiver())

	oldActor, err := addr.IDFromAddress(params.From)
	if err != nil {
		// TODO runtime handles this input failure
		panic(err)
	}

	newActor, err := addr.IDFromAddress(params.To)
	if err != nil {
		// TODO runtime handles this input failure
		panic(err)
	}

	h, st := a.State(rt)

	if _, found := st.AuthorizedParties[abi.ActorID(oldActor)]; !found {
		rt.AbortStateMsg("Party not found")
	}

	if _, found := st.AuthorizedParties[abi.ActorID(newActor)]; !found {
		rt.AbortStateMsg("Party already present")
	}

	delete(st.AuthorizedParties, abi.ActorID(oldActor))
	st.AuthorizedParties[abi.ActorID(newActor)] = true

	UpdateRelease_MultiSig(rt, h, st)
}

type ChangeNumApprovalsThresholdParams struct {
	NewThreshold int64
}

func (a *MultiSigActor) ChangeNumApprovalsThreshold(rt vmr.Runtime, params ChangeNumApprovalsThresholdParams) {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.CurrReceiver())

	h, st := a.State(rt)

	if params.NewThreshold <= 0 || params.NewThreshold > int64(len(st.AuthorizedParties)) {
		rt.AbortStateMsg("New threshold value not supported")
	}

	st.NumApprovalsThreshold = params.NewThreshold

	UpdateRelease_MultiSig(rt, h, st)
}

func (a *MultiSigActor) _rtApproveTransactionOrAbort(rt Runtime, callerAddr addr.Address, txnID TxnID) {

	h, st := a.State(rt)

	// TODO look up transaction based on its ID from the Multisig actor state
	txn := MultiSigTransaction{}

	txnCheck, found := st.PendingTxns[txnID]
	if !found || !txnCheck.Equals(txn) {
		rt.AbortStateMsg("Requested transcation not found or not matched")
	}

	expirationExceeded := (rt.CurrEpoch() > txn.Expiration)
	if expirationExceeded {
		// TODO: delete from state? https://github.com/filecoin-project/specs-actors/issues/5
		rt.AbortStateMsg("Transaction expiration exceeded")
	}

	actorID, err := addr.IDFromAddress(callerAddr)
	autil.AssertNoError(err)

	st.PendingApprovals[txnID][abi.ActorID(actorID)] = true
	thresholdMet := int64(len(st.PendingApprovals[txnID])) == st.NumApprovalsThreshold

	UpdateRelease_MultiSig(rt, h, st)

	if thresholdMet {
		if !st._hasAvailable(rt.CurrentBalance(), txn.Value, rt.CurrEpoch()) {
			rt.AbortArgMsg("insufficient funds unlocked")
		}

		// A sufficient number of approvals have arrived and sufficient funds have been unlocked: relay the message and delete from pending queue.
		rt.Send(
			txn.To,
			txn.Method,
			txn.Params,
			txn.Value,
		)
		a._rtDeletePendingTransaction(rt, txnID)
	}
}

func (a *MultiSigActor) _rtDeletePendingTransaction(rt Runtime, txnID TxnID) {
	h, st := a.State(rt)
	delete(st.PendingTxns, txnID)
	delete(st.PendingApprovals, txnID)
	UpdateRelease_MultiSig(rt, h, st)
}

func (a *MultiSigActor) _rtValidateAuthorizedPartyOrAbort(rt Runtime, address addr.Address) {
	AssertMsg(address.Protocol() == addr.ID, "caller address does not have ID")
	actorID, err := addr.IDFromAddress(address)
	autil.Assert(err == nil)

	h, st := a.State(rt)
	if _, found := st.AuthorizedParties[abi.ActorID(actorID)]; !found {
		rt.AbortArgMsg("Party not authorized")
	}
	Release_MultiSig(rt, h, st)
}

func Release_MultiSig(rt Runtime, h vmr.ActorStateHandle, st MultiSigActorState) {
	checkCID := abi.ActorSubstateCID(rt.IpldPut(&st))
	h.Release(checkCID)
}

func UpdateRelease_MultiSig(rt Runtime, h vmr.ActorStateHandle, st MultiSigActorState) {
	newCID := abi.ActorSubstateCID(rt.IpldPut(&st))
	h.UpdateRelease(newCID)
}
