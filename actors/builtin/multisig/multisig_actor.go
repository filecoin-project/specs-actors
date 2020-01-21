package multisig

import (
	"context"
	"encoding/binary"
	"fmt"

	addr "github.com/filecoin-project/go-address"
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	autil "github.com/filecoin-project/specs-actors/actors/util"
	cid "github.com/ipfs/go-cid"
	hamt "github.com/ipfs/go-hamt-ipld"
)

type TxnID int64

type MultiSigTransaction struct {
	To     addr.Address
	Value  abi.TokenAmount
	Method abi.MethodNum
	Params abi.MethodParams

	// This actorID at index 0 is the transaction proposer, order of this slice must be preserved.
	Approved []abi.ActorID
}

type MultiSigActor struct{}

func (a *MultiSigActor) State(rt vmr.Runtime) (vmr.ActorStateHandle, MultiSigActorState) {
	h := rt.AcquireState()
	stateCID := cid.Cid(h.Take())
	var state MultiSigActorState
	if !rt.IpldGet(stateCID, &state) {
		rt.Abort(exitcode.ErrPlaceholder, "state not found")
	}
	return h, state
}

type ConstructorParams struct {
	AuthorizedParties     []addr.Address
	NumApprovalsThreshold int64
	UnlockDuration        abi.ChainEpoch
}

func (a *MultiSigActor) Constructor(rt vmr.Runtime, params *ConstructorParams) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerIs(builtin.InitActorAddr)
	h := rt.AcquireState()

	var authPartyIDs []abi.ActorID
	for _, a := range params.AuthorizedParties {
		actID, err := addr.IDFromAddress(a)
		if err != nil {
			rt.AbortStateMsg(fmt.Sprintf("Address is not ID protocol: %v", err))
		}
		authPartyIDs = append(authPartyIDs, abi.ActorID(actID))
	}

	st := MultiSigActorState{
		AuthorizedParties:     authPartyIDs,
		NumApprovalsThreshold: params.NumApprovalsThreshold,
		PendingTxns:           autil.EmptyHAMT,
	}

	if params.UnlockDuration != 0 {
		st.UnlockDuration = params.UnlockDuration
		st.InitialBalance = rt.ValueReceived()
		st.StartEpoch = rt.CurrEpoch()
	}

	UpdateRelease_MultiSig(rt, h, st)
	return &vmr.EmptyReturn{}
}

type ProposeParams struct {
	To     addr.Address
	Value  abi.TokenAmount
	Method abi.MethodNum
	Params abi.MethodParams
}

type ProposeReturn struct {
	TxnID TxnID
}

func (a *MultiSigActor) Propose(rt vmr.Runtime, params *ProposeParams) *ProposeReturn {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	callerAddr := rt.ImmediateCaller()
	a._rtValidateAuthorizedPartyOrAbort(rt, callerAddr)

	callerID, err := addr.IDFromAddress(callerAddr)
	autil.AssertNoError(err)

	h, st := a.State(rt)
	txnID := st.NextTxnID
	st.NextTxnID += 1

	st.PendingTxns = setPendingTxn(rt.Context(), rt, st.PendingTxns, txnID, MultiSigTransaction{
		To:       params.To,
		Value:    params.Value,
		Method:   params.Method,
		Params:   params.Params,
		Approved: []abi.ActorID{abi.ActorID(callerID)},
	})

	UpdateRelease_MultiSig(rt, h, st)

	// Proposal implicitly includes approval of a transaction.
	a._rtApproveTransactionOrAbort(rt, txnID)

	// Note: this ID may not be stable across chain re-orgs.
	// https://github.com/filecoin-project/specs-actors/issues/7
	return &ProposeReturn{txnID}
}

type TxnIDParams struct {
	ID TxnID
}

func (a *MultiSigActor) Approve(rt vmr.Runtime, params *TxnIDParams) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	callerAddr := rt.ImmediateCaller()
	a._rtValidateAuthorizedPartyOrAbort(rt, callerAddr)
	a._rtApproveTransactionOrAbort(rt, params.ID)
	return &vmr.EmptyReturn{}
}

func (a *MultiSigActor) Cancel(rt vmr.Runtime, params *TxnIDParams) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	callerAddr := rt.ImmediateCaller()
	a._rtValidateAuthorizedPartyOrAbort(rt, callerAddr)

	callerID, err := addr.IDFromAddress(callerAddr)
	autil.AssertNoError(err)

	h, st := a.State(rt)
	txn := getPendingTxn(rt.Context(), rt, st.PendingTxns, params.ID)
	proposer := txn.Approved[0]
	if proposer != abi.ActorID(callerID) {
		rt.AbortStateMsg("Cannot cancel another signers transaction")
	}

	st.PendingTxns = deletePendingTxn(rt.Context(), rt, st.PendingTxns, params.ID)
	UpdateRelease_MultiSig(rt, h, st)
	return &vmr.EmptyReturn{}
}

type AddAuthorizedParty struct {
	AuthorizedParty addr.Address // must be an ID protocol address.
	Increase        bool
}

func (a *MultiSigActor) AddAuthorizedParty(rt vmr.Runtime, params *AddAuthorizedParty) *vmr.EmptyReturn {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.CurrReceiver())

	partyToAdd, err := addr.IDFromAddress(params.AuthorizedParty)
	if err != nil {
		rt.AbortStateMsg(fmt.Sprintf("party address is not ID protocol address: %v", err))
	}

	h, st := a.State(rt)
	if st.isAuthorizedParty(abi.ActorID(partyToAdd)) {
		rt.AbortStateMsg("Party is already authorized")
	}
	st.AuthorizedParties = append(st.AuthorizedParties, abi.ActorID(partyToAdd))
	if params.Increase {
		st.NumApprovalsThreshold = st.NumApprovalsThreshold + 1
	}

	UpdateRelease_MultiSig(rt, h, st)
	return &vmr.EmptyReturn{}
}

type RemoveAuthorizedParty struct {
	AuthorizedParty addr.Address // must be an ID protocol address.
	Decrease        bool
}

func (a *MultiSigActor) RemoveAuthorizedParty(rt vmr.Runtime, params *RemoveAuthorizedParty) *vmr.EmptyReturn {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.CurrReceiver())

	partyToRemove, err := addr.IDFromAddress(params.AuthorizedParty)
	if err != nil {
		rt.AbortStateMsg(fmt.Sprintf("party address is not ID protocol address: %v", err))
	}

	h, st := a.State(rt)

	if !st.isAuthorizedParty(abi.ActorID(partyToRemove)) {
		rt.AbortStateMsg("Party not found")
	}

	newAuthorizedParties := make([]abi.ActorID, 0, len(st.AuthorizedParties))
	for _, s := range st.AuthorizedParties {
		if s != abi.ActorID(partyToRemove) {
			newAuthorizedParties = append(newAuthorizedParties, s)
		}
	}
	if params.Decrease || int64(len(st.AuthorizedParties)-1) < st.NumApprovalsThreshold {
		st.NumApprovalsThreshold = st.NumApprovalsThreshold - 1
	}
	st.AuthorizedParties = newAuthorizedParties

	UpdateRelease_MultiSig(rt, h, st)
	return &vmr.EmptyReturn{}
}

type SwapAuthorizedPartyParams struct {
	From addr.Address
	To   addr.Address
}

func (a *MultiSigActor) SwapAuthorizedParty(rt vmr.Runtime, params *SwapAuthorizedPartyParams) *vmr.EmptyReturn {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.CurrReceiver())

	oldParty, err := addr.IDFromAddress(params.From)
	if err != nil {
		rt.AbortStateMsg(fmt.Sprintf("from party address is not ID protocol address: %v", err))
	}
	newParty, err := addr.IDFromAddress(params.To)
	if err != nil {
		rt.AbortStateMsg(fmt.Sprintf("to party address is not ID protocol address: %v", err))
	}

	h, st := a.State(rt)

	if !st.isAuthorizedParty(abi.ActorID(oldParty)) {
		rt.AbortStateMsg("Party not found")
	}

	if !st.isAuthorizedParty(abi.ActorID(newParty)) {
		rt.AbortStateMsg("Party already present")
	}

	newAuthorizedParties := make([]abi.ActorID, 0, len(st.AuthorizedParties))
	for _, s := range st.AuthorizedParties {
		if s != abi.ActorID(oldParty) {
			newAuthorizedParties = append(newAuthorizedParties, s)
		}
	}
	newAuthorizedParties = append(newAuthorizedParties, abi.ActorID(newParty))
	st.AuthorizedParties = newAuthorizedParties

	UpdateRelease_MultiSig(rt, h, st)
	return &vmr.EmptyReturn{}
}

type ChangeNumApprovalsThresholdParams struct {
	NewThreshold int64
}

func (a *MultiSigActor) ChangeNumApprovalsThreshold(rt vmr.Runtime, params *ChangeNumApprovalsThresholdParams) *vmr.EmptyReturn {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.CurrReceiver())

	h, st := a.State(rt)

	if params.NewThreshold <= 0 || params.NewThreshold > int64(len(st.AuthorizedParties)) {
		rt.AbortStateMsg("New threshold value not supported")
	}

	st.NumApprovalsThreshold = params.NewThreshold

	UpdateRelease_MultiSig(rt, h, st)
	return &vmr.EmptyReturn{}
}

func (a *MultiSigActor) _rtApproveTransactionOrAbort(rt vmr.Runtime, txnID TxnID) {
	h, st := a.State(rt)

	currentApproverID, err := addr.IDFromAddress(rt.ImmediateCaller())
	autil.AssertNoError(err)

	txn := getPendingTxn(rt.Context(), rt, st.PendingTxns, txnID)

	// abort duplicate approval
	for _, previousApprover := range txn.Approved {
		if previousApprover == abi.ActorID(currentApproverID) {
			rt.AbortStateMsg("already approved this message")
		}
	}
	// update approved on the transaction
	txn.Approved = append(txn.Approved, abi.ActorID(currentApproverID))
	st.PendingTxns = setPendingTxn(rt.Context(), rt, st.PendingTxns, txnID, txn)
	UpdateRelease_MultiSig(rt, h, st)

	thresholdMet := int64(len(txn.Approved)) >= st.NumApprovalsThreshold
	if thresholdMet {
		if !st._hasAvailable(rt.CurrentBalance(), txn.Value, rt.CurrEpoch()) {
			rt.Abort(exitcode.ErrInsufficientFunds, "insufficient funds unlocked")
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

func (a *MultiSigActor) _rtDeletePendingTransaction(rt vmr.Runtime, txnID TxnID) {
	h, st := a.State(rt)
	st.PendingTxns = deletePendingTxn(rt.Context(), rt, st.PendingTxns, txnID)
	UpdateRelease_MultiSig(rt, h, st)
}

func (a *MultiSigActor) _rtValidateAuthorizedPartyOrAbort(rt vmr.Runtime, address addr.Address) {
	autil.AssertMsg(address.Protocol() == addr.ID, "caller address does not have ID")
	actorID, err := addr.IDFromAddress(address)
	autil.Assert(err == nil)

	h, st := a.State(rt)
	if !st.isAuthorizedParty(abi.ActorID(actorID)) {
		rt.Abort(exitcode.ErrForbidden, "party not authorized")
	}
	Release_MultiSig(rt, h, st)
}

func Release_MultiSig(rt vmr.Runtime, h vmr.ActorStateHandle, st MultiSigActorState) {
	checkCID := abi.ActorSubstateCID(rt.IpldPut(&st))
	h.Release(checkCID)
}

func UpdateRelease_MultiSig(rt vmr.Runtime, h vmr.ActorStateHandle, st MultiSigActorState) {
	newCID := abi.ActorSubstateCID(rt.IpldPut(&st))
	h.UpdateRelease(newCID)
}

func getPendingTxn(ctx context.Context, rt vmr.Runtime, rootCID cid.Cid, txnID TxnID) MultiSigTransaction {
	root, err := hamt.LoadNode(ctx, vmr.AsStore(rt), rootCID)
	if err != nil {
		rt.AbortStateMsg(fmt.Sprintf("Failed to Load Pending Transaction HAMT from store: %v", err))
	}

	var txn MultiSigTransaction
	if err := root.Find(ctx, toKey(txnID), &txn); err != nil {
		rt.AbortStateMsg(fmt.Sprintf("Requested transaction not found in Pending Transaction HAMT: %v", err))
	}
	return txn
}

func setPendingTxn(ctx context.Context, rt vmr.Runtime, rootCID cid.Cid, txnID TxnID, txn MultiSigTransaction) cid.Cid {
	root, err := hamt.LoadNode(ctx, vmr.AsStore(rt), rootCID)
	if err != nil {
		rt.AbortStateMsg(fmt.Sprintf("Failed to Load Pending Transaction HAMT from store: %v", err))
	}
	if err := root.Set(ctx, toKey(txnID), &txn); err != nil {
		rt.AbortStateMsg(fmt.Sprintf("Failed to set transaction in HAMT: %v", err))
	}
	if err := root.Flush(ctx); err != nil {
		rt.AbortStateMsg(fmt.Sprintf("Failed to flush HAMT node: %v", err))
	}
	return rt.IpldPut(root)
}

func deletePendingTxn(ctx context.Context, rt vmr.Runtime, rootCID cid.Cid, txnID TxnID) cid.Cid {
	root, err := hamt.LoadNode(ctx, vmr.AsStore(rt), rootCID)
	if err != nil {
		rt.AbortStateMsg(fmt.Sprintf("Failed to Load Pending Transaction HAMT from store: %v", err))
	}
	if err := root.Delete(ctx, toKey(txnID)); err != nil {
		rt.AbortStateMsg(fmt.Sprintf("Failed to remove transation from HAMT: %v", err))
	}
	if err := root.Flush(ctx); err != nil {
		rt.AbortStateMsg(fmt.Sprintf("Failed to flush HAMT node: %v", err))
	}
	return rt.IpldPut(root)
}

// convert a TxnID to a HAMT key.
func toKey(txnID TxnID) string {
	txnKey := make([]byte, 0, binary.MaxVarintLen64)
	binary.PutVarint(txnKey, int64(txnID))
	return string(txnKey)
}
