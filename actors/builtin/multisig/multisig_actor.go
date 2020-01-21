package multisig

import (
	"context"
	"encoding/binary"
	"fmt"

	addr "github.com/filecoin-project/go-address"
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	autil "github.com/filecoin-project/specs-actors/actors/util"
	cid "github.com/ipfs/go-cid"
	hamt "github.com/ipfs/go-hamt-ipld"
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

type MultiSigApprovalSetHAMT map[TxnID]autil.ActorIDSetHAMT

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
	AuthorizedParties     []addr.Address
	NumApprovalsThreshold int64
	UnlockDuration        abi.ChainEpoch
}

func (a *MultiSigActor) Constructor(rt vmr.Runtime, params *ConstructorParams) {
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
		PendingApprovals:      MultiSigApprovalSetHAMT_Empty(),
	}

	if params.UnlockDuration != 0 {
		st.UnlockDuration = params.UnlockDuration
		st.InitialBalance = rt.ValueReceived()
		st.StartEpoch = rt.CurrEpoch()
	}

	UpdateRelease_MultiSig(rt, h, st)
}

type ProposeParams struct {
	To         addr.Address
	Value      abi.TokenAmount
	Method     abi.MethodNum
	Params     abi.MethodParams
	Expiration abi.ChainEpoch
}

func (a *MultiSigActor) Propose(rt vmr.Runtime, params *ProposeParams) TxnID {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	callerAddr := rt.ImmediateCaller()
	a._rtValidateAuthorizedPartyOrAbort(rt, callerAddr)

	h, st := a.State(rt)
	txnID := st.NextTxnID
	st.NextTxnID += 1

	txn := MultiSigTransaction{
		Proposer:   callerAddr,
		Expiration: params.Expiration,
		To:         params.To,
		Method:     params.Method,
		Params:     params.Params,
		Value:      params.Value,
	}

	st.PendingApprovals[txnID] = autil.ActorIDSetHAMT_Empty()
	st.PendingTxns = setPendingTxn(context.TODO(), rt, st.PendingTxns, txnID, txn)

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

func (a *MultiSigActor) Approve(rt vmr.Runtime, params *TxnIDParams) {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	callerAddr := rt.ImmediateCaller()
	a._rtValidateAuthorizedPartyOrAbort(rt, callerAddr)
	a._rtApproveTransactionOrAbort(rt, callerAddr, params.ID)
}

func (a *MultiSigActor) Cancel(rt vmr.Runtime, params *TxnIDParams) {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	callerAddr := rt.ImmediateCaller()
	a._rtValidateAuthorizedPartyOrAbort(rt, callerAddr)
	// TODO implement cancel logic
}

type AddAuthorizedParty struct {
	AuthorizedParty addr.Address // must be an ID protocol address.
	Increase        bool
}

func (a *MultiSigActor) AddAuthorizedParty(rt vmr.Runtime, params *AddAuthorizedParty) {
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
}

type RemoveAuthorizedParty struct {
	AuthorizedParty addr.Address // must be an ID protocol address.
	Decrease        bool
}

func (a *MultiSigActor) RemoveAuthorizedParty(rt vmr.Runtime, params *RemoveAuthorizedParty) {
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
}

type SwapAuthorizedPartyParams struct {
	From addr.Address
	To   addr.Address
}

func (a *MultiSigActor) SwapAuthorizedParty(rt vmr.Runtime, params *SwapAuthorizedPartyParams) {
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
}

type ChangeNumApprovalsThresholdParams struct {
	NewThreshold int64
}

func (a *MultiSigActor) ChangeNumApprovalsThreshold(rt vmr.Runtime, params *ChangeNumApprovalsThresholdParams) {
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

	txn := getPendingTxn(context.TODO(), rt, st.PendingTxns, txnID)

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
	st.PendingTxns = deletePendingTxn(context.TODO(), rt, st.PendingTxns, txnID)
	delete(st.PendingApprovals, txnID)
	UpdateRelease_MultiSig(rt, h, st)
}

func (a *MultiSigActor) _rtValidateAuthorizedPartyOrAbort(rt Runtime, address addr.Address) {
	AssertMsg(address.Protocol() == addr.ID, "caller address does not have ID")
	actorID, err := addr.IDFromAddress(address)
	autil.Assert(err == nil)

	h, st := a.State(rt)
	if !st.isAuthorizedParty(abi.ActorID(actorID)) {
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
