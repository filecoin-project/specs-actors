package multisig

import (
	"context"
	"fmt"

	dstore "github.com/ipfs/go-datastore"
	hamt "github.com/ipfs/go-hamt-ipld"
	bstore "github.com/ipfs/go-ipfs-blockstore"

	addr "github.com/filecoin-project/go-address"
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	autil "github.com/filecoin-project/specs-actors/actors/util"
	cid "github.com/ipfs/go-cid"
)

func init() {
	bs := bstore.NewBlockstore(dstore.NewMapDatastore())
	cst := hamt.CSTFromBstore(bs)
	nd := hamt.NewNode(cst)
	emptyHAMT, err := cst.Put(context.TODO(), nd)
	if err != nil {
		panic(err)
	}
	EmptyHAMT = emptyHAMT
}

var EmptyHAMT cid.Cid

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

func MultiSigTransactionHAMT_Empty() cid.Cid {
	return EmptyHAMT
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
		PendingTxns:           MultiSigTransactionHAMT_Empty(),
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

	pendingTxnCID, err := setPendingTxns(context.TODO(), rt, txnID, txn)
	if err != nil {
		rt.AbortStateMsg(err.Error())
	}
	st.PendingTxns = pendingTxnCID

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

	txn, err := getPendingTxns(context.TODO(), rt, st.PendingTxns, txnID)
	if err != nil {
		rt.AbortStateMsg(fmt.Sprintf("Requested transaction not found or not matched: %v", err))
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
		// TODO if getPendingTxns returns the hamt node we can save a look up by passing it here can deleting.
		a._rtDeletePendingTransaction(rt, txnID)
	}
}

func (a *MultiSigActor) _rtDeletePendingTransaction(rt Runtime, txnID TxnID) {
	h, st := a.State(rt)
	if err := deletePendingTxns(context.TODO(), rt, st.PendingTxns, txnID); err != nil {
		rt.AbortStateMsg(err.Error())
	}
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

func deletePendingTxns(ctx context.Context, rt vmr.Runtime, rcid cid.Cid, txnID TxnID) error {
	nd, err := hamt.LoadNode(ctx, rtCborStoreWrapper{rt}, rcid)
	if err != nil {
		return err
	}
	return nd.Delete(ctx, string(txnID))
}

func getPendingTxns(ctx context.Context, rt vmr.Runtime, rcid cid.Cid, txnID TxnID) (MultiSigTransaction, error) {
	nd, err := hamt.LoadNode(ctx, rtCborStoreWrapper{rt}, rcid)
	if err != nil {
		return MultiSigTransaction{}, err
	}

	var txn MultiSigTransaction
	// FIXME(frrist) pretty sure string(txnID) is wrong, lets go with it for now
	if err := nd.Find(ctx, string(txnID), &txn); err != nil {
		return MultiSigTransaction{}, err
	}
	return txn, nil
}

func setPendingTxns(ctx context.Context, rt vmr.Runtime, txnID TxnID, txn MultiSigTransaction) (cid.Cid, error) {
	// TODO is there a better place where we can perform this allocation?
	nd := hamt.NewNode(rtCborStoreWrapper{rt})

	// FIXME(frrist) pretty sure string(txnID) is wrong, lets go with it for now, maybe use cbor ints here?
	if err := nd.Set(ctx, string(txnID), &txn); err != nil {
		return cid.Undef, err
	}

	if err := nd.Flush(ctx); err != nil {
		return cid.Undef, err
	}

	return rt.IpldPut(nd), nil
}

type rtCborStoreWrapper struct {
	rt vmr.Runtime
}

func (r rtCborStoreWrapper) Get(ctx context.Context, c cid.Cid, out interface{}) error {
	if !r.rt.IpldGet(c, out) {
		r.rt.AbortStateMsg("not found")
	}
	return nil
}

func (r rtCborStoreWrapper) Put(ctx context.Context, v interface{}) (cid.Cid, error) {
	c := r.rt.IpldPut(v)
	return c, nil
}
