package multisig

import (
	"encoding/binary"
	"fmt"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	hamt "github.com/ipfs/go-hamt-ipld"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	autil "github.com/filecoin-project/specs-actors/actors/util"
)

type TxnID int64

type MultiSigTransaction struct {
	To     addr.Address
	Value  abi.TokenAmount
	Method abi.MethodNum
	Params abi.MethodParams

	// This address at index 0 is the transaction proposer, order of this slice must be preserved.
	Approved []addr.Address
}

type MultiSigActor struct{}

type ConstructorParams struct {
	Signers     []addr.Address
	NumApprovalsThreshold int64
	UnlockDuration        abi.ChainEpoch
}

func (a *MultiSigActor) Constructor(rt vmr.Runtime, params *ConstructorParams) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerIs(builtin.InitActorAddr)

	var signers []addr.Address
	for _, a := range params.Signers {
		signers = append(signers, a)
	}

	var st MultiSigActorState
	rt.State().Transaction(&st, func() interface{} {
		st.Signers = signers
		st.NumApprovalsThreshold = params.NumApprovalsThreshold
		st.PendingTxns = autil.EmptyHAMT
		if params.UnlockDuration != 0 {
			st.UnlockDuration = params.UnlockDuration
			st.InitialBalance = rt.ValueReceived()
			st.StartEpoch = rt.CurrEpoch()
		}
		return nil
	})
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

	var txnID TxnID
	var st MultiSigActorState
	rt.State().Transaction(&st, func() interface{} {
		a.validateSigner(rt, &st, callerAddr)
		txnID := st.NextTxnID
		st.NextTxnID += 1

		st.PendingTxns = setPendingTxn(rt, st.PendingTxns, txnID, MultiSigTransaction{
			To:       params.To,
			Value:    params.Value,
			Method:   params.Method,
			Params:   params.Params,
			Approved: []addr.Address{},
		})
		return nil
	})

	// Proposal implicitly includes approval of a transaction.
	a.approveTransaction(rt, txnID)

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
	var st MultiSigActorState
	rt.State().Transaction(&st, func() interface{} {
		a.validateSigner(rt, &st, callerAddr)
		return nil
	})
	a.approveTransaction(rt, params.ID)
	return &vmr.EmptyReturn{}
}

func (a *MultiSigActor) Cancel(rt vmr.Runtime, params *TxnIDParams) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	callerAddr := rt.ImmediateCaller()

	var st MultiSigActorState
	rt.State().Transaction(&st, func() interface{} {
		a.validateSigner(rt, &st, callerAddr)
		txn := getPendingTxn(rt, st.PendingTxns, params.ID)
		proposer := txn.Approved[0]
		if proposer != callerAddr {
			rt.AbortStateMsg("Cannot cancel another signers transaction")
		}

		st.PendingTxns = deletePendingTxn(rt, st.PendingTxns, params.ID)
		return nil
	})
	return &vmr.EmptyReturn{}
}

type AddSigner struct {
	Signer addr.Address // must be an ID protocol address.
	Increase        bool
}

func (a *MultiSigActor) AddSigner(rt vmr.Runtime, params *AddSigner) *vmr.EmptyReturn {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.CurrReceiver())

	var st MultiSigActorState
	rt.State().Transaction(&st, func() interface{} {
		if st.isSigner(params.Signer) {
			rt.AbortStateMsg("party is already a signer")
		}
		st.Signers = append(st.Signers, params.Signer)
		if params.Increase {
			st.NumApprovalsThreshold = st.NumApprovalsThreshold + 1
		}
		return nil
	})
	return &vmr.EmptyReturn{}
}

type RemoveSigner struct {
	Signer addr.Address // must be an ID protocol address.
	Decrease        bool
}

func (a *MultiSigActor) RemoveSigner(rt vmr.Runtime, params *RemoveSigner) *vmr.EmptyReturn {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.CurrReceiver())

	var st MultiSigActorState
	rt.State().Transaction(&st, func() interface{} {
		if !st.isSigner(params.Signer) {
			rt.AbortStateMsg("Party not found")
		}

		newSigners := make([]addr.Address, 0, len(st.Signers))
		for _, s := range st.Signers {
			if s != params.Signer {
				newSigners = append(newSigners, s)
			}
		}
		if params.Decrease || int64(len(st.Signers)-1) < st.NumApprovalsThreshold {
			st.NumApprovalsThreshold = st.NumApprovalsThreshold - 1
		}
		st.Signers = newSigners
		return nil
	})

	return &vmr.EmptyReturn{}
}

type SwapSignerParams struct {
	From addr.Address
	To   addr.Address
}

func (a *MultiSigActor) SwapSigner(rt vmr.Runtime, params *SwapSignerParams) *vmr.EmptyReturn {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.CurrReceiver())

	var st MultiSigActorState
	rt.State().Transaction(&st, func() interface{} {
		if !st.isSigner(params.From) {
			rt.AbortStateMsg("Party not found")
		}

		if !st.isSigner(params.To) {
			rt.AbortStateMsg("Party already present")
		}

		newSigners := make([]addr.Address, 0, len(st.Signers))
		for _, s := range st.Signers {
			if s != params.From {
				newSigners = append(newSigners, s)
			}
		}
		newSigners = append(newSigners, params.To)
		st.Signers = newSigners
		return nil
	})

	return &vmr.EmptyReturn{}
}

type ChangeNumApprovalsThresholdParams struct {
	NewThreshold int64
}

func (a *MultiSigActor) ChangeNumApprovalsThreshold(rt vmr.Runtime, params *ChangeNumApprovalsThresholdParams) *vmr.EmptyReturn {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.CurrReceiver())

	var st MultiSigActorState
	rt.State().Transaction(&st, func() interface{} {
		if params.NewThreshold <= 0 || params.NewThreshold > int64(len(st.Signers)) {
			rt.AbortStateMsg("New threshold value not supported")
		}

		st.NumApprovalsThreshold = params.NewThreshold
		return nil
	})
	return &vmr.EmptyReturn{}
}

func (a *MultiSigActor) approveTransaction(rt vmr.Runtime, txnID TxnID) {
	var st MultiSigActorState
	var txn MultiSigTransaction
	rt.State().Transaction(&st, func() interface{} {
		txn := getPendingTxn(rt, st.PendingTxns, txnID)

		// abort duplicate approval
		for _, previousApprover := range txn.Approved {
			if previousApprover == rt.ImmediateCaller() {
				rt.AbortStateMsg("already approved this message")
			}
		}
		// update approved on the transaction
		txn.Approved = append(txn.Approved, rt.ImmediateCaller())
		st.PendingTxns = setPendingTxn(rt, st.PendingTxns, txnID, txn)
		return nil
	})

	thresholdMet := int64(len(txn.Approved)) >= st.NumApprovalsThreshold
	if thresholdMet {
		if !st._hasAvailable(rt.CurrentBalance(), txn.Value, rt.CurrEpoch()) {
			rt.Abort(exitcode.ErrInsufficientFunds, "insufficient funds unlocked")
		}

		// A sufficient number of approvals have arrived and sufficient funds have been unlocked: relay the message and delete from pending queue.
		_, code := rt.Send(
			txn.To,
			txn.Method,
			txn.Params,
			txn.Value,
		)
		// The exit code is explicitly ignored. It's ok for the subcall to fail.
		_ = code

		// This could be rearranged to happen inside the first state transaction, before the send().
		rt.State().Transaction(&st, func() interface{} {
			a.deletePendingTransaction(rt, &st, txnID)
			return nil
		})
	}
}

func (a *MultiSigActor) deletePendingTransaction(rt vmr.Runtime, st *MultiSigActorState, txnID TxnID) {
	st.PendingTxns = deletePendingTxn(rt, st.PendingTxns, txnID)
}

func (a *MultiSigActor) validateSigner(rt vmr.Runtime, st *MultiSigActorState, address addr.Address) {
	if !st.isSigner(address) {
		rt.Abort(exitcode.ErrForbidden, "party not a signer")
	}
}

func getPendingTxn(rt vmr.Runtime, rootCID cid.Cid, txnID TxnID) MultiSigTransaction {
	root, err := hamt.LoadNode(rt.Context(), vmr.AsStore(rt), rootCID)
	if err != nil {
		rt.AbortStateMsg(fmt.Sprintf("Failed to Load Pending Transaction HAMT from store: %v", err))
	}

	var txn MultiSigTransaction
	if err := root.Find(rt.Context(), toKey(txnID), &txn); err != nil {
		rt.AbortStateMsg(fmt.Sprintf("Requested transaction not found in Pending Transaction HAMT: %v", err))
	}
	return txn
}

func setPendingTxn(rt vmr.Runtime, rootCID cid.Cid, txnID TxnID, txn MultiSigTransaction) cid.Cid {
	root, err := hamt.LoadNode(rt.Context(), vmr.AsStore(rt), rootCID)
	if err != nil {
		rt.AbortStateMsg(fmt.Sprintf("Failed to Load Pending Transaction HAMT from store: %v", err))
	}
	if err := root.Set(rt.Context(), toKey(txnID), &txn); err != nil {
		rt.AbortStateMsg(fmt.Sprintf("Failed to set transaction in HAMT: %v", err))
	}
	if err := root.Flush(rt.Context()); err != nil {
		rt.AbortStateMsg(fmt.Sprintf("Failed to flush HAMT node: %v", err))
	}
	return rt.IpldPut(root)
}

func deletePendingTxn(rt vmr.Runtime, rootCID cid.Cid, txnID TxnID) cid.Cid {
	root, err := hamt.LoadNode(rt.Context(), vmr.AsStore(rt), rootCID)
	if err != nil {
		rt.AbortStateMsg(fmt.Sprintf("Failed to Load Pending Transaction HAMT from store: %v", err))
	}
	if err := root.Delete(rt.Context(), toKey(txnID)); err != nil {
		rt.AbortStateMsg(fmt.Sprintf("Failed to remove transation from HAMT: %v", err))
	}
	if err := root.Flush(rt.Context()); err != nil {
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
