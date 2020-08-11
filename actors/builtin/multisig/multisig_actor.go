package multisig

import (
	"bytes"
	"encoding/binary"
	"fmt"

	addr "github.com/filecoin-project/go-address"
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	. "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type TxnID int64

func (t TxnID) Key() string {
	// convert a TxnID to a HAMT key.
	txnKey := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(txnKey, int64(t))
	return string(txnKey[:n])
}

type Transaction struct {
	To     addr.Address
	Value  abi.TokenAmount
	Method abi.MethodNum
	Params []byte

	// This address at index 0 is the transaction proposer, order of this slice must be preserved.
	Approved []addr.Address
}

// Data for a BLAKE2B-256 to be attached to methods referencing proposals via TXIDs.
// Ensures the existence of a cryptographic reference to the original proposal. Useful
// for offline signers and for protection when reorgs change a multisig TXID.
//
// Requester - The requesting multisig wallet member.
// All other fields - From the "Transaction" struct.
type ProposalHashData struct {
	Requester addr.Address
	To        addr.Address
	Value     abi.TokenAmount
	Method    abi.MethodNum
	Params    []byte
}

type Actor struct{}

func (a Actor) Exports() []interface{} {
	return []interface{}{
		builtin.MethodConstructor: a.Constructor,
		2:                         a.Propose,
		3:                         a.Approve,
		4:                         a.Cancel,
		5:                         a.AddSigner,
		6:                         a.RemoveSigner,
		7:                         a.SwapSigner,
		8:                         a.ChangeNumApprovalsThreshold,
	}
}

var _ abi.Invokee = Actor{}

type ConstructorParams struct {
	Signers               []addr.Address
	NumApprovalsThreshold uint64
	UnlockDuration        abi.ChainEpoch
}

func (a Actor) Constructor(rt vmr.Runtime, params *ConstructorParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.InitActorAddr)

	if len(params.Signers) < 1 {
		rt.Abortf(exitcode.ErrIllegalArgument, "must have at least one signer")
	}

	// resolve signer addresses and do not allow duplicate signers
	resolvedSigners := make([]addr.Address, 0, len(params.Signers))
	deDupSigners := make(map[addr.Address]struct{}, len(params.Signers))
	for _, signer := range params.Signers {
		resolved, err := builtin.ResolveToIDAddr(rt, signer)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve addr %v to ID addr", signer)

		if _, ok := deDupSigners[resolved]; ok {
			rt.Abortf(exitcode.ErrIllegalArgument, "duplicate signer not allowed: %s", signer)
		}

		resolvedSigners = append(resolvedSigners, resolved)
		deDupSigners[resolved] = struct{}{}
	}

	if params.NumApprovalsThreshold > uint64(len(params.Signers)) {
		rt.Abortf(exitcode.ErrIllegalArgument, "must not require more approvals than signers")
	}

	if params.NumApprovalsThreshold < 1 {
		rt.Abortf(exitcode.ErrIllegalArgument, "must require at least one approval")
	}

	if params.UnlockDuration < 0 {
		rt.Abortf(exitcode.ErrIllegalArgument, "negative unlock duration disallowed")
	}

	pending, err := adt.MakeEmptyMap(adt.AsStore(rt)).Root()
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to create empty map: %v", err)
	}

	var st State
	st.Signers = resolvedSigners
	st.NumApprovalsThreshold = params.NumApprovalsThreshold
	st.PendingTxns = pending
	st.InitialBalance = abi.NewTokenAmount(0)
	if params.UnlockDuration != 0 {
		st.InitialBalance = rt.Message().ValueReceived()
		st.UnlockDuration = params.UnlockDuration
		st.StartEpoch = rt.CurrEpoch()
	}

	rt.State().Create(&st)
	return nil
}

type ProposeParams struct {
	To     addr.Address
	Value  abi.TokenAmount
	Method abi.MethodNum
	Params []byte
}

type ProposeReturn struct {
	// TxnID is the ID of the proposed transaction
	TxnID TxnID
	// Applied indicates if the transaction was applied as opposed to proposed but not applied due to lack of approvals
	Applied bool
	// Code is the exitcode of the transaction, if Applied is false this field should be ignored.
	Code exitcode.ExitCode
	// Ret is the return vale of the transaction, if Applied is false this field should be ignored.
	Ret []byte
}

func (a Actor) Propose(rt vmr.Runtime, params *ProposeParams) *ProposeReturn {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	proposer := rt.Message().Caller()

	if params.Value.Sign() < 0 {
		rt.Abortf(exitcode.ErrIllegalArgument, "proposed value must be non-negative, was %v", params.Value)
	}

	var txnID TxnID
	var st State
	var txn *Transaction
	rt.State().Transaction(&st, func() {
		if !isSigner(proposer, st.Signers) {
			rt.Abortf(exitcode.ErrForbidden, "%s is not a signer", proposer)
		}

		ptx, err := adt.AsMap(adt.AsStore(rt), st.PendingTxns)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load pending transactions")

		txnID = st.NextTxnID
		st.NextTxnID += 1
		txn = &Transaction{
			To:       params.To,
			Value:    params.Value,
			Method:   params.Method,
			Params:   params.Params,
			Approved: []addr.Address{},
		}

		if err := ptx.Put(txnID, txn); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to put transaction for propose: %v", err)
		}

		st.PendingTxns, err = ptx.Root()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to flush pending transactions")
	})

	applied, ret, code := a.approveTransaction(rt, txnID, txn)

	// Note: this transaction ID may not be stable across chain re-orgs.
	// The proposal hash may be provided as a stability check when approving.
	return &ProposeReturn{
		TxnID:   txnID,
		Applied: applied,
		Code:    code,
		Ret:     ret,
	}
}

type TxnIDParams struct {
	ID           TxnID
	ProposalHash []byte
}

type ApproveReturn struct {
	// Applied indicates if the transaction was applied as opposed to proposed but not applied due to lack of approvals
	Applied bool
	// Code is the exitcode of the transaction, if Applied is false this field should be ignored.
	Code exitcode.ExitCode
	// Ret is the return vale of the transaction, if Applied is false this field should be ignored.
	Ret []byte
}

func (a Actor) Approve(rt vmr.Runtime, params *TxnIDParams) *ApproveReturn {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	callerAddr := rt.Message().Caller()

	var st State
	var txn *Transaction
	rt.State().Transaction(&st, func() {
		callerIsSigner := isSigner(callerAddr, st.Signers)
		if !callerIsSigner {
			rt.Abortf(exitcode.ErrForbidden, "%s is not a signer", callerAddr)
		}

		ptx, err := adt.AsMap(adt.AsStore(rt), st.PendingTxns)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load pending transactions")

		txn = getTransaction(rt, ptx, params.ID, params.ProposalHash, true)
	})

	// if the transaction already has enough approvers, execute it without "processing" this approval.
	approved, ret, code := executeTransactionIfApproved(rt, st, params.ID, txn)
	if !approved {
		// if the transaction hasn't already been approved, let's "process" this approval
		// and see if we can execute the transaction
		approved, ret, code = a.approveTransaction(rt, params.ID, txn)
	}

	return &ApproveReturn{
		Applied: approved,
		Code:    code,
		Ret:     ret,
	}
}

func (a Actor) Cancel(rt vmr.Runtime, params *TxnIDParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	callerAddr := rt.Message().Caller()

	var st State
	rt.State().Transaction(&st, func() {
		callerIsSigner := isSigner(callerAddr, st.Signers)
		if !callerIsSigner {
			rt.Abortf(exitcode.ErrForbidden, "%s is not a signer", callerAddr)
		}

		ptx, err := adt.AsMap(adt.AsStore(rt), st.PendingTxns)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load pending txns")

		txn, err := getPendingTransaction(ptx, params.ID)
		if err != nil {
			rt.Abortf(exitcode.ErrNotFound, "failed to get transaction for cancel: %v", err)
		}
		proposer := txn.Approved[0]
		if proposer != callerAddr {
			rt.Abortf(exitcode.ErrForbidden, "Cannot cancel another signers transaction")
		}

		// confirm the hashes match
		calculatedHash, err := ComputeProposalHash(&txn, rt.Syscalls().HashBlake2b)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to compute proposal hash for %v", params.ID)
		if params.ProposalHash != nil && !bytes.Equal(params.ProposalHash, calculatedHash[:]) {
			rt.Abortf(exitcode.ErrIllegalState, "hash does not match proposal params (ensure requester is an ID address)")
		}

		err = ptx.Delete(params.ID)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to delete pending transaction")

		st.PendingTxns, err = ptx.Root()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to flush pending transactions")
	})
	return nil
}

type AddSignerParams struct {
	Signer   addr.Address
	Increase bool
}

func (a Actor) AddSigner(rt vmr.Runtime, params *AddSignerParams) *adt.EmptyValue {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.Message().Receiver())
	resolvedNewSigner, err := builtin.ResolveToIDAddr(rt, params.Signer)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve address %v", params.Signer)

	var st State
	rt.State().Transaction(&st, func() {
		isSigner := isSigner(resolvedNewSigner, st.Signers)
		if isSigner {
			rt.Abortf(exitcode.ErrForbidden, "%s is already a signer", resolvedNewSigner)
		}

		st.Signers = append(st.Signers, resolvedNewSigner)
		if params.Increase {
			st.NumApprovalsThreshold = st.NumApprovalsThreshold + 1
		}
	})
	return nil
}

type RemoveSignerParams struct {
	Signer   addr.Address
	Decrease bool
}

func (a Actor) RemoveSigner(rt vmr.Runtime, params *RemoveSignerParams) *adt.EmptyValue {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.Message().Receiver())
	resolvedOldSigner, err := builtin.ResolveToIDAddr(rt, params.Signer)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve address %v", params.Signer)

	var st State
	rt.State().Transaction(&st, func() {
		isSigner := isSigner(resolvedOldSigner, st.Signers)
		if !isSigner {
			rt.Abortf(exitcode.ErrForbidden, "%s is not a signer", resolvedOldSigner)
		}

		if len(st.Signers) == 1 {
			rt.Abortf(exitcode.ErrForbidden, "cannot remove only signer")
		}

		newSigners := make([]addr.Address, 0, len(st.Signers))
		// signers have already been resolved
		for _, s := range st.Signers {
			if resolvedOldSigner != s {
				newSigners = append(newSigners, s)
			}
		}

		// if the number of signers is below the threshold after removing the given signer,
		// we should decrease the threshold by 1. This means that decrease should NOT be set to false
		// in such a scenario.
		if !params.Decrease && uint64(len(st.Signers)-1) < st.NumApprovalsThreshold {
			rt.Abortf(exitcode.ErrIllegalArgument, "can't reduce signers to %d below threshold %d with decrease=false", len(st.Signers)-1, st.NumApprovalsThreshold)
		}

		if params.Decrease {
			st.NumApprovalsThreshold = st.NumApprovalsThreshold - 1
		}
		st.Signers = newSigners
	})

	return nil
}

type SwapSignerParams struct {
	From addr.Address
	To   addr.Address
}

func (a Actor) SwapSigner(rt vmr.Runtime, params *SwapSignerParams) *adt.EmptyValue {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.Message().Receiver())

	fromResolved, err := builtin.ResolveToIDAddr(rt, params.From)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve from address %v", params.From)

	toResolved, err := builtin.ResolveToIDAddr(rt, params.To)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve to address %v", params.To)

	var st State
	rt.State().Transaction(&st, func() {
		fromIsSigner := isSigner(fromResolved, st.Signers)
		if !fromIsSigner {
			rt.Abortf(exitcode.ErrForbidden, "from addr %s is not a signer", fromResolved)
		}

		toIsSigner := isSigner(toResolved, st.Signers)
		if toIsSigner {
			rt.Abortf(exitcode.ErrIllegalArgument, "%s already a signer", toResolved)
		}

		newSigners := make([]addr.Address, 0, len(st.Signers))
		for _, s := range st.Signers {
			if s != fromResolved {
				newSigners = append(newSigners, s)
			}
		}
		newSigners = append(newSigners, toResolved)
		st.Signers = newSigners
	})

	return nil
}

type ChangeNumApprovalsThresholdParams struct {
	NewThreshold uint64
}

func (a Actor) ChangeNumApprovalsThreshold(rt vmr.Runtime, params *ChangeNumApprovalsThresholdParams) *adt.EmptyValue {
	// Can only be called by the multisig wallet itself.
	rt.ValidateImmediateCallerIs(rt.Message().Receiver())

	var st State
	rt.State().Transaction(&st, func() {
		if params.NewThreshold == 0 || params.NewThreshold > uint64(len(st.Signers)) {
			rt.Abortf(exitcode.ErrIllegalArgument, "New threshold value not supported")
		}

		st.NumApprovalsThreshold = params.NewThreshold
	})
	return nil
}

func (a Actor) approveTransaction(rt vmr.Runtime, txnID TxnID, txn *Transaction) (bool, []byte, exitcode.ExitCode) {
	caller := rt.Message().Caller()

	var st State
	// abort duplicate approval
	for _, previousApprover := range txn.Approved {
		if previousApprover == caller {
			rt.Abortf(exitcode.ErrForbidden, "%s already approved this message", previousApprover)
		}
	}

	// add the caller to the list of approvers
	rt.State().Transaction(&st, func() {
		ptx, err := adt.AsMap(adt.AsStore(rt), st.PendingTxns)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load pending transactions")

		// update approved on the transaction
		txn.Approved = append(txn.Approved, caller)
		err = ptx.Put(txnID, txn)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to put transaction %v for approval", txnID)

		st.PendingTxns, err = ptx.Root()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to flush pending transactions")
	})

	return executeTransactionIfApproved(rt, st, txnID, txn)
}

func getTransaction(rt vmr.Runtime, ptx *adt.Map, txnID TxnID, proposalHash []byte, checkHash bool) *Transaction {
	var txn Transaction

	// get transaction from the state trie
	var err error
	txn, err = getPendingTransaction(ptx, txnID)
	if err != nil {
		rt.Abortf(exitcode.ErrNotFound, "failed to get transaction for approval: %v", err)
	}

	// confirm the hashes match
	if checkHash {
		calculatedHash, err := ComputeProposalHash(&txn, rt.Syscalls().HashBlake2b)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to compute proposal hash for %v", txnID)
		if proposalHash != nil && !bytes.Equal(proposalHash, calculatedHash[:]) {
			rt.Abortf(exitcode.ErrIllegalArgument, "hash does not match proposal params (ensure requester is an ID address)")
		}
	}

	return &txn
}

func executeTransactionIfApproved(rt vmr.Runtime, st State, txnID TxnID, txn *Transaction) (bool, []byte, exitcode.ExitCode) {
	var out vmr.CBORBytes
	var code exitcode.ExitCode
	applied := false

	thresholdMet := uint64(len(txn.Approved)) >= st.NumApprovalsThreshold
	if thresholdMet {
		if err := st.assertAvailable(rt.CurrentBalance(), txn.Value, rt.CurrEpoch()); err != nil {
			rt.Abortf(exitcode.ErrInsufficientFunds, "insufficient funds unlocked: %v", err)
		}

		var ret vmr.SendReturn
		// A sufficient number of approvals have arrived and sufficient funds have been unlocked: relay the message and delete from pending queue.
		ret, code = rt.Send(
			txn.To,
			txn.Method,
			vmr.CBORBytes(txn.Params),
			txn.Value,
		)
		applied = true

		// Pass the return value through uninterpreted with the expectation that serializing into a CBORBytes never fails
		// since it just copies the bytes.
		err := ret.Into(&out)
		builtin.RequireNoErr(rt, err, exitcode.ErrSerialization, "failed to deserialize result")

		// This could be rearranged to happen inside the first state transaction, before the send().
		rt.State().Transaction(&st, func() {
			ptx, err := adt.AsMap(adt.AsStore(rt), st.PendingTxns)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load pending transactions")

			if err := ptx.Delete(txnID); err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "failed to delete transaction for cleanup: %v", err)
			}

			st.PendingTxns, err = ptx.Root()
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to flush pending transactions")
		})
	}

	return applied, out, code
}

func isSigner(address addr.Address, signers []addr.Address) bool {
	AssertMsg(address.Protocol() == addr.ID, "address %v passed to isSigner must be a resolved address", address)
	// signer addresses have already been resolved
	for _, signer := range signers {
		if signer == address {
			return true
		}
	}

	return false
}

// Computes a digest of a proposed transaction. This digest is used to confirm identity of the transaction
// associated with an ID, which might change under chain re-orgs.
func ComputeProposalHash(txn *Transaction, hash func([]byte) [32]byte) ([]byte, error) {
	hashData := ProposalHashData{
		Requester: txn.Approved[0],
		To:        txn.To,
		Value:     txn.Value,
		Method:    txn.Method,
		Params:    txn.Params,
	}

	data, err := hashData.Serialize()
	if err != nil {
		return nil, fmt.Errorf("failed to construct multisig approval hash: %w", err)
	}

	hashResult := hash(data)
	return hashResult[:], nil
}

func (phd *ProposalHashData) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := phd.MarshalCBOR(buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
