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
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
	"golang.org/x/xerrors"
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
	resolvedAddrs := make([]addr.Address, 0, len(params.Signers))
	deDupSigners := make(map[addr.Address]struct{}, len(params.Signers))
	for _, signer := range params.Signers {
		resolved, err := resolveToIDAddr(signer, rt.ResolveAddress, rt.Send)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve addr %v to ID addr", signer)

		if _, ok := deDupSigners[resolved]; ok {
			rt.Abortf(exitcode.ErrIllegalArgument, "duplicate signer not allowed: %s", signer)
		}

		resolvedAddrs = append(resolvedAddrs, resolved)
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
	st.Signers = resolvedAddrs
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
	callerAddr := rt.Message().Caller()

	if params.Value.Sign() < 0 {
		rt.Abortf(exitcode.ErrIllegalArgument, "proposed value must be non-negative, was %v", params.Value)
	}

	var txnID TxnID
	var st State
	var txn *Transaction
	var resolvedCaller addr.Address
	rt.State().Transaction(&st, func() {
		proposerIsSigner, resolved, err := isSigner(callerAddr, st.Signers, rt.ResolveAddress, rt.Send)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve address %v to ID address", callerAddr)
		if !proposerIsSigner {
			rt.Abortf(exitcode.ErrForbidden, "%s is not a signer", callerAddr)
		}
		resolvedCaller = resolved

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

	applied, ret, code := a.approveTransaction(rt, resolvedCaller, txnID, txn)

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
	var resolvedCaller addr.Address
	rt.State().Transaction(&st, func() {
		isSigner, resolved, err := isSigner(callerAddr, st.Signers, rt.ResolveAddress, rt.Send)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve to ID address")
		if !isSigner {
			rt.Abortf(exitcode.ErrForbidden, "%s is not a signer", callerAddr)
		}
		resolvedCaller = resolved

		ptx, err := adt.AsMap(adt.AsStore(rt), st.PendingTxns)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load pending transactions")

		txn = getTransaction(rt, ptx, params.ID, params.ProposalHash, true)
	})

	// if the transaction already has enough approvers, execute it without "processing" this approval.
	approved, ret, code := executeTransactionIfApproved(rt, st, params.ID, txn)
	if !approved {
		// if the transaction hasn't already been approved, let's "process" this approval
		// and see if we can execute the transaction
		approved, ret, code = a.approveTransaction(rt, resolvedCaller, params.ID, txn)
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
		callerIsSigner, callerResolved, err := isSigner(callerAddr, st.Signers, rt.ResolveAddress, rt.Send)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve to ID address")
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
		if proposer != callerResolved {
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

	var st State
	rt.State().Transaction(&st, func() {
		isSigner, resolved, err := isSigner(params.Signer, st.Signers, rt.ResolveAddress, rt.Send)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve to ID address")
		if isSigner {
			rt.Abortf(exitcode.ErrForbidden, "%s is already a signer", params.Signer)
		}

		st.Signers = append(st.Signers, resolved)
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

	var st State
	rt.State().Transaction(&st, func() {
		isSigner, resolved, err := isSigner(params.Signer, st.Signers, rt.ResolveAddress, rt.Send)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve to ID address")
		if !isSigner {
			rt.Abortf(exitcode.ErrForbidden, "%s is not a signer", params.Signer)
		}

		if len(st.Signers) == 1 {
			rt.Abortf(exitcode.ErrForbidden, "cannot remove only signer")
		}

		newSigners := make([]addr.Address, 0, len(st.Signers))
		// signers have already been resolved
		for _, s := range st.Signers {
			if resolved != s {
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

	var st State
	rt.State().Transaction(&st, func() {
		fromIsSigner, fromResolved, err := isSigner(params.From, st.Signers, rt.ResolveAddress, rt.Send)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve to ID address")
		if !fromIsSigner {
			rt.Abortf(exitcode.ErrForbidden, "%s is not a signer", params.From)
		}

		toIsSigner, toResolved, err := isSigner(params.To, st.Signers, rt.ResolveAddress, rt.Send)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to resolve to ID address")
		if toIsSigner {
			rt.Abortf(exitcode.ErrIllegalArgument, "%s already a signer", params.To)
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

func (a Actor) approveTransaction(rt vmr.Runtime, resolvedApprover addr.Address, txnID TxnID, txn *Transaction) (bool, []byte, exitcode.ExitCode) {
	var st State
	// abort duplicate approval
	for _, previousApprover := range txn.Approved {
		if previousApprover == rt.Message().Caller() {
			rt.Abortf(exitcode.ErrForbidden, "%s already approved this message", previousApprover)
		}
	}

	// add the caller to the list of approvers
	rt.State().Transaction(&st, func() {
		ptx, err := adt.AsMap(adt.AsStore(rt), st.PendingTxns)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load pending transactions")

		// update approved on the transaction
		txn.Approved = append(txn.Approved, resolvedApprover)
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

type AddressResolveFunc func(address addr.Address) (resolved addr.Address, found bool)
type SendFunc func(toAddr addr.Address, methodNum abi.MethodNum, params vmr.CBORMarshaler, value abi.TokenAmount) (vmr.SendReturn, exitcode.ExitCode)

func isSigner(address addr.Address, signers []addr.Address, resolveFunc AddressResolveFunc, sendFnc SendFunc) (signer bool,
	resolved addr.Address, err error) {
	candidateResolved, err := resolveToIDAddr(address, resolveFunc, sendFnc)
	if err != nil {
		return false, address, xerrors.Errorf("failed to resolve address %v to ID address: %w", address, err)
	}

	// signer addresses have already been resolved
	for _, ap := range signers {
		if ap == candidateResolved {
			return true, candidateResolved, nil
		}
	}

	return false, candidateResolved, nil
}

func resolveToIDAddr(address addr.Address, resolveFunc AddressResolveFunc, sendFunc SendFunc) (addr.Address, error) {
	// if it's already an ID address, we can return immediately
	if address.Protocol() == addr.ID {
		return address, nil
	}

	// if we are able to resolve it to an ID address, return the resolved address
	idAddr, found := resolveFunc(address)
	if found {
		return idAddr, nil
	}

	// send 0 balance to the account so an ID address for it is created and then try to resolve
	_, code := sendFunc(address, builtin.MethodSend, nil, abi.NewTokenAmount(0))
	if !code.IsSuccess() {
		return address, fmt.Errorf("failed to send zero balance to account %v, got code %v", address, code)
	}

	// now try to resolve it to an ID address -> fail if not possible
	idAddr, found = resolveFunc(address)
	if !found {
		return address, fmt.Errorf("failed to resolve address %v to ID address even after sending zero balance", address)
	}

	return idAddr, nil
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
