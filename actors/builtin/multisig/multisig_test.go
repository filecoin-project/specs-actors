package multisig_test

import (
	"context"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/multisig"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
)

func TestConstruction(t *testing.T) {
	actor := multisig.MultiSigActor{}

	receiver := newIDAddr(t, 100)
	anne := newIDAddr(t, 101)
	bob := newIDAddr(t, 102)
	charlie := newIDAddr(t, 103)

	builder := mock.NewBuilder(context.Background(), receiver).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("simple construction", func(t *testing.T) {
		rt := builder.Build(t)
		params := multisig.ConstructorParams{
			Signers:               []addr.Address{anne, bob, charlie},
			NumApprovalsThreshold: 2,
			UnlockDuration:        0,
		}

		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		ret := rt.Call(actor.Constructor, &params).(*adt.EmptyValue)
		assert.Equal(t, adt.EmptyValue{}, *ret)
		rt.Verify()

		var st multisig.MultiSigActorState
		rt.GetState(&st)
		assert.Equal(t, params.Signers, st.Signers)
		assert.Equal(t, params.NumApprovalsThreshold, st.NumApprovalsThreshold)
		assert.Equal(t, abi.NewTokenAmount(0), st.InitialBalance)
		assert.Equal(t, abi.ChainEpoch(0), st.UnlockDuration)
		assert.Equal(t, abi.ChainEpoch(0), st.StartEpoch)
		txns := adt.AsMap(rt.Store(), st.PendingTxns)
		keys, err := txns.CollectKeys()
		require.NoError(t, err)
		assert.Empty(t, keys)
	})

	t.Run("construction with vesting", func(t *testing.T) {
		rt := builder.WithEpoch(1234).Build(t)
		params := multisig.ConstructorParams{
			Signers:               []addr.Address{anne, bob, charlie},
			NumApprovalsThreshold: 3,
			UnlockDuration:        100,
		}
		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		ret := rt.Call(actor.Constructor, &params).(*adt.EmptyValue)
		assert.Equal(t, adt.EmptyValue{}, *ret)
		rt.Verify()

		var st multisig.MultiSigActorState
		rt.GetState(&st)
		assert.Equal(t, params.Signers, st.Signers)
		assert.Equal(t, params.NumApprovalsThreshold, st.NumApprovalsThreshold)
		assert.Equal(t, abi.NewTokenAmount(0), st.InitialBalance)
		assert.Equal(t, abi.ChainEpoch(100), st.UnlockDuration)
		assert.Equal(t, abi.ChainEpoch(1234), st.StartEpoch)
		// assert no transactions
	})
}

func TestPropose(t *testing.T) {
	actor := msActorHarness{multisig.MultiSigActor{}, t}

	receiver := newIDAddr(t, 100)
	anne := newIDAddr(t, 101)
	bob := newIDAddr(t, 102)
	chuck := newIDAddr(t, 103)

	const noUnlockDuration = int64(0)
	var sendValue = abi.NewTokenAmount(10)
	var nilParams = abi.MethodParams{}
	var signers = []addr.Address{anne, bob}

	builder := mock.NewBuilder(context.Background(), receiver).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("simple propose", func(t *testing.T) {
		const numApprovals = int64(2)
		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, signers...)
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, chuck, sendValue, builtin.MethodSend, nilParams)

		// the transaction remains awaiting second approval
		actor.assertTransactions(rt, multisig.MultiSigTransaction{
			To:       chuck,
			Value:    sendValue,
			Method:   builtin.MethodSend,
			Params:   nilParams,
			Approved: []addr.Address{anne},
		})
	})

	t.Run("propose with threshold met", func(t *testing.T) {
		const numApprovals = int64(1)

		rt := builder.WithBalance(abi.NewTokenAmount(20), abi.NewTokenAmount(0)).Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, signers...)

		rt.ExpectSend(chuck, builtin.MethodSend, nilParams, sendValue, nil, 0)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, chuck, sendValue, builtin.MethodSend, nilParams)

		// the transaction has been sent and cleaned up
		actor.assertTransactions(rt)
		rt.Verify()
	})

	t.Run("fail propose with threshold met and insufficient balance", func(t *testing.T) {
		const numApprovals = int64(1)
		rt := builder.WithBalance(abi.NewTokenAmount(0), abi.NewTokenAmount(0)).Build(t)
		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			actor.propose(rt, chuck, sendValue, builtin.MethodSend, nilParams)
		})

		// proposal failed since it should have but failed to immediately execute.
		actor.assertTransactions(rt)
		rt.Verify()
	})

	t.Run("fail propose from non-signer", func(t *testing.T) {
		// non-signer address
		richard := newIDAddr(t, 105)
		const numApprovals = int64(2)

		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, signers...)

		rt.SetCaller(richard, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.propose(rt, chuck, sendValue, builtin.MethodSend, nilParams)
		})

		// the transaction is not persisted
		actor.assertTransactions(rt)
		rt.Verify()
	})
}

func TestApprove(t *testing.T) {
	actor := msActorHarness{multisig.MultiSigActor{}, t}

	receiver := newIDAddr(t, 100)
	anne := newIDAddr(t, 101)
	bob := newIDAddr(t, 102)
	chuck := newIDAddr(t, 103)

	const noUnlockDuration = int64(0)
	const numApprovals = int64(2)
	const txnID = int64(0)
	const fakeMethod = abi.MethodNum(42)
	var fakeParams = abi.MethodParams{1, 2, 3, 4, 5}
	var sendValue = abi.NewTokenAmount(10)
	var nilParams = abi.MethodParams{}
	var signers = []addr.Address{anne, bob}

	builder := mock.NewBuilder(context.Background(), receiver).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("simple propose and approval", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, chuck, sendValue, fakeMethod, fakeParams)
		rt.Verify()

		actor.assertTransactions(rt, multisig.MultiSigTransaction{
			To:       chuck,
			Value:    sendValue,
			Method:   fakeMethod,
			Params:   fakeParams,
			Approved: []addr.Address{anne},
		})

		rt.SetBalance(sendValue)
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectSend(chuck, builtin.MethodSend, nilParams, sendValue, nil, 0)
		actor.approve(rt, txnID)
		rt.Verify()

		// Transaction should be removed from actor state after send
		actor.assertTransactions(rt)
	})

	t.Run("fail approve transaction more than once", func(t *testing.T) {
		const numApprovals = int64(2)
		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, chuck, sendValue, builtin.MethodSend, nilParams)
		rt.Verify()

		// anne is going to approve it twice and fail, poor anne.
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		// TODO replace with correct exit code when multisig actor breaks the AbortStateMsg pattern.
		rt.ExpectAbort(exitcode.ErrPlaceholder, func() {
			actor.approve(rt, txnID)
		})
		rt.Verify()

		// Transaction still exists
		actor.assertTransactions(rt, multisig.MultiSigTransaction{
			To:       chuck,
			Value:    sendValue,
			Method:   builtin.MethodSend,
			Params:   nilParams,
			Approved: []addr.Address{anne},
		})
	})

	t.Run("fail approve transaction that does not exist", func(t *testing.T) {
		const dneTxnID = int64(1)
		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, chuck, sendValue, builtin.MethodSend, nilParams)
		rt.Verify()

		// bob is going to approve a transaction that doesn't exist.
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.ErrIllegalState, func() {
			actor.approve(rt, dneTxnID)
		})
		rt.Verify()

		// Transaction was not removed from store.
		actor.assertTransactions(rt, multisig.MultiSigTransaction{
			To:       chuck,
			Value:    sendValue,
			Method:   builtin.MethodSend,
			Params:   nilParams,
			Approved: []addr.Address{anne},
		})
	})

	t.Run("fail to approve transaction by non-signer", func(t *testing.T) {
		// non-signer address
		richard := newIDAddr(t, 105)
		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, chuck, sendValue, builtin.MethodSend, nilParams)

		// richard is going to approve a transaction they are not a signer for.
		rt.SetCaller(richard, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.approve(rt, txnID)
		})
		rt.Verify()

		// Transaction was not removed from store.
		actor.assertTransactions(rt, multisig.MultiSigTransaction{
			To:       chuck,
			Value:    sendValue,
			Method:   builtin.MethodSend,
			Params:   nilParams,
			Approved: []addr.Address{anne},
		})
	})
}

func TestCancel(t *testing.T) {
	actor := msActorHarness{multisig.MultiSigActor{}, t}

	receiver := newIDAddr(t, 100)
	anne := newIDAddr(t, 101)
	bob := newIDAddr(t, 102)
	chuck := newIDAddr(t, 103)

	richard := newIDAddr(t, 104)

	const noUnlockDuration = int64(0)
	const numApprovals = int64(2)
	const txnID = int64(0)
	const fakeMethod = abi.MethodNum(42)
	var fakeParams = abi.MethodParams{1, 2, 3, 4, 5}
	var sendValue = abi.NewTokenAmount(10)
	var signers = []addr.Address{anne, bob}

	builder := mock.NewBuilder(context.Background(), t, receiver).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("simple propose and cancel", func(t *testing.T) {
		rt := builder.Build()

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, signers...)

		// anne proposes a transaction
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, chuck, sendValue, fakeMethod, fakeParams)
		rt.Verify()

		// anne cancels their transaction
		rt.SetBalance(sendValue)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.cancel(rt, txnID)
		rt.Verify()

		// Transaction should be removed from actor state after cancel
		actor.assertTransactions(rt)
	})

	t.Run("signer fails to cancel transaction from another signer", func(t *testing.T) {
		rt := builder.Build()

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, signers...)

		// anne proposes a transaction
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, chuck, sendValue, fakeMethod, fakeParams)
		rt.Verify()

		// bob (a signer) fails to cancel anne's transaction because bob didn't create it, nice try bob.
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.ErrPlaceholder, func() {
			actor.cancel(rt, txnID)
		})
		rt.Verify()

		// Transaction should remain after invalid cancel
		actor.assertTransactions(rt, multisig.MultiSigTransaction{
			To:       chuck,
			Value:    sendValue,
			Method:   fakeMethod,
			Params:   fakeParams,
			Approved: []addr.Address{anne},
		})
	})

	t.Run("fail to cancel transaction when not signer", func(t *testing.T) {
		rt := builder.Build()

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, signers...)

		// anne proposes a transaction
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, chuck, sendValue, fakeMethod, fakeParams)
		rt.Verify()

		// richard (not a signer) fails to cancel anne's transaction because richard isn't a signer, go away richard.
		rt.SetCaller(richard, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.cancel(rt, txnID)
		})
		rt.Verify()

		// Transaction should remain after invalid cancel
		actor.assertTransactions(rt, multisig.MultiSigTransaction{
			To:       chuck,
			Value:    sendValue,
			Method:   fakeMethod,
			Params:   fakeParams,
			Approved: []addr.Address{anne},
		})
	})

	t.Run("fail to cancel a transaction that does not exist", func(t *testing.T) {
		rt := builder.Build()
		const dneTxnID = int64(1)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, signers...)

		// anne proposes a transaction ID: 0
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, chuck, sendValue, fakeMethod, fakeParams)
		rt.Verify()

		// anne fails to cancel a transaction that does not exists ID: 1 (dneTxnID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.ErrIllegalState, func() {
			actor.cancel(rt, dneTxnID)
		})
		rt.Verify()

		// Transaction should remain after invalid cancel
		actor.assertTransactions(rt, multisig.MultiSigTransaction{
			To:       chuck,
			Value:    sendValue,
			Method:   fakeMethod,
			Params:   fakeParams,
			Approved: []addr.Address{anne},
		})
	})
}

//
// Helper methods for calling multisig actor methods
//

type msActorHarness struct {
	multisig.MultiSigActor
	t testing.TB
}

func (h *msActorHarness) constructAndVerify(rt *mock.Runtime, numApprovalsThresh, unlockDuration int64, signers ...addr.Address) {
	constructParams := multisig.ConstructorParams{
		Signers:               signers,
		NumApprovalsThreshold: numApprovalsThresh,
		UnlockDuration:        abi.ChainEpoch(unlockDuration),
	}

	rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
	constructRet := rt.Call(h.MultiSigActor.Constructor, &constructParams).(*adt.EmptyValue)
	assert.Equal(h.t, adt.EmptyValue{}, *constructRet)
	rt.Verify()
}

func (h *msActorHarness) propose(rt *mock.Runtime, to addr.Address, value abi.TokenAmount, method abi.MethodNum, params abi.MethodParams) {
	proposeParams := &multisig.ProposeParams{
		To:     to,
		Value:  value,
		Method: method,
		Params: params,
	}
	rt.Call(h.MultiSigActor.Propose, proposeParams)
}

// TODO In a follow-up, this method should also verify the return value from Approve contains the exit code prescribed in ExpectSend.
// exercise both un/successful sends.
func (h *msActorHarness) approve(rt *mock.Runtime, txnID int64) {
	approveParams := &multisig.TxnIDParams{ID: multisig.TxnID(txnID)}
	rt.Call(h.MultiSigActor.Approve, approveParams)
}

func (h *msActorHarness) cancel(rt *mock.Runtime, txnID int64) {
	cancelParams := &multisig.TxnIDParams{ID: multisig.TxnID(txnID)}
	h.MultiSigActor.Cancel(rt, cancelParams)
}

func (h *msActorHarness) assertTransactions(rt *mock.Runtime, expected ...multisig.MultiSigTransaction) {
	var st multisig.MultiSigActorState
	rt.GetState(&st)

	txns := adt.AsMap(rt.Store(), st.PendingTxns)
	keys, err := txns.CollectKeys()
	assert.NoError(h.t, err)

	require.Equal(h.t, len(expected), len(keys))
	for i, k := range keys {
		var actual multisig.MultiSigTransaction
		found, err := txns.Get(asKey(k), &actual)
		require.NoError(h.t, err)
		assert.True(h.t, found)
		assert.Equal(h.t, expected[i], actual)
	}
}

type key string

func (s key) Key() string {
	return string(s)
}

func asKey(in string) adt.Keyer {
	return key(in)
}

//
// Misc. Utility Functions
//

func newIDAddr(t *testing.T, id uint64) addr.Address {
	address, err := addr.NewIDAddress(id)
	if err != nil {
		t.Fatal(err)
	}
	return address
}
