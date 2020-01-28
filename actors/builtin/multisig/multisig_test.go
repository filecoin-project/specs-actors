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
	"github.com/filecoin-project/specs-actors/actors/runtime"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/actors/serde"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
)

func TestConstruction(t *testing.T) {
	actor := multisig.MultiSigActor{}

	receiver := newIDAddr(t, 100)
	anne := newIDAddr(t, 101)
	bob := newIDAddr(t, 102)
	charlie := newIDAddr(t, 103)

	builder := mock.NewBuilder(context.Background(), t, receiver).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("simple construction", func(t *testing.T) {
		rt := builder.Build()
		params := multisig.ConstructorParams{
			Signers:               []addr.Address{anne, bob, charlie},
			NumApprovalsThreshold: 2,
			UnlockDuration:        0,
		}

		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		ret := actor.Constructor(rt, &params)
		assert.Equal(t, runtime.EmptyReturn{}, *ret)
		rt.Verify()

		var st multisig.MultiSigActorState
		rt.Readonly(&st)
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
		rt := builder.WithEpoch(1234).Build()
		params := multisig.ConstructorParams{
			Signers:               []addr.Address{anne, bob, charlie},
			NumApprovalsThreshold: 3,
			UnlockDuration:        100,
		}
		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		ret := actor.Constructor(rt, &params)
		assert.Equal(t, runtime.EmptyReturn{}, *ret)
		rt.Verify()

		var st multisig.MultiSigActorState
		rt.Readonly(&st)
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

	builder := mock.NewBuilder(context.Background(), t, receiver).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("simple propose", func(t *testing.T) {
		const numApprovals = int64(2)

		rt := builder.Build()

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

		rt := builder.WithBalance(abi.NewTokenAmount(20), abi.NewTokenAmount(0)).Build()

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

		rt := builder.WithBalance(abi.NewTokenAmount(0), abi.NewTokenAmount(0)).Build()

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			actor.propose(rt, chuck, sendValue, builtin.MethodSend, nilParams)
		})

		// the final approval was ignored until the balance is sufficient
		actor.assertTransactions(rt, multisig.MultiSigTransaction{
			To:       chuck,
			Value:    sendValue,
			Method:   builtin.MethodSend,
			Params:   nilParams,
			Approved: []addr.Address{anne},
		})
		rt.Verify()
	})

	t.Run("fail propose from non-signer", func(t *testing.T) {
		// non-signer address
		richard := newIDAddr(t, 105)
		const numApprovals = int64(2)

		rt := builder.Build()

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
	var sendValue = abi.NewTokenAmount(10)
	var nilParams = abi.MethodParams{}
	var signers = []addr.Address{anne, bob}

	builder := mock.NewBuilder(context.Background(), t, receiver).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("simple propose and approval", func(t *testing.T) {
		rt := builder.Build()

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		serParams := serde.MustSerialize(&multisig.ChangeNumApprovalsThresholdParams{NewThreshold: 1000})
		actor.propose(rt, chuck, sendValue, builtin.Method_MultiSigActor_ChangeNumApprovalsThreshold, serParams)
		rt.Verify()

		actor.assertTransactions(rt, multisig.MultiSigTransaction{
			To:       chuck,
			Value:    sendValue,
			Method:   builtin.Method_MultiSigActor_ChangeNumApprovalsThreshold,
			Params:   serParams,
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
		rt.Verify()
	})

	t.Run("fail approve transaction more than once", func(t *testing.T) {
		const numApprovals = int64(2)
		rt := builder.Build()

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
		rt := builder.Build()

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
		rt := builder.Build()

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
	constructRet := h.MultiSigActor.Constructor(rt, &constructParams)
	assert.Equal(h.t, runtime.EmptyReturn{}, *constructRet)
	rt.Verify()
}

func (h *msActorHarness) propose(rt *mock.Runtime, to addr.Address, value abi.TokenAmount, method abi.MethodNum, params abi.MethodParams) {
	proposeParams := &multisig.ProposeParams{
		To:     to,
		Value:  value,
		Method: method,
		Params: params,
	}
	h.MultiSigActor.Propose(rt, proposeParams)
}

// TODO In a follow-up, this method should also verify the return value from Approve contains the exit code prescribed in ExpectSend.
// exercise both un/successful sends.
func (h *msActorHarness) approve(rt *mock.Runtime, txnID int64) {
	approveParams := &multisig.TxnIDParams{ID: multisig.TxnID(txnID)}
	h.MultiSigActor.Approve(rt, approveParams)
}

func (h *msActorHarness) assertTransactions(rt *mock.Runtime, expected ...multisig.MultiSigTransaction) {
	var st multisig.MultiSigActorState
	rt.Readonly(&st)

	txns := adt.AsMap(rt.Store(), st.PendingTxns)
	keys, err := txns.CollectKeys()
	assert.NoError(h.t, err)

	assert.Equal(h.t, len(expected), len(keys))
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
