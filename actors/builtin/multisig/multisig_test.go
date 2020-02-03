package multisig_test

import (
	"context"
	"testing"

	addr "github.com/filecoin-project/go-address"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	multisig "github.com/filecoin-project/specs-actors/actors/builtin/multisig"
	runtime "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
	mock "github.com/filecoin-project/specs-actors/support/mock"
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

func TestVesting(t *testing.T) {
	actor := msActorHarness{multisig.MultiSigActor{}, t}

	receiver := newIDAddr(t, 100)
	anne := newIDAddr(t, 101)
	bob := newIDAddr(t, 102)
	charlie := newIDAddr(t, 103)
	darlene := newIDAddr(t, 103)

	const unlockDuration = 10
	var multisigInitialBalance = abi.NewTokenAmount(100)
	var nilParams = runtime.CBORBytes([]byte{})

	builder := mock.NewBuilder(context.Background(), receiver).
		WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID).
		WithEpoch(0).
		// balance 0: current balance of the actor. receive: 100 the amount the multisig actor will be initalized with -- InitialBalance
		WithBalance(multisigInitialBalance, multisigInitialBalance)

	t.Run("happy path full vesting", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, 2, unlockDuration, []addr.Address{anne, bob, charlie}...)

		// anne proposes that darlene receives `multisgiInitialBalance` FIL.
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.SetReceived(big.Zero())
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, darlene, multisigInitialBalance, builtin.MethodSend, nilParams)
		rt.Verify()

		// Advance the epoch s.t. all funds are unlocked.
		rt.SetEpoch(0 + unlockDuration)
		// bob approves annes transaction
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		// expect darlene to receive the transaction proposed by anne.
		rt.ExpectSend(darlene, builtin.MethodSend, nilParams, multisigInitialBalance, nil, exitcode.Ok)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.approve(rt, 0)
		rt.Verify()

	})

	t.Run("partial vesting propose to send half the actor balance when the epoch is hald the unlock duration", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, 2, 10, []addr.Address{anne, bob, charlie}...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.SetReceived(big.Zero())
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, darlene, big.Div(multisigInitialBalance, big.NewInt(2)), builtin.MethodSend, nilParams)
		rt.Verify()

		// set the current balance of the multisig actor to its InitialBalance amount
		rt.SetEpoch(0 + unlockDuration/2)
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		rt.ExpectSend(darlene, builtin.MethodSend, nilParams, big.Div(multisigInitialBalance, big.NewInt(2)), nil, exitcode.Ok)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.approve(rt, 0)
		rt.Verify()

	})

	t.Run("send 150% vesting after 50% of lock period in 3 txs", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, 1, 10, []addr.Address{anne, bob, charlie}...)

		rt.SetEpoch(0 + unlockDuration/2)
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectSend(darlene, builtin.MethodSend, nilParams, big.Div(multisigInitialBalance, big.NewInt(2)), nil, exitcode.Ok)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, darlene, big.Div(multisigInitialBalance, big.NewInt(2)), builtin.MethodSend, nilParams)
		rt.Verify()

		// Should be thrown funds locked
		// set the current balance of the multisig actor to its InitialBalance amount
		rt.SetEpoch(0 + unlockDuration/2)
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		rt.ExpectSend(charlie, builtin.MethodSend, nilParams, big.Div(multisigInitialBalance, big.NewInt(2)), nil, exitcode.Ok)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, charlie, big.Div(multisigInitialBalance, big.NewInt(2)), builtin.MethodSend, nilParams)
		rt.Verify()

		// Should be thrown funds locked either
		// set the current balance of the multisig actor to its InitialBalance amount
		rt.SetEpoch(0 + unlockDuration/2)
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		rt.ExpectSend(bob, builtin.MethodSend, nilParams, big.Div(multisigInitialBalance, big.NewInt(2)), nil, exitcode.Ok)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, bob, big.Div(multisigInitialBalance, big.NewInt(2)), builtin.MethodSend, nilParams)
		rt.Verify()
	})

	t.Run("propose and autoapprove transaction above locked amount fails", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, 1, unlockDuration, []addr.Address{anne, bob, charlie}...)

		rt.SetReceived(big.Zero())
		// this propose will fail since it would send more than the required locked balance and num approvals == 1
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			actor.propose(rt, darlene, abi.NewTokenAmount(100), builtin.MethodSend, nilParams)
		})
		rt.Verify()

		// this will pass since sending below the locked amount is permitted
		rt.SetEpoch(1)
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectSend(darlene, builtin.MethodSend, nilParams, abi.NewTokenAmount(10), nil, 0)
		actor.propose(rt, darlene, abi.NewTokenAmount(10), builtin.MethodSend, nilParams)
		rt.Verify()

	})

	t.Run("fail to vest more than locked amount", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, 2, unlockDuration, []addr.Address{anne, bob, charlie}...)

		rt.SetReceived(big.Zero())
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, darlene, big.Div(multisigInitialBalance, big.NewInt(2)), builtin.MethodSend, nilParams)
		rt.Verify()

		// this propose will fail since it would send more than the required locked balance.
		rt.SetEpoch(1)
		rt.SetCaller(bob, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			actor.approve(rt, 0)
		})
		rt.Verify()
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
	var nilParams = runtime.CBORBytes([]byte{})
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
	var fakeParams = []byte{1, 2, 3, 4, 5}
	var sendValue = abi.NewTokenAmount(10)
	var nilParams = runtime.CBORBytes([]byte{})
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
		rt.ExpectAbort(exitcode.ErrIllegalState, func() {
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
		rt.ExpectAbort(exitcode.ErrNotFound, func() {
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

	richard := newIDAddr(t, 104)
	receiver := newIDAddr(t, 100)
	anne := newIDAddr(t, 101)
	bob := newIDAddr(t, 102)
	chuck := newIDAddr(t, 103)

	const noUnlockDuration = int64(0)
	const numApprovals = int64(2)
	const txnID = int64(0)
	const fakeMethod = abi.MethodNum(42)
	var fakeParams = []byte{1, 2, 3, 4, 5}
	var sendValue = abi.NewTokenAmount(10)
	var signers = []addr.Address{anne, bob}

	builder := mock.NewBuilder(context.Background(), receiver).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("simple propose and cancel", func(t *testing.T) {
		rt := builder.Build(t)

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
		rt := builder.Build(t)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, signers...)

		// anne proposes a transaction
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, chuck, sendValue, fakeMethod, fakeParams)
		rt.Verify()

		// bob (a signer) fails to cancel anne's transaction because bob didn't create it, nice try bob.
		rt.SetCaller(bob, builtin.AccountActorCodeID)
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

	t.Run("fail to cancel transaction when not signer", func(t *testing.T) {
		rt := builder.Build(t)

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
		rt := builder.Build(t)
		const dneTxnID = int64(1)

		actor.constructAndVerify(rt, numApprovals, noUnlockDuration, signers...)

		// anne proposes a transaction ID: 0
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.propose(rt, chuck, sendValue, fakeMethod, fakeParams)
		rt.Verify()

		// anne fails to cancel a transaction that does not exists ID: 1 (dneTxnID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.ErrNotFound, func() {
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

type addSignerTestCase struct {
	desc string

	initialSigners   []addr.Address
	initialApprovals int64

	addSigner addr.Address
	increase  bool

	expectSigners   []addr.Address
	expectApprovals int64
	code            exitcode.ExitCode
}

func TestAddSigner(t *testing.T) {
	actor := msActorHarness{multisig.MultiSigActor{}, t}

	multisigWalletAdd := newIDAddr(t, 100)
	anne := newIDAddr(t, 101)
	bob := newIDAddr(t, 102)
	chuck := newIDAddr(t, 103)
	const noUnlockDuration = int64(0)

	testCases := []addSignerTestCase{
		{
			desc: "happy path add signer",

			initialSigners:   []addr.Address{anne, bob},
			initialApprovals: int64(2),

			addSigner: chuck,
			increase:  false,

			expectSigners:   []addr.Address{anne, bob, chuck},
			expectApprovals: int64(2),
			code:            exitcode.Ok,
		},
		{
			desc: "add signer and increase threshold",

			initialSigners:   []addr.Address{anne, bob},
			initialApprovals: int64(2),

			addSigner: chuck,
			increase:  true,

			expectSigners:   []addr.Address{anne, bob, chuck},
			expectApprovals: int64(3),
			code:            exitcode.Ok,
		},
		{
			desc: "fail to add signer than already exists",

			initialSigners:   []addr.Address{anne, bob, chuck},
			initialApprovals: int64(3),

			addSigner: chuck,
			increase:  false,

			expectSigners:   []addr.Address{anne, bob, chuck},
			expectApprovals: int64(3),
			code:            exitcode.ErrIllegalArgument,
		},
	}

	builder := mock.NewBuilder(context.Background(), multisigWalletAdd).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rt := builder.Build(t)

			actor.constructAndVerify(rt, tc.initialApprovals, noUnlockDuration, tc.initialSigners...)

			rt.SetCaller(multisigWalletAdd, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerAddr(multisigWalletAdd)
			if tc.code != exitcode.Ok {
				rt.ExpectAbort(tc.code, func() {
					actor.addSigner(rt, tc.addSigner, tc.increase)
				})
			} else {
				actor.addSigner(rt, tc.addSigner, tc.increase)
				var st multisig.MultiSigActorState
				rt.Readonly(&st)
				assert.Equal(t, tc.expectSigners, st.Signers)
				assert.Equal(t, tc.expectApprovals, st.NumApprovalsThreshold)
			}
			rt.Verify()
		})
	}
}

type removeSignerTestCase struct {
	desc string

	initialSigners   []addr.Address
	initialApprovals int64

	removeSigner addr.Address
	decrease     bool

	expectSigners   []addr.Address
	expectApprovals int64
	code            exitcode.ExitCode
}

func TestRemoveSigner(t *testing.T) {
	actor := msActorHarness{multisig.MultiSigActor{}, t}

	multisigWalletAdd := newIDAddr(t, 100)
	anne := newIDAddr(t, 101)
	bob := newIDAddr(t, 102)
	chuck := newIDAddr(t, 103)
	richard := newIDAddr(t, 104)

	const noUnlockDuration = int64(0)

	testCases := []removeSignerTestCase{
		{
			desc: "happy path remove signer",

			initialSigners:   []addr.Address{anne, bob, chuck},
			initialApprovals: int64(2),

			removeSigner: chuck,
			decrease:     false,

			expectSigners:   []addr.Address{anne, bob},
			expectApprovals: int64(2),
			code:            exitcode.Ok,
		},
		{
			desc: "remove signer and decrease threshold",

			initialSigners:   []addr.Address{anne, bob, chuck},
			initialApprovals: int64(2),

			removeSigner: chuck,
			decrease:     true,

			expectSigners:   []addr.Address{anne, bob},
			expectApprovals: int64(1),
			code:            exitcode.Ok,
		},
		{
			desc: "remove signer with automatic threshold decrease",

			initialSigners:   []addr.Address{anne, bob, chuck},
			initialApprovals: int64(3),

			removeSigner: chuck,
			decrease:     false,

			expectSigners:   []addr.Address{anne, bob},
			expectApprovals: int64(2),
			code:            exitcode.Ok,
		},
		// TODO this is behaviour is poorly defined: https://github.com/filecoin-project/specs-actors/issues/72
		{
			desc: "remove signer from single singer list",

			initialSigners:   []addr.Address{anne},
			initialApprovals: int64(2),

			removeSigner: anne,
			decrease:     false,

			expectSigners:   nil,
			expectApprovals: int64(1),
			code:            exitcode.Ok,
		},
		{
			desc: "fail to remove non-signer",

			initialSigners:   []addr.Address{anne, bob, chuck},
			initialApprovals: int64(2),

			removeSigner: richard,
			decrease:     false,

			expectSigners:   []addr.Address{anne, bob, chuck},
			expectApprovals: int64(2),
			code:            exitcode.ErrNotFound,
		},
	}

	builder := mock.NewBuilder(context.Background(), multisigWalletAdd).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rt := builder.Build(t)

			actor.constructAndVerify(rt, tc.initialApprovals, noUnlockDuration, tc.initialSigners...)

			rt.SetCaller(multisigWalletAdd, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerAddr(multisigWalletAdd)
			if tc.code != exitcode.Ok {
				rt.ExpectAbort(tc.code, func() {
					actor.removeSigner(rt, tc.removeSigner, tc.decrease)
				})
			} else {
				actor.removeSigner(rt, tc.removeSigner, tc.decrease)
				var st multisig.MultiSigActorState
				rt.Readonly(&st)
				assert.Equal(t, tc.expectSigners, st.Signers)
				assert.Equal(t, tc.expectApprovals, st.NumApprovalsThreshold)
			}
			rt.Verify()
		})
	}
}

type swapTestCase struct {
	desc   string
	to     addr.Address
	from   addr.Address
	expect []addr.Address
	code   exitcode.ExitCode
}

func TestSwapSigners(t *testing.T) {
	actor := msActorHarness{multisig.MultiSigActor{}, t}

	multisigWalletAdd := newIDAddr(t, 100)
	anne := newIDAddr(t, 101)
	bob := newIDAddr(t, 102)
	chuck := newIDAddr(t, 103)
	darlene := newIDAddr(t, 104)

	const noUnlockDuration = int64(0)
	const numApprovals = int64(1)
	var initialSigner = []addr.Address{anne, bob}

	testCases := []swapTestCase{
		{
			desc:   "happy path signer swap",
			to:     chuck,
			from:   bob,
			expect: []addr.Address{anne, chuck},
			code:   exitcode.Ok,
		},
		{
			desc:   "fail to swap when from signer not found",
			to:     chuck,
			from:   darlene,
			expect: []addr.Address{anne, chuck},
			code:   exitcode.ErrNotFound,
		},
		{
			desc:   "fail to swap when to signer already present",
			to:     bob,
			from:   anne,
			expect: []addr.Address{anne, chuck},
			code:   exitcode.ErrIllegalArgument,
		},
	}

	builder := mock.NewBuilder(context.Background(), multisigWalletAdd).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rt := builder.Build(t)

			actor.constructAndVerify(rt, numApprovals, noUnlockDuration, initialSigner...)

			rt.SetCaller(multisigWalletAdd, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerAddr(multisigWalletAdd)
			if tc.code != exitcode.Ok {
				rt.ExpectAbort(tc.code, func() {
					actor.swapSigners(rt, tc.from, tc.to)
				})
			} else {
				actor.swapSigners(rt, tc.from, tc.to)
				var st multisig.MultiSigActorState
				rt.Readonly(&st)
				assert.Equal(t, tc.expect, st.Signers)
			}
			rt.Verify()
		})
	}
}

type thresholdTestCase struct {
	desc             string
	initialThreshold int64
	setThreshold     int64
	code             exitcode.ExitCode
}

func TestChangeThreshold(t *testing.T) {
	actor := msActorHarness{multisig.MultiSigActor{}, t}

	multisigWalletAdd := newIDAddr(t, 100)
	anne := newIDAddr(t, 101)
	bob := newIDAddr(t, 102)
	chuck := newIDAddr(t, 103)

	const noUnlockDuration = int64(0)
	var initialSigner = []addr.Address{anne, bob, chuck}

	testCases := []thresholdTestCase{
		{
			desc:             "happy path decrease threshold",
			initialThreshold: 2,
			setThreshold:     1,
			code:             exitcode.Ok,
		},
		{
			desc:             "happy path simple increase threshold",
			initialThreshold: 2,
			setThreshold:     3,
			code:             exitcode.Ok,
		},
		{
			desc:             "fail to set threshold to zero",
			initialThreshold: 2,
			setThreshold:     0,
			code:             exitcode.ErrIllegalArgument,
		},
		{
			desc:             "fail to set threshold less than zero",
			initialThreshold: 2,
			setThreshold:     -1,
			code:             exitcode.ErrIllegalArgument,
		},
		{
			desc:             "fail to set threshold above number of signers",
			initialThreshold: 2,
			setThreshold:     int64(len(initialSigner) + 1),
			code:             exitcode.ErrIllegalArgument,
		},
		// TODO missing test case that needs definition: https://github.com/filecoin-project/specs-actors/issues/71
		// what happens when threshold is reduced below the number of approvers an existing transaction already ha
	}

	builder := mock.NewBuilder(context.Background(), multisigWalletAdd).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			rt := builder.Build(t)

			actor.constructAndVerify(rt, tc.initialThreshold, noUnlockDuration, initialSigner...)

			rt.SetCaller(multisigWalletAdd, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerAddr(multisigWalletAdd)
			if tc.code != exitcode.Ok {
				rt.ExpectAbort(tc.code, func() {
					actor.changeNumApprovalsThreshold(rt, tc.setThreshold)
				})
			} else {
				actor.changeNumApprovalsThreshold(rt, tc.setThreshold)
				var st multisig.MultiSigActorState
				rt.Readonly(&st)
				assert.Equal(t, tc.setThreshold, st.NumApprovalsThreshold)
			}
			rt.Verify()
		})
	}
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

func (h *msActorHarness) propose(rt *mock.Runtime, to addr.Address, value abi.TokenAmount, method abi.MethodNum, params []byte) {
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
	rt.Call(h.MultiSigActor.Cancel, cancelParams)
}

func (h *msActorHarness) addSigner(rt *mock.Runtime, signer addr.Address, increase bool) {
	addSignerParams := &multisig.AddSignerParams{
		Signer:   signer,
		Increase: increase,
	}
	rt.Call(h.MultiSigActor.AddSigner, addSignerParams)
}

func (h *msActorHarness) removeSigner(rt *mock.Runtime, signer addr.Address, decrease bool) {
	rmSignerParams := &multisig.RemoveSignerParams{
		Signer:   signer,
		Decrease: decrease,
	}
	rt.Call(h.MultiSigActor.RemoveSigner, rmSignerParams)
}

func (h *msActorHarness) swapSigners(rt *mock.Runtime, oldSigner, newSigner addr.Address) {
	swpParams := &multisig.SwapSignerParams{
		From: oldSigner,
		To:   newSigner,
	}
	rt.Call(h.MultiSigActor.SwapSigner, swpParams)
}

func (h *msActorHarness) changeNumApprovalsThreshold(rt *mock.Runtime, newThreshold int64) {
	thrshParams := &multisig.ChangeNumApprovalsThresholdParams{NewThreshold: newThreshold}
	rt.Call(h.MultiSigActor.ChangeNumApprovalsThreshold, thrshParams)
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
		found, err_ := txns.Get(asKey(k), &actual)
		require.NoError(h.t, err_)
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
