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

	builder := mock.NewBuilder(context.Background(), t, receiver).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("simple propose", func(t *testing.T) {
		const unlockDuration = int64(0)
		const numApprovals = int64(2)
		const method = int64(0)
		const value = int64(10)

		var nilParams = abi.MethodParams{}
		var signers = []addr.Address{anne, bob}

		rt := builder.Build()

		actor.mustConstructMultisigActor(rt, numApprovals, unlockDuration, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.mustPropose(rt, chuck, value, method, nilParams)

		actor.mustContainTransactions(rt, multisig.MultiSigTransaction{
			To:       chuck,
			Value:    abi.NewTokenAmount(value),
			Method:   abi.MethodNum(method),
			Params:   nilParams,
			Approved: []addr.Address{anne},
		})
	})

	t.Run("propose with threshold met", func(t *testing.T) {
		const unlockDuration = int64(0)
		const numApprovals = int64(1)
		const method = int64(0)
		const value = int64(10)

		var nilParams = abi.MethodParams{}
		var signers = []addr.Address{anne, bob}

		rt := builder.WithBalance(abi.NewTokenAmount(20), abi.NewTokenAmount(0)).Build()

		actor.mustConstructMultisigActor(rt, numApprovals, unlockDuration, signers...)

		rt.ExpectSend(chuck, abi.MethodNum(method), nilParams, abi.NewTokenAmount(value), nil, 0)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.mustPropose(rt, chuck, value, method, nilParams)

		actor.mustContainTransactions(rt)
	})

	t.Run("fail propose with threshold met and insufficient balance", func(t *testing.T) {
		const unlockDuration = int64(0)
		const numApprovals = int64(1)
		const method = int64(0)
		const value = int64(10)

		var signers = []addr.Address{anne, bob}
		var nilParams = abi.MethodParams{}

		rt := builder.WithBalance(abi.NewTokenAmount(0), abi.NewTokenAmount(0)).Build()

		actor.mustConstructMultisigActor(rt, numApprovals, unlockDuration, signers...)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.mustProposeWithExitCode(rt, chuck, value, method, nilParams, exitcode.ErrInsufficientFunds)

		actor.mustContainTransactions(rt, multisig.MultiSigTransaction{
			To:       chuck,
			Value:    abi.NewTokenAmount(value),
			Method:   abi.MethodNum(method),
			Params:   nilParams,
			Approved: []addr.Address{anne},
		})
	})

	t.Run("fail propose from non-signer", func(t *testing.T) {
		// non-signer address
		richard := newIDAddr(t, 105)
		const unlockDuration = int64(0)
		const numApprovals = int64(2)
		const method = int64(0)
		const value = int64(10)

		var signers = []addr.Address{anne, bob}
		var nilParams = abi.MethodParams{}

		rt := builder.Build()

		actor.mustConstructMultisigActor(rt, numApprovals, unlockDuration, signers...)

		rt.SetCaller(richard, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.mustProposeWithExitCode(rt, chuck, value, method, nilParams, exitcode.ErrForbidden)

		actor.mustContainTransactions(rt)
	})
}

//
// Helper methods for calling multisig actor methods
//

type msActorHarness struct {
	multisig.MultiSigActor
	t testing.TB
}

func (h *msActorHarness) mustConstructMultisigActor(rt *mock.Runtime, numApprovalsThresh, unlockDuration int64, signers ...addr.Address) {
	constructParams := multisig.ConstructorParams{
		Signers:               signers,
		NumApprovalsThreshold: numApprovalsThresh,
		UnlockDuration:        abi.ChainEpoch(unlockDuration),
	}

	rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
	constructRet := h.MultiSigActor.Constructor(rt, &constructParams)
	require.Equal(h.t, runtime.EmptyReturn{}, *constructRet)
	rt.Verify()
}

func (h *msActorHarness) mustPropose(rt *mock.Runtime, to addr.Address, value, method int64, params abi.MethodParams) {
	proposeParams := &multisig.ProposeParams{
		To:     to,
		Value:  abi.NewTokenAmount(value),
		Method: abi.MethodNum(method),
		Params: params,
	}
	h.MultiSigActor.Propose(rt, proposeParams)
	rt.Verify()
}

func (h *msActorHarness) mustProposeWithExitCode(rt *mock.Runtime, to addr.Address, value, method int64, params abi.MethodParams, exitCode exitcode.ExitCode) {
	proposeParams := &multisig.ProposeParams{
		To:     to,
		Value:  abi.NewTokenAmount(value),
		Method: abi.MethodNum(method),
		Params: params,
	}

	rt.ExpectAbort(exitCode, func() {
		h.MultiSigActor.Propose(rt, proposeParams)
	})

	rt.Verify()
}

func (h *msActorHarness) mustContainTransactions(rt *mock.Runtime, expected ...multisig.MultiSigTransaction) {
	var st multisig.MultiSigActorState
	rt.Readonly(&st)

	txns := adt.AsMap(rt.Store(), st.PendingTxns)
	keys, err := txns.CollectKeys()
	require.NoError(h.t, err)

	if len(expected) == 0 {
		require.Empty(h.t, keys)
		return
	}

	require.Equal(h.t, len(expected), len(keys))

	for i, k := range keys {
		var actual multisig.MultiSigTransaction
		found, err := txns.Get(k, &actual)
		require.NoError(h.t, err)
		require.True(h.t, found)
		require.Equal(h.t, expected[i], actual)
	}

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
