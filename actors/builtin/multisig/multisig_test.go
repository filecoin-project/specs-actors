package multisig_test

import (
	"context"
	"math/rand"
	"testing"

	addr "github.com/filecoin-project/go-address"
	crypto "github.com/filecoin-project/go-crypto"
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
	actor := multisig.MultiSigActor{}

	receiver := newIDAddr(t, 100)
	anne := newSecpAddr(t, 101)
	bob := newSecpAddr(t, 102)
	chuck := newSecpAddr(t, 103)
	richard := newSecpAddr(t, 105)

	builder := mock.NewBuilder(context.Background(), t, receiver).WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("simple propose", func(t *testing.T) {
		rt := builder.Build()
		constructParams := multisig.ConstructorParams{
			Signers:               []addr.Address{anne, bob},
			NumApprovalsThreshold: 2,
			UnlockDuration:        0,
		}

		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		constructRet := actor.Constructor(rt, &constructParams)
		assert.Equal(t, runtime.EmptyReturn{}, *constructRet)
		rt.Verify()

		// going to assume the state is correct, a method that constructs and verifies would be nice.
		proposeParams := &multisig.ProposeParams{
			To:     chuck,
			Value:  abi.NewTokenAmount(10),
			Method: 0,
			Params: nil,
		}

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		proposeRet := actor.Propose(rt, proposeParams)
		assert.Equal(t, multisig.ProposeReturn{multisig.TxnID(0)}, *proposeRet)
		rt.Verify()

		// validate the actor state and transaction exist
		var st multisig.MultiSigActorState
		rt.Readonly(&st)
		assert.Equal(t, constructParams.Signers, st.Signers)
		assert.Equal(t, constructParams.NumApprovalsThreshold, st.NumApprovalsThreshold)
		assert.Equal(t, abi.NewTokenAmount(0), st.InitialBalance)
		assert.Equal(t, abi.ChainEpoch(0), st.UnlockDuration)
		assert.Equal(t, abi.ChainEpoch(0), st.StartEpoch)

		txns := adt.AsMap(rt.Store(), st.PendingTxns)
		keys, err := txns.CollectKeys()
		require.NoError(t, err)
		// there is exactly one transaction
		assert.Equal(t, 1, len(keys))

		var txn multisig.MultiSigTransaction
		found, err := txns.Get(keys[0], &txn)
		assert.True(t, found)

		// containing these values
		assert.Equal(t, multisig.MultiSigTransaction{
			To:       chuck,
			Value:    abi.NewTokenAmount(10),
			Method:   0,
			Params:   abi.MethodParams{},
			Approved: []addr.Address{anne},
		}, txn)
	})

	t.Run("propose with threshold met", func(t *testing.T) {
		// TODO implement mockrt message send
		t.SkipNow()
		rt := builder.WithBalance(abi.NewTokenAmount(20), abi.NewTokenAmount(0)).Build()
		constructParams := multisig.ConstructorParams{
			Signers:               []addr.Address{anne, bob},
			NumApprovalsThreshold: 1,
			UnlockDuration:        0,
		}

		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		constructRet := actor.Constructor(rt, &constructParams)
		assert.Equal(t, runtime.EmptyReturn{}, *constructRet)
		rt.Verify()

		// going to assume the state is correct, a method that constructs and verifies would be nice.
		proposeParams := &multisig.ProposeParams{
			To:     chuck,
			Value:  abi.NewTokenAmount(10),
			Method: 0,
			Params: nil,
		}

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		actor.Propose(rt, proposeParams)
		rt.Verify()
	})

	t.Run("fail propose with threshold met and insufficient balance", func(t *testing.T) {
		rt := builder.WithBalance(abi.NewTokenAmount(0), abi.NewTokenAmount(0)).Build()
		constructParams := multisig.ConstructorParams{
			Signers:               []addr.Address{anne, bob},
			NumApprovalsThreshold: 1,
			UnlockDuration:        0,
		}

		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		constructRet := actor.Constructor(rt, &constructParams)
		assert.Equal(t, runtime.EmptyReturn{}, *constructRet)
		rt.Verify()

		// going to assume the state is correct, a method that constructs and verifies would be nice.
		proposeParams := &multisig.ProposeParams{
			To:     chuck,
			Value:  abi.NewTokenAmount(10),
			Method: 0,
			Params: nil,
		}

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			actor.Propose(rt, proposeParams)
		})
		rt.Verify()
	})

	t.Run("fail propose from non-signer", func(t *testing.T) {
		rt := builder.Build()
		constructParams := multisig.ConstructorParams{
			Signers:               []addr.Address{anne, bob},
			NumApprovalsThreshold: 2,
			UnlockDuration:        0,
		}

		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		constructRet := actor.Constructor(rt, &constructParams)
		assert.Equal(t, runtime.EmptyReturn{}, *constructRet)
		rt.Verify()

		// going to assume the state is correct, a method that constructs and verifies would be nice.
		proposeParams := &multisig.ProposeParams{
			To:     chuck,
			Value:  abi.NewTokenAmount(10),
			Method: 0,
			Params: nil,
		}

		rt.SetCaller(richard, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.Propose(rt, proposeParams)
		})
		rt.Verify()
	})
}

func newIDAddr(t *testing.T, id uint64) addr.Address {
	address, err := addr.NewIDAddress(id)
	if err != nil {
		t.Fatal(err)
	}
	return address
}

func newSecpAddr(t *testing.T, seed int64) addr.Address {
	randSrc := rand.New(rand.NewSource(seed))
	prv, err := crypto.GenerateKeyFromSeed(randSrc)
	if err != nil {
		t.Fatal(err)
	}
	pk := crypto.PublicKey(prv)
	newAddr, err := addr.NewSecp256k1Address(pk)
	if err != nil {
		t.Fatal(err)
	}
	return newAddr
}
