package paych_test

import (
	"context"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	. "github.com/filecoin-project/specs-actors/actors/builtin/paych"
	"github.com/filecoin-project/specs-actors/actors/crypto"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

func TestPaymentChannelActor_Constructor(t *testing.T) {
	ctx := context.Background()
	actor := pcActorHarness{Actor{}, t}

	newPaychAddr := tutil.NewIDAddr(t, 100)
	payerAddr := tutil.NewIDAddr(t, 101)
	callerAddr := tutil.NewIDAddr(t, 102)

	t.Run("can create a payment channel actor", func(t *testing.T) {
		builder := mock.NewBuilder(ctx, newPaychAddr).
			WithCaller(callerAddr, builtin.InitActorCodeID).
			WithActorType(newPaychAddr, builtin.AccountActorCodeID).
			WithActorType(payerAddr, builtin.AccountActorCodeID)
		rt := builder.Build(t)
		actor.constructAndVerify(rt, payerAddr, newPaychAddr)
		var st State
		rt.GetState(&st)
		assert.Equal(t, newPaychAddr, st.To)
		assert.Equal(t, payerAddr, st.From)
		assert.Empty(t, st.LaneStates)
	})

	testCases := []struct {
		desc string
		newActorAddr addr.Address
		callerCode cid.Cid
		newActorCode cid.Cid
		payerCode cid.Cid
		expExitCode exitcode.ExitCode

	} {
		{ "fails if target (to) is not account actor",
			newPaychAddr,
			builtin.InitActorCodeID,
			builtin.CronActorCodeID,
			builtin.AccountActorCodeID,
			exitcode.ErrIllegalArgument,
		},{ "fails if sender (from) is not account actor",
			newPaychAddr,
			builtin.InitActorCodeID,
			builtin.CronActorCodeID,
			builtin.AccountActorCodeID,
			exitcode.ErrIllegalArgument,
		},{ "fails if addr is not ID type",
			tutil.NewActorAddr(t, "beach blanket babylon"),
			builtin.InitActorCodeID,
			builtin.CronActorCodeID,
			builtin.AccountActorCodeID,
			exitcode.ErrIllegalArgument,
		},
	}
	for _,tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			builder := mock.NewBuilder(ctx, newPaychAddr).
				WithCaller(callerAddr, tc.callerCode).
				WithActorType(newPaychAddr, tc.newActorCode).
				WithActorType(payerAddr, tc.payerCode)
			rt := builder.Build(t)
			rt.ExpectValidateCallerType(builtin.InitActorCodeID)
			rt.ExpectAbort(tc.expExitCode, func() {
				rt.Call(actor.Constructor, &ConstructorParams{To: newPaychAddr})
			})
		})
	}
}

func TestPaymentChannelActor_UpdateChannelState(t *testing.T) {
	ctx := context.Background()
	actor := pcActorHarness{Actor{}, t}

	newPaychAddr := tutil.NewIDAddr(t, 100)
	callerAddr := tutil.NewIDAddr(t, 101)
	payerAddr := tutil.NewIDAddr(t, 102)

	// set up trivial signature verifier and hasher
	versig := func(sig crypto.Signature, signer addr.Address, plaintext []byte) bool { return true }
	hasher := func(data []byte) []byte { return data }

	balance:= abi.NewTokenAmount(100)
	received := abi.NewTokenAmount(0)
	builder := mock.NewBuilder(ctx, newPaychAddr).
		WithBalance(balance, received).
		WithEpoch(abi.ChainEpoch(2)).
		WithCaller(callerAddr, builtin.InitActorCodeID).
		WithActorType(newPaychAddr, builtin.AccountActorCodeID).
		WithActorType(payerAddr, builtin.AccountActorCodeID).
		WithVerifiesSig(versig).
		WithHasher(hasher)


	t.Run("Can add a lane/update actor state", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt, payerAddr, newPaychAddr)
		amt := big.NewInt(10)
		lane := int64(999)
		nonce := int64(1)
		sig := &crypto.Signature{
			Type: crypto.SigTypeBLS,
			Data: []byte("doesn't matter"),
		}
		tl := abi.ChainEpoch(1)
		sv := SignedVoucher{
			TimeLock:  tl,
			Lane:      lane,
			Nonce:     nonce,
			Amount:    amt,
			Signature: sig,
		}
		ucp := &UpdateChannelStateParams{ Sv: sv }

		// payerAddr updates state, creates a lane
		rt.SetCaller(payerAddr, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(payerAddr, newPaychAddr)
		constructRet := rt.Call(actor.UpdateChannelState, ucp).(*adt.EmptyValue)
		assert.Equal(t, adt.EmptyValue{}, *constructRet)
		rt.Verify()

		var st State
		rt.GetState(&st)
		assert.Equal(t, newPaychAddr, st.To)
		assert.Equal(t, payerAddr, st.From)
		assert.Len(t, st.LaneStates, 1)
		ls := st.LaneStates[0]
		assert.Equal(t, amt, ls.Redeemed)
		assert.Equal(t, nonce, ls.Nonce)
		assert.Equal(t, lane, ls.ID)
	})

	t.Run("Fails to update state if too early for voucher", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt, payerAddr, newPaychAddr)
		tl := abi.ChainEpoch(10)
		amt := big.NewInt(9)
		lane := int64(8)
		nonce := int64(7)
		sig := &crypto.Signature{
			Type: crypto.SigTypeBLS,
			Data: []byte("doesn't matter"),
		}
		sv := SignedVoucher{
			TimeLock:  tl,
			Lane:      lane,
			Nonce:     nonce,
			Amount:    amt,
			Signature: sig,
		}
		ucp := &UpdateChannelStateParams{ Sv: sv }

		// payerAddr updates state, creates a lane
		rt.SetCaller(payerAddr, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(payerAddr, newPaychAddr)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.UpdateChannelState, ucp)
		})
	})
}

type pcActorHarness struct {
	Actor
	t testing.TB
}

func (h *pcActorHarness) constructAndVerify(rt *mock.Runtime, sender, receiver addr.Address) {
	params := &ConstructorParams{To: receiver, From: sender}

	rt.ExpectValidateCallerType(builtin.InitActorCodeID)
	constructRet := rt.Call(h.Actor.Constructor, params).(*adt.EmptyValue)
	assert.Equal(h.t, adt.EmptyValue{}, *constructRet)
	rt.Verify()

	var st State
	rt.GetState(&st)
	assert.Equal(h.t, receiver, st.To)
	assert.Equal(h.t, sender, st.From)
	assert.Equal(h.t, abi.NewTokenAmount(0), st.ToSend)
	assert.Equal(h.t, abi.ChainEpoch(0), st.SettlingAt)
	assert.Equal(h.t, abi.ChainEpoch(0), st.MinSettleHeight)
	assert.Len(h.t, st.LaneStates, 0)
}
