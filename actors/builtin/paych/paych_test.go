package paych_test

import (
	"context"
	"testing"

	addr "github.com/filecoin-project/go-address"
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

	pcaAddr := tutil.NewIDAddr(t, 100)
	callerAddr := tutil.NewIDAddr(t, 101)

	t.Run("can create a payment channel actor", func(t *testing.T) {
		builder := mock.NewBuilder(ctx, pcaAddr).
			WithCaller(callerAddr, builtin.AccountActorCodeID).
			WithReceiverType(builtin.AccountActorCodeID)

		rt := builder.Build(t)
		actor.constructAndVerify(rt, pcaAddr, callerAddr)
	})

	t.Run("fails if caller is not account actor", func(t *testing.T) {
		builder := mock.NewBuilder(ctx, pcaAddr).WithCaller(callerAddr, builtin.CronActorCodeID)
		rt := builder.Build(t)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID)
		rt.ExpectAbort(18, func() {
			rt.Call(actor.Constructor, &ConstructorParams{To: pcaAddr})
		})
	})

	t.Run("fails if target is not account actor", func(t *testing.T) {
		builder := mock.NewBuilder(ctx, pcaAddr).
			WithCaller(callerAddr, builtin.AccountActorCodeID).
			WithReceiverType(builtin.CronActorCodeID)
		rt := builder.Build(t)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID)
		expMsg := "target actor [0 - 64] must be an account (bafkqadlgnfwc6mjpmfrwg33vnz2a), was bafkqactgnfwc6mjpmnzg63q"
		rt.ExpectAbortWithMsg(16, expMsg, func() {
			rt.Call(actor.Constructor, &ConstructorParams{To: pcaAddr})
		})
	})

	t.Run("fails if addr is not ID type", func(t *testing.T) {
		pcaAddr1 := tutil.NewActorAddr(t, "beach blanket babylon")
		builder := mock.NewBuilder(ctx, pcaAddr1).
			WithCaller(callerAddr, builtin.AccountActorCodeID).
			WithReceiverType(builtin.AccountActorCodeID)
		rt := builder.Build(t)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID)
		expMsg := "target address must be an ID-address, [2 - 343095ece71e03006965c10e88314fb7c103f413] is 2"
		rt.ExpectAbortWithMsg(16, expMsg, func() {
			rt.Call(actor.Constructor, &ConstructorParams{To: pcaAddr1})
		})
	})
}

func TestPaymentChannelActor_UpdateChannelState(t *testing.T) {
	ctx := context.Background()
	actor := pcActorHarness{Actor{}, t}

	pcaAddr := tutil.NewIDAddr(t, 100)
	callerAddr := tutil.NewIDAddr(t, 101)
	syscalls := tutil.MockSyscalls{VerifiesSig: true}

	t.Run("Can add a lane/update actor state", func(t *testing.T) {
		balance:= abi.NewTokenAmount(100)
		received := abi.NewTokenAmount(0)
		builder := mock.NewBuilder(ctx, pcaAddr).
			WithBalance(balance, received).
			WithEpoch(abi.ChainEpoch(2)).
			WithCaller(callerAddr, builtin.AccountActorCodeID).
			WithReceiverType(builtin.AccountActorCodeID).
			WithSysCalls(&syscalls)

		rt := builder.Build(t)
		actor.constructAndVerify(rt, pcaAddr, callerAddr)
		rt.ExpectValidateCallerAddr(callerAddr, pcaAddr)

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

		constructRet := rt.Call(actor.UpdateChannelState, ucp).(*adt.EmptyValue)
		assert.Equal(t, adt.EmptyValue{}, *constructRet)
		rt.Verify()

		var st State
		rt.GetState(&st)
		assert.Equal(t, pcaAddr, st.To)
		assert.Equal(t, callerAddr, st.From)
		assert.Len(t, st.LaneStates, 1)
		ls := st.LaneStates[0]
		assert.Equal(t, amt, ls.Redeemed)
		assert.Equal(t, nonce, ls.Nonce)
		assert.Equal(t, lane, ls.ID)
	})

	t.Run("Fails to update state if too early for voucher", func(t *testing.T) {
		builder := mock.NewBuilder(ctx, pcaAddr).
			WithCaller(callerAddr, builtin.AccountActorCodeID).
			WithReceiverType(builtin.AccountActorCodeID).
			WithSysCalls(&syscalls)

		rt := builder.Build(t)
		actor.constructAndVerify(rt, pcaAddr, callerAddr)
		rt.ExpectValidateCallerAddr(callerAddr, pcaAddr)

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

		expectMsg := "cannot use this voucher yet!"
		rt.ExpectAbortWithMsg(exitcode.ErrIllegalArgument, expectMsg, func() {
			rt.Call(actor.UpdateChannelState, ucp)
		})
	})
}

type pcActorHarness struct {
	Actor
	t testing.TB
}

func (h *pcActorHarness) constructAndVerify(rt *mock.Runtime, receiver, caller addr.Address) {
	rt.ExpectValidateCallerType(builtin.AccountActorCodeID)
	constructRet := rt.Call(h.Actor.Constructor, &ConstructorParams{To: receiver}).(*adt.EmptyValue)
	assert.Equal(h.t, adt.EmptyValue{}, *constructRet)
	rt.Verify()

	var st State
	rt.GetState(&st)
	assert.Equal(h.t, receiver, st.To)
	assert.Equal(h.t, caller, st.From)
	assert.Equal(h.t, abi.NewTokenAmount(0), st.ToSend)
	assert.Equal(h.t, abi.ChainEpoch(0), st.SettlingAt)
	assert.Equal(h.t, abi.ChainEpoch(0), st.MinSettleHeight)
	assert.Len(h.t, st.LaneStates, 0)
}
