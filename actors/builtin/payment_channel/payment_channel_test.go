package payment_channel_test

import (
	"context"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/payment_channel"
	"github.com/filecoin-project/specs-actors/actors/crypto"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

func TestPaymentChannelActor_Constructor(t *testing.T) {
	ctx := context.Background()
	actor := pcActorHarness{payment_channel.PaymentChannelActor{}, t}

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
			rt.Call(actor.Constructor, &payment_channel.ConstructorParams{To: pcaAddr})
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
			rt.Call(actor.Constructor, &payment_channel.ConstructorParams{To: pcaAddr})
		})
	})

	t.Run("fails if addr is not ID type", func(t *testing.T) {
		pcaAddr1 := tutil.NewActorAddr(t, []byte("beach blanket babylon"))
		builder := mock.NewBuilder(ctx, pcaAddr1).
			WithCaller(callerAddr, builtin.AccountActorCodeID).
			WithReceiverType(builtin.AccountActorCodeID)
		rt := builder.Build(t)
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID)
		expMsg := "target address must be an ID-address, [2 - 343095ece71e03006965c10e88314fb7c103f413] is 2"
		rt.ExpectAbortWithMsg(16, expMsg, func() {
			rt.Call(actor.Constructor, &payment_channel.ConstructorParams{To: pcaAddr1})
		})
	})
}

func TestPaymentChannelActor_UpdateChannelState(t *testing.T) {
	ctx := context.Background()
	actor := pcActorHarness{payment_channel.PaymentChannelActor{}, t}

	pcaAddr := tutil.NewIDAddr(t, 100)
	callerAddr := tutil.NewIDAddr(t, 101)
	syscalls := tutil.MockSyscalls{VerifiesSig: true}

	t.Run("Can add a lane", func(t *testing.T) {
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

		tl := abi.ChainEpoch(1)
		amt := big.NewInt(10)
		nonce := int64(1)
		sig := &crypto.Signature{
			Type: crypto.SigTypeBLS,
			Data: []byte("doesn't matter"),
		}
		sv := payment_channel.SignedVoucher{
			TimeLock:  tl,
			Lane:      99999,
			Nonce:     nonce,
			Amount:    amt,
			Signature: sig,
		}
		ucp := &payment_channel.UpdateChannelStateParams{ Sv: sv }

		constructRet := rt.Call(actor.UpdateChannelState, ucp).(*adt.EmptyValue)
		assert.Equal(t, adt.EmptyValue{}, *constructRet)
		rt.Verify()
	})
	t.Run("Can merge lanes", func(t *testing.T) {})
	t.Run("Can add funds to voucher", func(t *testing.T) {})
	t.Run("fails to create lane if not enough funds", func(t *testing.T){})
	t.Run("Fails to update state if too early for voucher", func(t *testing.T) {
		builder := mock.NewBuilder(ctx, pcaAddr).
			WithCaller(callerAddr, builtin.AccountActorCodeID).
			WithReceiverType(builtin.AccountActorCodeID).
			WithSysCalls(&syscalls)

		rt := builder.Build(t)
		actor.constructAndVerify(rt, pcaAddr, callerAddr)

		tl := abi.ChainEpoch(10)
		amt := big.NewInt(9)
		lane := int64(8)
		nonce := int64(7)
		sig := &crypto.Signature{
			Type: crypto.SigTypeBLS,
			Data: []byte("doesn't matter"),
		}
		sv := payment_channel.SignedVoucher{
			TimeLock:  tl,
			Lane:      lane,
			Nonce:     nonce,
			Amount:    amt,
			Signature: sig,
		}
		ucp := &payment_channel.UpdateChannelStateParams{ Sv: sv }

		expectMsg := "cannot use this voucher yet!"
		rt.ExpectAbortWithMsg(exitcode.ErrIllegalArgument, expectMsg, func() {
			rt.Call(actor.UpdateChannelState, ucp)
		})
	})
}

type pcActorHarness struct {
	payment_channel.PaymentChannelActor
	t testing.TB
}

func (h *pcActorHarness) constructAndVerify(rt *mock.Runtime, receiver, caller addr.Address) {
	rt.ExpectValidateCallerType(builtin.AccountActorCodeID)
	constructRet := rt.Call(h.PaymentChannelActor.Constructor, &payment_channel.ConstructorParams{To: receiver}).(*adt.EmptyValue)
	assert.Equal(h.t, adt.EmptyValue{}, *constructRet)
	rt.Verify()

	var st payment_channel.PaymentChannelActorState
	rt.GetState(&st)
	assert.Equal(h.t, receiver, st.To)
	assert.Equal(h.t, caller, st.From)
	assert.Equal(h.t, abi.NewTokenAmount(0), st.ToSend)
	assert.Equal(h.t, abi.ChainEpoch(0), st.SettlingAt)
	assert.Equal(h.t, abi.ChainEpoch(0), st.MinSettleHeight)
	assert.Len(h.t, st.LaneStates, 0)
}
