package payment_channel_test

import (
	"context"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/payment_channel"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

func TestPaymentChannelActor_Constructor(t *testing.T) {
	ctx := context.Background()
	actor := pcActorHarness{payment_channel.PaymentChannelActor{}, t}

	pcaAddr := tutil.NewIDAddr(t, 100)
	callerAddr:= tutil.NewIDAddr(t, 101)

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
