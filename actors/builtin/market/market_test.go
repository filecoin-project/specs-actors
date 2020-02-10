package market_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

func TestActorConstruction(t *testing.T) {
	actor := market.Actor{}
	receiver := tutil.NewIDAddr(t, 100)
	builder := mock.NewBuilder(context.Background(), receiver).
		WithCaller(builtin.SystemActorAddr, builtin.InitActorCodeID)

	t.Run("simple construction", func(t *testing.T) {
		rt := builder.Build(t)

		rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)

		params := adt.EmptyValue{}
		ret := rt.Call(actor.Constructor, &params).(*adt.EmptyValue)

		assert.Equal(t, adt.EmptyValue{}, *ret)
		rt.Verify()
	})
}

func TestAddBalance(t *testing.T) {
	marketActor := tutil.NewIDAddr(t, 100)
	provider := tutil.NewIDAddr(t, 101)
	worker := tutil.NewIDAddr(t, 102)
	client := tutil.NewIDAddr(t, 103)

	var st market.State

	setup := func() (*mock.Runtime, *marketActorTestHarness) {
		builder := mock.NewBuilder(context.Background(), marketActor).
			WithBalance(abi.NewTokenAmount(100), abi.NewTokenAmount(0)).
			WithCaller(builtin.SystemActorAddr, builtin.InitActorCodeID)

		rt := builder.Build(t)

		actor := marketActorTestHarness{t: t}
		actor.constructAndVerify(rt)

		return rt, &actor
	}

	t.Run("adds to provider escrow funds", func(t *testing.T) {
		rt, actor := setup()

		testCases := []struct {
			delta int64
			total int64
		}{
			{10, 10},
			{20, 30},
			{40, 70},
		}

		for _, tc := range testCases {
			rt.SetCaller(provider, builtin.StorageMinerActorCodeID)
			rt.SetReceived(abi.NewTokenAmount(tc.delta))
			actor.expectProviderControlAddress(rt, provider, worker)

			rt.Call(actor.AddBalance, &provider)

			rt.Verify()

			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(tc.total), st.GetEscrowBalance(rt, provider))
		}
	})

	t.Run("adds to non-provider escrow funds", func(t *testing.T) {
		rt, actor := setup()

		testCases := []struct {
			delta int64
			total int64
		}{
			{10, 10},
			{20, 30},
			{40, 70},
		}

		for _, tc := range testCases {
			rt.SetCaller(client, builtin.AccountActorCodeID)
			rt.SetReceived(abi.NewTokenAmount(tc.delta))
			rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)

			rt.Call(actor.AddBalance, &client)

			rt.Verify()

			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(tc.total), st.GetEscrowBalance(rt, client))
		}
	})

}

func TestWithdrawBalance(t *testing.T) {
	actor := marketActorTestHarness{t: t}
	marketActor := tutil.NewIDAddr(t, 100)
	provider := tutil.NewIDAddr(t, 101)
	worker := tutil.NewIDAddr(t, 102)
	client := tutil.NewIDAddr(t, 103)

	var st market.State

	builder := mock.NewBuilder(context.Background(), marketActor).
		WithBalance(abi.NewTokenAmount(100), abi.NewTokenAmount(0)).
		WithCaller(builtin.SystemActorAddr, builtin.InitActorCodeID)

	t.Run("fails with a negative withdraw amount", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		params := market.WithdrawBalanceParams{
			ProviderOrClientAddress: provider,
			Amount:                  abi.NewTokenAmount(-1),
		}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.WithdrawBalance, &params)
		})

		rt.Verify()
	})

	t.Run("withdraws from provider escrow funds", func(t *testing.T) {
		rt := builder.WithBalance(abi.NewTokenAmount(100), abi.NewTokenAmount(0)).Build(t)
		actor.constructAndVerify(rt)
		actor.addProviderFunds(rt, provider, worker, abi.NewTokenAmount(20))

		rt.GetState(&st)
		assert.Equal(t, abi.NewTokenAmount(20), st.GetEscrowBalance(rt, provider))

		rt.SetCaller(provider, builtin.StorageMinerActorCodeID)
		actor.expectProviderControlAddress(rt, provider, worker)
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, abi.NewTokenAmount(0), nil, exitcode.Ok)

		withdrawAmount := abi.NewTokenAmount(1)
		rt.ExpectSend(provider, builtin.MethodSend, nil, withdrawAmount, nil, exitcode.Ok)

		params := market.WithdrawBalanceParams{
			ProviderOrClientAddress: provider,
			Amount:                  withdrawAmount,
		}

		rt.Call(actor.WithdrawBalance, &params)

		rt.Verify()

		rt.GetState(&st)
		assert.Equal(t, abi.NewTokenAmount(19), st.GetEscrowBalance(rt, provider))
	})

	t.Run("withdraws from non-provider escrow funds", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		actor.addParticipantFunds(rt, client, abi.NewTokenAmount(20))

		rt.GetState(&st)
		assert.Equal(t, abi.NewTokenAmount(20), st.GetEscrowBalance(rt, client))

		withdrawAmount := abi.NewTokenAmount(1)

		// setup WithdrawBalance call
		rt.SetCaller(client, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
		params := market.WithdrawBalanceParams{
			ProviderOrClientAddress: client,
			Amount:                  withdrawAmount,
		}

		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, abi.NewTokenAmount(0), nil, exitcode.Ok)
		rt.ExpectSend(client, builtin.MethodSend, nil, withdrawAmount, nil, exitcode.Ok)

		rt.Call(actor.WithdrawBalance, &params)

		rt.Verify()

		rt.GetState(&st)
		assert.Equal(t, abi.NewTokenAmount(19), st.GetEscrowBalance(rt, client))
	})
}

type marketActorTestHarness struct {
	market.Actor
	t testing.TB
}

func (h *marketActorTestHarness) constructAndVerify(rt *mock.Runtime) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)

	params := adt.EmptyValue{}
	ret := rt.Call(h.Constructor, &params).(*adt.EmptyValue)

	assert.Equal(h.t, adt.EmptyValue{}, *ret)
	rt.Verify()
}

// addProviderFunds is a helper method to setup provider market funds
func (h *marketActorTestHarness) addProviderFunds(rt *mock.Runtime, provider address.Address, worker address.Address, amount abi.TokenAmount) {
	rt.SetReceived(amount)
	rt.SetCaller(provider, builtin.StorageMinerActorCodeID)
	h.expectProviderControlAddress(rt, provider, worker)

	rt.Call(h.AddBalance, &provider)

	rt.Verify()
}

// addParticipantFunds is a helper method to setup non-provider storage market participant funds
func (h *marketActorTestHarness) addParticipantFunds(rt *mock.Runtime, addr address.Address, amount abi.TokenAmount) {
	rt.SetReceived(amount)
	rt.SetCaller(addr, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)

	rt.Call(h.AddBalance, &addr)

	rt.Verify()
}

func (h *marketActorTestHarness) expectProviderControlAddress(rt *mock.Runtime, provider address.Address, worker address.Address) {
	rt.ExpectValidateCallerAddr(provider, worker)

	expectRet := &miner.GetControlAddressesReturn{Owner: provider, Worker: worker}

	rt.ExpectSend(
		provider,
		builtin.MethodsMiner.ControlAddresses,
		nil,
		big.Zero(),
		&mock.ReturnWrapper{V: expectRet},
		exitcode.Ok,
	)
}
