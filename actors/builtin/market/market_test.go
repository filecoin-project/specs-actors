package market_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/crypto"
	"github.com/filecoin-project/specs-actors/actors/runtime"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

func mustCbor(o runtime.CBORMarshaler) []byte {
	buf := new(bytes.Buffer)
	if err := o.MarshalCBOR(buf); err != nil {
		panic(err)
	}

	return buf.Bytes()
}

func TestExports(t *testing.T) {
	mock.CheckActorExports(t, market.Actor{})
}

func TestRemoveAllError(t *testing.T) {
	marketActor := tutil.NewIDAddr(t, 100)
	builder := mock.NewBuilder(context.Background(), marketActor)
	rt := builder.Build(t)
	store := adt.AsStore(rt)

	smm := market.MakeEmptySetMultimap(store)

	if err := smm.RemoveAll(42); err != nil {
		t.Fatalf("expected no error, got: %s", err)
	}
}

func TestMarketActor(t *testing.T) {
	marketActor := tutil.NewIDAddr(t, 100)
	owner := tutil.NewIDAddr(t, 101)
	provider := tutil.NewIDAddr(t, 102)
	worker := tutil.NewIDAddr(t, 103)
	client := tutil.NewIDAddr(t, 104)
	var st market.State

	setup := func() (*mock.Runtime, *marketActorTestHarness) {
		builder := mock.NewBuilder(context.Background(), marketActor).
			WithCaller(builtin.SystemActorAddr, builtin.InitActorCodeID).
			WithActorType(owner, builtin.AccountActorCodeID).
			WithActorType(worker, builtin.AccountActorCodeID).
			WithActorType(provider, builtin.StorageMinerActorCodeID).
			WithActorType(client, builtin.AccountActorCodeID)

		rt := builder.Build(t)

		actor := marketActorTestHarness{t: t}
		actor.constructAndVerify(rt)

		return rt, &actor
	}

	t.Run("simple construction", func(t *testing.T) {
		actor := market.Actor{}
		receiver := tutil.NewIDAddr(t, 100)
		builder := mock.NewBuilder(context.Background(), receiver).
			WithCaller(builtin.SystemActorAddr, builtin.InitActorCodeID)

		rt := builder.Build(t)

		rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)

		ret := rt.Call(actor.Constructor, nil).(*adt.EmptyValue)
		assert.Nil(t, ret)
		rt.Verify()

		store := adt.AsStore(rt)

		emptyMap, err := adt.MakeEmptyMap(store).Root()
		assert.NoError(t, err)

		emptyArray, err := adt.MakeEmptyArray(store).Root()
		assert.NoError(t, err)

		emptyMultiMap, err := market.MakeEmptySetMultimap(store).Root()
		assert.NoError(t, err)

		var state market.State
		rt.GetState(&state)

		assert.Equal(t, emptyArray, state.Proposals)
		assert.Equal(t, emptyArray, state.States)
		assert.Equal(t, emptyMap, state.EscrowTable)
		assert.Equal(t, emptyMap, state.LockedTable)
		assert.Equal(t, abi.DealID(0), state.NextID)
		assert.Equal(t, emptyMultiMap, state.DealOpsByEpoch)
		assert.Equal(t, abi.ChainEpoch(-1), state.LastCron)
	})

	t.Run("AddBalance", func(t *testing.T) {
		t.Run("adds to provider escrow funds", func(t *testing.T) {
			testCases := []struct {
				delta int64
				total int64
			}{
				{10, 10},
				{20, 30},
				{40, 70},
			}

			// Test adding provider funds from both worker and owner address
			for _, callerAddr := range []address.Address{owner, worker} {
				rt, actor := setup()

				for _, tc := range testCases {
					rt.SetCaller(callerAddr, builtin.AccountActorCodeID)
					rt.SetReceived(abi.NewTokenAmount(tc.delta))
					actor.expectProviderControlAddressesAndValidateCaller(rt, provider, owner, worker)

					rt.Call(actor.AddBalance, &provider)

					rt.Verify()

					rt.GetState(&st)
					assert.Equal(t, abi.NewTokenAmount(tc.total), st.GetEscrowBalance(rt, provider))
				}
			}
		})

		t.Run("fails unless called by an account actor", func(t *testing.T) {
			rt, actor := setup()

			rt.SetReceived(abi.NewTokenAmount(10))
			actor.expectProviderControlAddressesAndValidateCaller(rt, provider, owner, worker)

			rt.SetCaller(provider, builtin.StorageMinerActorCodeID)
			rt.ExpectAbort(exitcode.ErrForbidden, func() {
				rt.Call(actor.AddBalance, &provider)
			})

			rt.Verify()
		})

		t.Run("adds to non-provider escrow funds", func(t *testing.T) {
			testCases := []struct {
				delta int64
				total int64
			}{
				{10, 10},
				{20, 30},
				{40, 70},
			}

			// Test adding non-provider funds from both worker and client addresses
			for _, callerAddr := range []address.Address{client, worker} {
				rt, actor := setup()

				for _, tc := range testCases {
					rt.SetCaller(callerAddr, builtin.AccountActorCodeID)
					rt.SetReceived(abi.NewTokenAmount(tc.delta))
					rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)

					rt.Call(actor.AddBalance, &callerAddr)

					rt.Verify()

					rt.GetState(&st)
					assert.Equal(t, abi.NewTokenAmount(tc.total), st.GetEscrowBalance(rt, callerAddr))
				}
			}
		})
	})

	t.Run("WithdrawBalance", func(t *testing.T) {
		t.Run("fails with a negative withdraw amount", func(t *testing.T) {
			rt, actor := setup()

			params := market.WithdrawBalanceParams{
				ProviderOrClientAddress: provider,
				Amount:                  abi.NewTokenAmount(-1),
			}

			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				rt.Call(actor.WithdrawBalance, &params)
			})

			rt.Verify()
		})

		t.Run("withdraws from provider escrow funds and sends to owner", func(t *testing.T) {
			rt, actor := setup()

			actor.addProviderFunds(rt, provider, owner, worker, abi.NewTokenAmount(20))

			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(20), st.GetEscrowBalance(rt, provider))

			// worker calls WithdrawBalance, balance is transferred to owner
			rt.SetCaller(worker, builtin.AccountActorCodeID)
			actor.expectProviderControlAddressesAndValidateCaller(rt, provider, owner, worker)

			withdrawAmount := abi.NewTokenAmount(1)
			rt.ExpectSend(owner, builtin.MethodSend, nil, withdrawAmount, nil, exitcode.Ok)

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
			rt, actor := setup()
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

			rt.ExpectSend(client, builtin.MethodSend, nil, withdrawAmount, nil, exitcode.Ok)

			rt.Call(actor.WithdrawBalance, &params)

			rt.Verify()

			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(19), st.GetEscrowBalance(rt, client))
		})

		t.Run("client withdrawing more than escrow balance limits to available funds", func(t *testing.T) {
			rt, actor := setup()
			actor.addParticipantFunds(rt, client, abi.NewTokenAmount(20))

			// withdraw amount greater than escrow balance
			withdrawAmount := abi.NewTokenAmount(25)

			rt.SetCaller(client, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
			params := market.WithdrawBalanceParams{
				ProviderOrClientAddress: client,
				Amount:                  withdrawAmount,
			}

			rt.ExpectSend(client, builtin.MethodSend, nil, abi.NewTokenAmount(20), nil, exitcode.Ok)

			rt.Call(actor.WithdrawBalance, &params)

			rt.Verify()

			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(0), st.GetEscrowBalance(rt, client))
		})

		t.Run("worker withdrawing more than escrow balance limits to available funds", func(t *testing.T) {
			rt, actor := setup()
			actor.addProviderFunds(rt, provider, owner, worker, abi.NewTokenAmount(20))

			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(20), st.GetEscrowBalance(rt, provider))

			// withdraw amount greater than escrow balance
			withdrawAmount := abi.NewTokenAmount(25)

			rt.SetCaller(worker, builtin.AccountActorCodeID)
			params := market.WithdrawBalanceParams{
				ProviderOrClientAddress: provider,
				Amount:                  withdrawAmount,
			}

			actor.expectProviderControlAddressesAndValidateCaller(rt, provider, owner, worker)
			rt.ExpectSend(owner, builtin.MethodSend, nil, abi.NewTokenAmount(20), nil, exitcode.Ok)

			rt.Call(actor.WithdrawBalance, &params)

			rt.Verify()

			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(0), st.GetEscrowBalance(rt, provider))
		})

		// TODO: withdraws limited by slashing
		// TODO: withdraws limited by locked balance
		// https://github.com/filecoin-project/specs-actors/issues/465
	})
}

func basicMarketSetup(t *testing.T, ma, owner, provider, worker, client address.Address) (*mock.Runtime, *marketActorTestHarness) {
	builder := mock.NewBuilder(context.Background(), ma).
		WithCaller(builtin.SystemActorAddr, builtin.InitActorCodeID).
		WithActorType(owner, builtin.AccountActorCodeID).
		WithActorType(worker, builtin.AccountActorCodeID).
		WithActorType(provider, builtin.StorageMinerActorCodeID).
		WithActorType(client, builtin.AccountActorCodeID)

	rt := builder.Build(t)

	actor := marketActorTestHarness{t: t}
	actor.constructAndVerify(rt)

	return rt, &actor
}

func mkDealProposal(client, prov address.Address) market.DealProposal {
	return market.DealProposal{
		PieceCID:  tutil.MakeCID("a piece cid"),
		PieceSize: abi.PaddedPieceSize(100),
		Client:    client,
		Provider:  prov,

		StartEpoch:           100,
		EndEpoch:             200,
		StoragePricePerEpoch: abi.NewTokenAmount(1),

		ProviderCollateral: abi.NewTokenAmount(1),
		ClientCollateral:   abi.NewTokenAmount(1),
	}
}

func TestMarketActorDeals(t *testing.T) {
	marketActor := tutil.NewIDAddr(t, 100)
	owner := tutil.NewIDAddr(t, 101)
	provider := tutil.NewIDAddr(t, 102)
	worker := tutil.NewIDAddr(t, 103)
	client := tutil.NewIDAddr(t, 104)

	var st market.State

	// Test adding provider funds from both worker and owner address
	rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)

	rt.SetCaller(owner, builtin.AccountActorCodeID)
	rt.SetReceived(abi.NewTokenAmount(10000))
	actor.expectProviderControlAddressesAndValidateCaller(rt, provider, owner, worker)

	rt.Call(actor.AddBalance, &provider)

	rt.Verify()

	rt.GetState(&st)
	assert.Equal(t, abi.NewTokenAmount(10000), st.GetEscrowBalance(rt, provider))

	actor.addParticipantFunds(rt, client, abi.NewTokenAmount(10000))

	rt.SetReceived(abi.NewTokenAmount(10))

	params := &market.PublishStorageDealsParams{
		Deals: []market.ClientDealProposal{
			market.ClientDealProposal{
				Proposal: mkDealProposal(client, provider),
			},
		},
	}

	// First attempt at publishing the deal should work
	{
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectSend(provider, builtin.MethodsMiner.ControlAddresses, nil, abi.NewTokenAmount(0), &miner.GetControlAddressesReturn{Worker: worker, Owner: owner}, 0)

		rt.ExpectVerifySignature(crypto.Signature{}, client, mustCbor(&params.Deals[0].Proposal), nil)

		rt.SetCaller(worker, builtin.AccountActorCodeID)
		rt.Call(actor.PublishStorageDeals, params)
		rt.Verify()
	}

	// Second attempt at publishing the same deal should fail
	{
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectSend(provider, builtin.MethodsMiner.ControlAddresses, nil, abi.NewTokenAmount(0), &miner.GetControlAddressesReturn{Worker: worker, Owner: owner}, 0)

		rt.ExpectVerifySignature(crypto.Signature{}, client, mustCbor(&params.Deals[0].Proposal), nil)
		rt.SetCaller(worker, builtin.AccountActorCodeID)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(actor.PublishStorageDeals, params)
		})

		rt.Verify()
	}

	params.Deals[0].Proposal.Label = "foo"

	// Same deal with a different label should work
	{
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
		rt.ExpectSend(provider, builtin.MethodsMiner.ControlAddresses, nil, abi.NewTokenAmount(0), &miner.GetControlAddressesReturn{Worker: worker, Owner: owner}, 0)

		rt.ExpectVerifySignature(crypto.Signature{}, client, mustCbor(&params.Deals[0].Proposal), nil)
		rt.SetCaller(worker, builtin.AccountActorCodeID)
		rt.Call(actor.PublishStorageDeals, params)

		rt.Verify()
	}
}

type marketActorTestHarness struct {
	market.Actor
	t testing.TB
}

func (h *marketActorTestHarness) constructAndVerify(rt *mock.Runtime) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	ret := rt.Call(h.Constructor, nil)
	assert.Nil(h.t, ret)
	rt.Verify()
}

// addProviderFunds is a helper method to setup provider market funds
func (h *marketActorTestHarness) addProviderFunds(rt *mock.Runtime, provider address.Address, owner address.Address, worker address.Address, amount abi.TokenAmount) {
	rt.SetReceived(amount)
	rt.SetCaller(owner, builtin.AccountActorCodeID)
	h.expectProviderControlAddressesAndValidateCaller(rt, provider, owner, worker)

	rt.Call(h.AddBalance, &provider)

	rt.Verify()

	rt.SetBalance(big.Add(rt.Balance(), amount))
}

// addParticipantFunds is a helper method to setup non-provider storage market participant funds
func (h *marketActorTestHarness) addParticipantFunds(rt *mock.Runtime, addr address.Address, amount abi.TokenAmount) {
	rt.SetReceived(amount)
	rt.SetCaller(addr, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)

	rt.Call(h.AddBalance, &addr)

	rt.Verify()

	rt.SetBalance(big.Add(rt.Balance(), amount))
}

func (h *marketActorTestHarness) expectProviderControlAddressesAndValidateCaller(rt *mock.Runtime, provider address.Address, owner address.Address, worker address.Address) {
	rt.ExpectValidateCallerAddr(owner, worker)

	expectRet := &miner.GetControlAddressesReturn{Owner: owner, Worker: worker}

	rt.ExpectSend(
		provider,
		builtin.MethodsMiner.ControlAddresses,
		nil,
		big.Zero(),
		expectRet,
		exitcode.Ok,
	)
}
