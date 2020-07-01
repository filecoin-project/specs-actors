package market_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/filecoin-project/go-address"
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)

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
			rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)

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
				rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)

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
		startEpoch := abi.ChainEpoch(10)
		endEpoch := abi.ChainEpoch(20)
		publishEpoch := abi.ChainEpoch(5)

		t.Run("fails with a negative withdraw amount", func(t *testing.T) {
			rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)

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
			rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)

			actor.addProviderFunds(rt, abi.NewTokenAmount(20))

			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(20), st.GetEscrowBalance(rt, provider))

			// worker calls WithdrawBalance, balance is transferred to owner
			withdrawAmount := abi.NewTokenAmount(1)
			actor.withdrawProviderBalance(rt, withdrawAmount, withdrawAmount)

			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(19), st.GetEscrowBalance(rt, provider))
		})

		t.Run("withdraws from non-provider escrow funds", func(t *testing.T) {
			rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)
			actor.addParticipantFunds(rt, client, abi.NewTokenAmount(20))

			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(20), st.GetEscrowBalance(rt, client))

			withdrawAmount := abi.NewTokenAmount(1)
			actor.withdrawClientBalance(rt, client, withdrawAmount, withdrawAmount)

			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(19), st.GetEscrowBalance(rt, client))
		})

		t.Run("client withdrawing more than escrow balance limits to available funds", func(t *testing.T) {
			rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)
			actor.addParticipantFunds(rt, client, abi.NewTokenAmount(20))

			// withdraw amount greater than escrow balance
			withdrawAmount := abi.NewTokenAmount(25)
			expectedAmount := abi.NewTokenAmount(20)
			actor.withdrawClientBalance(rt, client, withdrawAmount, expectedAmount)

			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(0), st.GetEscrowBalance(rt, client))
		})

		t.Run("worker withdrawing more than escrow balance limits to available funds", func(t *testing.T) {
			rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)
			actor.addProviderFunds(rt, abi.NewTokenAmount(20))

			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(20), st.GetEscrowBalance(rt, provider))

			// withdraw amount greater than escrow balance
			withdrawAmount := abi.NewTokenAmount(25)
			actualWithdrawn := abi.NewTokenAmount(20)
			actor.withdrawProviderBalance(rt, withdrawAmount, actualWithdrawn)

			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(0), st.GetEscrowBalance(rt, provider))
		})

		t.Run("balance after withdrawal must ALWAYS be greater than or equal to locked amount", func(t *testing.T) {
			rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)

			// create the deal to publish
			deal := generateDealProposal(client, provider, startEpoch, endEpoch)

			// ensure client and provider have enough funds to lock for the deal
			actor.addParticipantFunds(rt, client, deal.ClientBalanceRequirement())
			actor.addProviderFunds(rt, deal.ProviderBalanceRequirement())

			// publish the deal so that client AND provider collateral is locked
			rt.SetEpoch(publishEpoch)
			actor.publishDeals(rt, []market.DealProposal{deal})
			rt.GetState(&st)
			require.Equal(t, deal.ProviderCollateral, st.GetLockedBalance(rt, provider))
			require.Equal(t, deal.ClientBalanceRequirement(), st.GetLockedBalance(rt, client))

			withDrawAmt := abi.NewTokenAmount(1)
			withDrawableAmt := abi.NewTokenAmount(0)
			// client cannot withdraw any funds since all it's balance is locked
			actor.withdrawClientBalance(rt, client, withDrawAmt, withDrawableAmt)
			//  provider cannot withdraw any funds since all it's balance is locked
			actor.withdrawProviderBalance(rt, withDrawAmt, withDrawableAmt)

			// add some more funds to the provider & ensure withdrawal is limited by the locked funds
			withDrawAmt = abi.NewTokenAmount(30)
			withDrawableAmt = abi.NewTokenAmount(25)
			actor.addProviderFunds(rt, withDrawableAmt)
			actor.withdrawProviderBalance(rt, withDrawAmt, withDrawableAmt)

			// add some more funds to the client & ensure withdrawal is limited by the locked funds
			actor.addParticipantFunds(rt, client, withDrawableAmt)
			actor.withdrawClientBalance(rt, client, withDrawAmt, withDrawableAmt)
		})

		t.Run("worker balance after withdrawal must account for slashed funds", func(t *testing.T) {
			rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)

			// create the deal to publish
			deal := generateDealProposal(client, provider, startEpoch, endEpoch)

			// ensure client and provider have enough funds to lock for the deal
			actor.addParticipantFunds(rt, client, deal.ClientBalanceRequirement())
			actor.addProviderFunds(rt, deal.ProviderBalanceRequirement())

			// publish the deal
			rt.SetEpoch(publishEpoch)
			dealID := actor.publishDeals(rt, []market.DealProposal{deal})[0]

			// activate the deal
			actor.activateDeals(rt, []abi.DealID{dealID}, endEpoch+1, provider)
			st := actor.getDealState(rt, dealID)
			require.EqualValues(t, publishEpoch, st.SectorStartEpoch)

			// slash the deal
			rt.SetEpoch(publishEpoch + 1)
			actor.terminateDeals(rt, []abi.DealID{dealID}, provider)
			st = actor.getDealState(rt, dealID)
			require.EqualValues(t, publishEpoch+1, st.SlashEpoch)

			// provider cannot withdraw any funds since all it's balance is locked
			withDrawAmt := abi.NewTokenAmount(1)
			actualWithdrawn := abi.NewTokenAmount(0)
			actor.withdrawProviderBalance(rt, withDrawAmt, actualWithdrawn)

			// add some more funds to the provider & ensure withdrawal is limited by the locked funds
			actor.addProviderFunds(rt, abi.NewTokenAmount(25))
			withDrawAmt = abi.NewTokenAmount(30)
			actualWithdrawn = abi.NewTokenAmount(25)

			actor.withdrawProviderBalance(rt, withDrawAmt, actualWithdrawn)
		})
	})
}

func TestPublishStorageDeals(t *testing.T) {
	marketActor := tutil.NewIDAddr(t, 100)
	owner := tutil.NewIDAddr(t, 101)
	provider := tutil.NewIDAddr(t, 102)
	worker := tutil.NewIDAddr(t, 103)
	client := tutil.NewIDAddr(t, 104)

	t.Run("publish a deal after activating a previous deal which has a start epoch far in the future", func(t *testing.T) {
		startEpoch := abi.ChainEpoch(1000)
		endEpoch := abi.ChainEpoch(2000)
		publishEpoch := abi.ChainEpoch(1)

		rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)
		deal1 := generateDealProposal(client, provider, startEpoch, endEpoch)

		// ensure client and provider have enough funds to lock for the deal
		actor.addParticipantFunds(rt, client, deal1.ClientBalanceRequirement())
		actor.addProviderFunds(rt, deal1.ProviderBalanceRequirement())

		// publish the deal and activate it
		rt.SetEpoch(publishEpoch)
		deal1ID := actor.publishDeals(rt, []market.DealProposal{deal1})[0]
		actor.activateDeals(rt, []abi.DealID{deal1ID}, endEpoch, provider)
		st := actor.getDealState(rt, deal1ID)
		require.EqualValues(t, publishEpoch, st.SectorStartEpoch)

		// now publish a second deal and activate it
		deal2 := generateDealProposal(client, provider, startEpoch+1, endEpoch+1)
		actor.addParticipantFunds(rt, client, deal2.ClientBalanceRequirement())
		actor.addProviderFunds(rt, deal2.ProviderBalanceRequirement())
		rt.SetEpoch(publishEpoch + 1)
		deal2ID := actor.publishDeals(rt, []market.DealProposal{deal2})[0]
		actor.activateDeals(rt, []abi.DealID{deal2ID}, endEpoch+1, provider)
	})
}

func TestPublishStorageDealsFailures(t *testing.T) {
	marketActor := tutil.NewIDAddr(t, 100)
	owner := tutil.NewIDAddr(t, 101)
	provider := tutil.NewIDAddr(t, 102)
	worker := tutil.NewIDAddr(t, 103)
	client := tutil.NewIDAddr(t, 104)

	// simple failures because of invalid deal params
	{
		tcs := map[string]struct {
			setup                      func(*mock.Runtime, *marketActorTestHarness, *market.DealProposal)
			exitCode                   exitcode.ExitCode
			signatureVerificationError error
		}{
			"deal end after deal start": {
				setup: func(_ *mock.Runtime, _ *marketActorTestHarness, d *market.DealProposal) {
					d.StartEpoch = 10
					d.EndEpoch = 9
				},
				exitCode: exitcode.ErrIllegalArgument,
			},
			"current epoch greater than start epoch": {
				setup: func(rt *mock.Runtime, _ *marketActorTestHarness, d *market.DealProposal) {
					rt.SetEpoch(d.StartEpoch + 1)
				},
				exitCode: exitcode.ErrIllegalArgument,
			},
			"deal duration greater than max deal duration": {
				setup: func(rt *mock.Runtime, _ *marketActorTestHarness, d *market.DealProposal) {
					d.StartEpoch = abi.ChainEpoch(10)
					d.EndEpoch = d.StartEpoch + (1 * builtin.EpochsInYear) + 1
				},
				exitCode: exitcode.ErrIllegalArgument,
			},
			"negative price per epoch": {
				setup: func(rt *mock.Runtime, _ *marketActorTestHarness, d *market.DealProposal) {
					d.StoragePricePerEpoch = abi.NewTokenAmount(-1)
				},
				exitCode: exitcode.ErrIllegalArgument,
			},
			"price per epoch greater than total filecoin": {
				setup: func(rt *mock.Runtime, _ *marketActorTestHarness, d *market.DealProposal) {
					d.StoragePricePerEpoch = big.Add(abi.TotalFilecoin, big.NewInt(1))
				},
				exitCode: exitcode.ErrIllegalArgument,
			},
			"negative provider collateral": {
				setup: func(rt *mock.Runtime, _ *marketActorTestHarness, d *market.DealProposal) {
					d.ProviderCollateral = big.NewInt(-1)
				},
				exitCode: exitcode.ErrIllegalArgument,
			},
			"provider collateral greater than max collateral": {
				setup: func(rt *mock.Runtime, _ *marketActorTestHarness, d *market.DealProposal) {
					d.ProviderCollateral = big.Add(abi.TotalFilecoin, big.NewInt(1))
				},
				exitCode: exitcode.ErrIllegalArgument,
			},
			"negative client collateral": {
				setup: func(rt *mock.Runtime, _ *marketActorTestHarness, d *market.DealProposal) {
					d.ClientCollateral = big.NewInt(-1)
				},
				exitCode: exitcode.ErrIllegalArgument,
			},
			"client collateral greater than max collateral": {
				setup: func(rt *mock.Runtime, _ *marketActorTestHarness, d *market.DealProposal) {
					d.ClientCollateral = big.Add(abi.TotalFilecoin, big.NewInt(1))
				},
				exitCode: exitcode.ErrIllegalArgument,
			},
			"client does not have enough balance for collateral": {
				setup: func(rt *mock.Runtime, a *marketActorTestHarness, d *market.DealProposal) {
					a.addParticipantFunds(rt, client, big.Sub(d.ClientBalanceRequirement(), big.NewInt(1)))
					a.addProviderFunds(rt, d.ProviderCollateral)
				},
				exitCode: exitcode.ErrInsufficientFunds,
			},
			"provider does not have enough balance for collateral": {
				setup: func(rt *mock.Runtime, a *marketActorTestHarness, d *market.DealProposal) {
					a.addParticipantFunds(rt, client, d.ClientBalanceRequirement())
					a.addProviderFunds(rt, big.Sub(d.ProviderCollateral, big.NewInt(1)))
				},
				exitCode: exitcode.ErrInsufficientFunds,
			},
			"unable to resolve client address": {
				setup: func(rt *mock.Runtime, a *marketActorTestHarness, d *market.DealProposal) {
					d.Client = tutil.NewBLSAddr(t, 1)
				},
				exitCode: exitcode.ErrNotFound,
			},
			"signature is invalid": {
				setup: func(rt *mock.Runtime, a *marketActorTestHarness, d *market.DealProposal) {

				},
				exitCode:                   exitcode.ErrIllegalArgument,
				signatureVerificationError: errors.New("error"),
			},
			"no entry for client in locked  balance table": {
				setup: func(rt *mock.Runtime, a *marketActorTestHarness, d *market.DealProposal) {
					a.addProviderFunds(rt, d.ProviderCollateral)
				},
				exitCode: exitcode.ErrIllegalState,
			},
			"no entry for provider in locked  balance table": {
				setup: func(rt *mock.Runtime, a *marketActorTestHarness, d *market.DealProposal) {
					a.addParticipantFunds(rt, client, d.ClientBalanceRequirement())
				},
				exitCode: exitcode.ErrIllegalState,
			},
		}

		for name, tc := range tcs {
			t.Run(name, func(t *testing.T) {
				rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)
				dealProposal := generateDealProposal(client, provider, abi.ChainEpoch(1), abi.ChainEpoch(5))
				tc.setup(rt, actor, &dealProposal)
				params := mkPublishStorageParams(dealProposal)

				rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
				rt.ExpectSend(provider, builtin.MethodsMiner.ControlAddresses, nil, abi.NewTokenAmount(0), &miner.GetControlAddressesReturn{Worker: worker, Owner: owner}, 0)
				rt.SetCaller(worker, builtin.AccountActorCodeID)
				rt.ExpectVerifySignature(crypto.Signature{}, dealProposal.Client, mustCbor(&dealProposal), tc.signatureVerificationError)
				rt.ExpectAbort(tc.exitCode, func() {
					rt.Call(actor.PublishStorageDeals, params)
				})

				rt.Verify()
			})
		}
	}

	// fail when deals have different providers
	{
		t.Run("fail when deals have different providers", func(t *testing.T) {
			rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)
			deal1 := generateDealProposal(client, provider, abi.ChainEpoch(1), abi.ChainEpoch(5))
			actor.addProviderFunds(rt, deal1.ProviderCollateral)
			actor.addParticipantFunds(rt, client, deal1.ClientBalanceRequirement())

			deal2 := generateDealProposal(client, tutil.NewIDAddr(t, 1000), abi.ChainEpoch(1), abi.ChainEpoch(5))
			actor.addProviderFunds(rt, deal2.ProviderCollateral)
			actor.addParticipantFunds(rt, client, deal2.ClientBalanceRequirement())

			params := mkPublishStorageParams(deal1, deal2)

			rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
			rt.ExpectSend(provider, builtin.MethodsMiner.ControlAddresses, nil, abi.NewTokenAmount(0), &miner.GetControlAddressesReturn{Worker: worker, Owner: owner}, 0)
			rt.SetCaller(worker, builtin.AccountActorCodeID)
			rt.ExpectVerifySignature(crypto.Signature{}, deal1.Client, mustCbor(&deal1), nil)
			rt.ExpectVerifySignature(crypto.Signature{}, deal2.Client, mustCbor(&deal2), nil)
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				rt.Call(actor.PublishStorageDeals, params)
			})

			rt.Verify()
		})

		//  failures because of incorrect call params
		t.Run("fail when caller is not of signable type", func(t *testing.T) {
			rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)
			params := mkPublishStorageParams(generateDealProposal(client, provider, abi.ChainEpoch(1), abi.ChainEpoch(5)))
			w := tutil.NewIDAddr(t, 1000)
			rt.SetCaller(w, builtin.StorageMinerActorCodeID)
			rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
			rt.ExpectAbort(exitcode.ErrForbidden, func() {
				rt.Call(actor.PublishStorageDeals, params)
			})
		})

		t.Run("fail when no deals in params", func(t *testing.T) {
			rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)
			params := mkPublishStorageParams()
			rt.SetCaller(worker, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				rt.Call(actor.PublishStorageDeals, params)
			})
		})

		t.Run("fail to resolve provider address", func(t *testing.T) {
			rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)
			deal := generateDealProposal(client, provider, abi.ChainEpoch(1), abi.ChainEpoch(5))
			deal.Provider = tutil.NewBLSAddr(t, 100)

			params := mkPublishStorageParams(deal)
			rt.SetCaller(worker, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
			rt.ExpectAbort(exitcode.ErrNotFound, func() {
				rt.Call(actor.PublishStorageDeals, params)
			})
		})

		t.Run("caller is not the same as the worker address for miner", func(t *testing.T) {
			rt, actor := basicMarketSetup(t, marketActor, owner, provider, worker, client)
			deal := generateDealProposal(client, provider, abi.ChainEpoch(1), abi.ChainEpoch(5))
			params := mkPublishStorageParams(deal)
			rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)
			rt.ExpectSend(provider, builtin.MethodsMiner.ControlAddresses, nil, abi.NewTokenAmount(0), &miner.GetControlAddressesReturn{Worker: tutil.NewIDAddr(t, 999), Owner: owner}, 0)
			rt.SetCaller(worker, builtin.AccountActorCodeID)
			rt.ExpectAbort(exitcode.ErrForbidden, func() {
				rt.Call(actor.PublishStorageDeals, params)
			})

			rt.Verify()
		})
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
	actor.addProviderFunds(rt, abi.NewTokenAmount(10000))
	rt.GetState(&st)
	assert.Equal(t, abi.NewTokenAmount(10000), st.GetEscrowBalance(rt, provider))

	actor.addParticipantFunds(rt, client, abi.NewTokenAmount(10000))

	dealProposal := generateDealProposal(client, provider, abi.ChainEpoch(1), abi.ChainEpoch(5))
	params := &market.PublishStorageDealsParams{Deals: []market.ClientDealProposal{market.ClientDealProposal{Proposal: dealProposal}}}

	// First attempt at publishing the deal should work
	{
		actor.publishDeals(rt, []market.DealProposal{dealProposal})
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

	dealProposal.Label = "foo"

	// Same deal with a different label should work
	{
		actor.publishDeals(rt, []market.DealProposal{dealProposal})
	}
}

type marketActorTestHarness struct {
	market.Actor
	t testing.TB

	owner    address.Address
	worker   address.Address
	provider address.Address
}

func (h *marketActorTestHarness) constructAndVerify(rt *mock.Runtime) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	ret := rt.Call(h.Constructor, nil)
	assert.Nil(h.t, ret)
	rt.Verify()
}

// addProviderFunds is a helper method to setup provider market funds
func (h *marketActorTestHarness) addProviderFunds(rt *mock.Runtime, amount abi.TokenAmount) {
	rt.SetReceived(amount)
	rt.SetCaller(h.owner, builtin.AccountActorCodeID)
	h.expectProviderControlAddressesAndValidateCaller(rt, h.provider, h.owner, h.worker)

	rt.Call(h.AddBalance, &h.provider)

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

func (h *marketActorTestHarness) withdrawProviderBalance(rt *mock.Runtime, withDrawAmt, expectedSend abi.TokenAmount) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	h.expectProviderControlAddressesAndValidateCaller(rt, h.provider, h.owner, h.worker)

	params := market.WithdrawBalanceParams{
		ProviderOrClientAddress: h.provider,
		Amount:                  withDrawAmt,
	}

	rt.ExpectSend(h.owner, builtin.MethodSend, nil, expectedSend, nil, exitcode.Ok)
	rt.Call(h.WithdrawBalance, &params)
	rt.Verify()
}

func (h *marketActorTestHarness) withdrawClientBalance(rt *mock.Runtime, client address.Address, withDrawAmt, expectedSend abi.TokenAmount) {
	rt.SetCaller(client, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
	rt.ExpectSend(client, builtin.MethodSend, nil, expectedSend, nil, exitcode.Ok)

	params := market.WithdrawBalanceParams{
		ProviderOrClientAddress: client,
		Amount:                  withDrawAmt,
	}

	rt.Call(h.WithdrawBalance, &params)
	rt.Verify()
}

func (h *marketActorTestHarness) publishDeals(rt *mock.Runtime, deals []market.DealProposal) []abi.DealID {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
	rt.ExpectSend(
		h.provider,
		builtin.MethodsMiner.ControlAddresses,
		nil,
		big.Zero(),
		&miner.GetControlAddressesReturn{Owner: h.owner, Worker: h.worker},
		exitcode.Ok,
	)

	var params market.PublishStorageDealsParams

	for _, deal := range deals {
		//  create a client proposal with a valid signature
		buf := bytes.Buffer{}
		require.NoError(h.t, deal.MarshalCBOR(&buf), "failed to marshal deal proposal")
		sig := crypto.Signature{Type: crypto.SigTypeBLS, Data: []byte("does not matter")}
		clientProposal := market.ClientDealProposal{deal, sig}
		params.Deals = append(params.Deals, clientProposal)

		// expect a call to verify the above signature
		rt.ExpectVerifySignature(sig, deal.Client, buf.Bytes(), nil)
	}

	ret := rt.Call(h.PublishStorageDeals, &params)
	rt.Verify()

	resp, ok := ret.(*market.PublishStorageDealsReturn)
	require.True(h.t, ok, "unexpected type returned from call to PublishStorageDeals")
	require.Len(h.t, resp.IDs, len(deals))

	return resp.IDs
}

func (h *marketActorTestHarness) activateDeals(rt *mock.Runtime, dealIDs []abi.DealID, sectorExpiry abi.ChainEpoch, minerAddr address.Address) {
	rt.SetCaller(minerAddr, builtin.StorageMinerActorCodeID)
	rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)

	params := &market.ActivateDealsParams{DealIDs: dealIDs, SectorExpiry: sectorExpiry}

	ret := rt.Call(h.ActivateDeals, params)
	rt.Verify()

	require.Nil(h.t, ret)
}

func (h *marketActorTestHarness) getDealState(rt *mock.Runtime, dealID abi.DealID) *market.DealState {
	var st market.State
	rt.GetState(&st)

	states, err := market.AsDealStateArray(adt.AsStore(rt), st.States)
	require.NoError(h.t, err)

	s, found, err := states.Get(dealID)
	require.NoError(h.t, err)
	require.True(h.t, found)
	require.NotNil(h.t, s)

	return s
}

func (h *marketActorTestHarness) terminateDeals(rt *mock.Runtime, dealIDs []abi.DealID, minerAddr address.Address) {
	rt.SetCaller(minerAddr, builtin.StorageMinerActorCodeID)
	rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)

	params := &market.OnMinerSectorsTerminateParams{DealIDs: dealIDs}

	ret := rt.Call(h.OnMinerSectorsTerminate, params)
	rt.Verify()

	require.Nil(h.t, ret)
}

func generateDealProposal(client, provider address.Address, startEpoch, endEpoch abi.ChainEpoch) market.DealProposal {
	pieceCid := tutil.MakeCID("1")
	pieceSize := abi.PaddedPieceSize(2048)
	storagePerEpoch := big.NewInt(10)
	clientCollateral := big.NewInt(10)
	providerCollateral := big.NewInt(10)

	return market.DealProposal{pieceCid, pieceSize, false, client, provider, "label", startEpoch,
		endEpoch, storagePerEpoch, providerCollateral, clientCollateral}
}

func basicMarketSetup(t *testing.T, ma, owner, provider, worker, client address.Address) (*mock.Runtime, *marketActorTestHarness) {
	builder := mock.NewBuilder(context.Background(), ma).
		WithCaller(builtin.SystemActorAddr, builtin.InitActorCodeID).
		WithActorType(owner, builtin.AccountActorCodeID).
		WithActorType(worker, builtin.AccountActorCodeID).
		WithActorType(provider, builtin.StorageMinerActorCodeID).
		WithActorType(client, builtin.AccountActorCodeID)

	rt := builder.Build(t)

	actor := marketActorTestHarness{t: t, owner: owner, worker: worker, provider: provider}
	actor.constructAndVerify(rt)

	return rt, &actor
}

func mkPublishStorageParams(proposals ...market.DealProposal) *market.PublishStorageDealsParams {
	m := &market.PublishStorageDealsParams{}
	for _, p := range proposals {
		m.Deals = append(m.Deals, market.ClientDealProposal{Proposal: p})
	}
	return m
}
