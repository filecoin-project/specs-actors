package market_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/crypto"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

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

		params := adt.EmptyValue{}
		ret := rt.Call(actor.Constructor, &params).(*adt.EmptyValue)
		assert.Nil(t, ret)
		rt.Verify()

		store := adt.AsStore(rt)

		emptyMap, err := adt.MakeEmptyMap(store)
		assert.NoError(t, err)

		emptyArray, err := adt.MakeEmptyArray(store)
		assert.NoError(t, err)

		emptyMultiMap, err := market.MakeEmptySetMultimap(store)
		assert.NoError(t, err)

		var state market.State
		rt.GetState(&state)

		assert.Equal(t, emptyArray.Root(), state.Proposals)
		assert.Equal(t, emptyArray.Root(), state.States)
		assert.Equal(t, emptyMap.Root(), state.EscrowTable)
		assert.Equal(t, emptyMap.Root(), state.LockedTable)
		assert.Equal(t, abi.DealID(0), state.NextID)
		assert.Equal(t, emptyMultiMap.Root(), state.DealIDsByParty)
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
	})

	t.Run("PublishStorageDeals", func(t *testing.T) {
		t.Run("fails with empty deals", func(t *testing.T) {
			rt, actor := setup()
			params := market.PublishStorageDealsParams{Deals: nil}

			rt.SetCaller(worker, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				rt.Call(actor.PublishStorageDeals, &params)
			})

			rt.Verify()
		})

		t.Run("fails if deal provider doesn't match the caller", func(t *testing.T) {
			rt, actor := setup()

			providerActor2 := tutil.NewActorAddr(t, "provider 2")
			worker2 := tutil.NewIDAddr(t, 1002)

			params := market.PublishStorageDealsParams{Deals: []market.ClientDealProposal{
				actor.testProposal(providerActor2, client, "test"),
			}}

			rt.SetCaller(worker, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
			actor.expectProviderControlAddresses(rt, providerActor2, owner, worker2)

			rt.ExpectAbort(exitcode.ErrForbidden, func() {
				rt.Call(actor.PublishStorageDeals, &params)
			})

			rt.Verify()
		})

		t.Run("fails with invalid proposals", func(t *testing.T) {
			assertInvalidProposal := func(mutateProposal func(rt *mock.Runtime, dp *market.ClientDealProposal)) {
				rt, actor := setup()
				rt.SetVerifier(fakeVerifier(true))

				p := actor.testProposal(provider, client, "test")

				mutateProposal(rt, &p)

				params := market.PublishStorageDealsParams{Deals: []market.ClientDealProposal{p}}

				rt.SetCaller(worker, builtin.AccountActorCodeID)
				rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
				actor.expectProviderControlAddresses(rt, provider, owner, worker)

				rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
					rt.Call(actor.PublishStorageDeals, &params)
				})

				rt.Verify()
			}

			// deal start and end don't make sense
			assertInvalidProposal(func(rt *mock.Runtime, dp *market.ClientDealProposal) {
				dp.Proposal.StartEpoch = 100
				dp.Proposal.EndEpoch = 90
			})

			// invalid client signature
			assertInvalidProposal(func(rt *mock.Runtime, dp *market.ClientDealProposal) {
				rt.SetVerifier(fakeVerifier(false))
			})

			// deal.StartEpoch is in the past
			assertInvalidProposal(func(rt *mock.Runtime, dp *market.ClientDealProposal) {
				dp.Proposal.StartEpoch = 100
				rt.SetEpoch(101)
			})

			// deal.Duration out of bounds
			assertInvalidProposal(func(rt *mock.Runtime, dp *market.ClientDealProposal) {
				dp.Proposal.EndEpoch = dp.Proposal.StartEpoch + 10001
			})

			// deal.StoragePricePerEpoch out of bounds
			assertInvalidProposal(func(rt *mock.Runtime, dp *market.ClientDealProposal) {
				dp.Proposal.StoragePricePerEpoch = abi.NewTokenAmount(1<<20 + 1)
			})

			// deal.ProviderCollateral out of bounds
			assertInvalidProposal(func(rt *mock.Runtime, dp *market.ClientDealProposal) {
				dp.Proposal.ProviderCollateral = abi.NewTokenAmount(1<<20 + 1)
			})

			// deal.ClientCollateral out of bounds
			assertInvalidProposal(func(rt *mock.Runtime, dp *market.ClientDealProposal) {
				dp.Proposal.ClientCollateral = abi.NewTokenAmount(1<<20 + 1)
			})
		})

		t.Run("fails if it cannot lock client funds", func(t *testing.T) {
			rt, actor := setup()

			params := market.PublishStorageDealsParams{Deals: []market.ClientDealProposal{actor.testProposal(provider, client, "data")}}

			rt.SetVerifier(fakeVerifier(true))
			actor.addParticipantFunds(rt, client, abi.NewTokenAmount(500)) // need 550 for deal

			rt.SetCaller(worker, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
			actor.expectProviderControlAddresses(rt, provider, owner, worker)

			rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
				rt.Call(actor.PublishStorageDeals, &params)
			})

			rt.Verify()
		})

		t.Run("fails if it cannot lock provider funds", func(t *testing.T) {
			rt, actor := setup()

			params := market.PublishStorageDealsParams{Deals: []market.ClientDealProposal{actor.testProposal(provider, client, "data")}}

			rt.SetVerifier(fakeVerifier(true))
			actor.addParticipantFunds(rt, client, abi.NewTokenAmount(550))
			actor.addProviderFunds(rt, provider, owner, worker, abi.NewTokenAmount(40)) // need 50 for deal

			rt.SetCaller(worker, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
			actor.expectProviderControlAddresses(rt, provider, owner, worker)

			rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
				rt.Call(actor.PublishStorageDeals, &params)
			})

			rt.Verify()
		})

		t.Run("returns deal ids of published deals and locks funds", func(t *testing.T) {
			rt, actor := setup()

			params := market.PublishStorageDealsParams{Deals: []market.ClientDealProposal{
				actor.testProposal(provider, client, "data1"),
				actor.testProposal(provider, client, "data2"),
			}}

			rt.SetVerifier(fakeVerifier(true))
			actor.addParticipantFunds(rt, client, abi.NewTokenAmount(550*2))
			actor.addProviderFunds(rt, provider, owner, worker, abi.NewTokenAmount(50*2))

			rt.SetCaller(worker, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
			actor.expectProviderControlAddresses(rt, provider, owner, worker)

			ret := rt.Call(actor.PublishStorageDeals, &params)

			rt.Verify()

			retval := ret.(*market.PublishStorageDealsReturn)
			assert.Equal(t, len(retval.IDs), 2)

			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(550*2), st.GetLockedBalance(rt, client))
			assert.Equal(t, abi.NewTokenAmount(50*2), st.GetLockedBalance(rt, provider))
		})

		t.Run("slashes funds if necessary", func(t *testing.T) {
			rt, actor := setup()
			rt.SetVerifier(fakeVerifier(true))

			{
				// publish a deal that will timeout
				cdp := actor.testProposal(provider, client, "timed out")
				cdp.Proposal.StartEpoch = 25
				cdp.Proposal.EndEpoch = 75
				cdp.Proposal.ProviderCollateral = abi.NewTokenAmount(42)

				params := market.PublishStorageDealsParams{Deals: []market.ClientDealProposal{cdp}}

				actor.addParticipantFunds(rt, client, abi.NewTokenAmount(300))
				actor.addProviderFunds(rt, provider, owner, worker, abi.NewTokenAmount(42))

				rt.SetCaller(worker, builtin.AccountActorCodeID)
				rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
				actor.expectProviderControlAddresses(rt, provider, owner, worker)

				rt.Call(actor.PublishStorageDeals, &params)

				rt.Verify()
			}

			{
				// previously published deal had to be started by epoch 25,
				// so set epoch to after the deadline so slashing will occur
				rt.SetEpoch(30)

				params := market.PublishStorageDealsParams{Deals: []market.ClientDealProposal{
					actor.testProposal(provider, client, "data1"),
					actor.testProposal(provider, client, "data2"),
				}}

				actor.addParticipantFunds(rt, client, abi.NewTokenAmount(550*2))
				actor.addProviderFunds(rt, provider, owner, worker, abi.NewTokenAmount(50*2))

				rt.SetCaller(worker, builtin.AccountActorCodeID)
				rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
				actor.expectProviderControlAddresses(rt, provider, owner, worker)
				rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, adt.EmptyValue{}, abi.NewTokenAmount(42), nil, exitcode.Ok)

				rt.Call(actor.PublishStorageDeals, &params)

				rt.Verify()
			}
		})
	})

	t.Run("VerifyDealsOnSectorProveCommit", func(t *testing.T) {
		t.Run("fails if dealid for a different provider is sent", func(t *testing.T) {
			rt, actor := setup()

			dealIds := actor.addTestProposals(rt, provider, owner, worker, client, 1)

			params := market.VerifyDealsOnSectorProveCommitParams{DealIDs: dealIds, SectorExpiry: 200}

			// have `wrongProvider` call the method with deals published by `provider`
			wrongProvider := tutil.NewIDAddr(t, 1001)
			rt.SetCaller(wrongProvider, builtin.StorageMinerActorCodeID)
			rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)

			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				rt.Call(actor.VerifyDealsOnSectorProveCommit, &params)
			})

			rt.Verify()

		})

		t.Run("fails if deal is already activated", func(t *testing.T) {
			rt, actor := setup()

			dealIds := actor.addTestProposals(rt, provider, owner, worker, client, 1)

			params := market.VerifyDealsOnSectorProveCommitParams{DealIDs: dealIds, SectorExpiry: 200}

			// activate deals normally
			rt.SetEpoch(99)
			rt.SetCaller(provider, builtin.StorageMinerActorCodeID)
			rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)
			rt.Call(actor.VerifyDealsOnSectorProveCommit, &params)
			rt.Verify()

			// activate deal again
			rt.SetCaller(provider, builtin.StorageMinerActorCodeID)
			rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)

			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				rt.Call(actor.VerifyDealsOnSectorProveCommit, &params)
			})

			rt.Verify()

		})

		t.Run("fails if it is too late", func(t *testing.T) {
			rt, actor := setup()

			dealIds := actor.addTestProposals(rt, provider, owner, worker, client, 1)

			params := market.VerifyDealsOnSectorProveCommitParams{DealIDs: dealIds, SectorExpiry: 200}

			// StartEpoch == 100
			rt.SetEpoch(101)
			rt.SetCaller(provider, builtin.StorageMinerActorCodeID)
			rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)

			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				rt.Call(actor.VerifyDealsOnSectorProveCommit, &params)
			})

			rt.Verify()
		})

		t.Run("fails if sector expires too early", func(t *testing.T) {
			rt, actor := setup()

			dealIds := actor.addTestProposals(rt, provider, owner, worker, client, 1)

			// Deal EndEpoch == 200
			params := market.VerifyDealsOnSectorProveCommitParams{DealIDs: dealIds, SectorExpiry: 199}

			rt.SetEpoch(100)
			rt.SetCaller(provider, builtin.StorageMinerActorCodeID)
			rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)

			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				rt.Call(actor.VerifyDealsOnSectorProveCommit, &params)
			})

			rt.Verify()
		})

		t.Run("returns the weight represented by the deals", func(t *testing.T) {
			rt, actor := setup()

			dealIds := actor.addTestProposals(rt, provider, owner, worker, client, 4)

			params := market.VerifyDealsOnSectorProveCommitParams{DealIDs: dealIds, SectorExpiry: 200}

			rt.SetEpoch(100)
			rt.SetCaller(provider, builtin.StorageMinerActorCodeID)
			rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)

			ret := rt.Call(actor.VerifyDealsOnSectorProveCommit, &params)

			rt.Verify()

			// 4 deals, 6 bytes each, duration == 100
			assert.Equal(t, *(ret.(*abi.DealWeight)), big.NewInt(4*6*100))
		})
	})
}

type marketActorTestHarness struct {
	market.Actor
	t testing.TB
}

func (h *marketActorTestHarness) constructAndVerify(rt *mock.Runtime) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	params := adt.EmptyValue{}
	ret := rt.Call(h.Constructor, &params)
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

	rt.SetBalance(big.Add(rt.GetBalance(), amount))
}

// addParticipantFunds is a helper method to setup non-provider storage market participant funds
func (h *marketActorTestHarness) addParticipantFunds(rt *mock.Runtime, addr address.Address, amount abi.TokenAmount) {
	rt.SetReceived(amount)
	rt.SetCaller(addr, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)

	rt.Call(h.AddBalance, &addr)

	rt.Verify()

	rt.SetBalance(big.Add(rt.GetBalance(), amount))
}

func (h *marketActorTestHarness) expectProviderControlAddresses(rt *mock.Runtime, provider address.Address, owner address.Address, worker address.Address) {
	expectRet := &miner.GetControlAddressesReturn{Owner: owner, Worker: worker}

	rt.ExpectSend(
		provider,
		builtin.MethodsMiner.ControlAddresses,
		nil,
		big.Zero(),
		&mock.ReturnWrapper{V: expectRet},
		exitcode.Ok,
	)
}

func (h *marketActorTestHarness) expectProviderControlAddressesAndValidateCaller(rt *mock.Runtime, provider address.Address, owner address.Address, worker address.Address) {
	rt.ExpectValidateCallerAddr(owner, worker)
	h.expectProviderControlAddresses(rt, provider, owner, worker)
}

func (h *marketActorTestHarness) testProposal(provider, client address.Address, data string) market.ClientDealProposal {
	bytes := []byte(data)
	cidHash, _ := mh.Sum(bytes, mh.SHA3, 4)

	// ClientBalanceRequirement: 550
	// ProviderBalanceRequirement: 50
	proposal := market.DealProposal{
		PieceCID:             cid.NewCidV1(cid.Raw, cidHash),
		PieceSize:            abi.PaddedPieceSize(len(bytes)),
		Client:               client,
		Provider:             provider,
		StartEpoch:           100,
		EndEpoch:             200,
		StoragePricePerEpoch: abi.NewTokenAmount(5),
		ProviderCollateral:   abi.NewTokenAmount(50),
		ClientCollateral:     abi.NewTokenAmount(50),
	}

	return market.ClientDealProposal{
		Proposal:        proposal,
		ClientSignature: crypto.Signature{},
	}
}

func (h *marketActorTestHarness) addTestProposals(rt *mock.Runtime, provider, owner, worker, client address.Address, count int64) []abi.DealID {
	var proposals []market.ClientDealProposal
	for i := int64(0); i < count; i++ {
		proposals = append(proposals, h.testProposal(provider, client, fmt.Sprintf("data %d", count)))
	}
	params := market.PublishStorageDealsParams{Deals: proposals}

	rt.SetVerifier(fakeVerifier(true))
	h.addParticipantFunds(rt, client, abi.NewTokenAmount(550*count))
	h.addProviderFunds(rt, provider, owner, worker, abi.NewTokenAmount(50*count))

	rt.SetCaller(worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
	h.expectProviderControlAddresses(rt, provider, owner, worker)

	ret := rt.Call(h.PublishStorageDeals, &params)

	rt.Verify()

	retval := ret.(*market.PublishStorageDealsReturn)
	return retval.IDs
}

func fakeVerifier(valid bool) mock.VerifyFunc {
	return func(signature crypto.Signature, signer address.Address, plaintext []byte) error {
		if valid {
			return nil
		}
		return xerrors.New("invalid signature")
	}
}
