package market_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	cbg "github.com/whyrusleeping/cbor-gen"
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

		rt.SetVerifier(fakeVerifier(true))

		actor := marketActorTestHarness{
			t:        t,
			owner:    owner,
			provider: provider,
			worker:   worker,
			client:   client,
		}
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

			actor.addProviderFunds(rt, 20)

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
			actor.addClientFunds(rt, 20)

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
			actor.addClientFunds(rt, 20)

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
			actor.addProviderFunds(rt, 20)

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

			proposals := actor.testProposals(testProposalParams{
				StartEpoch:           100,
				EndEpoch:             200,
				StoragePricePerEpoch: 5,
			}, 1)

			wrongCaller := tutil.NewIDAddr(t, 1001)
			rt.SetCaller(wrongCaller, builtin.AccountActorCodeID)

			rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
			actor.expectProviderControlAddresses(rt, provider, owner, worker)

			rt.ExpectAbort(exitcode.ErrForbidden, func() {
				rt.Call(actor.PublishStorageDeals, &market.PublishStorageDealsParams{Deals: proposals})
			})

			rt.Verify()
		})

		t.Run("fails with invalid proposals", func(t *testing.T) {
			assertInvalidProposal := func(mutateProposal func(rt *mock.Runtime, dp *market.ClientDealProposal)) {
				rt, actor := setup()

				baseProposal := testProposalParams{
					Data:                 "data",
					Client:               client,
					Provider:             provider,
					StartEpoch:           100,
					EndEpoch:             200,
					StoragePricePerEpoch: 5,
					ProviderCollateral:   50,
					ClientCollateral:     50,
				}

				p := actor.testProposal(baseProposal)
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

		t.Run("different escrow balance scenarios", func(t *testing.T) {
			testCases := []struct {
				caseName        string
				clientBalance   int64
				providerBalance int64
				success         bool
			}{
				{"fails with insufficient client escrow balance", 549, 50, false},
				{"fails with insufficient provider escrow balance", 550, 49, false},
				{"succeeds with insufficient client escrow balance", 550, 50, true},
			}

			for _, testCase := range testCases {
				t.Run(testCase.caseName, func(t *testing.T) {
					rt, actor := setup()

					proposalParams := testProposalParams{
						StartEpoch:           100,
						EndEpoch:             200,
						StoragePricePerEpoch: 5,
						ClientCollateral:     50,
						ProviderCollateral:   50,
					}

					proposals := actor.testProposals(proposalParams, 1)

					actor.addClientFunds(rt, testCase.clientBalance)
					actor.addProviderFunds(rt, testCase.providerBalance)

					rt.SetCaller(worker, builtin.AccountActorCodeID)
					rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
					actor.expectProviderControlAddresses(rt, provider, owner, worker)

					if testCase.success {

						ret := rt.Call(actor.PublishStorageDeals, &market.PublishStorageDealsParams{Deals: proposals})

						rt.Verify()

						retval := ret.(*market.PublishStorageDealsReturn)
						assert.Equal(t, len(retval.IDs), len(proposals))

						rt.GetState(&st)
						assert.Equal(t, abi.NewTokenAmount(testCase.clientBalance), st.GetLockedBalance(rt, client))
						assert.Equal(t, abi.NewTokenAmount(testCase.providerBalance), st.GetLockedBalance(rt, provider))

					} else {

						rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
							rt.Call(actor.PublishStorageDeals, &market.PublishStorageDealsParams{Deals: proposals})
						})

					}

					rt.Verify()
				})
			}
		})

		t.Run("slashes funds if necessary", func(t *testing.T) {
			rt, actor := setup()

			{
				// publish a deal that will timeout
				expiringProposalParams := testProposalParams{
					Data:                 "data",
					Client:               client,
					Provider:             provider,
					StartEpoch:           25,
					EndEpoch:             75,
					StoragePricePerEpoch: 5,
					ProviderCollateral:   42,
					ClientCollateral:     50,
				}

				params := market.PublishStorageDealsParams{Deals: actor.testProposals(expiringProposalParams, 1)}

				actor.addClientFunds(rt, 300)
				actor.addProviderFunds(rt, 42)

				rt.SetCaller(worker, builtin.AccountActorCodeID)
				rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
				actor.expectProviderControlAddresses(rt, provider, owner, worker)

				rt.Call(actor.PublishStorageDeals, &params)

				rt.Verify()
			}

			{
				// previously published deal had to be started by epoch 25,
				// so set epoch to after the deadline so slashing will occur
				rt.SetEpoch(26)

				proposalParams := testProposalParams{
					Data:                 "data",
					Client:               client,
					Provider:             provider,
					StartEpoch:           100,
					EndEpoch:             200,
					StoragePricePerEpoch: 5,
					ProviderCollateral:   50,
					ClientCollateral:     50,
				}
				params := market.PublishStorageDealsParams{Deals: actor.testProposals(proposalParams, 2)}

				actor.addClientFunds(rt, 550*2)
				actor.addProviderFunds(rt, 50*2)

				rt.SetCaller(worker, builtin.AccountActorCodeID)
				rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
				actor.expectProviderControlAddresses(rt, provider, owner, worker)
				rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, abi.NewTokenAmount(42), nil, exitcode.Ok)

				rt.Call(actor.PublishStorageDeals, &params)

				rt.Verify()
			}
		})
	})

	t.Run("VerifyDealsOnSectorProveCommit", func(t *testing.T) {
		t.Run("fails if dealid for a different provider is sent", func(t *testing.T) {
			rt, actor := setup()

			dealIds := actor.addTestProposals(rt, testProposalParams{
				StartEpoch:           100,
				EndEpoch:             200,
				StoragePricePerEpoch: 5,
			}, 1)

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

			dealIds := actor.addTestProposals(rt, testProposalParams{
				StartEpoch:           100,
				EndEpoch:             200,
				StoragePricePerEpoch: 5,
			}, 1)

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

			dealIds := actor.addTestProposals(rt, testProposalParams{
				StartEpoch:           100,
				EndEpoch:             200,
				StoragePricePerEpoch: 5,
			}, 1)

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

			dealIds := actor.addTestProposals(rt, testProposalParams{
				StartEpoch:           100,
				EndEpoch:             200,
				StoragePricePerEpoch: 5,
			}, 1)

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

			dealIds := actor.addTestProposals(rt, testProposalParams{
				Data:                 "123456",
				StartEpoch:           100,
				EndEpoch:             200,
				StoragePricePerEpoch: 5,
				ClientCollateral:     50,
				ProviderCollateral:   50,
			}, 4)

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

	t.Run("ComputeDataCommitment", func(t *testing.T) {
		t.Run("executes the syscall with the correct parameters", func(t *testing.T) {
			rt, actor := setup()
			returnedCid := testCid("commd")
			computeCID := newFakeComputeCID(t, testCid("commd"))
			rt.SetComputeUnsealedCID(computeCID)

			dealIds := actor.addTestProposals(rt, testProposalParams{
				StartEpoch:           100,
				EndEpoch:             200,
				StoragePricePerEpoch: 5,
			}, 4)

			params := market.ComputeDataCommitmentParams{
				DealIDs:    dealIds,
				SectorType: abi.RegisteredProof_StackedDRG32GiBSeal,
			}

			rt.SetCaller(provider, builtin.StorageMinerActorCodeID)
			rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)

			retval := rt.Call(actor.ComputeDataCommitment, &params)

			// Ensure that the call to compute the commd was called with the correct params
			assert.Equal(t, (*cbg.CborCid)(&returnedCid), retval)
			assert.Equal(t, abi.RegisteredProof_StackedDRG32GiBSeal, computeCID.CallParamProof)
			assert.Len(t, computeCID.CallParamPieces, 4)
		})
	})

	t.Run("OnMinerSectorsTerminate", func(t *testing.T) {
		t.Run("updates the SlashEpoch in the deal state", func(t *testing.T) {
			rt, actor := setup()

			dealIds := actor.addTestProposals(rt, testProposalParams{
				StartEpoch:           100,
				EndEpoch:             200,
				StoragePricePerEpoch: 5,
			}, 1)

			rt.SetCaller(provider, builtin.StorageMinerActorCodeID)
			rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)
			rt.SetEpoch(42)

			params := market.OnMinerSectorsTerminateParams{dealIds}
			retval := rt.Call(actor.OnMinerSectorsTerminate, &params)

			assert.IsType(t, &adt.EmptyValue{}, retval)

			rt.GetState(&st)

			// Only side-effect is setting the slash epoch
			for _, id := range dealIds {
				ds := st.MustGetDealState(rt, id)
				assert.Equal(t, ds.SlashEpoch, abi.ChainEpoch(42))
			}
		})
	})

	t.Run("HandleExpiredDeals", func(t *testing.T) {
		t.Run("unlocks collateral and transfers funds if deals have expired normally", func(t *testing.T) {
			rt, actor := setup()

			proposalParams := testProposalParams{
				StartEpoch:           100,
				EndEpoch:             200,
				StoragePricePerEpoch: 5,
				ProviderCollateral:   50,
				ClientCollateral:     50,
			}
			dealIds := actor.addTestProposals(rt, proposalParams, 1)
			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(50), st.GetLockedBalance(rt, provider))
			assert.Equal(t, abi.NewTokenAmount(50), st.GetEscrowBalance(rt, provider))
			assert.Equal(t, abi.NewTokenAmount(100*5+50), st.GetLockedBalance(rt, client))
			assert.Equal(t, abi.NewTokenAmount(100*5+50), st.GetEscrowBalance(rt, client))

			// verify/activate deal
			params := market.VerifyDealsOnSectorProveCommitParams{DealIDs: dealIds, SectorExpiry: 200}
			rt.SetEpoch(100)
			rt.SetCaller(provider, builtin.StorageMinerActorCodeID)
			rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)
			rt.Call(actor.VerifyDealsOnSectorProveCommit, &params)
			rt.Verify()

			// fast forward to where deal has expired
			rt.SetEpoch(201)
			rt.SetCaller(provider, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)

			retval := rt.Call(actor.HandleExpiredDeals, &market.HandleExpiredDealsParams{dealIds})

			assert.IsType(t, &adt.EmptyValue{}, retval)

			// ensure balances have been transferred and funds unlocked
			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(100*5+50), st.GetEscrowBalance(rt, provider))
			assert.Equal(t, abi.NewTokenAmount(0), st.GetLockedBalance(rt, provider))
			assert.Equal(t, abi.NewTokenAmount(50), st.GetEscrowBalance(rt, client))
			assert.Equal(t, abi.NewTokenAmount(0), st.GetLockedBalance(rt, client))
		})

		t.Run("burns collateral and unlocks balances if deal has timed-out", func(t *testing.T) {
			rt, actor := setup()

			// add a deal
			dealIds := actor.addTestProposals(rt, testProposalParams{
				StartEpoch:           100,
				EndEpoch:             200,
				StoragePricePerEpoch: 5,
				ClientCollateral:     50,
				ProviderCollateral:   50,
			}, 1)
			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(50), st.GetLockedBalance(rt, provider))
			assert.Equal(t, abi.NewTokenAmount(50), st.GetEscrowBalance(rt, provider))
			assert.Equal(t, abi.NewTokenAmount(100*5+50), st.GetLockedBalance(rt, client))
			assert.Equal(t, abi.NewTokenAmount(100*5+50), st.GetEscrowBalance(rt, client))

			// let this deal time out (`SectorStartEpoch == epochUndefined`)

			// test deals start at height 100
			rt.SetEpoch(101)
			thirdParty := tutil.NewIDAddr(t, 1001)
			rt.SetCaller(thirdParty, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
			rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, abi.NewTokenAmount(50), nil, exitcode.Ok)

			retval := rt.Call(actor.HandleExpiredDeals, &market.HandleExpiredDealsParams{dealIds})

			assert.IsType(t, &adt.EmptyValue{}, retval)

			// ensure balances have been transferred and funds unlocked
			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(0), st.GetEscrowBalance(rt, provider))
			assert.Equal(t, abi.NewTokenAmount(0), st.GetLockedBalance(rt, provider))
			assert.Equal(t, abi.NewTokenAmount(100*5+50), st.GetEscrowBalance(rt, client))
			assert.Equal(t, abi.NewTokenAmount(0), st.GetLockedBalance(rt, client))
		})

		// Test is commented out pending resolution of partial payment calculation.  Relevant
		// discussion here: https://filecoinproject.slack.com/archives/CHMNDCK9P/p1582594536049700
		t.Run("burns collateral, transfers partial payment, and unlocks balances if deal has has been slashed", func(t *testing.T) {
			t.Skip("skipped pending spec resolution")
			rt, actor := setup()

			proposalParams := testProposalParams{
				StartEpoch:           100,
				EndEpoch:             200,
				StoragePricePerEpoch: 5,
				ProviderCollateral:   50,
				ClientCollateral:     50,
			}

			// add a deal
			dealIds := actor.addTestProposals(rt, proposalParams, 0)
			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(50), st.GetLockedBalance(rt, provider))
			assert.Equal(t, abi.NewTokenAmount(50), st.GetEscrowBalance(rt, provider))
			assert.Equal(t, abi.NewTokenAmount(100*5+50), st.GetLockedBalance(rt, client))
			assert.Equal(t, abi.NewTokenAmount(100*5+50), st.GetEscrowBalance(rt, client))

			// verify/activate deal
			params := market.VerifyDealsOnSectorProveCommitParams{DealIDs: dealIds, SectorExpiry: 200}
			rt.SetEpoch(100)
			rt.SetCaller(provider, builtin.StorageMinerActorCodeID)
			rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)
			rt.Call(actor.VerifyDealsOnSectorProveCommit, &params)
			rt.Verify()

			// terminate the deal early at height 150
			rt.SetCaller(provider, builtin.StorageMinerActorCodeID)
			rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)
			rt.SetEpoch(150)
			rt.Call(actor.OnMinerSectorsTerminate, &market.OnMinerSectorsTerminateParams{dealIds})
			rt.Verify()

			rt.SetEpoch(160)
			rt.SetCaller(provider, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
			rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, abi.NewTokenAmount(50), nil, exitcode.Ok)

			retval := rt.Call(actor.HandleExpiredDeals, &market.HandleExpiredDealsParams{dealIds})

			assert.IsType(t, &adt.EmptyValue{}, retval)

			// ensure balances have been transferred and funds unlocked
			rt.GetState(&st)
			assert.Equal(t, abi.NewTokenAmount(50*5), st.GetEscrowBalance(rt, provider))
			assert.Equal(t, abi.NewTokenAmount(0), st.GetLockedBalance(rt, provider))
			assert.Equal(t, abi.NewTokenAmount(50*5+50), st.GetEscrowBalance(rt, client))
			assert.Equal(t, abi.NewTokenAmount(0), st.GetLockedBalance(rt, client))
		})
	})
}

// Support functions

type fakeComputeCID struct {
	T               *testing.T
	ReturnCID       cid.Cid
	CallParamProof  abi.RegisteredProof
	CallParamPieces []abi.PieceInfo
}

func newFakeComputeCID(t *testing.T, returnCid cid.Cid) *fakeComputeCID {
	return &fakeComputeCID{T: t, ReturnCID: returnCid}
}

func (c *fakeComputeCID) Compute(reg abi.RegisteredProof, pieces []abi.PieceInfo) (cid.Cid, error) {
	assert.Nil(c.T, c.CallParamPieces)
	c.CallParamPieces = pieces
	c.CallParamProof = reg
	return c.ReturnCID, nil
}

type marketActorTestHarness struct {
	market.Actor
	t        testing.TB
	owner    address.Address
	provider address.Address
	worker   address.Address
	client   address.Address
}

func (h *marketActorTestHarness) constructAndVerify(rt *mock.Runtime) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	params := adt.EmptyValue{}
	ret := rt.Call(h.Constructor, &params)
	assert.Nil(h.t, ret)
	rt.Verify()
}

// addProviderFunds is a helper method to setup provider market funds
func (h *marketActorTestHarness) addProviderFunds(rt *mock.Runtime, amount int64) {
	ta := abi.NewTokenAmount(amount)
	rt.SetReceived(ta)
	rt.SetCaller(h.owner, builtin.AccountActorCodeID)
	h.expectProviderControlAddressesAndValidateCaller(rt, h.provider, h.owner, h.worker)

	rt.Call(h.AddBalance, &h.provider)

	rt.Verify()

	rt.SetBalance(big.Add(rt.GetBalance(), ta))
}

func (h *marketActorTestHarness) addClientFunds(rt *mock.Runtime, amount int64) {
	ta := abi.NewTokenAmount(amount)
	rt.SetReceived(ta)
	rt.SetCaller(h.client, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)

	rt.Call(h.AddBalance, &h.client)

	rt.Verify()

	rt.SetBalance(big.Add(rt.GetBalance(), ta))
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

type testProposalParams struct {
	Data                 string
	Client               address.Address
	Provider             address.Address
	StartEpoch           int64
	EndEpoch             int64
	StoragePricePerEpoch int64
	ProviderCollateral   int64
	ClientCollateral     int64
}

func (h *marketActorTestHarness) testProposal(p testProposalParams) market.ClientDealProposal {
	proposal := market.DealProposal{
		Client:               p.Client,
		Provider:             p.Provider,
		StartEpoch:           abi.ChainEpoch(p.StartEpoch),
		EndEpoch:             abi.ChainEpoch(p.EndEpoch),
		StoragePricePerEpoch: abi.NewTokenAmount(p.StoragePricePerEpoch),
		ProviderCollateral:   abi.NewTokenAmount(p.ProviderCollateral),
		ClientCollateral:     abi.NewTokenAmount(p.ClientCollateral),
	}

	if p.Data == "" {
		p.Data = "test data"
	}
	proposal.PieceCID = testCid(p.Data)
	proposal.PieceSize = abi.UnpaddedPieceSize(len(p.Data)).Padded()

	if proposal.Client == address.Undef {
		proposal.Client = h.client
	}

	if proposal.Provider == address.Undef {
		proposal.Provider = h.provider
	}

	return market.ClientDealProposal{
		Proposal:        proposal,
		ClientSignature: crypto.Signature{},
	}
}

func (h *marketActorTestHarness) testProposals(params testProposalParams, count int) []market.ClientDealProposal {
	var out []market.ClientDealProposal
	for i := 0; i < count; i++ {
		p := params
		if len(p.Data) == 0 {
			p.Data = fmt.Sprintf("data %d", i)
		}
		out = append(out, h.testProposal(p))
	}
	return out
}

func (h *marketActorTestHarness) addTestProposals(rt *mock.Runtime, proposalParams testProposalParams, count int) []abi.DealID {
	proposals := h.testProposals(proposalParams, count)

	params := market.PublishStorageDealsParams{Deals: proposals}

	for _, proposal := range proposals {
		h.addClientFunds(rt, proposal.Proposal.ClientBalanceRequirement().Int64())
		h.addProviderFunds(rt, proposal.Proposal.ProviderBalanceRequirement().Int64())
	}

	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)
	h.expectProviderControlAddresses(rt, h.provider, h.owner, h.worker)

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

func testCid(data string) cid.Cid {
	bytes := []byte(data)
	cidHash, _ := mh.Sum(bytes, mh.SHA3, 4)
	return cid.NewCidV1(cid.Raw, cidHash)
}
