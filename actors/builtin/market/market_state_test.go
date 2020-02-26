package market

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

func TestMarketState(t *testing.T) {
	builder := mock.NewBuilder(context.Background(), address.Undef)
	provider := tutil.NewIDAddr(t, 101)
	client := tutil.NewIDAddr(t, 102)

	createTestState := func(t *testing.T) (*mock.Runtime, *State) {
		rt := builder.Build(t)
		store := adt.AsStore(rt)

		emptyMap, err := adt.MakeEmptyMap(store)
		assert.NoError(t, err)

		emptyArray, err := adt.MakeEmptyArray(store)
		assert.NoError(t, err)

		emptyMultiMap, err := MakeEmptySetMultimap(store)
		assert.NoError(t, err)

		state := ConstructState(emptyArray.Root(), emptyMap.Root(), emptyMultiMap.Root())

		return rt, state
	}

	t.Run("constructor initializes all fields", func(t *testing.T) {
		rt := builder.Build(t)
		store := adt.AsStore(rt)

		emptyMap, err := adt.MakeEmptyMap(store)
		assert.NoError(t, err)

		emptyArray, err := adt.MakeEmptyArray(store)
		assert.NoError(t, err)

		emptyMultiMap, err := MakeEmptySetMultimap(store)
		assert.NoError(t, err)

		state := ConstructState(emptyArray.Root(), emptyMap.Root(), emptyMultiMap.Root())

		assert.Equal(t, emptyArray.Root(), state.Proposals)
		assert.Equal(t, emptyArray.Root(), state.States)
		assert.Equal(t, emptyMap.Root(), state.EscrowTable)
		assert.Equal(t, emptyMap.Root(), state.LockedTable)
		assert.Equal(t, abi.DealID(0), state.NextID)
		assert.Equal(t, emptyMultiMap.Root(), state.DealIDsByParty)
	})

	t.Run("balance tables", func(t *testing.T) {
		testCases := []struct {
			delta int64
			total int64
		}{
			{10, 10},
			{20, 30},
			{40, 70},
		}

		t.Run("AddEscrowBalance adds to escrow table, GetEscrowBalance returns balance", func(t *testing.T) {
			rt, st := createTestState(t)
			for _, tc := range testCases {
				st.AddEscrowBalance(adt.AsStore(rt), provider, abi.NewTokenAmount(tc.delta))
				assert.Equal(t, abi.NewTokenAmount(tc.total), st.GetEscrowBalance(rt, provider))
			}
		})

		t.Run("AddLockedBalance adds to escrow table, GetLockedBalance returns balance", func(t *testing.T) {
			rt, st := createTestState(t)
			for _, tc := range testCases {
				st.AddLockedBalance(adt.AsStore(rt), provider, abi.NewTokenAmount(tc.delta))
				assert.Equal(t, abi.NewTokenAmount(tc.total), st.GetLockedBalance(rt, provider))
			}
		})
	})

	t.Run("mustGetDeal errors or returns deal", func(t *testing.T) {
		rt, st := createTestState(t)
		rt.SetInCall(true)
		id := st.generateStorageDealID()

		rt.ExpectAbort(exitcode.ErrIllegalState, func() {
			st.mustGetDeal(rt, id)
		})

		dp := testProposal(t, provider, client)

		proposals := AsDealProposalArray(adt.AsStore(rt), st.Proposals)
		err := proposals.Set(id, dp)
		assert.NoError(t, err)

		st.Proposals = proposals.Root()

		assert.Equal(t, dp, st.mustGetDeal(rt, id))
	})

	t.Run("deleteDeal", func(t *testing.T) {
		rt, st := createTestState(t)
		rt.SetInCall(true)
		dealId := st.generateStorageDealID()

		rt.ExpectAbort(exitcode.ErrIllegalState, func() {
			st.deleteDeal(rt, dealId)
		})

		dp := testProposal(t, provider, client)
		addTestDeal(t, rt, st, dealId, dp)

		// make sure that the deals are in the correct places
		st.mustGetDeal(rt, dealId)

		dbp := AsSetMultimap(adt.AsStore(rt), st.DealIDsByParty)
		err := dbp.ForEach(client, func(id abi.DealID) error {
			assert.Equal(t, id, dealId)
			return nil
		})
		assert.NoError(t, err)

		err = dbp.ForEach(provider, func(id abi.DealID) error {
			assert.Equal(t, id, dealId)
			return nil
		})
		assert.NoError(t, err)

		st.deleteDeal(rt, dealId)

		// make sure everything is gone
		rt.ExpectAbort(exitcode.ErrIllegalState, func() {
			st.mustGetDeal(rt, dealId)
		})

		dbp = AsSetMultimap(adt.AsStore(rt), st.DealIDsByParty)
		err = dbp.ForEach(client, func(id abi.DealID) error {
			assert.Failf(t, "unexpected state", "didn't expect to find any deal ids for client %s", client)
			return nil
		})
		assert.NoError(t, err)

		err = dbp.ForEach(provider, func(id abi.DealID) error {
			assert.Failf(t, "unexpected state", "didn't expect to find any deal ids for provider %s", provider)
			return nil
		})
		assert.NoError(t, err)
	})

	t.Run("updatePendingDealState", func(t *testing.T) {
		// Parameters for deal:

		dealStartEpoch := abi.ChainEpoch(100)
		dealEndEpoch := abi.ChainEpoch(200)

		setup := func(ds *DealState) (*mock.Runtime, *State, abi.DealID) {
			dealProposal := testProposal(t, provider, client)
			dealProposal.StartEpoch = dealStartEpoch
			dealProposal.EndEpoch = dealEndEpoch
			dealProposal.ProviderCollateral = abi.NewTokenAmount(7)
			dealProposal.StoragePricePerEpoch = abi.NewTokenAmount(5)

			rt, st := createTestState(t)
			rt.SetInCall(true)
			dealId := st.generateStorageDealID()

			addTestDeal(t, rt, st, dealId, dealProposal)
			addTestDealState(t, rt, st, dealId, ds)

			err := st.AddLockedBalance(adt.AsStore(rt), client, abi.NewTokenAmount(500))
			assert.NoError(t, err)
			err = st.AddEscrowBalance(adt.AsStore(rt), client, abi.NewTokenAmount(550))
			assert.NoError(t, err)

			err = st.AddLockedBalance(adt.AsStore(rt), provider, abi.NewTokenAmount(7))
			assert.NoError(t, err)
			err = st.AddEscrowBalance(adt.AsStore(rt), provider, abi.NewTokenAmount(7))
			assert.NoError(t, err)

			return rt, st, dealId
		}

		t.Run("provider gets paid", func(t *testing.T) {
			dealState := &DealState{
				SectorStartEpoch: dealStartEpoch,
				LastUpdatedEpoch: epochUndefined,
				SlashEpoch:       epochUndefined,
			}

			rt, st, dealId := setup(dealState)
			curEpoch := abi.ChainEpoch(105)
			rt.SetEpoch(curEpoch)

			st.updatePendingDealState(rt, dealId, curEpoch)

			assert.Equal(t, abi.NewTokenAmount(32), st.GetEscrowBalance(rt, provider))
			dealState = st.mustGetDealState(rt, dealId)
			assert.Equal(t, curEpoch, dealState.LastUpdatedEpoch)
		})

		t.Run("provider gets partial payment due to a recently updated deal state", func(t *testing.T) {
			// Set up an elapsed update time of 2 epochs
			dealState := &DealState{
				SectorStartEpoch: dealStartEpoch,
				LastUpdatedEpoch: abi.ChainEpoch(102),
				SlashEpoch:       epochUndefined,
			}

			rt, st, dealId := setup(dealState)
			curEpoch := abi.ChainEpoch(104)
			rt.SetEpoch(curEpoch)

			assertBalances(t, rt, st, client, 550, 500)
			assertBalances(t, rt, st, provider, 7, 7)

			st.updatePendingDealState(rt, dealId, curEpoch)

			assertBalances(t, rt, st, client, 540, 490)
			assertBalances(t, rt, st, provider, 17, 7)

			dealState = st.mustGetDealState(rt, dealId)
			assert.Equal(t, curEpoch, dealState.LastUpdatedEpoch)
		})

		t.Run("provider gets partial payment after slashing", func(t *testing.T) {
			// Set up an elapsed update time of 2 epochs
			dealState := &DealState{
				SectorStartEpoch: dealStartEpoch,
				LastUpdatedEpoch: epochUndefined,
				SlashEpoch:       abi.ChainEpoch(104),
			}

			rt, st, dealId := setup(dealState)
			curEpoch := abi.ChainEpoch(105)
			rt.SetEpoch(curEpoch)

			assertBalances(t, rt, st, client, 550, 500)

			// TODO: Find out about "payment remaining" calculation, is it correct
			// this test fails if we don't add one more StoragePricePerEpoch to LockedBalance
			err := st.AddLockedBalance(adt.AsStore(rt), client, abi.NewTokenAmount(5))
			assert.NoError(t, err)

			assertBalances(t, rt, st, client, 550, 505)

			assertBalances(t, rt, st, provider, 7, 7)

			st.updatePendingDealState(rt, dealId, curEpoch)

			assertBalances(t, rt, st, client, 530, 0)
			assertBalances(t, rt, st, provider, 20, 0)

			rt.ExpectAbort(exitcode.ErrIllegalState, func() {
				st.mustGetDeal(rt, dealId)
			})
		})

		t.Run("provider slashed for failing to start the deal on time", func(t *testing.T) {
			dealState := &DealState{
				SectorStartEpoch: epochUndefined,
				LastUpdatedEpoch: epochUndefined,
				SlashEpoch:       epochUndefined,
			}

			rt, st, dealId := setup(dealState)
			curEpoch := abi.ChainEpoch(105)
			rt.SetEpoch(curEpoch)

			//assertBalances(t, rt, st, client, 550, 500)
			assertBalances(t, rt, st, client, 550, 500)
			assertBalances(t, rt, st, provider, 7, 7)

			st.updatePendingDealState(rt, dealId, curEpoch)

			assertBalances(t, rt, st, client, 550, 0)
			assertBalances(t, rt, st, provider, 0, 0)

			rt.ExpectAbort(exitcode.ErrIllegalState, func() {
				st.mustGetDeal(rt, dealId)
			})
		})

		t.Run("deal is expired/completed so funds are unlocked after payment", func(t *testing.T) {
			dealState := &DealState{
				SectorStartEpoch: dealStartEpoch,
				LastUpdatedEpoch: epochUndefined,
				SlashEpoch:       epochUndefined,
			}

			rt, st, dealId := setup(dealState)
			curEpoch := dealEndEpoch + 1
			rt.SetEpoch(curEpoch)

			assertBalances(t, rt, st, client, 550, 500)
			assertBalances(t, rt, st, provider, 7, 7)

			st.updatePendingDealState(rt, dealId, curEpoch)

			// There should be no balance changes, as payments are done
			assertBalances(t, rt, st, client, 50, 0)
			assertBalances(t, rt, st, provider, 507, 0)

			// State stops tracking this deal
			rt.ExpectAbort(exitcode.ErrIllegalState, func() {
				st.mustGetDeal(rt, dealId)
			})
		})
	})
}

func testProposal(t *testing.T, provider, client address.Address) *DealProposal {
	h, err := mh.Sum([]byte("test"), mh.SHA3, 4)
	assert.NoError(t, err)

	return &DealProposal{
		PieceCID:             cid.NewCidV1(cid.Raw, h),
		PieceSize:            0,
		Client:               client,
		Provider:             provider,
		StartEpoch:           0,
		EndEpoch:             0,
		StoragePricePerEpoch: big.Zero(),
		ProviderCollateral:   big.Zero(),
		ClientCollateral:     big.Zero(),
	}
}

func addTestDealState(t *testing.T, rt Runtime, st *State, id abi.DealID, ds *DealState) {
	states := AsDealStateArray(adt.AsStore(rt), st.States)
	err := states.Set(id, ds)
	assert.NoError(t, err)
	st.States = states.Root()
}

func addTestDeal(t *testing.T, rt Runtime, st *State, id abi.DealID, dp *DealProposal) {
	proposals := AsDealProposalArray(adt.AsStore(rt), st.Proposals)
	err := proposals.Set(id, dp)
	st.Proposals = proposals.Root()

	dbp := AsSetMultimap(adt.AsStore(rt), st.DealIDsByParty)
	if err = dbp.Put(dp.Client, id); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "set client deal id: %v", err)
	}
	if err = dbp.Put(dp.Provider, id); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "set provider deal id: %v", err)
	}
	st.DealIDsByParty = dbp.Root()

	assert.NoError(t, err)
}

func assertBalances(t *testing.T, rt Runtime, st *State, addr address.Address, unlocked, locked int64) {
	assert.Equal(t, abi.NewTokenAmount(unlocked), st.GetEscrowBalance(rt, addr))
	assert.Equal(t, abi.NewTokenAmount(locked), st.GetLockedBalance(rt, addr))
}
