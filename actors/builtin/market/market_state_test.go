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

	setup := func(t *testing.T) (*mock.Runtime, *State) {
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
			rt, st := setup(t)
			for _, tc := range testCases {
				st.AddEscrowBalance(rt, provider, abi.NewTokenAmount(tc.delta))
				assert.Equal(t, abi.NewTokenAmount(tc.total), st.GetEscrowBalance(rt, provider))
			}
		})

		t.Run("AddLockedBalance adds to escrow table, GetLockedBalance returns balance", func(t *testing.T) {
			rt, st := setup(t)
			for _, tc := range testCases {
				st.AddLockedBalance(rt, provider, abi.NewTokenAmount(tc.delta))
				assert.Equal(t, abi.NewTokenAmount(tc.total), st.GetLockedBalance(rt, provider))
			}
		})
	})

	t.Run("mustGetDeal errors or returns deal", func(t *testing.T) {
		rt, st := setup(t)
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
		rt, st := setup(t)
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
