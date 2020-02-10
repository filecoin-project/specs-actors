package market_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

func TestMarketState(t *testing.T) {
	t.Run("constructor initializes all fields", func(t *testing.T) {
		builder := mock.NewBuilder(context.Background(), address.Undef)

		rt := builder.Build(t)
		store := adt.AsStore(rt)

		emptyMap, err := adt.MakeEmptyMap(store)
		assert.NoError(t, err)

		emptyArray, err := adt.MakeEmptyArray(store)
		assert.NoError(t, err)

		emptyMultiMap, err := market.MakeEmptySetMultimap(store)
		assert.NoError(t, err)

		state := market.ConstructState(emptyArray.Root(), emptyMap.Root(), emptyMultiMap.Root())

		assert.Equal(t, emptyArray.Root(), state.Proposals)
		assert.Equal(t, emptyArray.Root(), state.States)
		assert.Equal(t, emptyMap.Root(), state.EscrowTable)
		assert.Equal(t, emptyMap.Root(), state.LockedTable)
		assert.Equal(t, abi.DealID(0), state.NextID)
		assert.Equal(t, emptyMultiMap.Root(), state.DealIDsByParty)
	})

	t.Run("balance tables", func(t *testing.T) {
		builder := mock.NewBuilder(context.Background(), address.Undef)
		addr := tutil.NewIDAddr(t, 100)

		testCases := []struct {
			delta int64
			total int64
		}{
			{10, 10},
			{20, 30},
			{40, 70},
		}

		setup := func(t *testing.T) (*mock.Runtime, *market.State) {
			rt := builder.Build(t)
			store := adt.AsStore(rt)
			state, err := market.ConstructState(store)
			assert.NoError(t, err)

			return rt, state
		}

		t.Run("AddEscrowBalance adds to escrow table, GetEscrowBalance returns balance", func(t *testing.T) {
			rt, st := setup(t)
			for _, tc := range testCases {
				st.AddEscrowBalance(rt, addr, abi.NewTokenAmount(tc.delta))
				assert.Equal(t, abi.NewTokenAmount(tc.total), st.GetEscrowBalance(rt, addr))
			}
		})

		t.Run("AddLockedBalance adds to escrow table, GetLockedBalance returns balance", func(t *testing.T) {
			rt, st := setup(t)
			for _, tc := range testCases {
				st.AddLockedBalance(rt, addr, abi.NewTokenAmount(tc.delta))
				assert.Equal(t, abi.NewTokenAmount(tc.total), st.GetLockedBalance(rt, addr))
			}
		})
	})
}
