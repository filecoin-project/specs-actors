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
)

func TestConstruction(t *testing.T) {
	rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)

	t.Run("initializes all fields", func(t *testing.T) {
		state, err := market.ConstructState(adt.AsStore(rt))

		assert.NoError(t, err)
		assert.True(t, state.Proposals.Defined())
		assert.True(t, state.States.Defined())
		assert.True(t, state.EscrowTable.Defined())
		assert.NotNil(t, state.LockedTable.Defined())
		assert.Equal(t, abi.DealID(0), state.NextID)
		assert.True(t, state.DealIDsByParty.Defined())
	})
}
