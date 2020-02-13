package adt_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

func TestBalanceTable(t *testing.T) {
	t.Run("AddCreate adds or creates", func(t *testing.T) {
		addr := tutil.NewIDAddr(t, 100)
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		store := adt.AsStore(rt)
		emptyMap, err := adt.MakeEmptyMap(store)
		assert.NoError(t, err)

		bt := adt.AsBalanceTable(store, emptyMap.Root())

		has, err := bt.Has(addr)
		assert.NoError(t, err)
		assert.False(t, has)

		err = bt.AddCreate(addr, abi.NewTokenAmount(10))
		assert.NoError(t, err)

		amount, err := bt.Get(addr)
		assert.NoError(t, err)
		assert.Equal(t, abi.NewTokenAmount(10), amount)

		err = bt.AddCreate(addr, abi.NewTokenAmount(20))
		assert.NoError(t, err)

		amount, err = bt.Get(addr)
		assert.NoError(t, err)
		assert.Equal(t, abi.NewTokenAmount(30), amount)
	})
}
