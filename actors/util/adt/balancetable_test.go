package adt_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

func TestBalanceTable(t *testing.T) {
	buildBalanceTable := func() *adt.BalanceTable {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		store := adt.AsStore(rt)
		emptyMap := adt.MakeEmptyMap(store)

		bt, err := adt.AsBalanceTable(store, tutil.MustRoot(t, emptyMap))
		require.NoError(t, err)
		return bt
	}

	t.Run("AddCreate adds or creates", func(t *testing.T) {
		addr := tutil.NewIDAddr(t, 100)
		bt := buildBalanceTable()

		has, err := bt.Has(addr)
		assert.NoError(t, err)
		assert.False(t, has)

		err = bt.AddCreate(addr, abi.NewTokenAmount(10))
		assert.NoError(t, err)

		amount, found, err := bt.Get(addr)
		require.True(t, found)
		assert.NoError(t, err)
		assert.Equal(t, abi.NewTokenAmount(10), amount)

		err = bt.AddCreate(addr, abi.NewTokenAmount(20))
		assert.NoError(t, err)

		amount, found, err = bt.Get(addr)
		require.True(t, found)
		assert.NoError(t, err)
		assert.Equal(t, abi.NewTokenAmount(30), amount)
	})

	t.Run("Add works only if account has been created", func(t *testing.T) {
		addr := tutil.NewIDAddr(t, 100)
		bt := buildBalanceTable()

		// fail if account hasnt been created
		require.Error(t, bt.Add(addr, abi.NewTokenAmount(10)))

		// now create account -> add should work
		require.NoError(t, bt.AddCreate(addr, abi.NewTokenAmount(10)))
		require.NoError(t, bt.Add(addr, abi.NewTokenAmount(5)))
		bal, found, err := bt.Get(addr)
		require.True(t, found)
		require.NoError(t, err)
		require.Equal(t, abi.NewTokenAmount(15), bal)
	})

	t.Run("Get works only if account has been created", func(t *testing.T) {
		addr := tutil.NewIDAddr(t, 100)
		bt := buildBalanceTable()

		// found is false if account hasnt been created
		bal, found, err := bt.Get(addr)
		require.False(t, found)
		require.NoError(t, err)
		require.Equal(t, big.Zero(), bal)

		// now create account -> get should work
		require.NoError(t, bt.AddCreate(addr, abi.NewTokenAmount(10)))
		bal, found, err = bt.Get(addr)
		require.True(t, found)
		require.NoError(t, err)
		require.Equal(t, abi.NewTokenAmount(10), bal)
	})

	t.Run("Must subtract fails if account not created or balance is insufficient", func(t *testing.T) {
		addr := tutil.NewIDAddr(t, 100)
		bt := buildBalanceTable()

		// fail if account hasnt been created
		require.Error(t, bt.MustSubtract(addr, abi.NewTokenAmount(0)))

		// create account -> should work now
		require.NoError(t, bt.AddCreate(addr, abi.NewTokenAmount(0)))
		require.NoError(t, bt.MustSubtract(addr, abi.NewTokenAmount(0)))

		require.NoError(t, bt.Add(addr, abi.NewTokenAmount(5)))
		// error if insufficient balance -> however, balance is still subtracted
		// TODO Is that correct ?
		// https://github.com/filecoin-project/specs-actors/issues/652
		require.Error(t, bt.MustSubtract(addr, abi.NewTokenAmount(6)))
		bal, found, err := bt.Get(addr)
		require.True(t, found)
		require.NoError(t, err)
		require.Equal(t, abi.NewTokenAmount(0), bal)

		// balance is sufficient -> should work
		require.NoError(t, bt.Add(addr, abi.NewTokenAmount(5)))
		require.NoError(t, bt.MustSubtract(addr, abi.NewTokenAmount(4)))
		bal, found, err = bt.Get(addr)
		require.True(t, found)
		require.NoError(t, err)
		require.Equal(t, abi.NewTokenAmount(1), bal)
	})

	t.Run("Total returns total amount tracked", func(t *testing.T) {
		addr1 := tutil.NewIDAddr(t, 100)
		addr2 := tutil.NewIDAddr(t, 101)

		bt := buildBalanceTable()
		total, err := bt.Total()
		assert.NoError(t, err)
		assert.Equal(t, big.Zero(), total)

		testCases := []struct {
			amount int64
			addr   address.Address
			total  int64
		}{
			{10, addr1, 10},
			{20, addr1, 30},
			{40, addr2, 70},
			{50, addr2, 120},
		}

		for _, tc := range testCases {
			err = bt.AddCreate(tc.addr, abi.NewTokenAmount(tc.amount))
			assert.NoError(t, err)

			total, err = bt.Total()
			assert.NoError(t, err)
			assert.Equal(t, abi.NewTokenAmount(tc.total), total)
		}
	})
}

func TestSubtractWithMinimum(t *testing.T) {
	buildBalanceTable := func() *adt.BalanceTable {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		store := adt.AsStore(rt)
		emptyMap := adt.MakeEmptyMap(store)

		bt, err := adt.AsBalanceTable(store, tutil.MustRoot(t, emptyMap))
		require.NoError(t, err)
		return bt
	}
	addr := tutil.NewIDAddr(t, 100)
	zeroAmt := abi.NewTokenAmount(0)

	t.Run("fails when account does not exist", func(t *testing.T) {
		bt := buildBalanceTable()
		s, err := bt.SubtractWithMinimum(addr, zeroAmt, zeroAmt)
		require.Error(t, err)
		require.EqualValues(t, zeroAmt, s)
	})

	t.Run("withdraw available when account does not have sufficient balance", func(t *testing.T) {
		bt := buildBalanceTable()
		require.NoError(t, bt.AddCreate(addr, abi.NewTokenAmount(5)))

		s, err := bt.SubtractWithMinimum(addr, abi.NewTokenAmount(2), abi.NewTokenAmount(4))
		require.NoError(t, err)
		require.EqualValues(t, abi.NewTokenAmount(1), s)
	})

	t.Run("account has sufficient balance", func(t *testing.T) {
		bt := buildBalanceTable()
		require.NoError(t, bt.AddCreate(addr, abi.NewTokenAmount(5)))

		s, err := bt.SubtractWithMinimum(addr, abi.NewTokenAmount(3), abi.NewTokenAmount(2))
		require.NoError(t, err)
		require.EqualValues(t, abi.NewTokenAmount(3), s)
	})
}

func TestRemove(t *testing.T) {
	buildBalanceTable := func() *adt.BalanceTable {
		rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
		store := adt.AsStore(rt)
		emptyMap := adt.MakeEmptyMap(store)

		bt, err := adt.AsBalanceTable(store, tutil.MustRoot(t, emptyMap))
		require.NoError(t, err)
		return bt
	}

	addr := tutil.NewIDAddr(t, 100)
	bt := buildBalanceTable()

	// remove will give error if account has not been created
	bal, err := bt.Remove(addr)
	require.Error(t, err)
	require.Equal(t, big.Zero(), bal)

	// now create account -> remove should work
	require.NoError(t, bt.AddCreate(addr, abi.NewTokenAmount(10)))
	bal, found, err := bt.Get(addr)
	require.True(t, found)
	require.NoError(t, err)
	require.Equal(t, abi.NewTokenAmount(10), bal)

	amt, err := bt.Remove(addr)
	require.NoError(t, err)
	require.EqualValues(t, abi.NewTokenAmount(10), amt)

	_, found, err = bt.Get(addr)
	require.NoError(t, err)
	require.False(t, found)
}
