package test_test

import (
	"context"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"testing"

	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	vm "github.com/filecoin-project/specs-actors/support/vm"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarketWithdraw(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t)
	initialBalance := big.Mul(big.NewInt(6), big.NewInt(1e18))
	addrs := vm.CreateAccounts(ctx, t, v, 1, initialBalance, 93837778)
	caller := addrs[0]

	// add market collateral for clients and miner
	collateral := big.Mul(big.NewInt(3), vm.FIL)
	_, code := v.ApplyMessage(caller, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &caller)
	require.Equal(t, exitcode.Ok, code)

	a, found, err := v.GetActor(caller)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, big.Sub(initialBalance, collateral), a.Balance)

	// withdraw collateral from market
	params := &market.WithdrawBalanceParams{
		ProviderOrClientAddress: caller,
		Amount:                  collateral,
	}
	_, code = v.ApplyMessage(caller, builtin.StorageMarketActorAddr, big.Zero(), builtin.MethodsMarket.WithdrawBalance, params)
	require.Equal(t, exitcode.Ok, code)

	a, found, err = v.GetActor(caller)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, initialBalance, a.Balance)
}
