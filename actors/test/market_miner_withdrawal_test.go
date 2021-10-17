package test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v7/actors/builtin"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v7/support/ipld"
	"github.com/filecoin-project/specs-actors/v7/support/vm"
)

func TestMarketWithdraw(t *testing.T) {

	t.Run("withdraw all funds", func(t *testing.T) {
		ctx := context.Background()
		v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
		initialBalance := big.Mul(big.NewInt(6), big.NewInt(1e18))
		addrs := vm.CreateAccounts(ctx, t, v, 1, initialBalance, 93837778)
		caller := addrs[0]

		threeFIL := big.Mul(big.NewInt(3), vm.FIL)
		assertAddCollateralAndWithdraw(t, ctx, v, threeFIL, threeFIL, threeFIL, builtin.StorageMarketActorAddr, caller)
	})
	t.Run("withdraw as much as possible", func(t *testing.T) {
		ctx := context.Background()
		v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
		initialBalance := big.Mul(big.NewInt(6), big.NewInt(1e18))
		addrs := vm.CreateAccounts(ctx, t, v, 1, initialBalance, 93837778)
		caller := addrs[0]

		// Add 2 FIL of collateral attempt to withdraw 3
		twoFIL := big.Mul(big.NewInt(2), vm.FIL)
		threeFIL := big.Mul(big.NewInt(3), vm.FIL)

		assertAddCollateralAndWithdraw(t, ctx, v, twoFIL, twoFIL, threeFIL, builtin.StorageMarketActorAddr, caller)
	})
	t.Run("withdraw 0", func(t *testing.T) {
		ctx := context.Background()
		v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
		initialBalance := big.Mul(big.NewInt(6), big.NewInt(1e18))
		addrs := vm.CreateAccounts(ctx, t, v, 1, initialBalance, 93837778)
		caller := addrs[0]

		// Add 0 FIL of collateral attempt to withdraw 3
		threeFIL := big.Mul(big.NewInt(3), vm.FIL)

		assertAddCollateralAndWithdraw(t, ctx, v, big.Zero(), big.Zero(), threeFIL, builtin.StorageMarketActorAddr, caller)
	})
}

func TestMinerWithdraw(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	addrs := vm.CreateAccounts(ctx, t, v, 2, big.Mul(big.NewInt(10_000), vm.FIL), 93837778)
	worker, owner := addrs[0], addrs[1]

	// create miner
	params := power.CreateMinerParams{
		Owner:               owner,
		Worker:              worker,
		WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		Peer:                abi.PeerID("not really a peer id"),
	}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.StoragePowerActorAddr, big.Zero(), builtin.MethodsPower.CreateMiner, &params)

	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)
	mAddr := minerAddrs.IDAddress

	t.Run("withdraw all funds", func(t *testing.T) {

		threeFIL := big.Mul(big.NewInt(3), vm.FIL)
		assertAddCollateralAndWithdraw(t, ctx, v, threeFIL, threeFIL, threeFIL, mAddr, owner)
	})

	t.Run("withdraw as much as possible", func(t *testing.T) {
		twoFIL := big.Mul(big.NewInt(2), vm.FIL)
		threeFIL := big.Mul(big.NewInt(3), vm.FIL)

		assertAddCollateralAndWithdraw(t, ctx, v, twoFIL, twoFIL, threeFIL, mAddr, owner)
	})

	t.Run("withdraw 0", func(t *testing.T) {
		threeFIL := big.Mul(big.NewInt(3), vm.FIL)

		assertAddCollateralAndWithdraw(t, ctx, v, big.Zero(), big.Zero(), threeFIL, mAddr, owner)
	})

	t.Run("withdraw from non-owner address fails", func(t *testing.T) {
		oneFIL := big.Mul(big.NewInt(1), vm.FIL)
		vm.ApplyOk(t, v, worker, mAddr, oneFIL, builtin.MethodSend, nil)
		params := &miner.WithdrawBalanceParams{
			AmountRequested: oneFIL,
		}
		res := vm.RequireApplyMessage(t, v, worker, mAddr, big.Zero(), builtin.MethodsMiner.WithdrawBalance, params, t.Name())
		assert.Equal(t, exitcode.ErrForbidden, res.Code)
	})
}

// Precondition: escrow is a market or miner addr.  If miner address caller must be the owner address.
// 1. Add collateral to escrow address
// 2. Send a withdraw message attempting to remove `requested` funds
// 3. Assert correct return value and actor balance transfer
func assertAddCollateralAndWithdraw(t *testing.T, ctx context.Context, v *vm.VM, collateral, expectedWithdrawn, requested abi.TokenAmount, escrow, caller address.Address) {
	// get code cid
	e := requireActor(t, v, escrow)
	aType := e.Code
	if !aType.Equals(builtin.StorageMinerActorCodeID) && !aType.Equals(builtin.StorageMarketActorCodeID) {
		t.Fatalf("unexepcted escrow address actor type: %s", builtin.ActorNameByCode(e.Code))
	}

	// caller initial balance
	c := requireActor(t, v, caller)
	callerInitialBalance := c.Balance

	// add collateral
	if collateral.GreaterThan(big.Zero()) {
		switch aType {
		case builtin.StorageMinerActorCodeID:
			vm.ApplyOk(t, v, caller, escrow, collateral, builtin.MethodSend, nil)
		case builtin.StorageMarketActorCodeID:
			vm.ApplyOk(t, v, caller, escrow, collateral, builtin.MethodsMarket.AddBalance, &caller)
		default:
			t.Fatalf("unreachable")
		}
	}

	c = requireActor(t, v, caller)
	assert.Equal(t, big.Sub(callerInitialBalance, collateral), c.Balance)

	// attempt to withdraw withdrawal
	var ret interface{}
	switch aType {
	case builtin.StorageMinerActorCodeID:
		params := &miner.WithdrawBalanceParams{
			AmountRequested: requested,
		}
		ret = vm.ApplyOk(t, v, caller, escrow, big.Zero(), builtin.MethodsMiner.WithdrawBalance, params)
	case builtin.StorageMarketActorCodeID:
		params := &market.WithdrawBalanceParams{
			ProviderOrClientAddress: caller,
			Amount:                  collateral,
		}
		ret = vm.ApplyOk(t, v, caller, escrow, big.Zero(), builtin.MethodsMarket.WithdrawBalance, params)
	default:
		t.Fatalf("unreachable")
	}
	withdrawn, ok := ret.(*abi.TokenAmount)
	require.True(t, ok)
	require.NotNil(t, withdrawn)
	assert.Equal(t, expectedWithdrawn, *withdrawn)

	c = requireActor(t, v, caller)
	assert.Equal(t, callerInitialBalance, c.Balance)
}
