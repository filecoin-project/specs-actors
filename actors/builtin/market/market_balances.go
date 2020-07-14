package market

import (
	"errors"
	"fmt"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	. "github.com/filecoin-project/specs-actors/actors/util"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"golang.org/x/xerrors"
)

func (st *State) lockBalancesAndFlush(store adt.Store, deals ...*dealWithId) (error, exitcode.ExitCode) {
	et, err := adt.AsBalanceTable(store, st.EscrowTable)
	if err != nil {
		return fmt.Errorf("failed to load escrow balances: %w", err), exitcode.ErrIllegalState
	}

	lt, err := adt.AsBalanceTable(store, st.LockedTable)
	if err != nil {
		return fmt.Errorf("failed to load locked balances: %w", err), exitcode.ErrIllegalState
	}

	for i := range deals {
		d := deals[i]

		err, code := st.maybeLockBalance(et, lt, d.proposal.Client, d.proposal.ClientBalanceRequirement())
		if err != nil {
			return fmt.Errorf("failed to lock client funds: %w", err), code
		}

		err, code = st.maybeLockBalance(et, lt, d.proposal.Provider, d.proposal.ProviderCollateral)
		if err != nil {
			return fmt.Errorf("failed to lock provider funds: %w", err), code
		}

		st.TotalClientLockedCollateral = big.Add(st.TotalClientLockedCollateral, d.proposal.ClientCollateral)
		st.TotalClientStorageFee = big.Add(st.TotalClientStorageFee, d.proposal.TotalStorageFee())
		st.TotalProviderLockedCollateral = big.Add(st.TotalProviderLockedCollateral, d.proposal.ProviderCollateral)
	}

	if err := st.flushBalances(et, lt); err != nil {
		return err, exitcode.ErrIllegalState
	}
	return nil, exitcode.Ok
}

func (st *State) getBalance(bt *adt.BalanceTable, a addr.Address) (abi.TokenAmount, error, exitcode.ExitCode) {
	ret, found, err := bt.Get(a)
	if err != nil {
		return big.Zero(), fmt.Errorf("failed to load balance table :%w", err), exitcode.ErrIllegalArgument
	}
	if !found {
		return big.Zero(), errors.New("account does not exist"), exitcode.ErrInsufficientFunds
	}

	return ret, nil, exitcode.Ok
}

func (st *State) flushBalances(et, lt *adt.BalanceTable) error {
	var err error
	if st.EscrowTable, err = et.Root(); err != nil {
		return fmt.Errorf("failed to flush escrow balances: %w", err)
	}
	if st.LockedTable, err = lt.Root(); err != nil {
		return fmt.Errorf("failed to flush locked balances: %w", err)
	}
	return nil
}

func (st *State) maybeLockBalance(et, lt *adt.BalanceTable, addr addr.Address, amount abi.TokenAmount) (error, exitcode.ExitCode) {
	Assert(amount.GreaterThanEqual(big.Zero()))

	prevLocked, err, code := st.getBalance(lt, addr)
	if err != nil {
		return fmt.Errorf("failed to get locked balance: %w", err), code
	}

	escrowBalance, err, code := st.getBalance(et, addr)
	if err != nil {
		return fmt.Errorf("failed to get escrow balance: %w", err), code
	}

	if big.Add(prevLocked, amount).GreaterThan(escrowBalance) {
		return xerrors.Errorf("not enough balance to lock for addr %s: %s <  %s + %s", addr, escrowBalance, prevLocked, amount),
			exitcode.ErrInsufficientFunds
	}

	if err := lt.Add(addr, amount); err != nil {
		return fmt.Errorf("failed to add locked balance: %w", err), exitcode.ErrIllegalState
	}
	return nil, exitcode.Ok
}

func (st *State) unlockBalance(lt *adt.BalanceTable, addr addr.Address, amount abi.TokenAmount, lockReason BalanceLockingReason) error {
	Assert(amount.GreaterThanEqual(big.Zero()))

	err := lt.MustSubtract(addr, amount)
	if err != nil {
		return xerrors.Errorf("subtracting from locked balance: %v", err)
	}

	switch lockReason {
	case ClientCollateral:
		st.TotalClientLockedCollateral = big.Sub(st.TotalClientLockedCollateral, amount)
	case ClientStorageFee:
		st.TotalClientStorageFee = big.Sub(st.TotalClientStorageFee, amount)
	case ProviderCollateral:
		st.TotalProviderLockedCollateral = big.Sub(st.TotalProviderLockedCollateral, amount)
	}

	return nil
}

// move funds from locked in client to available in provider
func (st *State) transferBalance(rt Runtime, fromAddr addr.Address, toAddr addr.Address, amount abi.TokenAmount, et, lt *adt.BalanceTable) {
	Assert(amount.GreaterThanEqual(big.Zero()))

	if err := et.MustSubtract(fromAddr, amount); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "subtract from escrow: %v", err)
	}

	if err := st.unlockBalance(lt, fromAddr, amount, ClientStorageFee); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "subtract from locked: %v", err)
	}

	if err := et.Add(toAddr, amount); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "add to escrow: %v", err)
	}
}

func (st *State) slashBalance(et, lt *adt.BalanceTable, addr addr.Address, amount abi.TokenAmount, reason BalanceLockingReason) error {
	Assert(amount.GreaterThanEqual(big.Zero()))

	if err := et.MustSubtract(addr, amount); err != nil {
		return xerrors.Errorf("subtract from escrow: %v", err)
	}

	return st.unlockBalance(lt, addr, amount, reason)
}
