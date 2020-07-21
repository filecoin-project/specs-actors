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

// if the returned error is not nil, the Runtime will exit with the returned exit code.
// if the error is nil, we don't care about the exitcode.
func (m *marketStateMutation) lockClientAndProviderBalances(proposal *DealProposal) (error, exitcode.ExitCode) {
	err, code := m.maybeLockBalance(proposal.Client, proposal.ClientBalanceRequirement())
	if err != nil {
		return fmt.Errorf("failed to lock client funds: %w", err), code
	}

	err, code = m.maybeLockBalance(proposal.Provider, proposal.ProviderCollateral)
	if err != nil {
		return fmt.Errorf("failed to lock provider funds: %w", err), code
	}

	m.totalClientLockedCollateral = big.Add(m.totalClientLockedCollateral, proposal.ClientCollateral)
	m.totalClientStorageFee = big.Add(m.totalClientStorageFee, proposal.TotalStorageFee())
	m.totalProviderLockedCollateral = big.Add(m.totalProviderLockedCollateral, proposal.ProviderCollateral)

	return nil, exitcode.Ok
}

func (m *marketStateMutation) unlockBalance(addr addr.Address, amount abi.TokenAmount, lockReason BalanceLockingReason) error {
	Assert(amount.GreaterThanEqual(big.Zero()))

	err := m.lockedTable.MustSubtract(addr, amount)
	if err != nil {
		return xerrors.Errorf("subtracting from locked balance: %v", err)
	}

	switch lockReason {
	case ClientCollateral:
		m.totalClientLockedCollateral = big.Sub(m.totalClientLockedCollateral, amount)
	case ClientStorageFee:
		m.totalClientStorageFee = big.Sub(m.totalClientStorageFee, amount)
	case ProviderCollateral:
		m.totalProviderLockedCollateral = big.Sub(m.totalProviderLockedCollateral, amount)
	}

	return nil
}

// move funds from locked in client to available in provider
func (m *marketStateMutation) transferBalance(rt Runtime, fromAddr addr.Address, toAddr addr.Address, amount abi.TokenAmount) {
	Assert(amount.GreaterThanEqual(big.Zero()))

	if err := m.escrowTable.MustSubtract(fromAddr, amount); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "subtract from escrow: %v", err)
	}

	if err := m.unlockBalance(fromAddr, amount, ClientStorageFee); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "subtract from locked: %v", err)
	}

	if err := m.escrowTable.Add(toAddr, amount); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "add to escrow: %v", err)
	}
}

func (m *marketStateMutation) slashBalance(addr addr.Address, amount abi.TokenAmount, reason BalanceLockingReason) error {
	Assert(amount.GreaterThanEqual(big.Zero()))

	if err := m.escrowTable.MustSubtract(addr, amount); err != nil {
		return xerrors.Errorf("subtract from escrow: %v", err)
	}

	return m.unlockBalance(addr, amount, reason)
}

func (m *marketStateMutation) maybeLockBalance(addr addr.Address, amount abi.TokenAmount) (error, exitcode.ExitCode) {
	Assert(amount.GreaterThanEqual(big.Zero()))

	prevLocked, err, code := getBalance(m.lockedTable, addr)
	if err != nil {
		return fmt.Errorf("failed to get locked balance: %w", err), code
	}

	escrowBalance, err, code := getBalance(m.escrowTable, addr)
	if err != nil {
		return fmt.Errorf("failed to get escrow balance: %w", err), code
	}

	if big.Add(prevLocked, amount).GreaterThan(escrowBalance) {
		return xerrors.Errorf("not enough balance to lock for addr %s: %s <  %s + %s", addr, escrowBalance, prevLocked, amount),
			exitcode.ErrInsufficientFunds
	}

	if err := m.lockedTable.Add(addr, amount); err != nil {
		return fmt.Errorf("failed to add locked balance: %w", err), exitcode.ErrIllegalState
	}
	return nil, exitcode.Ok
}

func (m *marketStateMutation) removeAccountIfNoBalance(addr addr.Address) (error, exitcode.ExitCode) {
	bal, err, code := getBalance(m.escrowTable, addr)
	if err != nil {
		return xerrors.Errorf("failed to get escrow balance: %w", err), code
	}

	if bal.Equals(big.Zero()) {
		prev, err := m.escrowTable.Remove(addr)
		if err != nil {
			return xerrors.Errorf("failed to remove account: %w", err), exitcode.ErrIllegalState
		}
		AssertMsg(prev.Equals(big.Zero()), "previous escrow balance should be zero")

		// remove locked balance account
		bal, err, code = getBalance(m.lockedTable, addr)
		if err != nil {
			return xerrors.Errorf("failed to get locked balance: %w", err), code
		}
		if bal.Equals(big.Zero()) {
			prev, err = m.lockedTable.Remove(addr)
			if err != nil {
				return xerrors.Errorf("failed to remove locked account: %w", err), exitcode.ErrIllegalState
			}
			AssertMsg(prev.Equals(big.Zero()), "previous locked balance should be zero")
		}
	}

	return nil, exitcode.Ok
}

func getBalance(bt *adt.BalanceTable, a addr.Address) (abi.TokenAmount, error, exitcode.ExitCode) {
	ret, found, err := bt.Get(a)
	if err != nil {
		return big.Zero(), fmt.Errorf("failed to load balance table :%w", err), exitcode.ErrIllegalArgument
	}
	if !found {
		return big.Zero(), errors.New("account does not exist"), exitcode.ErrInsufficientFunds
	}

	return ret, nil, exitcode.Ok
}
