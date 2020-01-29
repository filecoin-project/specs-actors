package adt

import (
	"fmt"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
)

// A specialization of a map of addresses to token amounts.
// It is an error to query for a key that doesn't exist.
type BalanceTable Map

// Interprets a store as balance table with root `r`.
func AsBalanceTable(s Store, r cid.Cid) *BalanceTable {
	return &BalanceTable{
		root:  r,
		store: s,
	}
}

// Returns the root cid of underlying HAMT.
func (t *BalanceTable) Root() cid.Cid {
	return t.root
}

// Gets the balance for a key. The entry must have been previously initialized.
func (t *BalanceTable) Get(key addr.Address) (abi.TokenAmount, error) {
	var value abi.TokenAmount
	found, err := (*Map)(t).Get(AddrKey(key), &value)
	if err != nil {
		return big.Zero(), err // The errors from Map carry good information, no need to wrap here.
	}
	if !found {
		return big.Zero(), ErrNotFound{t.root, key}
	}
	return value, nil
}

// Sets the balance for an address, overwriting any previous balance.
func (t *BalanceTable) Set(key addr.Address, value abi.TokenAmount) error {
	return (*Map)(t).Put(AddrKey(key), &value)
}

// Adds an amount to a balance. The entry must have been previously initialized.
func (t *BalanceTable) Add(key addr.Address, value abi.TokenAmount) error {
	prev, err := t.Get(key)
	if err != nil {
		return err
	}
	sum := big.Add(prev, value)
	return (*Map)(t).Put(AddrKey(key), &sum)
}

// Subtracts up to the specified amount from a balance, without reducing the balance below some minimum.
// Returns the amount subtracted (always positive or zero).
func (t *BalanceTable) SubtractWithMininum(key addr.Address, req abi.TokenAmount, floor abi.TokenAmount) (abi.TokenAmount, error) {
	prev, err := t.Get(key)
	if err != nil {
		return big.Zero(), err
	}
	available := big.Max(big.Zero(), big.Sub(prev, floor))
	sub := big.Min(available, req)
	if sub.Sign() > 0 {
		err = t.Add(key, sub.Neg())
		if err != nil {
			return big.Zero(), err
		}
	}
	return sub, nil
}

// Removes an entry from the table, returning the prior value. The entry must have been previously initialized.
func (t *BalanceTable) Remove(key addr.Address) (abi.TokenAmount, error) {
	prev, err := t.Get(key)
	if err != nil {
		return big.Zero(), err
	}
	err = (*Map)(t).Delete(AddrKey(key))
	if err != nil {
		return big.Zero(), err
	}
	return prev, nil
}

// Error type returned when an expected key is absent.
type ErrNotFound struct {
	Root cid.Cid
	Key addr.Address
}

func (e ErrNotFound) Error() string {
	return fmt.Sprintf("no key %v in map root %v", e.Key, e.Root)
}
