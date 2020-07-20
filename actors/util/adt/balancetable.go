package adt

import (
	"errors"
	"fmt"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
)

// A specialization of a map of addresses to token amounts.
type BalanceTable Map

// Interprets a store as balance table with root `r`.
func AsBalanceTable(s Store, r cid.Cid) (*BalanceTable, error) {
	m, err := AsMap(s, r)
	if err != nil {
		return nil, err
	}

	return &BalanceTable{
		root:  m.root,
		store: s,
	}, nil
}

// Returns the root cid of underlying HAMT.
func (t *BalanceTable) Root() (cid.Cid, error) {
	return (*Map)(t).Root()
}

// Gets the balance for a key. The entry must have been previously initialized.
func (t *BalanceTable) Get(key addr.Address) (abi.TokenAmount, bool, error) {
	var value abi.TokenAmount
	found, err := (*Map)(t).Get(AddrKey(key), &value)
	if !found || err != nil {
		value = big.Zero()
	}

	return value, found, err
}

// Has checks if the balance for a key exists
func (t *BalanceTable) Has(key addr.Address) (bool, error) {
	var value abi.TokenAmount
	return (*Map)(t).Get(AddrKey(key), &value)
}

// Adds an amount to a balance. The entry must have been previously initialized.
func (t *BalanceTable) Add(key addr.Address, value abi.TokenAmount) error {
	prev, found, err := t.Get(key)
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound{t.lastCid, key}
	}
	sum := big.Add(prev, value)
	return (*Map)(t).Put(AddrKey(key), &sum)
}

// Adds an amount to a balance. Create entry if not exists
func (t *BalanceTable) AddCreate(key addr.Address, value abi.TokenAmount) error {
	var prev abi.TokenAmount
	found, err := (*Map)(t).Get(AddrKey(key), &prev)
	if err != nil {
		return err
	}
	if found {
		value = big.Add(prev, value)
	}

	return (*Map)(t).Put(AddrKey(key), &value)
}

// Subtracts up to the specified amount from a balance, without reducing the balance below some minimum.
// Returns the amount subtracted (always positive or zero).
func (t *BalanceTable) SubtractWithMinimum(key addr.Address, req abi.TokenAmount, floor abi.TokenAmount) (abi.TokenAmount, error) {
	prev, found, err := t.Get(key)
	if err != nil {
		return big.Zero(), err
	}
	if !found {
		return big.Zero(), ErrNotFound{t.lastCid, key}
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

// MustSubtract subtracts the given amount from the account's balance
// returns an error if account isn't created or if it has insufficient balance
func (t *BalanceTable) MustSubtract(key addr.Address, req abi.TokenAmount) error {
	subst, err := t.SubtractWithMinimum(key, req, big.Zero())
	if err != nil {
		return err
	}
	if !subst.Equals(req) {
		return errors.New("couldn't subtract the requested amount")
	}
	return nil
}

// Returns the total balance held by this BalanceTable
func (t *BalanceTable) Total() (abi.TokenAmount, error) {
	total := big.Zero()
	var cur abi.TokenAmount
	err := (*Map)(t).ForEach(&cur, func(key string) error {
		total = big.Add(total, cur)
		return nil
	})
	return total, err
}

// Removes an entry from the table, returning the prior value. The entry must have been previously initialized.
func (t *BalanceTable) Remove(key addr.Address) (abi.TokenAmount, error) {
	prev, found, err := t.Get(key)
	if err != nil {
		return big.Zero(), err
	}
	if !found {
		return big.Zero(), ErrNotFound{t.lastCid, key}
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
	Key  interface{}
}

func (e ErrNotFound) Error() string {
	return fmt.Sprintf("no key %v in map root %v", e.Key, e.Root)
}
