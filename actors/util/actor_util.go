package util

import (
	addr "github.com/filecoin-project/go-address"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
)

type BalanceTableHAMT map[addr.Address]abi.TokenAmount

type SectorStorageWeightDesc struct {
	SectorSize abi.SectorSize
	Duration   abi.ChainEpoch
	DealWeight abi.DealWeight
}

// Create a new entry in the balance table, with the specified initial balance.
// May fail if the specified address already exists in the table.
func BalanceTable_WithNewAddressEntry(table BalanceTableHAMT, address addr.Address, initBalance abi.TokenAmount) (
	ret BalanceTableHAMT, ok bool) {

	IMPL_FINISH()
	panic("")
}

// Delete the specified entry in the balance table.
// May fail if the specified address does not exist in the table.
func BalanceTable_WithDeletedAddressEntry(table BalanceTableHAMT, address addr.Address) (
	ret BalanceTableHAMT, ok bool) {

	IMPL_FINISH()
	panic("")
}

// Add the given amount to the given address's balance table entry.
func BalanceTable_WithAdd(table BalanceTableHAMT, address addr.Address, amount abi.TokenAmount) (
	ret BalanceTableHAMT, ok bool) {

	IMPL_FINISH()
	panic("")
}

// Subtract the given amount (or as much as possible, without making the resulting amount negative)
// from the given address's balance table entry, independent of any minimum balance maintenance
// requirement.
// Note: ok should be set to true here, even if the operation caused the entry to hit zero.
// The only failure case is when the address does not exist in the table.
func BalanceTable_WithSubtractPreservingNonnegative(
	table BalanceTableHAMT, address addr.Address, amount abi.TokenAmount) (
	ret BalanceTableHAMT, amountSubtracted abi.TokenAmount, ok bool) {

	return BalanceTable_WithExtractPartial(table, address, amount, abi.NewTokenAmount(0))
}

// Extract as much as possible (may be zero) up to the specified amount from the given address's
// balance table entry, subject to the requirement of a minimum balance `minBalanceMaintain`.
func BalanceTable_WithExtractPartial(
	table BalanceTableHAMT, address addr.Address, amount abi.TokenAmount, minBalanceMaintain abi.TokenAmount) (
	ret BalanceTableHAMT, amountExtracted abi.TokenAmount, ok bool) {

	IMPL_FINISH()
	panic("")
}

// Extract all available from the given address's balance table entry.
func BalanceTable_WithExtractAll(table BalanceTableHAMT, address addr.Address) (
	ret BalanceTableHAMT, amountExtracted abi.TokenAmount, ok bool) {

	IMPL_FINISH()
	panic("")
}

// Retrieve the balance table entry corresponding to the given address.
func BalanceTable_GetEntry(
	table BalanceTableHAMT, address addr.Address) (
	ret abi.TokenAmount, ok bool) {

	IMPL_FINISH()
	panic("")
}

func BalanceTableHAMT_Empty() BalanceTableHAMT {
	IMPL_FINISH()
	panic("")
}
