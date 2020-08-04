package miner

import (
	"github.com/filecoin-project/specs-actors/actors/abi"
)

// VestingFunds represents the vesting table state for the miner.
// It is a slice of (VestingEpoch, VestingAmount).
// The slice will always be sorted by the VestingEpoch.
type VestingFunds struct {
	Funds []VestingFund
}

// VestingFund represents miner funds that will vest at the given epoch.
type VestingFund struct {
	Epoch  abi.ChainEpoch
	Amount abi.TokenAmount
}

// ConstructVestingFunds constructs empty VestingFunds state.
func ConstructVestingFunds() *VestingFunds {
	v := new(VestingFunds)
	v.Funds = nil
	return v
}
