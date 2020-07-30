package miner

import (
	"github.com/filecoin-project/specs-actors/actors/abi"
)

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
