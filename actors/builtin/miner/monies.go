package miner

import (
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
)

// Computes the pledge requirement for committing new quality-adjusted power to the network, given the current
// total power, total pledge commitment, epoch block reward, and circulating token supply.
// In plain language, the pledge requirement is a multiple of the block reward expected to be earned by the
// newly-committed power, holding the per-epoch block reward constant (though in reality it will change over time).
// The network total pledge and circulating supply parameters are currently unused, but may be included in a
// future calculation.
func InitialPledgeForPower(newQAPower abi.StoragePower, networkQAPower abi.StoragePower, networkTotalPledge abi.TokenAmount, epochTargetReward abi.TokenAmount, networkCirculatingSupply abi.TokenAmount) abi.TokenAmount {
	// Details here are still subject to change.
	// PARAM_FINISH
	// https://github.com/filecoin-project/specs-actors/issues/468
	_ = networkCirculatingSupply // TODO: ce use this
	_ = networkTotalPledge       // TODO: ce use this

	if networkQAPower.IsZero() {
		return epochTargetReward
	}
	return big.Div(big.Mul(newQAPower, epochTargetReward), networkQAPower)
}

