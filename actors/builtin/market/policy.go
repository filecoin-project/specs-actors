package market

import "github.com/filecoin-project/specs-actors/actors/abi"

// DealUpdatesInterval is the number of blocks between payouts for deals
const DealUpdatesInterval = 100

// Bounds (inclusive) on deal duration
func dealDurationBounds(size abi.PaddedPieceSize) (min abi.ChainEpoch, max abi.ChainEpoch) {
	return abi.ChainEpoch(0), abi.ChainEpoch(10000) // PARAM_FINISH
}

func dealPricePerEpochBounds(size abi.PaddedPieceSize, duration abi.ChainEpoch) (min abi.TokenAmount, max abi.TokenAmount) {
	return abi.NewTokenAmount(0), abi.TotalFilecoin // PARAM_FINISH
}

func dealProviderCollateralBounds(pieceSize abi.PaddedPieceSize, duration abi.ChainEpoch) (min abi.TokenAmount, max abi.TokenAmount) {
	return abi.NewTokenAmount(0), abi.TotalFilecoin // PARAM_FINISH
}

func dealClientCollateralBounds(pieceSize abi.PaddedPieceSize, duration abi.ChainEpoch) (min abi.TokenAmount, max abi.TokenAmount) {
	return abi.NewTokenAmount(0), abi.TotalFilecoin // PARAM_FINISH
}

// Penalty to provider deal collateral if the deadline expires before sector commitment.
func collateralPenaltyForDealActivationMissed(providerCollateral abi.TokenAmount) abi.TokenAmount {
	return providerCollateral // PARAM_FINISH
}
