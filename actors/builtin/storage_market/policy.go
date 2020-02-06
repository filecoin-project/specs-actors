package storage_market

import "github.com/filecoin-project/specs-actors/actors/abi"

// Bounds (inclusive) on deal duration
func dealDurationBounds(size abi.PieceSize) (min abi.ChainEpoch, max abi.ChainEpoch) {
	return abi.ChainEpoch(0), abi.ChainEpoch(20) // PARAM_FINISH
}

func dealPricePerEpochBounds(size abi.PieceSize, duration abi.ChainEpoch) (min abi.TokenAmount, max abi.TokenAmount) {
	return abi.NewTokenAmount(0), abi.NewTokenAmount(1 << 20) // PARAM_FINISH
}

func dealProviderCollateralBounds(pieceSize abi.PieceSize, duration abi.ChainEpoch) (min abi.TokenAmount, max abi.TokenAmount) {
	return abi.NewTokenAmount(0), abi.NewTokenAmount(1 << 20) // PARAM_FINISH
}

func dealClientCollateralBounds(pieceSize abi.PieceSize, duration abi.ChainEpoch) (min abi.TokenAmount, max abi.TokenAmount) {
	return abi.NewTokenAmount(0), abi.NewTokenAmount(1 << 20) // PARAM_FINISH
}

// Penalty to provider deal collateral if the deadline expires before sector commitment.
func collateralPenaltyForDealActivationMissed(providerCollateral abi.TokenAmount) abi.TokenAmount {
	return providerCollateral // PARAM_FINISH
}
