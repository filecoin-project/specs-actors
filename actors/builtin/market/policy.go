package market

import (
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
)

// PARAM_SPEC
// The number of blocks between payouts for deals
const DealUpdatesInterval = builtin.EpochsInDay

// PARAM_SPEC
// The percentage of normalized cirulating
// supply that must be covered by provider collateral in a deal
var ProvCollateralPercentSupplyNum = big.NewInt(5)
var ProvCollateralPercentSupplyDenom = big.NewInt(100)

// PARAM_SPEC
// Minimum Deal Duration
var DealMinDuration = abi.ChainEpoch(180 * builtin.EpochsInDay)

// PARAM_SPEC
// Maximum Deal Duration
var DealMaxDuration = abi.ChainEpoch(540 * builtin.EpochsInDay)

// Bounds (inclusive) on deal duration
func dealDurationBounds(size abi.PaddedPieceSize) (min abi.ChainEpoch, max abi.ChainEpoch) {
	return DealMinDuration, DealMaxDuration // PARAM_FINISH
}

func dealPricePerEpochBounds(size abi.PaddedPieceSize, duration abi.ChainEpoch) (min abi.TokenAmount, max abi.TokenAmount) {
	return abi.NewTokenAmount(0), abi.TotalFilecoin // PARAM_FINISH
}

func DealProviderCollateralBounds(pieceSize abi.PaddedPieceSize, verified bool, networkQAPower, baselinePower abi.StoragePower, networkCirculatingSupply abi.TokenAmount) (min abi.TokenAmount, max abi.TokenAmount) {
	// minimumProviderCollateral = (ProvCollateralPercentSupplyNum / ProvCollateralPercentSupplyDenom) * normalizedCirculatingSupply
	// normalizedCirculatingSupply = FILCirculatingSupply * dealPowerShare
	// dealPowerShare = dealQAPower / max(BaselinePower(t), NetworkQAPower(t), dealQAPower)

	lockTargetNum := big.Mul(ProvCollateralPercentSupplyNum, networkCirculatingSupply)
	lockTargetDenom := ProvCollateralPercentSupplyDenom

	qaPower := dealQAPower(pieceSize, verified)
	powerShareNum := qaPower
	powerShareDenom := big.Max(big.Max(networkQAPower, baselinePower), qaPower)

	num := big.Mul(lockTargetNum, powerShareNum)
	denom := big.Mul(lockTargetDenom, powerShareDenom)
	minCollateral := big.Div(num, denom)
	return minCollateral, abi.TotalFilecoin // PARAM_FINISH
}

func DealClientCollateralBounds(pieceSize abi.PaddedPieceSize, duration abi.ChainEpoch) (min abi.TokenAmount, max abi.TokenAmount) {
	return abi.NewTokenAmount(0), abi.TotalFilecoin // PARAM_FINISH
}

// Penalty to provider deal collateral if the deadline expires before sector commitment.
func collateralPenaltyForDealActivationMissed(providerCollateral abi.TokenAmount) abi.TokenAmount {
	return providerCollateral // PARAM_FINISH
}

// Computes the weight for a deal proposal, which is a function of its size and duration.
func DealWeight(proposal *DealProposal) abi.DealWeight {
	dealDuration := big.NewInt(int64(proposal.Duration()))
	dealSize := big.NewIntUnsigned(uint64(proposal.PieceSize))
	dealSpaceTime := big.Mul(dealDuration, dealSize)
	return dealSpaceTime
}

func dealQAPower(dealSize abi.PaddedPieceSize, verified bool) abi.StoragePower {
	scaledUpQuality := big.Zero() // nolint:ineffassign
	if verified {
		scaledUpQuality = big.Lsh(builtin.VerifiedDealWeightMultiplier, builtin.SectorQualityPrecision)
		scaledUpQuality = big.Div(scaledUpQuality, builtin.QualityBaseMultiplier)
	} else {
		scaledUpQuality = big.Lsh(builtin.DealWeightMultiplier, builtin.SectorQualityPrecision)
		scaledUpQuality = big.Div(scaledUpQuality, builtin.QualityBaseMultiplier)
	}
	scaledUpQAPower := big.Mul(scaledUpQuality, big.NewIntUnsigned(uint64(dealSize)))
	return big.Rsh(scaledUpQAPower, builtin.SectorQualityPrecision)
}
