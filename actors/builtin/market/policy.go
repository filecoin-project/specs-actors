package market

import (
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/specs-actors/v7/actors/builtin"
)

type Policy struct {
	DealUpdatesInterval            abi.ChainEpoch
	providerCollateralSupplyTarget builtin.BigFrac
	DealMinDuration                abi.ChainEpoch
	DealMaxDuration                abi.ChainEpoch
}

func (p *Policy) SetPercentageCollateralSupply(percentageCollateralSupply int64)  {
	p.providerCollateralSupplyTarget.Numerator = big.NewInt(percentageCollateralSupply)
}

func MakeMarketPolicy(dealUpdatesInterval abi.ChainEpoch,
	percentageCollateralSupply int64,
	dealMinDuration abi.ChainEpoch,
	dealMaxDuration abi.ChainEpoch) Policy {
	return Policy{
		DealUpdatesInterval: dealUpdatesInterval,
		providerCollateralSupplyTarget: builtin.BigFrac{
			Numerator:   big.NewInt(percentageCollateralSupply),
			Denominator: big.NewInt(100),
		},
		DealMinDuration: dealMinDuration,
		DealMaxDuration: dealMaxDuration,
	}
}

var DefaultMarketPolicy = MakeMarketPolicy(builtin.DefaultNetworkPolicy.EpochsInDay(),
	1,
	180*builtin.DefaultNetworkPolicy.EpochsInDay(),
	540*builtin.DefaultNetworkPolicy.EpochsInDay())

var CurrentMarketPolicy = DefaultMarketPolicy

// DealMaxLabelSize is the maximum size of a deal label.
const DealMaxLabelSize = 256

func DealUpdatesInterval() abi.ChainEpoch {
	return CurrentMarketPolicy.DealUpdatesInterval
}

func ProviderCollateralSupplyTarget() builtin.BigFrac {
	return CurrentMarketPolicy.providerCollateralSupplyTarget
}
func DealMinDuration() abi.ChainEpoch {
	return CurrentMarketPolicy.DealMinDuration
}
func DealMaxDuration() abi.ChainEpoch {
	return CurrentMarketPolicy.DealMaxDuration
}

// Bounds (inclusive) on deal duration
func DealDurationBounds(_ abi.PaddedPieceSize) (min abi.ChainEpoch, max abi.ChainEpoch) {
	return CurrentMarketPolicy.DealMinDuration, CurrentMarketPolicy.DealMaxDuration
}

func DealPricePerEpochBounds(_ abi.PaddedPieceSize, _ abi.ChainEpoch) (min abi.TokenAmount, max abi.TokenAmount) {
	return abi.NewTokenAmount(0), builtin.TotalFilecoin
}

func DealProviderCollateralBounds(pieceSize abi.PaddedPieceSize, verified bool, networkRawPower, networkQAPower, baselinePower abi.StoragePower,
	networkCirculatingSupply abi.TokenAmount) (min, max abi.TokenAmount) {
	// minimumProviderCollateral = ProviderCollateralSupplyTarget * normalizedCirculatingSupply
	// normalizedCirculatingSupply = networkCirculatingSupply * dealPowerShare
	// dealPowerShare = dealRawPower / max(BaselinePower(t), NetworkRawPower(t), dealRawPower)

	lockTargetNum := big.Mul(CurrentMarketPolicy.providerCollateralSupplyTarget.Numerator, networkCirculatingSupply)
	lockTargetDenom := CurrentMarketPolicy.providerCollateralSupplyTarget.Denominator
	powerShareNum := big.NewIntUnsigned(uint64(pieceSize))
	powerShareDenom := big.Max(big.Max(networkRawPower, baselinePower), powerShareNum)

	num := big.Mul(lockTargetNum, powerShareNum)
	denom := big.Mul(lockTargetDenom, powerShareDenom)
	minCollateral := big.Div(num, denom)
	return minCollateral, builtin.TotalFilecoin
}

func DealClientCollateralBounds(_ abi.PaddedPieceSize, _ abi.ChainEpoch) (min abi.TokenAmount, max abi.TokenAmount) {
	return abi.NewTokenAmount(0), builtin.TotalFilecoin
}

// Penalty to provider deal collateral if the deadline expires before sector commitment.
func CollateralPenaltyForDealActivationMissed(providerCollateral abi.TokenAmount) abi.TokenAmount {
	return providerCollateral
}

// Computes the weight for a deal proposal, which is a function of its size and duration.
func DealWeight(proposal *DealProposal) abi.DealWeight {
	dealDuration := big.NewInt(int64(proposal.Duration()))
	dealSize := big.NewIntUnsigned(uint64(proposal.PieceSize))
	dealSpaceTime := big.Mul(dealDuration, dealSize)
	return dealSpaceTime
}
