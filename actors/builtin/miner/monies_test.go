package miner_test

import (
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime"
	"github.com/filecoin-project/specs-actors/v2/actors/util/smoothing"
)

// Test termination fee
func TestPledgePenaltyForTermination(t *testing.T) {
	nv := runtime.NetworkVersionLatest
	epochTargetReward := abi.NewTokenAmount(1 << 50)
	qaSectorPower := abi.NewStoragePower(1 << 36)
	networkQAPower := abi.NewStoragePower(1 << 50)

	rewardEstimate := smoothing.TestingConstantEstimate(epochTargetReward)
	powerEstimate := smoothing.TestingConstantEstimate(networkQAPower)

	undeclaredPenalty := miner.PledgePenaltyForUndeclaredFault(rewardEstimate, powerEstimate, qaSectorPower, nv)
	bigInitialPledgeFactor := big.NewInt(int64(miner.InitialPledgeFactor))
	bigLifetimeCap := big.NewInt(int64(miner.TerminationLifetimeCapV2))

	t.Run("when undeclared fault fee exceeds expected reward, returns undeclaraed fault fee", func(t *testing.T) {
		// small pledge and means undeclared penalty will be bigger
		initialPledge := abi.NewTokenAmount(1 << 10)
		dayReward := big.Div(initialPledge, bigInitialPledgeFactor)
		twentyDayReward := big.Mul(dayReward, bigInitialPledgeFactor)
		sectorAge := 20 * abi.ChainEpoch(builtin.EpochsInDay)

		fee := miner.PledgePenaltyForTermination(dayReward, sectorAge, twentyDayReward, powerEstimate, qaSectorPower,
			rewardEstimate, big.Zero(), 0, nv)

		assert.Equal(t, undeclaredPenalty, fee)
	})

	t.Run("when expected reward exceeds undeclared fault fee, returns expected reward", func(t *testing.T) {
		// initialPledge equal to undeclaredPenalty guarantees expected reward is greater
		initialPledge := undeclaredPenalty
		dayReward := big.Div(initialPledge, bigInitialPledgeFactor)
		twentyDayReward := big.Mul(dayReward, bigInitialPledgeFactor)
		sectorAgeInDays := int64(20)
		sectorAge := abi.ChainEpoch(sectorAgeInDays * builtin.EpochsInDay)

		fee := miner.PledgePenaltyForTermination(dayReward, sectorAge, twentyDayReward, powerEstimate, qaSectorPower, rewardEstimate, big.Zero(), 0, nv)

		// expect fee to be pledge + br * age * factor where br = pledge/initialPledgeFactor
		expectedFee := big.Add(
			initialPledge,
			big.Div(
				big.Product(initialPledge, big.NewInt(sectorAgeInDays), miner.TerminationRewardFactorV2.Numerator),
				big.Product(bigInitialPledgeFactor, miner.TerminationRewardFactorV2.Denominator)))
		assert.Equal(t, expectedFee, fee)
	})

	t.Run("sector age is capped", func(t *testing.T) {
		initialPledge := undeclaredPenalty
		dayReward := big.Div(initialPledge, bigInitialPledgeFactor)
		twentyDayReward := big.Mul(dayReward, bigInitialPledgeFactor)
		sectorAge := abi.ChainEpoch(500 * builtin.EpochsInDay)

		fee := miner.PledgePenaltyForTermination(dayReward, sectorAge, twentyDayReward, powerEstimate, qaSectorPower, rewardEstimate, big.Zero(), 0, nv)

		// expect fee to be pledge * br * age-cap * factor where br = pledge/initialPledgeFactor
		expectedFee := big.Add(
			initialPledge,
			big.Div(
				big.Product(initialPledge, bigLifetimeCap, miner.TerminationRewardFactorV2.Numerator),
				big.Product(bigInitialPledgeFactor, miner.TerminationRewardFactorV2.Denominator)))
		assert.Equal(t, expectedFee, fee)
	})

	t.Run("fee for replacement = fee for original sector when power, BR are unchanged", func(t *testing.T) {
		// initialPledge equal to undeclaredPenalty guarantees expected reward is greater
		initialPledge := undeclaredPenalty
		dayReward := big.Div(initialPledge, bigInitialPledgeFactor)
		twentyDayReward := big.Mul(dayReward, bigInitialPledgeFactor)
		sectorAge := abi.ChainEpoch(20 * builtin.EpochsInDay)
		replacementAge := abi.ChainEpoch(2 * builtin.EpochsInDay)

		// use low power, so we don't test SP=SP
		power := big.NewInt(1)

		// fee for old sector if had terminated when it was replaced
		unreplacedFee := miner.PledgePenaltyForTermination(dayReward, sectorAge, twentyDayReward, powerEstimate, power, rewardEstimate, big.Zero(), 0, nv)

		// actual fee including replacement parameters
		actualFee := miner.PledgePenaltyForTermination(dayReward, replacementAge, twentyDayReward, powerEstimate, power, rewardEstimate, dayReward, sectorAge-replacementAge, nv)

		assert.Equal(t, unreplacedFee, actualFee)
	})

	t.Run("fee for replacement = fee for same sector without replacement after lifetime cap", func(t *testing.T) {
		// initialPledge equal to undeclaredPenalty guarantees expected reward is greater
		initialPledge := undeclaredPenalty
		dayReward := big.Div(initialPledge, bigInitialPledgeFactor)
		twentyDayReward := big.Mul(dayReward, bigInitialPledgeFactor)
		sectorAge := abi.ChainEpoch(20 * builtin.EpochsInDay)
		replacementAge := abi.ChainEpoch(miner.TerminationLifetimeCapV2 + 1) * builtin.EpochsInDay

		// use low power, so we don't test SP=SP
		power := big.NewInt(1)

		// fee for new sector with no replacement
		noReplace := miner.PledgePenaltyForTermination(dayReward, replacementAge, twentyDayReward, powerEstimate, power, rewardEstimate, big.Zero(), 0, nv)

		// actual fee including replacement parameters
		withReplace := miner.PledgePenaltyForTermination(dayReward, replacementAge, twentyDayReward, powerEstimate, power, rewardEstimate, dayReward, sectorAge, nv)

		assert.Equal(t, noReplace, withReplace)
	})

	t.Run("charges for replaced sector at replaced sector day rate", func(t *testing.T) {
		// initialPledge equal to undeclaredPenalty guarantees expected reward is greater
		initialPledge := undeclaredPenalty
		dayReward := big.Div(initialPledge, bigInitialPledgeFactor)
		oldDayReward := big.Mul(big.NewInt(2), dayReward)
		twentyDayReward := big.Mul(dayReward, bigInitialPledgeFactor)
		oldSectorAgeInDays := int64(20)
		oldSectorAge := abi.ChainEpoch(oldSectorAgeInDays * builtin.EpochsInDay)
		replacementAgeInDays := int64(15)
		replacementAge := abi.ChainEpoch(replacementAgeInDays * builtin.EpochsInDay)

		// use low power, so termination fee exceeds SP
		power := big.NewInt(1)

		oldPenalty := big.Div(
			big.Product(oldDayReward, big.NewInt(oldSectorAgeInDays), miner.TerminationRewardFactorV2.Numerator),
			miner.TerminationRewardFactorV2.Denominator,
		)
		newPenalty := big.Div(
			big.Product(dayReward, big.NewInt(replacementAgeInDays), miner.TerminationRewardFactorV2.Numerator),
			miner.TerminationRewardFactorV2.Denominator,
		)
		expectedFee := big.Sum(initialPledge, oldPenalty, newPenalty)

		fee := miner.PledgePenaltyForTermination(dayReward, replacementAge, twentyDayReward, powerEstimate, power, rewardEstimate, oldDayReward, oldSectorAge, nv)

		assert.Equal(t, expectedFee, fee)
	})
}

func TestNegativeBRClamp(t *testing.T) {
	epochTargetReward := abi.NewTokenAmount(1 << 50)
	qaSectorPower := abi.NewStoragePower(1 << 36)
	networkQAPower := abi.NewStoragePower(1 << 10)
	powerRateOfChange := abi.NewStoragePower(1 << 10).Neg()
	rewardEstimate := smoothing.NewEstimate(epochTargetReward, big.Zero())
	powerEstimate := smoothing.NewEstimate(networkQAPower, powerRateOfChange)

	fourBR := miner.ExpectedRewardForPower(rewardEstimate, powerEstimate, qaSectorPower, abi.ChainEpoch(4))
	assert.Equal(t, big.Zero(), fourBR)
}
