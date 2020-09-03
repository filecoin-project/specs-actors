package miner_test

import (
	"testing"

	"github.com/filecoin-project/specs-actors/v1/actors/builtin"
	"github.com/filecoin-project/specs-actors/v1/actors/util/smoothing"

	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/v1/actors/abi"
	"github.com/filecoin-project/specs-actors/v1/actors/abi/big"
	"github.com/filecoin-project/specs-actors/v1/actors/builtin/miner"
)

// Test termination fee
func TestPledgePenaltyForTermination(t *testing.T) {
	epochTargetReward := abi.NewTokenAmount(1 << 50)
	qaSectorPower := abi.NewStoragePower(1 << 36)
	networkQAPower := abi.NewStoragePower(1 << 50)

	rewardEstimate := smoothing.TestingConstantEstimate(epochTargetReward)
	powerEstimate := smoothing.TestingConstantEstimate(networkQAPower)

	undeclaredPenalty := miner.PledgePenaltyForUndeclaredFault(rewardEstimate, powerEstimate, qaSectorPower)
	bigInitialPledgeFactor := big.NewInt(int64(miner.InitialPledgeFactor))

	t.Run("when undeclared fault fee exceeds expected reward, returns undeclaraed fault fee", func(t *testing.T) {
		// small pledge and means undeclared penalty will be bigger
		initialPledge := abi.NewTokenAmount(1 << 10)
		dayReward := big.Div(initialPledge, big.NewInt(int64(miner.InitialPledgeFactor)))
		twentyDayReward := big.Mul(dayReward, big.NewInt(int64(miner.InitialPledgeFactor)))
		sectorAge := 20 * abi.ChainEpoch(builtin.EpochsInDay)

		fee := miner.PledgePenaltyForTermination(dayReward, sectorAge, twentyDayReward, powerEstimate, qaSectorPower, rewardEstimate, big.Zero(), 0)

		assert.Equal(t, undeclaredPenalty, fee)
	})

	t.Run("when expected reward exceeds undeclared fault fee, returns expected reward", func(t *testing.T) {
		// initialPledge equal to undeclaredPenalty guarantees expected reward is greater
		initialPledge := undeclaredPenalty
		dayReward := big.Div(initialPledge, bigInitialPledgeFactor)
		twentyDayReward := big.Mul(dayReward, bigInitialPledgeFactor)
		sectorAgeInDays := int64(20)
		sectorAge := abi.ChainEpoch(sectorAgeInDays * builtin.EpochsInDay)

		fee := miner.PledgePenaltyForTermination(dayReward, sectorAge, twentyDayReward, powerEstimate, qaSectorPower, rewardEstimate, big.Zero(), 0)

		// expect fee to be pledge * br * age where br = pledge/initialPledgeFactor
		expectedFee := big.Add(
			initialPledge,
			big.Div(
				big.Mul(initialPledge, big.NewInt(sectorAgeInDays)),
				big.NewInt(int64(miner.InitialPledgeFactor))))
		assert.Equal(t, expectedFee, fee)
	})

	t.Run("sector age is capped", func(t *testing.T) {
		initialPledge := undeclaredPenalty
		dayReward := big.Div(initialPledge, bigInitialPledgeFactor)
		twentyDayReward := big.Mul(dayReward, bigInitialPledgeFactor)
		sectorAgeInDays := 500
		sectorAge := abi.ChainEpoch(sectorAgeInDays * builtin.EpochsInDay)

		fee := miner.PledgePenaltyForTermination(dayReward, sectorAge, twentyDayReward, powerEstimate, qaSectorPower, rewardEstimate, big.Zero(), 0)

		// expect fee to be pledge * br * age where br = pledge/initialPledgeFactor
		expectedFee := big.Add(
			initialPledge,
			big.Div(
				big.Mul(initialPledge, big.NewInt(int64(miner.TerminationLifetimeCap))),
				bigInitialPledgeFactor))
		assert.Equal(t, expectedFee, fee)
	})

	t.Run("fee for replacement = fee for original sector when power, BR are unchanged", func(t *testing.T) {
		// initialPledge equal to undeclaredPenalty guarantees expected reward is greater
		initialPledge := undeclaredPenalty
		dayReward := big.Div(initialPledge, bigInitialPledgeFactor)
		twentyDayReward := big.Mul(dayReward, bigInitialPledgeFactor)
		sectorAgeInDays := int64(20)
		sectorAge := abi.ChainEpoch(sectorAgeInDays * builtin.EpochsInDay)
		replacementAge := abi.ChainEpoch(2 * builtin.EpochsInDay)

		// use low power, so we don't test SP=SP
		power := big.NewInt(1)

		// fee for old sector if had terminated when it was replaced
		unreplacedFee := miner.PledgePenaltyForTermination(dayReward, sectorAge, twentyDayReward, powerEstimate, power, rewardEstimate, big.Zero(), 0)

		// actual fee including replacement parameters
		actualFee := miner.PledgePenaltyForTermination(dayReward, replacementAge, twentyDayReward, powerEstimate, power, rewardEstimate, dayReward, sectorAge-replacementAge)

		assert.Equal(t, unreplacedFee, actualFee)
	})

	t.Run("fee for replacement = fee for same sector without replacement after 70 days", func(t *testing.T) {
		// initialPledge equal to undeclaredPenalty guarantees expected reward is greater
		initialPledge := undeclaredPenalty
		dayReward := big.Div(initialPledge, bigInitialPledgeFactor)
		twentyDayReward := big.Mul(dayReward, bigInitialPledgeFactor)
		sectorAgeInDays := int64(20)
		sectorAge := abi.ChainEpoch(sectorAgeInDays * builtin.EpochsInDay)
		replacementAge := abi.ChainEpoch(71 * builtin.EpochsInDay)

		// use low power, so we don't test SP=SP
		power := big.NewInt(1)

		// fee for new sector with no replacement
		noReplace := miner.PledgePenaltyForTermination(dayReward, replacementAge, twentyDayReward, powerEstimate, power, rewardEstimate, big.Zero(), 0)

		// actual fee including replacement parameters
		withReplace := miner.PledgePenaltyForTermination(dayReward, replacementAge, twentyDayReward, powerEstimate, power, rewardEstimate, dayReward, sectorAge)

		assert.Equal(t, noReplace, withReplace)
	})

	t.Run("charges for replaced sector at replaced sector day rate", func(t *testing.T) {
		// initialPledge equal to undeclaredPenalty guarantees expected reward is greater
		initialPledge := undeclaredPenalty
		dayReward := big.Div(initialPledge, bigInitialPledgeFactor)
		oldDayReward := big.Mul(big.NewInt(2), dayReward)
		twentyDayReward := big.Mul(dayReward, bigInitialPledgeFactor)
		sectorAgeInDays := int64(20)
		sectorAge := abi.ChainEpoch(sectorAgeInDays * builtin.EpochsInDay)
		replacementAge := abi.ChainEpoch(15 * builtin.EpochsInDay)

		// use low power, so termination fee exceeds SP
		power := big.NewInt(1)

		expectedFee := big.Div(big.Add(
			big.Mul(big.NewInt(int64(sectorAge)), oldDayReward),
			big.Mul(big.NewInt(int64(replacementAge)), dayReward)),
			big.NewInt(builtin.EpochsInDay))

		fee := miner.PledgePenaltyForTermination(dayReward, replacementAge, twentyDayReward, powerEstimate, power, rewardEstimate, dayReward, sectorAge)

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
