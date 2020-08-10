package miner_test

import (
	"testing"

	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/util/smoothing"

	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
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

		fee := miner.PledgePenaltyForTermination(dayReward, big.Zero(), twentyDayReward, sectorAge, 0, rewardEstimate, powerEstimate, qaSectorPower)

		assert.Equal(t, undeclaredPenalty, fee)
	})

	t.Run("when expected reward exceeds undeclared fault fee, returns expected reward", func(t *testing.T) {
		// initialPledge equal to undeclaredPenalty guarantees expected reward is greater
		initialPledge := undeclaredPenalty
		dayReward := big.Div(initialPledge, bigInitialPledgeFactor)
		twentyDayReward := big.Mul(dayReward, bigInitialPledgeFactor)
		sectorAgeInDays := int64(20)
		sectorAge := abi.ChainEpoch(sectorAgeInDays * builtin.EpochsInDay)

		fee := miner.PledgePenaltyForTermination(dayReward, big.Zero(), twentyDayReward, sectorAge, 0, rewardEstimate, powerEstimate, qaSectorPower)

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

		fee := miner.PledgePenaltyForTermination(dayReward, big.Zero(), twentyDayReward, sectorAge, 0, rewardEstimate, powerEstimate, qaSectorPower)

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
		unreplacedFee := miner.PledgePenaltyForTermination(
			dayReward,
			big.Zero(),
			twentyDayReward,
			sectorAge,
			0,
			rewardEstimate, powerEstimate, power)

		// actual fee including replacement parameters
		actualFee := miner.PledgePenaltyForTermination(
			dayReward,
			dayReward,
			twentyDayReward,
			replacementAge,
			sectorAge-replacementAge,
			rewardEstimate, powerEstimate, power)

		// newFee + oldFee double counts the twentyDay reward so subtract it from expectation
		assert.Equal(t, unreplacedFee, actualFee)
	})
}
