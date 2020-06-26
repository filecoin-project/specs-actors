package miner_test

import (
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"testing"

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
	undeclaredPenalty := miner.PledgePenaltyForUndeclaredFault(epochTargetReward, networkQAPower, qaSectorPower)

	t.Run("when undeclared fault fee exceeds expected reward, returns undeclaraed fault fee", func(t *testing.T) {
		// small pledge and means undeclared penalty will be bigger
		initialPledge := abi.NewTokenAmount(1 << 10)
		sectorAge := abi.ChainEpoch(100)

		fee := miner.PledgePenaltyForTermination(initialPledge, sectorAge, epochTargetReward, networkQAPower, qaSectorPower)

		assert.Equal(t, undeclaredPenalty, fee)
	})

	t.Run("when expected reward exceeds undeclared fault fee, returns expected reward", func(t *testing.T) {
		// initialPledge equal to undeclaredPenalty guarantees expected reward is greater
		initialPledge := undeclaredPenalty
		sectorAge := abi.ChainEpoch(100)

		fee := miner.PledgePenaltyForTermination(initialPledge, sectorAge, epochTargetReward, networkQAPower, qaSectorPower)

		// expect fee to be pledge * br * age where br = pledge/initialPledgeFactor
		expectedFee := big.Add(
			initialPledge,
			big.Div(
				big.Mul(initialPledge, big.NewInt(int64(sectorAge))),
				miner.InitialPledgeFactor))
		assert.Equal(t, expectedFee, fee)
	})

	t.Run("sector age is capped", func(t *testing.T) {
		initialPledge := undeclaredPenalty
		sectorAge := abi.ChainEpoch(200 * builtin.EpochsInDay)

		fee := miner.PledgePenaltyForTermination(initialPledge, sectorAge, epochTargetReward, networkQAPower, qaSectorPower)

		// expect fee to be pledge * br * age where br = pledge/initialPledgeFactor
		expectedFee := big.Add(
			initialPledge,
			big.Div(
				big.Mul(initialPledge, big.NewInt(180*builtin.EpochsInDay)),
				miner.InitialPledgeFactor))
		assert.Equal(t, expectedFee, fee)
	})
}
