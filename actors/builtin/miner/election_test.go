package miner_test

import (
	"testing"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMinerEligibleForElection(t *testing.T) {
	tenFIL := big.Mul(big.NewInt(1e18), big.NewInt(10))
	rSt := &reward.State{
		ThisEpochReward: tenFIL,
	}
	periodOffset := abi.ChainEpoch(1808)
	actor := newHarness(t, periodOffset)

	builder := builderForHarness(actor)

	t.Run("miner eligible", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		store := adt.AsStore(rt)
		mSt := getState(rt)
		mSt.InitialPledgeRequirement = miner.ConsensusFaultPenalty(rSt.ThisEpochReward)
		minerBalance := big.Add(mSt.InitialPledgeRequirement, tenFIL)
		currEpoch := abi.ChainEpoch(100000)

		eligible, err := miner.MinerEligibleForElection(store, mSt, rSt, minerBalance, currEpoch)
		require.NoError(t, err)
		assert.True(t, eligible)
	})

	t.Run("active consensus fault", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		store := rt.AdtStore()
		mSt := getState(rt)
		info, err := mSt.GetInfo(store)
		require.NoError(t, err)
		info.ConsensusFaultElapsed = abi.ChainEpoch(55)
		err = mSt.SaveInfo(store, info)
		require.NoError(t, err)

		mSt.InitialPledgeRequirement = miner.ConsensusFaultPenalty(rSt.ThisEpochReward)
		minerBalance := big.Add(mSt.InitialPledgeRequirement, tenFIL)
		currEpoch := abi.ChainEpoch(33) // 33 less than 55 so consensus fault still active

		eligible, err := miner.MinerEligibleForElection(store, mSt, rSt, minerBalance, currEpoch)
		require.NoError(t, err)
		assert.False(t, eligible)
	})

	t.Run("fee debt", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		store := rt.AdtStore()
		mSt := getState(rt)
		mSt.FeeDebt = abi.NewTokenAmount(1000)

		mSt.InitialPledgeRequirement = miner.ConsensusFaultPenalty(rSt.ThisEpochReward)
		minerBalance := big.Add(mSt.InitialPledgeRequirement, tenFIL)
		currEpoch := abi.ChainEpoch(100000)

		eligible, err := miner.MinerEligibleForElection(store, mSt, rSt, minerBalance, currEpoch)
		require.NoError(t, err)
		assert.False(t, eligible)
	})

	t.Run("ip debt", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		store := rt.AdtStore()
		mSt := getState(rt)

		mSt.InitialPledgeRequirement = miner.ConsensusFaultPenalty(rSt.ThisEpochReward)
		minerBalance := big.Sub(mSt.InitialPledgeRequirement, abi.NewTokenAmount(1))
		currEpoch := abi.ChainEpoch(100000)

		eligible, err := miner.MinerEligibleForElection(store, mSt, rSt, minerBalance, currEpoch)
		require.NoError(t, err)
		assert.False(t, eligible)
	})

	t.Run("ip requirement below threshold", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		store := rt.AdtStore()
		mSt := getState(rt)

		mSt.InitialPledgeRequirement = big.Sub(miner.ConsensusFaultPenalty(rSt.ThisEpochReward), abi.NewTokenAmount(1))
		minerBalance := big.Add(mSt.InitialPledgeRequirement, tenFIL)
		currEpoch := abi.ChainEpoch(100000)

		eligible, err := miner.MinerEligibleForElection(store, mSt, rSt, minerBalance, currEpoch)
		require.NoError(t, err)
		assert.False(t, eligible)
	})
}
