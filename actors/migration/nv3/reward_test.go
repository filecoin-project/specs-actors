package nv3

import (
	"context"
	gbig "math/big"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/network"
	assert "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/support/ipld"
)

func TestRewardMigrator(t *testing.T) {
	ctx := context.Background()
	store := ipld.NewADTStore(ctx)

	power := abi.NewStoragePower(1 << 50)
	st := reward.ConstructState(power)
	_, err := store.Put(ctx, st)
	require.NoError(t, err)

	// Simulate some time passing and baseline growth.
	epochsInYear := abi.ChainEpoch(365 * builtin.EpochsInDay)
	epoch := abi.ChainEpoch(0)
	for ; epoch < epochsInYear; epoch++ {
		st.UpdateToNextEpochWithReward(power, network.Version0)
	}
	head, err := store.Put(ctx, st)
	require.NoError(t, err)

	{
		// With network version 0, after 1 year expect value to be 200% higher (allowing for small error).
		multiplier := big.NewInt(3)
		expectedValue := big.Mul(reward.BaselineInitialValueV0, multiplier)
		diff := big.Sub(expectedValue, st.ThisEpochBaselinePower)
		perrFrac := gbig.NewRat(1, 1).SetFrac(diff.Int, expectedValue.Int)
		perr, _ := perrFrac.Float64()
		assert.Less(t, perr, 1e-8)
	}

	// Upgrade to network version 1.
	rw := rewardMigrator{}
	newHead, err := rw.MigrateState(ctx, store, head, epoch, builtin.RewardActorAddr, nil)
	require.NoError(t, err)

	// Expect the baseline value to be reset.
	require.NoError(t, store.Get(ctx, newHead, st))
	assert.True(t, reward.BaselineInitialValueV3.Equals(st.ThisEpochBaselinePower))

	// Simulate another year
	for ; epoch < 2*epochsInYear; epoch++ {
		st.UpdateToNextEpochWithReward(power, network.Version3)
	}

	{
		// After 1 more year expect value to be 100% higher than the reset initial value (allowing for small error).
		multiplier := big.NewInt(2)
		expectedValue := big.Mul(reward.BaselineInitialValueV3, multiplier)
		diff := big.Sub(expectedValue, st.ThisEpochBaselinePower)
		perrFrac := gbig.NewRat(1, 1).SetFrac(diff.Int, expectedValue.Int)
		perr, _ := perrFrac.Float64()
		assert.Less(t, perr, 1e-8)
	}
}
