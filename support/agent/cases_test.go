package agent_test

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v2/actors/states"
	"github.com/filecoin-project/specs-actors/v2/support/agent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestCreate100Miners(t *testing.T) {
	t.Skip("this is slow")
	ctx := context.Background()
	initialBalance := big.Mul(big.NewInt(1000), big.NewInt(1e18))
	minerCount := 100

	sim := agent.NewSim(ctx, t, agent.SimConfig{
		AccountCount:           minerCount,
		AccountInitialBalance:  initialBalance,
		Seed:                   42,
		CreateMinerProbability: 1.0,
	})

	for i := 0; i < minerCount; i++ {
		require.NoError(t, sim.Tick())
	}

	assert.Equal(t, minerCount, len(sim.Miners))

	for _, miner := range sim.Miners {
		actor, found, err := sim.GetVM().GetActor(miner.IDAddress)
		require.NoError(t, err)
		require.True(t, found)

		// demonstrate actor is created and has correct balance
		assert.Equal(t, initialBalance, actor.Balance)
	}
}

func TestCommitSectors(t *testing.T) {
	t.Skip("this is slow")
	ctx := context.Background()
	initialBalance := big.Mul(big.NewInt(1000000), big.NewInt(1e18))
	minerCount := 10

	sim := agent.NewSim(ctx, t, agent.SimConfig{
		AccountCount:           minerCount,
		AccountInitialBalance:  initialBalance,
		Seed:                   42,
		CreateMinerProbability: 1.0,
	})

	var pwrSt power.State
	for i := 0; i < 10000; i++ {
		require.NoError(t, sim.Tick())

		if i%100 == 0 {
			stateTree, err := sim.GetVM().GetStateTree()
			require.NoError(t, err)

			totalBalance, err := sim.GetVM().GetTotalActorBalance()
			require.NoError(t, err)

			acc, err := states.CheckStateInvariants(stateTree, totalBalance, sim.GetVM().GetEpoch())
			require.NoError(t, err)
			require.True(t, acc.IsEmpty(), strings.Join(acc.Messages(), "\n"))

			require.NoError(t, sim.GetVM().GetState(builtin.StoragePowerActorAddr, &pwrSt))
			fmt.Printf("Power at %d: raw: %v   qa: %v   cmtRaw: %v   cmtQa: %v   cnsMnrs: %d\n", sim.GetVM().GetEpoch(),
				pwrSt.TotalRawBytePower, pwrSt.TotalQualityAdjPower, pwrSt.TotalBytesCommitted, pwrSt.TotalQABytesCommitted, pwrSt.MinerAboveMinPowerCount)
		}
	}

	assert.Equal(t, minerCount, len(sim.Miners))
}
