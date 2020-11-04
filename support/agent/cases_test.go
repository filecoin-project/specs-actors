package agent_test

import (
	"context"
	"fmt"
	"github.com/filecoin-project/specs-actors/v2/actors/states"
	"strings"
	"testing"

	"github.com/filecoin-project/go-state-types/big"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v2/support/agent"
	"github.com/filecoin-project/specs-actors/v2/support/ipld"
	"github.com/filecoin-project/specs-actors/v2/support/vm"
)

func TestCreate100Miners(t *testing.T) {
	ctx := context.Background()
	initialBalance := big.Mul(big.NewInt(1000), big.NewInt(1e18))
	minerCount := 100

	sim := agent.NewSim(ctx, t, ipld.NewADTStore(ctx), agent.SimConfig{
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

func TestCommitPowerAndCheckInvariants(t *testing.T) {
	t.Skip("this is slow")
	ctx := context.Background()
	initialBalance := big.Mul(big.NewInt(1000000), big.NewInt(1e18))
	minerCount := 10

	sim := agent.NewSim(ctx, t, ipld.NewADTStore(ctx), agent.SimConfig{
		AccountCount:           minerCount,
		AccountInitialBalance:  initialBalance,
		Seed:                   42,
		CreateMinerProbability: 1.0,
	})

	var pwrSt power.State
	for i := 0; i < 20000; i++ {
		require.NoError(t, sim.Tick())

		if sim.GetVM().GetEpoch()%100 == 0 {
			stateTree, err := sim.GetVM().GetStateTree()
			require.NoError(t, err)

			totalBalance, err := sim.GetVM().GetTotalActorBalance()
			require.NoError(t, err)

			acc, err := states.CheckStateInvariants(stateTree, totalBalance, sim.GetVM().GetEpoch()-1)
			require.NoError(t, err)
			require.True(t, acc.IsEmpty(), strings.Join(acc.Messages(), "\n"))

			require.NoError(t, sim.GetVM().GetState(builtin.StoragePowerActorAddr, &pwrSt))
			fmt.Printf("Power at %d: raw: %v  qa: %v  cmtRaw: %v  cmtQa: %v  cnsMnrs: %d\n",
				sim.GetVM().GetEpoch(), pwrSt.TotalRawBytePower, pwrSt.TotalQualityAdjPower, pwrSt.TotalBytesCommitted,
				pwrSt.TotalQABytesCommitted, pwrSt.MinerAboveMinPowerCount)
		}
	}
}

func TestCommitAndCheckReadWriteStats(t *testing.T) {
	t.Skip("this is slow")
	ctx := context.Background()
	initialBalance := big.Mul(big.NewInt(1000000), big.NewInt(1e18))
	minerCount := 10
	cumulativeStats := make(vm_test.StatsByCall)

	store, storeMetrics := metricsADTStore(ctx)
	sim := agent.NewSim(ctx, t, store, agent.SimConfig{
		AccountCount:           minerCount,
		AccountInitialBalance:  initialBalance,
		Seed:                   42,
		CreateMinerProbability: 1.0,
	})
	sim.GetVM().SetStatsSource(storeMetrics)

	var pwrSt power.State
	for i := 0; i < 20000; i++ {
		require.NoError(t, sim.Tick())

		if sim.GetVM().GetEpoch()%100 == 0 {
			require.NoError(t, sim.GetVM().GetState(builtin.StoragePowerActorAddr, &pwrSt))
			fmt.Printf("Power at %d: raw: %v  qa: %v  cmtRaw: %v  cmtQa: %v  cnsMnrs: %d  puts: %d  gets: %d\n",
				sim.GetVM().GetEpoch(), pwrSt.TotalRawBytePower, pwrSt.TotalQualityAdjPower, pwrSt.TotalBytesCommitted,
				pwrSt.TotalQABytesCommitted, pwrSt.MinerAboveMinPowerCount, storeMetrics.Puts, storeMetrics.Reads)
		}

		cumulativeStats.MergeAllStats(sim.GetCallStats())

		if sim.GetVM().GetEpoch()%1000 == 0 {
			for method, stats := range cumulativeStats {
				printCallStats(method, stats, "")
			}
			cumulativeStats = make(vm_test.StatsByCall)
		}
	}
}

func printCallStats(method vm_test.MethodKey, stats *vm_test.CallStats, indent string) { // nolint:unused
	fmt.Printf("%s%v:%d: calls: %d  gets: %d  puts: %d  avg gets: %.2f, avg puts: %.2f\n",
		indent, builtin.ActorNameByCode(method.Code), method.Method, stats.Calls, stats.Reads, stats.Puts,
		float32(stats.Reads)/float32(stats.Calls), float32(stats.Puts)/float32(stats.Calls))

	if stats.SubStats == nil {
		return
	}

	for m, s := range stats.SubStats {
		printCallStats(m, s, indent+"  ")
	}
}

func metricsADTStore(ctx context.Context) (adt.Store, *ipld.MetricsStore) { // nolint:unused
	ms := ipld.NewMetricsStore(ipld.NewBlockStoreInMemory())
	return adt.WrapStore(ctx, cbor.NewCborStore(ms)), ms
}
