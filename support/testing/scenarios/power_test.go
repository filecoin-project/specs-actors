package scenarios_test

import (
	"context"
	initactor "github.com/filecoin-project/specs-actors/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/support/testing/scenarios"

	"github.com/stretchr/testify/assert"
)

func TestCreateMiner(t *testing.T) {
	ctx := context.Background()
	vm, addrs := scenarios.NewVMWithSingletons(ctx, t, 1)

	params := power.CreateMinerParams{
		Owner:         addrs[0],
		Worker:        addrs[0],
		SealProofType: abi.RegisteredSealProof_StackedDrg32GiBV1,
		Peer:          abi.PeerID("not really a peer id"),
	}
	ret, code := vm.ApplyMessage(addrs[0], builtin.StoragePowerActorAddr, big.NewInt(1e10), builtin.MethodsPower.CreateMiner, &params)
	assert.Equal(t, exitcode.Ok, code)

	createMinerRet, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	// check that address has been added to init map
	var initState initactor.State
	err := vm.GetState(builtin.InitActorAddr, &initState)
	require.NoError(t, err)

	idAddr, err := initState.ResolveAddress(vm.Store(), createMinerRet.RobustAddress)
	require.NoError(t, err)
	assert.Equal(t, createMinerRet.IDAddress, idAddr)

	// check that the miner exists and has expected state
	var minerState miner.State
	err = vm.GetState(createMinerRet.IDAddress, &minerState)
	require.NoError(t, err)
	var minerInfo miner.MinerInfo
	err = vm.Store().Get(ctx, minerState.Info, &minerInfo)
	require.NoError(t, err)

	ownerIdAddr, err := initState.ResolveAddress(vm.Store(), addrs[0])
	require.NoError(t, err)

	assert.Equal(t, ownerIdAddr, minerInfo.Owner)
	assert.Equal(t, ownerIdAddr, minerInfo.Worker)
	assert.Equal(t, abi.RegisteredSealProof_StackedDrg32GiBV1, minerInfo.SealProofType)

	// check that miner is registered in power actor
	var powerState power.State
	err = vm.GetState(builtin.StoragePowerActorAddr, &powerState)
	require.NoError(t, err)

	assert.Equal(t, int64(1), powerState.MinerCount)

	// Check that miner has added itself to cron queue exactly once
	// This is a call back to the power actor from its constructor
	queue, err := adt.AsMultimap(vm.Store(), powerState.CronEventQueue)
	require.NoError(t, err)

	cronCount := uint64(0)
	var ev power.CronEvent
	err = queue.ForAll(func(k string, arr *adt.Array) error {
		return arr.ForEach(&ev, func(i int64) error {
			assert.Equal(t, createMinerRet.IDAddress, ev.MinerAddr)
			cronCount++
			return nil
		})
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), cronCount)
}
