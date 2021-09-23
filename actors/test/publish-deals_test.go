package test

import (
	"context"
	"math"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v6/support/ipld"
	"github.com/filecoin-project/specs-actors/v6/support/vm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublishStorageDealsFailure(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	addrs := vm.CreateAccounts(ctx, t, v, 4, big.Mul(big.NewInt(10_000), vm.FIL), 93837778)
	worker, client1, client2, notMiner := addrs[0], addrs[1], addrs[2], addrs[3]
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1

	// create miner
	params := power.CreateMinerParams{
		Owner:               worker,
		Worker:              worker,
		WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		Peer:                abi.PeerID("not really a peer id"),
	}
	ret := vm.ApplyOk(t, v, worker, builtin.StoragePowerActorAddr, big.Mul(big.NewInt(100), vm.FIL), builtin.MethodsPower.CreateMiner, &params)

	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	//
	// publish deals
	//

	// add market collateral for clients and miner
	clientCollateral := big.Mul(big.NewInt(3), vm.FIL)
	vm.ApplyOk(t, v, client1, builtin.StorageMarketActorAddr, clientCollateral, builtin.MethodsMarket.AddBalance, &client1)
	vm.ApplyOk(t, v, client2, builtin.StorageMarketActorAddr, clientCollateral, builtin.MethodsMarket.AddBalance, &client2)
	minerCollateral := big.Mul(big.NewInt(64), vm.FIL)
	vm.ApplyOk(t, v, worker, builtin.StorageMarketActorAddr, minerCollateral, builtin.MethodsMarket.AddBalance, &minerAddrs.IDAddress)

	dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	batcher := newDealBatcher(v)
	// good deal
	batcher.stage(t, client1, minerAddrs.IDAddress, "deal1", 1<<30, false, dealStart, 181*builtin.EpochsInDay)
	// bad deal, provider doesn't match worker
	batcher.stage(t, client1, notMiner, "deal2", 1<<30, false, dealStart, 181*builtin.EpochsInDay)
	// good deal
	batcher.stage(t, client1, minerAddrs.IDAddress, "deal3", 1<<30, false, dealStart, 181*builtin.EpochsInDay)
	dealRet := batcher.publish(t, worker)
	goodInputs, err := dealRet.ValidDeals.All(math.MaxUint64)
	require.NoError(t, err)
	assert.Equal(t, []uint64{0, 2}, goodInputs)
}
