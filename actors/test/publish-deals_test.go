package test

import (
	"context"
	"math"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/verifreg"
	"github.com/filecoin-project/specs-actors/v7/support/ipld"
	tutil "github.com/filecoin-project/specs-actors/v7/support/testing"
	"github.com/filecoin-project/specs-actors/v7/support/vm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var defaultPricePerEpoch = abi.NewTokenAmount(1 << 20)
var defaultProviderCollateral = big.Mul(big.NewInt(2), vm.FIL)
var defaultClientCollateral = big.Mul(big.NewInt(1), vm.FIL)
var dealLifeTime = abi.ChainEpoch(181 * builtin.EpochsInDay)

func TestPublishStorageDealsFailure(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	addrs := vm.CreateAccounts(ctx, t, v, 5, big.Mul(big.NewInt(10_000), vm.FIL), 93837778)
	worker, client1, client2, notMiner, cheapClient := addrs[0], addrs[1], addrs[2], addrs[3], addrs[4]
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

	// add market collateral for clients and miner
	clientCollateral := big.Mul(big.NewInt(100), vm.FIL)
	vm.ApplyOk(t, v, client1, builtin.StorageMarketActorAddr, clientCollateral, builtin.MethodsMarket.AddBalance, &client1)
	vm.ApplyOk(t, v, client2, builtin.StorageMarketActorAddr, clientCollateral, builtin.MethodsMarket.AddBalance, &client2)
	minerCollateral := big.Mul(big.NewInt(100), vm.FIL)
	vm.ApplyOk(t, v, worker, builtin.StorageMarketActorAddr, minerCollateral, builtin.MethodsMarket.AddBalance, &minerAddrs.IDAddress)

	//
	// publish one bad deal
	//
	t.Run("mismatched provider", func(t *testing.T) {
		dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
		batcher := newDealBatcher(v)
		// good deal
		batcher.stage(t, client1, minerAddrs.IDAddress, "run0-deal0", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal, provider doesn't match worker
		batcher.stage(t, client1, notMiner, "run0-deal1", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// good deal
		batcher.stage(t, client1, minerAddrs.IDAddress, "run0-deal2", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		dealRet := batcher.publishOK(t, worker)
		goodInputs, err := dealRet.ValidDeals.All(math.MaxUint64)
		require.NoError(t, err)
		assert.Equal(t, []uint64{0, 2}, goodInputs)
		assert.Equal(t, 2, len(dealRet.IDs))
	})

	t.Run("invalid deal: bad piece size", func(t *testing.T) {
		dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
		batcher := newDealBatcher(v)
		// bad deal piece size too small
		batcher.stage(t, client1, minerAddrs.IDAddress, "run1-deal0", 0, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// good deal
		batcher.stage(t, client1, minerAddrs.IDAddress, "run1-deal1", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		dealRet := batcher.publishOK(t, worker)
		goodInputs, err := dealRet.ValidDeals.All(math.MaxUint64)
		require.NoError(t, err)
		assert.Equal(t, []uint64{1}, goodInputs)
		assert.Equal(t, 1, len(dealRet.IDs))
	})

	t.Run("invalid deal: start time in the past", func(t *testing.T) {
		dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
		badStart := v.GetEpoch() - 1
		batcher := newDealBatcher(v)
		// bad deal, start time in the past
		batcher.stage(t, client1, minerAddrs.IDAddress, "run2-deal0", 1<<30, false, badStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// good deal
		batcher.stage(t, client1, minerAddrs.IDAddress, "run2-deal1", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		dealRet := batcher.publishOK(t, worker)
		goodInputs, err := dealRet.ValidDeals.All(math.MaxUint64)
		require.NoError(t, err)
		assert.Equal(t, []uint64{1}, goodInputs)
		assert.Equal(t, 1, len(dealRet.IDs))
	})

	t.Run("client address cannot be resolved", func(t *testing.T) {
		badClient := tutil.NewIDAddr(t, 5_000_000)
		dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
		batcher := newDealBatcher(v)
		// good deal
		batcher.stage(t, client1, minerAddrs.IDAddress, "run3-deal0", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal -- client addr is not a resolvable account
		batcher.stage(t, badClient, minerAddrs.IDAddress, "run3-deal1", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		dealRet := batcher.publishOK(t, worker)
		goodInputs, err := dealRet.ValidDeals.All(math.MaxUint64)
		require.NoError(t, err)
		assert.Equal(t, []uint64{0}, goodInputs)
		assert.Equal(t, 1, len(dealRet.IDs))
	})

	t.Run("no client lockup", func(t *testing.T) {
		/* added no market collateral for cheapClient */

		dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
		batcher := newDealBatcher(v)
		// bad deal client can't pay
		batcher.stage(t, cheapClient, minerAddrs.IDAddress, "run4-deal0", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// good deal
		batcher.stage(t, client1, minerAddrs.IDAddress, "run4-deal1", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		dealRet := batcher.publishOK(t, worker)
		goodInputs, err := dealRet.ValidDeals.All(math.MaxUint64)
		require.NoError(t, err)
		assert.Equal(t, []uint64{1}, goodInputs)
		assert.Equal(t, 1, len(dealRet.IDs))
	})

	t.Run("insufficient client lockup when considering the whole batch", func(t *testing.T) {
		oneLifeTimeCost := big.Sum(defaultClientCollateral, big.Mul(big.NewInt(int64(dealLifeTime)), defaultPricePerEpoch))
		// add only one lifetime cost to cheapClient's collateral but attempt to make 3 deals
		vm.ApplyOk(t, v, cheapClient, builtin.StorageMarketActorAddr, oneLifeTimeCost, builtin.MethodsMarket.AddBalance, &cheapClient)

		dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
		batcher := newDealBatcher(v)
		// good deal
		batcher.stage(t, cheapClient, minerAddrs.IDAddress, "run5-deal0", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal -- insufficient funds
		batcher.stage(t, cheapClient, minerAddrs.IDAddress, "run5-deal1", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal -- insufficient funds
		batcher.stage(t, cheapClient, minerAddrs.IDAddress, "run5-deal2", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		dealRet := batcher.publishOK(t, worker)
		goodInputs, err := dealRet.ValidDeals.All(math.MaxUint64)
		require.NoError(t, err)
		assert.Equal(t, []uint64{0}, goodInputs)
		assert.Equal(t, 1, len(dealRet.IDs))
	})

	t.Run("insufficient provider lockup when considering the whole batch", func(t *testing.T) {
		// Note: its important to use a different seed here because same seed will generate same key
		// and overwrite init actor mapping
		cheapWorker := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(10_000), vm.FIL), 444)[0]
		// create miner
		params := power.CreateMinerParams{
			Owner:               cheapWorker,
			Worker:              cheapWorker,
			WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			Peer:                abi.PeerID("not really a peer id"),
		}
		ret := vm.ApplyOk(t, v, worker, builtin.StoragePowerActorAddr, big.Mul(big.NewInt(100), vm.FIL), builtin.MethodsPower.CreateMiner, &params)

		cheapMinerAddrs, ok := ret.(*power.CreateMinerReturn)
		require.True(t, ok)

		minerCollateral := defaultProviderCollateral
		vm.ApplyOk(t, v, worker, builtin.StorageMarketActorAddr, minerCollateral, builtin.MethodsMarket.AddBalance, &minerAddrs.IDAddress)

		dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
		batcher := newDealBatcher(v)

		vm.ApplyOk(t, v, worker, builtin.StorageMarketActorAddr, minerCollateral, builtin.MethodsMarket.AddBalance, &cheapMinerAddrs.IDAddress)
		// good deal
		batcher.stage(t, client1, cheapMinerAddrs.IDAddress, "run6-deal0", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal -- insufficient provider funds
		batcher.stage(t, client2, cheapMinerAddrs.IDAddress, "run6-deal1", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		dealRet := batcher.publishOK(t, cheapWorker)
		goodInputs, err := dealRet.ValidDeals.All(math.MaxUint64)
		require.NoError(t, err)
		assert.Equal(t, []uint64{0}, goodInputs)
		assert.Equal(t, 1, len(dealRet.IDs))
	})

	t.Run("duplicate deal in batch", func(t *testing.T) {
		dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
		batcher := newDealBatcher(v)
		// good deal
		batcher.stage(t, client1, minerAddrs.IDAddress, "run7-deal0", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// good deal
		batcher.stage(t, client1, minerAddrs.IDAddress, "run7-deal1", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal duplicate
		batcher.stage(t, client1, minerAddrs.IDAddress, "run7-deal0", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal duplicate
		batcher.stage(t, client1, minerAddrs.IDAddress, "run7-deal0", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// good deal
		batcher.stage(t, client2, minerAddrs.IDAddress, "run7-deal2", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal duplicate
		batcher.stage(t, client1, minerAddrs.IDAddress, "run7-deal1", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		dealRet := batcher.publishOK(t, worker)
		goodInputs, err := dealRet.ValidDeals.All(math.MaxUint64)
		require.NoError(t, err)
		assert.Equal(t, []uint64{0, 1, 4}, goodInputs)
		assert.Equal(t, 3, len(dealRet.IDs))
	})

	t.Run("duplicate deal in state", func(t *testing.T) {
		dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
		batcher := newDealBatcher(v)
		// good deal
		batcher.stage(t, client2, minerAddrs.IDAddress, "run8-deal0", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		dealRet := batcher.publishOK(t, worker)
		goodInputs, err := dealRet.ValidDeals.All(math.MaxUint64)
		require.NoError(t, err)
		assert.Equal(t, []uint64{0}, goodInputs)

		batcher = newDealBatcher(v)
		// good deal
		batcher.stage(t, client2, minerAddrs.IDAddress, "run8-deal1", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal duplicate in batch
		batcher.stage(t, client2, minerAddrs.IDAddress, "run8-deal1", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal duplicate in state
		batcher.stage(t, client2, minerAddrs.IDAddress, "run8-deal0", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		dealRet = batcher.publishOK(t, worker)
		goodInputs, err = dealRet.ValidDeals.All(math.MaxUint64)
		require.NoError(t, err)
		assert.Equal(t, []uint64{0}, goodInputs)
		assert.Equal(t, 1, len(dealRet.IDs))
	})

	t.Run("verified deal fails to acquire datacap", func(t *testing.T) {
		vAddrs := vm.CreateAccounts(ctx, t, v, 2, big.Mul(big.NewInt(10_000), vm.FIL), 555)
		verifier, verifiedClient := vAddrs[0], vAddrs[1]

		addVerifierParams := verifreg.AddVerifierParams{
			Address:   verifier,
			Allowance: abi.NewStoragePower(32 << 40),
		}
		vm.ApplyOk(t, v, vm.VerifregRoot, builtin.VerifiedRegistryActorAddr, big.Zero(), builtin.MethodsVerifiedRegistry.AddVerifier, &addVerifierParams)
		addClientParams := verifreg.AddVerifiedClientParams{
			Address:   verifiedClient,
			Allowance: abi.NewStoragePower(1 << 32),
		}
		vm.ApplyOk(t, v, verifier, builtin.VerifiedRegistryActorAddr, big.Zero(), builtin.MethodsVerifiedRegistry.AddVerifiedClient, &addClientParams)
		vm.ApplyOk(t, v, verifiedClient, builtin.StorageMarketActorAddr, big.Mul(big.NewInt(100), vm.FIL), builtin.MethodsMarket.AddBalance, &verifiedClient)

		dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
		batcher := newDealBatcher(v)
		// good deal
		batcher.stage(t, verifiedClient, minerAddrs.IDAddress, "run9-deal0", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)

		// good verified deal, uses up all datacap
		batcher.stage(t, verifiedClient, minerAddrs.IDAddress, "run9-deal1", 1<<32, true, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad verified deal, no data cap left
		batcher.stage(t, verifiedClient, minerAddrs.IDAddress, "run9-deal2", 1<<32, true, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		dealRet := batcher.publishOK(t, worker)
		goodInputs, err := dealRet.ValidDeals.All(math.MaxUint64)
		require.NoError(t, err)
		assert.Equal(t, []uint64{0, 1}, goodInputs)
		assert.Equal(t, 2, len(dealRet.IDs))
	})

	t.Run("random assortment of different failures", func(t *testing.T) {
		xtraAddrs := vm.CreateAccounts(ctx, t, v, 4, big.Mul(big.NewInt(10_000), vm.FIL), 555)
		verifier, verifiedClient, cheapClient, brokeClient := xtraAddrs[0], xtraAddrs[1], xtraAddrs[2], xtraAddrs[3]

		addVerifierParams := verifreg.AddVerifierParams{
			Address:   verifier,
			Allowance: abi.NewStoragePower(32 << 40),
		}
		vm.ApplyOk(t, v, vm.VerifregRoot, builtin.VerifiedRegistryActorAddr, big.Zero(), builtin.MethodsVerifiedRegistry.AddVerifier, &addVerifierParams)
		addClientParams := verifreg.AddVerifiedClientParams{
			Address:   verifiedClient,
			Allowance: abi.NewStoragePower(1 << 32),
		}
		vm.ApplyOk(t, v, verifier, builtin.VerifiedRegistryActorAddr, big.Zero(), builtin.MethodsVerifiedRegistry.AddVerifiedClient, &addClientParams)
		vm.ApplyOk(t, v, verifiedClient, builtin.StorageMarketActorAddr, big.Mul(big.NewInt(100), vm.FIL), builtin.MethodsMarket.AddBalance, &verifiedClient)
		oneLifeTimeCost := big.Sum(defaultClientCollateral, big.Mul(big.NewInt(int64(dealLifeTime)), defaultPricePerEpoch))

		vm.ApplyOk(t, v, cheapClient, builtin.StorageMarketActorAddr, oneLifeTimeCost, builtin.MethodsMarket.AddBalance, &cheapClient)
		dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
		batcher := newDealBatcher(v)

		// good deal
		batcher.stage(t, verifiedClient, minerAddrs.IDAddress, "run10-deal1", 1<<32, true, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal -- duplicate
		batcher.stage(t, verifiedClient, minerAddrs.IDAddress, "run10-deal1", 1<<32, true, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// good deal
		batcher.stage(t, cheapClient, minerAddrs.IDAddress, "run10-deal2", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal -- no client funds
		batcher.stage(t, brokeClient, minerAddrs.IDAddress, "run10-deal3", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal -- provider address does not match
		batcher.stage(t, client1, client2, "run10-deal4", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal -- insufficient data cap
		batcher.stage(t, verifiedClient, minerAddrs.IDAddress, "run10-deal5", 1<<32, true, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal -- cheap client out of funds
		batcher.stage(t, cheapClient, minerAddrs.IDAddress, "run10-deal6", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal -- provider collateral too low
		batcher.stage(t, cheapClient, minerAddrs.IDAddress, "run10-deal7", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, big.Zero(), defaultClientCollateral)
		// good deal
		batcher.stage(t, client1, minerAddrs.IDAddress, "run10-deal8", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)

		dealRet := batcher.publishOK(t, worker)
		goodInputs, err := dealRet.ValidDeals.All(math.MaxUint64)
		require.NoError(t, err)
		assert.Equal(t, []uint64{0, 2, 8}, goodInputs)
		assert.Equal(t, 3, len(dealRet.IDs))
	})

	t.Run("all deals are bad", func(t *testing.T) {

		batcher := newDealBatcher(v)
		dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
		badClient := tutil.NewIDAddr(t, 1_000)

		// bad deal -- provider collateral too low
		batcher.stage(t, client1, minerAddrs.IDAddress, "run11-deal0", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, big.Zero(), defaultClientCollateral)
		// bad deal -- provider address does not match
		batcher.stage(t, client1, client2, "run11-deal1", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal -- verified deal from non-verified client
		batcher.stage(t, client1, client2, "run11-deal2", 1<<30, true, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal -- client addr is not a resolvable account
		batcher.stage(t, badClient, minerAddrs.IDAddress, "run11-deal3", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		// bad deal piece size too small
		batcher.stage(t, client1, minerAddrs.IDAddress, "run11-deal4", 0, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)

		batcher.publishFail(t, worker)
	})

	t.Run("all deals are good", func(t *testing.T) {
		dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
		batcher := newDealBatcher(v)
		// good deals
		batcher.stage(t, client1, minerAddrs.IDAddress, "run12-deal0", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		batcher.stage(t, client1, minerAddrs.IDAddress, "run12-deal1", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		batcher.stage(t, client1, minerAddrs.IDAddress, "run12-deal2", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		batcher.stage(t, client1, minerAddrs.IDAddress, "run12-deal3", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)
		batcher.stage(t, client1, minerAddrs.IDAddress, "run12-deal4", 1<<30, false, dealStart, dealLifeTime,
			defaultPricePerEpoch, defaultProviderCollateral, defaultClientCollateral)

		dealRet := batcher.publishOK(t, worker)
		goodInputs, err := dealRet.ValidDeals.All(math.MaxUint64)
		require.NoError(t, err)
		assert.Equal(t, []uint64{0, 1, 2, 3, 4}, goodInputs)
		assert.Equal(t, 5, len(dealRet.IDs))
	})

}
