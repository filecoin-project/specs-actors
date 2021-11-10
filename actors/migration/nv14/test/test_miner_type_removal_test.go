package test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/rt"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/require"

	ipld2 "github.com/filecoin-project/specs-actors/v2/support/ipld"
	power4 "github.com/filecoin-project/specs-actors/v4/actors/builtin/power"
	vm4 "github.com/filecoin-project/specs-actors/v4/support/vm"

	"github.com/filecoin-project/specs-actors/v5/actors/migration/nv13"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/exported"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v6/actors/migration/nv14"

	"github.com/filecoin-project/specs-actors/v6/actors/util/adt"

	"github.com/filecoin-project/specs-actors/v6/support/vm"
)

/*
	Migration testing criteria:

		Miners with bad proof types need to go away
		Signature of miners with bad proof types in power actor need to change
		— claim is removed (there should not be any claims)
		— total miners miners over conseusns …. is removed
		Market actor escrow table removes these bad miners
		Owner address of these bad miners is given the funds belonging to the miner actor
		Owner address also gets funds put in the market actor escrow table

*/
func TestTestMinerTypeDeletion(t *testing.T) {
	// XXX: what happens if someone sends these addresses some funds??? no good.
	ctx := context.Background()
	log := nv14.TestLogger{TB: t}
	bs := ipld2.NewSyncBlockStoreInMemory()
	v := vm4.NewVMWithSingletons(ctx, t, bs)
	adtStore := adt.WrapStore(ctx, cbor.NewCborStore(bs))

	addrs := vm4.CreateAccounts(ctx, t, v, 3, big.Mul(big.NewInt(10_000), vm.FIL), 93837778)
	worker1, worker2, minerOwner := addrs[0], addrs[1], addrs[2]

	minerBalance := big.Mul(big.NewInt(1_000), vm.FIL)

	// getting the market actor balance here for CheckStateInvariants later down
	market4Actor, found, err := v.GetActor(builtin.StorageMarketActorAddr)
	require.NoError(t, err)
	require.True(t, found)
	oldMarketActorBalance := market4Actor.Balance

	// create a miner with the wrong post/seal proof type
	params := power4.CreateMinerParams{
		Owner:               minerOwner,
		Worker:              worker1,
		WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow8MiBV1,
		Peer:                abi.PeerID("not really a peer id"),
	}
	ret := vm4.ApplyOk(t, v, addrs[0], builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)

	minerAddrs1, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	params = power4.CreateMinerParams{
		Owner:               minerOwner,
		Worker:              worker2,
		WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		Peer:                abi.PeerID("not really another peer id"),
	}
	ret = vm4.ApplyOk(t, v, addrs[0], builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)

	minerAddrs2, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	// add market collateral for miners
	collateral := big.Mul(big.NewInt(64), vm.FIL)
	vm4.ApplyOk(t, v, worker1, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &minerAddrs1.IDAddress)
	vm4.ApplyOk(t, v, worker2, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &minerAddrs2.IDAddress)

	// do the migration, it should delete one of the miners and transfer its escrow and balance back to the owner
	startRoot := v.StateRoot()
	nextRoot, err := nv13.MigrateStateTree(ctx, adtStore, startRoot, abi.ChainEpoch(0),
		nv13.Config{MaxWorkers: 1}, log, nv13.NewMemMigrationCache())
	require.NoError(t, err)

	nextNextRoot, err := nv14.MigrateStateTree(ctx, adtStore, nextRoot, abi.ChainEpoch(0),
		nv14.Config{MaxWorkers: 1}, log, nv14.NewMemMigrationCache())
	require.NoError(t, err)

	lookup := map[cid.Cid]rt.VMActor{}
	for _, ba := range exported.BuiltinActors() {
		lookup[ba.Code()] = ba
	}

	v6, err := vm.NewVMAtEpoch(ctx, lookup, v.Store(), nextNextRoot, v.GetEpoch()+1)
	require.NoError(t, err)

	// check the market state after migration
	var market6State market.State
	require.NoError(t, v6.GetState(builtin.StorageMarketActorAddr, &market6State))
	market.CheckStateInvariants(&market6State, v6.Store(), oldMarketActorBalance, v.GetEpoch()+1)

	// check both miners' escrow balances (should be zero and collateral, respectively)
	et, err := adt.AsBalanceTable(adtStore, market6State.EscrowTable)
	require.NoError(t, err)
	bal, err := et.Get(minerAddrs1.IDAddress)
	require.NoError(t, err)
	require.Equal(t, bal, big.Zero())
	bal, err = et.Get(minerAddrs2.IDAddress)
	require.NoError(t, err)
	require.Equal(t, bal, collateral)

	// check that the power statistics were correctly recomputed (total miners, total miners above threshold)
	var power6State power.State
	require.NoError(t, v6.GetState(builtin.StoragePowerActorAddr, &power6State))
	power.CheckStateInvariants(&power6State, v6.Store())
	require.Equal(t, power6State.MinerAboveMinPowerCount, 1)
	require.Equal(t, power6State.MinerCount, 1)

	// XXX: there shouldn't be any claims period for the test miner type, so i shouldn't need to worry about those?
	// .... so we hope?

	// check that the total amount of filecoin stayed constant!
	totalRuntimeBalance, err := v6.GetTotalActorBalance()
	require.NoError(t, err)
	require.Equal(t, totalRuntimeBalance, big.Mul(big.NewInt(2), minerBalance))

	// check that the test miner was properly deleted
	_, deletedMinerFound, err := v6.GetActor(minerAddrs1.IDAddress)
	require.NoError(t, err)
	require.False(t, deletedMinerFound)

	// check that the non-test miner was not deleted and still has its remaining money
	nonTestMiner, found, err := v6.GetActor(minerAddrs2.IDAddress)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, nonTestMiner.Balance, big.Sub(minerBalance, collateral))

	// check the owner's new balance and make sure it's same as the miner1 balance
	ownerActor, found, err := v6.GetActor(minerOwner)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, ownerActor.Balance, minerBalance)
}
