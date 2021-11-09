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
	power5 "github.com/filecoin-project/specs-actors/v5/actors/builtin/power"
	vm5 "github.com/filecoin-project/specs-actors/v5/support/vm"

	"github.com/filecoin-project/specs-actors/v6/actors/builtin"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/exported"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v6/actors/migration/nv14"
	"github.com/filecoin-project/specs-actors/v6/actors/util/adt"

	"github.com/filecoin-project/specs-actors/v6/support/vm"
)

func TestTestMinerTypeDeletion(t *testing.T) {
	// XXX: what happens if someone sends these addresses some funds??? no good.
	ctx := context.Background()
	log := nv14.TestLogger{TB: t}
	bs := ipld2.NewSyncBlockStoreInMemory()
	v := vm5.NewVMWithSingletons(ctx, t, bs)
	adtStore := adt.WrapStore(ctx, cbor.NewCborStore(bs))

	addrs := vm5.CreateAccounts(ctx, t, v, 3, big.Mul(big.NewInt(10_000), vm.FIL), 93837778)
	worker1, worker2, minerOwner := addrs[0], addrs[1], addrs[2]

	minerBalance := big.Mul(big.NewInt(1_000), vm.FIL)

	// Set up new actor record with the migrated state.
	/*
		Migration testing criteria:

			Miners with bad proof types need to go away
			Signature of miners with bad proof types in power actor need to change
			— claim is removed
			— total miners miners over conseusns …. is removed
			Market actor escrow table removes these bad miners
			Owner address of these bad miners is given the funds belonging to the miner actor
			Owner address also gets funds put in the market actor escrow table

	*/

	// create a miner with the wrong post/seal proof type
	params := power5.CreateMinerParams{
		Owner:               minerOwner,
		Worker:              worker1,
		WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow8MiBV1,
		Peer:                abi.PeerID("not really a peer id"),
	}
	ret := vm5.ApplyOk(t, v, addrs[0], builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)

	minerAddrs1, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	params = power5.CreateMinerParams{
		Owner:               minerOwner,
		Worker:              worker2,
		WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		Peer:                abi.PeerID("not really another peer id"),
	}
	ret = vm5.ApplyOk(t, v, addrs[0], builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)

	minerAddrs2, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	// add market collateral for miners
	collateral := big.Mul(big.NewInt(64), vm.FIL)
	vm5.ApplyOk(t, v, worker1, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &minerAddrs1.IDAddress)
	vm5.ApplyOk(t, v, worker2, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &minerAddrs2.IDAddress)

	// TODO: check that both their balances are the original minerbalances minus the collateral balance in escrow

	// TODO: do the math to see how much the owner will have after the migration here too

	// do the migration, it should delete one of the miners and transfer its escrow and balance back to the owner
	startRoot := v.StateRoot()
	nextRoot, err := nv14.MigrateStateTree(ctx, adtStore, startRoot, abi.ChainEpoch(0),
		nv14.Config{MaxWorkers: 1}, log, nv14.NewMemMigrationCache())
	require.NoError(t, err)

	lookup := map[cid.Cid]rt.VMActor{}
	for _, ba := range exported.BuiltinActors() {
		lookup[ba.Code()] = ba
	}

	v6, err := vm.NewVMAtEpoch(ctx, lookup, v.Store(), nextRoot, v.GetEpoch()+1)
	require.NoError(t, err)

	// check things about the state after the migrations
	var market6State market.State
	require.NoError(t, v6.GetState(builtin.StorageMarketActorAddr, &market6State))
	market.CheckStateInvariants(&market6State, v6.Store(), oldMarketActor.Balance, v.GetEpoch()+1)
	// TODO: check that the power statistics were correctly recomputed (total miners, total miners above threshold)

	// TODO: check that the test miner was properly deleted
	// TODO: check the owner's new balance and make sure the miner2's
}
