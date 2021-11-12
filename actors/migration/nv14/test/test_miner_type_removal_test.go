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
	builtin4 "github.com/filecoin-project/specs-actors/v4/actors/builtin"
	power4 "github.com/filecoin-project/specs-actors/v4/actors/builtin/power"
	vm4 "github.com/filecoin-project/specs-actors/v4/support/vm"
	vm5 "github.com/filecoin-project/specs-actors/v5/support/vm"

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
	// XXX: what happens if someone sends these addresses some funds???
	ctx := context.Background()
	log := nv14.TestLogger{TB: t}
	bs := ipld2.NewSyncBlockStoreInMemory()
	v4 := vm4.NewVMWithSingletons(ctx, t, bs)
	adtStore := adt.WrapStore(ctx, cbor.NewCborStore(bs))

	addrs := vm4.CreateAccounts(ctx, t, v4, 3, big.Mul(big.NewInt(10_000), vm.FIL), 93837778)
	worker1, worker2, minerOwner := addrs[0], addrs[1], addrs[2]

	minerBalance := big.Mul(big.NewInt(1_000), vm.FIL)

	// getting the market actor balance here for CheckStateInvariants later down
	market4Actor, found, err := v4.GetActor(builtin4.StorageMarketActorAddr)
	require.NoError(t, err)
	require.True(t, found)
	oldMarketActorBalance := market4Actor.Balance

	ownerActor, found, err := v4.GetActor(minerOwner)
	require.NoError(t, err)
	require.True(t, found)
	oldOwnerBalance := ownerActor.Balance

	// create a miner with the wrong post/seal proof type
	params := power4.CreateMinerParams{
		Owner:               minerOwner,
		Worker:              worker1,
		WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow8MiBV1,
		Peer:                abi.PeerID("not really a peer id"),
	}
	ret := vm4.ApplyOk(t, v4, addrs[0], builtin4.StoragePowerActorAddr, minerBalance, builtin4.MethodsPower.CreateMiner, &params)

	testMinerTypeAddr, ok := ret.(*power4.CreateMinerReturn)
	require.True(t, ok)

	params = power4.CreateMinerParams{
		Owner:               minerOwner,
		Worker:              worker2,
		WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		Peer:                abi.PeerID("not really another peer id"),
	}
	ret = vm4.ApplyOk(t, v4, addrs[0], builtin4.StoragePowerActorAddr, minerBalance, builtin4.MethodsPower.CreateMiner, &params)

	realMinerTypeAddr, ok := ret.(*power4.CreateMinerReturn)
	require.True(t, ok)

	// add market collateral for miners
	collateral := big.Mul(big.NewInt(64), vm.FIL)
	vm4.ApplyOk(t, v4, worker1, builtin4.StorageMarketActorAddr, collateral, builtin4.MethodsMarket.AddBalance, &testMinerTypeAddr.IDAddress)
	vm4.ApplyOk(t, v4, worker2, builtin4.StorageMarketActorAddr, collateral, builtin4.MethodsMarket.AddBalance, &realMinerTypeAddr.IDAddress)

	// and claim some power (zero power, but just to get into the claims thing)
	claimParams := power4.UpdateClaimedPowerParams{
		RawByteDelta:         big.Zero(),
		QualityAdjustedDelta: big.Zero(),
	}
	vm4.ApplyOk(t, v4, testMinerTypeAddr.IDAddress, builtin4.StoragePowerActorAddr,
		big.Zero(), builtin4.MethodsPower.UpdateClaimedPower, &claimParams)
	vm4.ApplyOk(t, v4, realMinerTypeAddr.IDAddress, builtin4.StoragePowerActorAddr,
		big.Zero(), builtin4.MethodsPower.UpdateClaimedPower, &claimParams)

	totalPreviousRuntimeBalance, err := v4.GetTotalActorBalance()
	require.NoError(t, err)

	// do the migration, it should delete one of the miners and transfer its escrow and balance back to the owner
	v4startRoot := v4.StateRoot()
	v5nextRoot, err := nv13.MigrateStateTree(ctx, adtStore, v4startRoot, abi.ChainEpoch(0),
		nv13.Config{MaxWorkers: 1}, log, nv13.NewMemMigrationCache())
	require.NoError(t, err)

	lookup := map[cid.Cid]rt.VMActor{}
	for _, ba := range exported.BuiltinActors() {
		lookup[ba.Code()] = ba
	}
	v5, err := vm5.NewVMAtEpoch(ctx, lookup, v4.Store(), v5nextRoot, v4.GetEpoch()+1)
	require.NoError(t, err)
	v5startRoot := v5.StateRoot()

	v6nextRoot, err := nv14.MigrateStateTree(ctx, adtStore, v5startRoot, abi.ChainEpoch(0),
		nv14.Config{MaxWorkers: 1}, log, nv14.NewMemMigrationCache())
	require.NoError(t, err)

	lookup = map[cid.Cid]rt.VMActor{}
	for _, ba := range exported.BuiltinActors() {
		lookup[ba.Code()] = ba
	}

	v6, err := vm.NewVMAtEpoch(ctx, lookup, v5.Store(), v6nextRoot, v5.GetEpoch()+1)
	require.NoError(t, err)

	// check the market state after migration
	var market6State market.State
	require.NoError(t, v6.GetState(builtin.StorageMarketActorAddr, &market6State))
	market.CheckStateInvariants(&market6State, v6.Store(), oldMarketActorBalance, v6.GetEpoch()+1)

	// check both miners' escrow balances (should be zero and collateral, respectively)
	et, err := adt.AsBalanceTable(adtStore, market6State.EscrowTable)
	require.NoError(t, err)
	bal, err := et.Get(testMinerTypeAddr.IDAddress)
	require.NoError(t, err)
	require.Equal(t, bal, big.Zero())
	bal, err = et.Get(realMinerTypeAddr.IDAddress)
	require.NoError(t, err)
	require.Equal(t, bal, collateral)

	// check that the power statistics were correctly recomputed (total miners, total miners above threshold)
	var power6State power.State
	require.NoError(t, v6.GetState(builtin.StoragePowerActorAddr, &power6State))
	power.CheckStateInvariants(&power6State, v6.Store())
	require.Equal(t, power6State.MinerCount, int64(1))
	// maybe should make one or both of these miners above
	require.Equal(t, power6State.MinerAboveMinPowerCount, int64(0))

	// check claims removal here- ensure the test miner is not in claims, real miner is
	_, found, err = power6State.GetClaim(adtStore, testMinerTypeAddr.IDAddress)
	require.NoError(t, err)
	require.False(t, found)
	_, found, err = power6State.GetClaim(adtStore, realMinerTypeAddr.IDAddress)
	require.NoError(t, err)
	require.True(t, found)

	// check that the total amount of filecoin stayed constant!
	totalCurrentRuntimeBalance, err := v6.GetTotalActorBalance()
	require.NoError(t, err)
	require.Equal(t, totalPreviousRuntimeBalance, totalCurrentRuntimeBalance)

	// check that the test miner was properly deleted
	_, deletedMinerFound, err := v6.GetActor(testMinerTypeAddr.IDAddress)
	require.NoError(t, err)
	require.False(t, deletedMinerFound)

	// check that the non-test miner was not deleted and still has its remaining money
	nonTestMiner, found, err := v6.GetActor(realMinerTypeAddr.IDAddress)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, nonTestMiner.Balance, minerBalance)

	marketActor, found, err := v6.GetActor(builtin.StorageMarketActorAddr)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, marketActor.Balance, collateral)

	// check the owner's new balance and make sure it's same as the miner1 balance
	ownerActor, found, err = v6.GetActor(minerOwner)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t,
		big.Add(oldOwnerBalance, big.Add(minerBalance, collateral)),
		ownerActor.Balance)
}
