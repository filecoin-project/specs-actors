package test_test

import (
	"context"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/cbor"
	builtin2 "github.com/filecoin-project/specs-actors/v2/actors/builtin"
	account2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/account"
	cron2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/cron"
	init2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/init"
	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	power2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	reward2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/reward"
	system2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/system"
	verifreg2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/verifreg"
	states2 "github.com/filecoin-project/specs-actors/v2/actors/states"
	adt2 "github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	ipld2 "github.com/filecoin-project/specs-actors/v2/support/ipld"
	cid "github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/filecoin-project/specs-actors/v3/actors/migration/nv9"
)

func TestParallelMigrationCalls(t *testing.T) {
	// Construct simple prior state tree over a locking store
	ctx := context.Background()
	log := TestLogger{t}
	syncStore := ipld2.NewSyncADTStore(ctx)
	initializeActor := func(ctx context.Context, t *testing.T, actors *states2.Tree, state cbor.Marshaler, code cid.Cid, a addr.Address, balance abi.TokenAmount) {
		stateCID, err := actors.Store.Put(ctx, state)
		require.NoError(t, err)
		actor := &states2.Actor{
			Head:    stateCID,
			Code:    code,
			Balance: balance,
		}
		err = actors.SetActor(a, actor)
		require.NoError(t, err)
	}
	actorsIn, err := states2.NewTree(syncStore)
	require.NoError(t, err)

	emptyMapCID, err := adt2.MakeEmptyMap(syncStore).Root()
	require.NoError(t, err)
	emptyArray := adt2.MakeEmptyArray(syncStore)
	emptyArrayCID, err := emptyArray.Root()
	require.NoError(t, err)
	emptyMultimapCID, err := adt2.MakeEmptyMultimap(syncStore).Root()
	require.NoError(t, err)

	initializeActor(ctx, t, actorsIn, &system2.State{}, builtin2.SystemActorCodeID, builtin2.SystemActorAddr, big.Zero())

	initState := init2.ConstructState(emptyMapCID, "scenarios")
	initializeActor(ctx, t, actorsIn, initState, builtin2.InitActorCodeID, builtin2.InitActorAddr, big.Zero())

	rewardState := reward2.ConstructState(abi.NewStoragePower(0))
	initializeActor(ctx, t, actorsIn, rewardState, builtin2.RewardActorCodeID, builtin2.RewardActorAddr, builtin2.TotalFilecoin)

	cronState := cron2.ConstructState(cron2.BuiltInEntries())
	initializeActor(ctx, t, actorsIn, cronState, builtin2.CronActorCodeID, builtin2.CronActorAddr, big.Zero())

	powerState := power2.ConstructState(emptyMapCID, emptyMultimapCID)
	initializeActor(ctx, t, actorsIn, powerState, builtin2.StoragePowerActorCodeID, builtin2.StoragePowerActorAddr, big.Zero())

	marketState := market2.ConstructState(emptyArrayCID, emptyMapCID, emptyMultimapCID)
	initializeActor(ctx, t, actorsIn, marketState, builtin2.StorageMarketActorCodeID, builtin2.StorageMarketActorAddr, big.Zero())

	// this will need to be replaced with the address of a multisig actor for the verified registry to be tested accurately
	verifregRoot, err := addr.NewIDAddress(80)
	require.NoError(t, err)
	initializeActor(ctx, t, actorsIn, &account2.State{Address: verifregRoot}, builtin2.AccountActorCodeID, verifregRoot, big.Zero())
	vrState := verifreg2.ConstructState(emptyMapCID, verifregRoot)
	initializeActor(ctx, t, actorsIn, vrState, builtin2.VerifiedRegistryActorCodeID, builtin2.VerifiedRegistryActorAddr, big.Zero())

	// burnt funds
	initializeActor(ctx, t, actorsIn, &account2.State{Address: builtin2.BurntFundsActorAddr}, builtin2.AccountActorCodeID, builtin2.BurntFundsActorAddr, big.Zero())

	startRoot, err := actorsIn.Flush()
	require.NoError(t, err)

	// Run migration

	endRootSerial, err := nv9.MigrateStateTree(ctx, syncStore, startRoot, abi.ChainEpoch(0), nv9.Config{MaxWorkers: 1}, log)
	require.NoError(t, err)

	// Migrate in parallel
	var endRootParallel1, endRootParallel2 cid.Cid
	grp, ctx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		var err1 error
		endRootParallel1, err1 = nv9.MigrateStateTree(ctx, syncStore, startRoot, abi.ChainEpoch(0), nv9.Config{MaxWorkers: 2}, log)
		return err1
	})
	grp.Go(func() error {
		var err2 error
		endRootParallel2, err2 = nv9.MigrateStateTree(ctx, syncStore, startRoot, abi.ChainEpoch(0), nv9.Config{MaxWorkers: 2}, log)
		return err2
	})
	require.NoError(t, grp.Wait())
	assert.Equal(t, endRootSerial, endRootParallel1)
	assert.Equal(t, endRootParallel1, endRootParallel2)
}
