package migration

import (
	"context"

	address "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/sync/semaphore"
	"golang.org/x/xerrors"

	builtin0 "github.com/filecoin-project/specs-actors/actors/builtin"
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	power0 "github.com/filecoin-project/specs-actors/actors/builtin/power"
	states0 "github.com/filecoin-project/specs-actors/actors/states"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/states"
)

var (
	defaultMaxWorkers = 16
)

// SyncCtx traverses the migration to coordinate safe multithreaded state processing
type SyncContext struct {
	sem           semaphore.Weighted
	actorsOutCh   chan actorUpdate
	powerUpdateCh chan PowerUpdater
	transferCh    chan big.Int
	errCh         chan error
}

// Config parameterizes a state tree migration
type Config struct {
	MaxWorkers int
}

func DefaultConfig() Config {
	return Config{
		MaxWorkers: defaultMaxWorkers,
	}
}

type PowerUpdates struct {
	claims map[address.Address]power0.Claim
	crons  map[abi.ChainEpoch][]power0.CronEvent
}

type PowerUpdater interface {
	Apply(*PowerUpdates)
}

type claimUpdate struct {
	addr  address.Address
	claim power0.Claim
}

// Overwrite claim
func (c claimUpdate) Apply(p *PowerUpdates) {
	p.claims[c.addr] = c.claim
}

type cronUpdate struct {
	epoch abi.ChainEpoch
	event power0.CronEvent
}

type actorUpdate struct {
	addr     address.Address
	actorOut states.Actor
}

// Append cron event
func (c cronUpdate) Apply(p *PowerUpdates) {
	p.crons[c.epoch] = append(p.crons[c.epoch], c.event)
}

type MigrationInfo struct {
	address    address.Address // actor's address
	balance    abi.TokenAmount // actor's balance
	priorEpoch abi.ChainEpoch  // epoch of last state transition prior to migration
	syncCtx    *SyncContext    // used to coordinate safe multithreading
}

type StateMigration interface {
	// Loads an actor's state from an input store and writes new state to an output store.
	// Returns the new state head CID.
	MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, info MigrationInfo) (newHead cid.Cid, balanceChange abi.TokenAmount, err error)
}

type ActorMigration struct {
	OutCodeCID     cid.Cid
	StateMigration StateMigration
}

var migrations = map[cid.Cid]ActorMigration{ // nolint:varcheck,deadcode,unused
	builtin0.AccountActorCodeID: ActorMigration{
		OutCodeCID:     builtin.AccountActorCodeID,
		StateMigration: &accountMigrator{},
	},
	builtin0.CronActorCodeID: ActorMigration{
		OutCodeCID:     builtin.CronActorCodeID,
		StateMigration: &cronMigrator{},
	},
	builtin0.InitActorCodeID: ActorMigration{
		OutCodeCID:     builtin.InitActorCodeID,
		StateMigration: &initMigrator{},
	},
	builtin0.StorageMarketActorCodeID: ActorMigration{
		OutCodeCID:     builtin.StorageMarketActorCodeID,
		StateMigration: &marketMigrator{},
	},
	builtin0.MultisigActorCodeID: ActorMigration{
		OutCodeCID:     builtin.MultisigActorCodeID,
		StateMigration: &multisigMigrator{},
	},
	builtin0.PaymentChannelActorCodeID: ActorMigration{
		OutCodeCID:     builtin.PaymentChannelActorCodeID,
		StateMigration: &paychMigrator{},
	},
	builtin0.StoragePowerActorCodeID: ActorMigration{
		OutCodeCID:     builtin.StoragePowerActorCodeID,
		StateMigration: &powerMigrator{},
	},
	builtin0.RewardActorCodeID: ActorMigration{
		OutCodeCID:     builtin.RewardActorCodeID,
		StateMigration: &rewardMigrator{},
	},
	builtin0.SystemActorCodeID: ActorMigration{
		OutCodeCID:     builtin.SystemActorCodeID,
		StateMigration: &systemMigrator{},
	},
	builtin0.VerifiedRegistryActorCodeID: ActorMigration{
		OutCodeCID:     builtin.VerifiedRegistryActorCodeID,
		StateMigration: &verifregMigrator{},
	},
	builtin0.StorageMinerActorCodeID: ActorMigration{
		OutCodeCID:     builtin.StorageMinerActorCodeID,
		StateMigration: &minerMigrator{},
	},
}

var deferredMigrations = map[cid.Cid]bool{
	builtin0.VerifiedRegistryActorCodeID: true,
	builtin0.StoragePowerActorCodeID:     true,
}

// Migrates the filecoin state tree starting from the global state tree and upgrading all actor state.
func MigrateStateTree(ctx context.Context, store cbor.IpldStore, stateRootIn cid.Cid, priorEpoch abi.ChainEpoch, cfg Config) (cid.Cid, error) {
	// Setup input and output state tree helpers
	adtStore := adt.WrapStore(ctx, store)
	actorsIn, err := states0.LoadTree(adtStore, stateRootIn)
	if err != nil {
		return cid.Undef, err
	}
	stateRootOut, err := adt.MakeEmptyMap(adtStore).Root()
	if err != nil {
		return cid.Undef, err
	}
	actorsOut, err := states.LoadTree(adtStore, stateRootOut)
	if err != nil {
		return cid.Undef, err
	}

	// Extra actor setup

	// miner
	transferFromBurnt := big.Zero()
	powerUpdates := &PowerUpdates{
		claims: make(map[address.Address]power0.Claim),
		crons:  make(map[abi.ChainEpoch][]power0.CronEvent),
	}

	// Setup synchronization
	syncCtx := &SyncContext{
		sem: *semaphore.NewWeighted(int64(cfg.MaxWorkers)),
		// For a channel sent on by at most n threads at most k times, a buffer
		// size of n*k ensures that all threads run to completion once started.
		actorsOutCh:   make(chan actorUpdate, cfg.MaxWorkers),
		powerUpdateCh: make(chan PowerUpdater, 2*cfg.MaxWorkers),
		transferCh:    make(chan big.Int, cfg.MaxWorkers),
		errCh:         make(chan error, cfg.MaxWorkers),
	}

	// Iterate all actors in old state root
	// Set new state root actors as we go
	err = actorsIn.ForEach(func(addr address.Address, actorIn *states.Actor) error {
		// Hand off migration of one actor, blocking if we are out of worker goroutines
		if err := syncCtx.sem.Acquire(ctx, 1); err != nil {
			return err
		}
		go migrateOneActor(ctx, store, addr, *actorIn, actorsOut, priorEpoch, powerUpdates, syncCtx)

		// Read from err and transfer channels without blocking.
		// Terminate on the first error.
		// Accumulate funds transfered from burnt to miners.
		if err := accumulateResults(syncCtx, transferFromBurnt, powerUpdates, actorsOut); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return cid.Undef, err
	}
	// Wait on all jobs finishing
	if err := syncCtx.sem.Acquire(ctx, int64(cfg.MaxWorkers)); err != nil {
		return cid.Undef, xerrors.Errorf("failed to wait for all worker jobs: %w", err)
	}
	// Check for outstanding transfers and errors
	if err := accumulateResults(syncCtx, transferFromBurnt, powerUpdates, actorsOut); err != nil {
		return cid.Undef, err
	}

	// Migrate Power actor
	pm := migrations[builtin0.StoragePowerActorCodeID].StateMigration.(*powerMigrator)
	pm.actorsIn = actorsIn
	pm.powerUpdates = powerUpdates
	powerActorIn, found, err := actorsIn.GetActor(builtin0.StoragePowerActorAddr)
	if err != nil {
		return cid.Undef, err
	}
	if !found {
		return cid.Undef, xerrors.Errorf("could not find power actor in state")
	}
	powerActorHeadOut, transfer, err := pm.MigrateState(ctx, store, powerActorIn.Head, MigrationInfo{
		address:    builtin0.StoragePowerActorAddr,
		balance:    powerActorIn.Balance,
		priorEpoch: priorEpoch,
		syncCtx:    syncCtx,
	})
	if err != nil {
		return cid.Undef, err
	}
	powerActorOut := states.Actor{
		Code:       builtin.StoragePowerActorCodeID,
		Head:       powerActorHeadOut,
		CallSeqNum: powerActorIn.CallSeqNum,
		Balance:    big.Add(powerActorIn.Balance, transfer),
	}
	err = actorsOut.SetActor(builtin.StoragePowerActorAddr, &powerActorOut)
	if err != nil {
		return cid.Undef, err
	}

	// Migrate verified registry
	vm := migrations[builtin0.VerifiedRegistryActorCodeID].StateMigration.(*verifregMigrator)
	vm.actorsOut = actorsOut
	verifRegActorIn, found, err := actorsIn.GetActor(builtin0.VerifiedRegistryActorAddr)
	if err != nil {
		return cid.Undef, err
	}
	if !found {
		return cid.Undef, xerrors.Errorf("could not find verifreg actor in state")
	}
	verifRegHeadOut, transfer, err := vm.MigrateState(ctx, store, verifRegActorIn.Head, MigrationInfo{
		address:    builtin0.VerifiedRegistryActorAddr,
		balance:    verifRegActorIn.Balance,
		priorEpoch: priorEpoch,
		syncCtx:    syncCtx,
	})
	if err != nil {
		return cid.Undef, err
	}
	verifRegActorOut := states.Actor{
		Code:       builtin.VerifiedRegistryActorCodeID,
		Head:       verifRegHeadOut,
		CallSeqNum: verifRegActorIn.CallSeqNum,
		Balance:    big.Add(verifRegActorIn.Balance, transfer),
	}
	err = actorsOut.SetActor(builtin.VerifiedRegistryActorAddr, &verifRegActorOut)
	if err != nil {
		return cid.Undef, err
	}

	// Track deductions to burntFunds actor's balance
	burntFundsActor, found, err := actorsOut.GetActor(builtin.BurntFundsActorAddr)
	if err != nil {
		return cid.Undef, err
	}
	if !found {
		return cid.Undef, xerrors.Errorf("burnt funds actor not in tree")
	}
	burntFundsActor.Balance = big.Sub(burntFundsActor.Balance, transferFromBurnt)
	if burntFundsActor.Balance.LessThan(big.Zero()) {
		return cid.Undef, xerrors.Errorf("miner transfers send burnt funds actor balance below zero")
	}
	err = actorsOut.SetActor(builtin.BurntFundsActorAddr, burntFundsActor)
	if err != nil {
		return cid.Undef, err
	}

	return actorsOut.Flush()
}

func migrateOneActor(ctx context.Context, store cbor.IpldStore, addr address.Address, actorIn states.Actor,
	actorsOut *states.Tree, priorEpoch abi.ChainEpoch, powerUpdates *PowerUpdates, syncCtx *SyncContext,
) {
	defer syncCtx.sem.Release(1)
	// This will be migrated at the end
	if _, found := deferredMigrations[actorIn.Code]; found {
		return
	}

	migration := migrations[actorIn.Code]
	codeOut := migration.OutCodeCID
	headOut, transfer, err := migration.StateMigration.MigrateState(ctx, store, actorIn.Head, MigrationInfo{
		address:    addr,
		balance:    actorIn.Balance,
		priorEpoch: priorEpoch,
		syncCtx:    syncCtx,
	})

	if err != nil {
		err = xerrors.Errorf("state migration error on %s actor at addr %s: %w", builtin.ActorNameByCode(codeOut), addr, err)
		syncCtx.errCh <- err
		return
	}

	// set up new state root with the migrated state
	actorOut := states.Actor{
		Code:       codeOut,
		Head:       headOut,
		CallSeqNum: actorIn.CallSeqNum,
		Balance:    big.Add(actorIn.Balance, transfer),
	}
	syncCtx.actorsOutCh <- actorUpdate{addr: addr, actorOut: actorOut}
	if transfer.GreaterThan(big.Zero()) {
		syncCtx.transferCh <- transfer
	}
}

func accumulateResults(syncCtx *SyncContext, transferAcc big.Int, powerUpdatesAcc *PowerUpdates, actorsOut *states.Tree) error {
	// Read and process worker results from all channels
READLOOP:
	for {
		select {
		case err := <-syncCtx.errCh:
			return err
		case transfer := <-syncCtx.transferCh:
			transferAcc = big.Add(transferAcc, transfer)
		case pwrUpdate := <-syncCtx.powerUpdateCh:
			pwrUpdate.Apply(powerUpdatesAcc)
		case actorUpdate := <-syncCtx.actorsOutCh:
			if err := actorsOut.SetActor(actorUpdate.addr, &actorUpdate.actorOut); err != nil {
				return err
			}
		default:
			break READLOOP
		}
	}
	return nil
}

func InputTreeBalance(ctx context.Context, store cbor.IpldStore, stateRootIn cid.Cid) (abi.TokenAmount, error) {
	adtStore := adt.WrapStore(ctx, store)
	actorsIn, err := states0.LoadTree(adtStore, stateRootIn)
	if err != nil {
		return big.Zero(), err
	}
	total := abi.NewTokenAmount(0)
	err = actorsIn.ForEach(func(addr address.Address, a *states.Actor) error {
		total = big.Add(total, a.Balance)
		return nil
	})
	return total, err
}

// InputTreeMinerAvailableBalance returns a map of every miner's outstanding
// available balance at the provided state tree.  It is used for validating
// that the system has enough funds to unburn all debts and add them to fee debt.
func InputTreeMinerAvailableBalance(ctx context.Context, store cbor.IpldStore, stateRootIn cid.Cid) (map[address.Address]abi.TokenAmount, error) {
	adtStore := adt.WrapStore(ctx, store)
	actorsIn, err := states0.LoadTree(adtStore, stateRootIn)
	if err != nil {
		return nil, err
	}
	available := make(map[address.Address]abi.TokenAmount)
	err = actorsIn.ForEach(func(addr address.Address, a *states.Actor) error {
		if !a.Code.Equals(builtin0.StorageMinerActorCodeID) {
			return nil
		}
		var inState miner0.State
		if err := store.Get(ctx, a.Head, &inState); err != nil {
			return err
		}
		minerLiabilities := big.Sum(inState.LockedFunds, inState.PreCommitDeposits, inState.InitialPledgeRequirement)
		availableBalance := big.Sub(a.Balance, minerLiabilities)
		available[addr] = availableBalance
		return nil
	})
	return available, err
}

// InputTreeBurntFunds returns the current balance of the burnt funds actor
// as defined by the given state tree
func InputTreeBurntFunds(ctx context.Context, store cbor.IpldStore, stateRootIn cid.Cid) (abi.TokenAmount, error) {
	adtStore := adt.WrapStore(ctx, store)
	actorsIn, err := states0.LoadTree(adtStore, stateRootIn)
	if err != nil {
		return big.Zero(), err
	}
	bf, found, err := actorsIn.GetActor(builtin0.BurntFundsActorAddr)
	if err != nil {
		return big.Zero(), err
	}
	if !found {
		return big.Zero(), xerrors.Errorf("burnt funds actor not found")
	}
	return bf.Balance, nil
}
