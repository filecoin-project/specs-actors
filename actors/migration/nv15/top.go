package nv14

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/rt"
	builtin5 "github.com/filecoin-project/specs-actors/v5/actors/builtin"

	states5 "github.com/filecoin-project/specs-actors/v5/actors/states"
	builtin6 "github.com/filecoin-project/specs-actors/v6/actors/builtin"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/power"
	states6 "github.com/filecoin-project/specs-actors/v6/actors/states"
	"github.com/filecoin-project/specs-actors/v6/actors/util/adt"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

// Config parameterizes a state tree migration
type Config struct {
	// Number of migration worker goroutines to run.
	// More workers enables higher CPU utilization doing migration computations (including state encoding)
	MaxWorkers uint
	// Capacity of the queue of jobs available to workers (zero for unbuffered).
	// A queue length of hundreds to thousands improves throughput at the cost of memory.
	JobQueueSize uint
	// Capacity of the queue receiving migration results from workers, for persisting (zero for unbuffered).
	// A queue length of tens to hundreds improves throughput at the cost of memory.
	ResultQueueSize uint
	// Time between progress logs to emit.
	// Zero (the default) results in no progress logs.
	ProgressLogPeriod time.Duration
}

type Logger interface {
	// This is the same logging interface provided by the Runtime
	Log(level rt.LogLevel, msg string, args ...interface{})
}

func ActorHeadKey(addr address.Address, head cid.Cid) string {
	return addr.String() + "-h-" + head.String()
}

// Migrates from v14 to v15
//
// This migration only updates the actor code CIDs in the state tree.
// MigrationCache stores and loads cached data. Its implementation must be threadsafe
type MigrationCache interface {
	Write(key string, newCid cid.Cid) error
	Read(key string) (bool, cid.Cid, error)
	Load(key string, loadFunc func() (cid.Cid, error)) (cid.Cid, error)
}

func IsTestPostProofType(proofType abi.RegisteredPoStProof) bool {
	testPoStProofTypes := [6]abi.RegisteredPoStProof{abi.RegisteredPoStProof_StackedDrgWinning2KiBV1,
		abi.RegisteredPoStProof_StackedDrgWinning8MiBV1,
		abi.RegisteredPoStProof_StackedDrgWinning512MiBV1,
		abi.RegisteredPoStProof_StackedDrgWindow2KiBV1,
		abi.RegisteredPoStProof_StackedDrgWindow8MiBV1,
		abi.RegisteredPoStProof_StackedDrgWindow512MiBV1,
	}
	for i := 0; i < 6; i++ {
		if proofType == testPoStProofTypes[i] {
			return true
		}
	}
	return false
}

// XXX balanceTransferMap should become a BalanceTable, maybe?
func addIfPresent(balanceTransferMap *map[address.Address]big.Int, address address.Address, balance big.Int) {
	if balance.Equals(big.Zero()) { // do nothing if it's equal...
		return
	}
	if balance.LessThan(big.Zero()) {
		return
	} // XXX what are you doing why is it negative??? should we put an assertion here???

	oldVal, found := (*balanceTransferMap)[address]
	if !found {
		(*balanceTransferMap)[address] = balance
	} else {
		(*balanceTransferMap)[address] = big.Add(balance, oldVal)
	}
}

// migrates the power actor for this actor type. deletes claims of invalid miner types, and updates MinerCount and MinerAboveMinPowerCount according to how they were computed before.
func MigratePowerActor(ctx context.Context, store cbor.IpldStore, actorsIn *states5.Tree,
	actorsOut *states5.Tree, totalDeletedMiners int, totalDeletedMinersAboveMinPower int,
) error {
	powerActor, found, err := actorsIn.GetActor(builtin5.StoragePowerActorAddr)
	if err != nil || !found {
		return xerrors.Errorf("Could not get power actor for migration.")
	}
	var powerState power.State
	if err := store.Get(ctx, powerActor.Head, &powerState); err != nil {
		return err
	}
	wrappedStore := adt.WrapStore(ctx, store)
	claims, err := adt.AsMap(wrappedStore, powerState.Claims, builtin5.DefaultHamtBitwidth)
	if err != nil {
		return err
	}

	var claim power.Claim
	err = claims.ForEach(&claim, func(key string) error {
		if IsTestPostProofType(claim.WindowPoStProofType) {
			// this just deletes any claims that ended up in the power actor's claims table which had the test miner types
			// handling the power statistics (MinerCount, MinerAboveMinPowerCount) will be done in top.go after we've collected all that from the miners.
			addr, err := address.NewFromString(key)
			if err != nil {
				return err
			}
			if claim.RawBytePower.GreaterThan(big.Zero()) {
				return xerrors.Errorf("nonzero RawBytePower on claim from miner with test proof size. This is not good.")
			}
			if claim.QualityAdjPower.GreaterThan(big.Zero()) {
				return xerrors.Errorf("nonzero QualityAdjPower on claim from miner with test proof size. This is not good.")
			}
			// XXX: I think you might need to manually change claims. this might be wrong. you need to write back a CID somewhere... do you?
			if _, err := powerState.DeleteClaim(claims, addr); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil
	}
	powerState.MinerCount -= int64(totalDeletedMiners)
	powerState.MinerAboveMinPowerCount -= int64(totalDeletedMinersAboveMinPower)

	newHead, err := store.Put(ctx, &powerState)
	if err != nil {
		return nil
	}

	powerActor.Head = newHead

	return actorsOut.SetActor(builtin6.StoragePowerActorAddr, powerActor)
}

// Migrates the filecoin state tree starting from the global state tree and upgrading all actor state.
// The store must support concurrent writes (even if the configured worker count is 1).
func MigrateStateTree(ctx context.Context, store cbor.IpldStore, actorsRootIn cid.Cid, priorEpoch abi.ChainEpoch, cfg Config, log Logger, cache MigrationCache) (cid.Cid, error) {
	if cfg.MaxWorkers <= 0 {
		return cid.Undef, xerrors.Errorf("invalid migration config with %d workers", cfg.MaxWorkers)
	}

	// Maps prior version code CIDs to migration functions.
	var migrations = map[cid.Cid]actorMigration{
		builtin5.AccountActorCodeID:          nilMigrator{builtin6.AccountActorCodeID},
		builtin5.CronActorCodeID:             nilMigrator{builtin6.CronActorCodeID},
		builtin5.InitActorCodeID:             nilMigrator{builtin6.InitActorCodeID},
		builtin5.MultisigActorCodeID:         nilMigrator{builtin6.MultisigActorCodeID},
		builtin5.PaymentChannelActorCodeID:   nilMigrator{builtin6.PaymentChannelActorCodeID},
		builtin5.RewardActorCodeID:           nilMigrator{builtin6.RewardActorCodeID},
		builtin5.StorageMinerActorCodeID:     cachedMigration(cache, minerMigrator{}),
		builtin5.StorageMarketActorCodeID:    nilMigrator{builtin6.StorageMarketActorCodeID},
		builtin5.SystemActorCodeID:           nilMigrator{builtin6.SystemActorCodeID},
		builtin5.VerifiedRegistryActorCodeID: nilMigrator{builtin6.VerifiedRegistryActorCodeID},
	}

	// Set of prior version code CIDs for actors to defer during iteration, for explicit migration afterwards.
	var deferredCodeIDs = map[cid.Cid]struct{}{
		builtin5.StoragePowerActorCodeID: {},
	}

	if len(migrations)+len(deferredCodeIDs) != 11 {
		panic(fmt.Sprintf("incomplete migration specification with %d code CIDs", len(migrations)))
	}
	startTime := time.Now()

	// Load input and output state trees
	adtStore := adt.WrapStore(ctx, store)
	actorsIn, err := states5.LoadTree(adtStore, actorsRootIn)
	if err != nil {
		return cid.Undef, err
	}
	actorsOut, err := states5.NewTree(adtStore)
	if err != nil {
		return cid.Undef, err
	}

	// Setup synchronization
	grp, ctx := errgroup.WithContext(ctx)
	// Input and output queues for workers.
	jobCh := make(chan *migrationJob, cfg.JobQueueSize)
	jobResultCh := make(chan *migrationJobResult, cfg.ResultQueueSize)
	// Atomically-modified counters for logging progress
	var jobCount uint32
	var doneCount uint32

	// Iterate all actors in old state root to create migration jobs for each non-deferred actor.
	grp.Go(func() error {
		defer close(jobCh)
		log.Log(rt.INFO, "Creating migration jobs for tree %s", actorsRootIn)
		if err = actorsIn.ForEach(func(addr address.Address, actorIn *states5.Actor) error {
			if _, ok := deferredCodeIDs[actorIn.Code]; ok {
				return nil // Deferred for explicit migration later.
			}
			migration, ok := migrations[actorIn.Code]
			if !ok {
				return xerrors.Errorf("actor with code %s has no registered migration function", actorIn.Code)
			}
			nextInput := &migrationJob{
				Address:        addr,
				Actor:          *actorIn, // Must take a copy, the pointer is not stable.
				cache:          cache,
				actorMigration: migration,
			}
			select {
			case jobCh <- nextInput:
			case <-ctx.Done():
				return ctx.Err()
			}
			atomic.AddUint32(&jobCount, 1)
			return nil
		}); err != nil {
			return err
		}
		log.Log(rt.INFO, "Done creating %d migration jobs for tree %s after %v", jobCount, actorsRootIn, time.Since(startTime))
		return nil
	})

	// Worker threads run jobs.
	var workerWg sync.WaitGroup
	for i := uint(0); i < cfg.MaxWorkers; i++ {
		workerWg.Add(1)
		workerId := i
		grp.Go(func() error {
			defer workerWg.Done()
			for job := range jobCh {
				result, err := job.run(ctx, store, priorEpoch)
				if err != nil {
					return err
				}
				select {
				case jobResultCh <- result:
				case <-ctx.Done():
					return ctx.Err()
				}
				atomic.AddUint32(&doneCount, 1)
			}
			log.Log(rt.INFO, "Worker %d done", workerId)
			return nil
		})
	}
	log.Log(rt.INFO, "Started %d workers", cfg.MaxWorkers)

	// Monitor the job queue. This non-critical goroutine is outside the errgroup and exits when
	// workersFinished is closed, or the context done.
	workersFinished := make(chan struct{}) // Closed when waitgroup is emptied.
	if cfg.ProgressLogPeriod > 0 {
		go func() {
			defer log.Log(rt.DEBUG, "Job queue monitor done")
			for {
				select {
				case <-time.After(cfg.ProgressLogPeriod):
					jobsNow := jobCount // Snapshot values to avoid incorrect-looking arithmetic if they change.
					doneNow := doneCount
					pendingNow := jobsNow - doneNow
					elapsed := time.Since(startTime)
					rate := float64(doneNow) / elapsed.Seconds()
					log.Log(rt.INFO, "%d jobs created, %d done, %d pending after %v (%.0f/s)",
						jobsNow, doneNow, pendingNow, elapsed, rate)
				case <-workersFinished:
					return
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Close result channel when workers are done sending to it.
	grp.Go(func() error {
		workerWg.Wait()
		close(jobResultCh)
		close(workersFinished)
		log.Log(rt.INFO, "All workers done after %v", time.Since(startTime))
		return nil
	})

	// building up a map of balance transfers- address to amount
	// this mutex will only get held like 30 times over a map of all actors, so it will have zero contention, but better safe than sorry!
	var balanceTransferMapGuard = &sync.Mutex{}
	var balanceTransfers = make(map[address.Address]big.Int)
	var totalDeletedMinersAboveMinPower = 0
	var totalDeletedMiners = 0

	marketActor, found, err := actorsIn.GetActor(builtin5.StorageMarketActorAddr)
	if err != nil {
		return cid.Undef, err
	}
	if !found {
		return cid.Undef, fmt.Errorf("failed to get market actor for second migration")
	}
	var marketState market.State
	if err := store.Get(ctx, marketActor.Head, &marketState); err != nil {
		return cid.Undef, err
	}
	wrappedStore := adt.WrapStore(ctx, store)
	escrowTable, err := adt.AsBalanceTable(wrappedStore, marketState.EscrowTable)
	if err != nil {
		return cid.Undef, err
	}

	// Insert migrated records in output state tree and accumulators.
	// also, remove miners from tree if needed, and check if they're in the marketstate escrow table
	// also remove them from the market state escrow table if needed
	grp.Go(func() error {
		log.Log(rt.INFO, "Result writer started")
		resultCount := 0
		deletedActorCount := 0
		for result := range jobResultCh {
			if result.minerTypeMigrationShouldDelete {
				balanceTransferMapGuard.Lock()
				ownerAddressT := result.minerTypeMigrationBalanceTransferInfo.address
				valueT := result.minerTypeMigrationBalanceTransferInfo.value
				if !valueT.GreaterThanEqual(big.Zero()) {
					return xerrors.Errorf("deleted test miner's balance was negative and we tried to send it to address %v", ownerAddressT)
				}
				// transfer miner's balance back to owner
				addIfPresent(&balanceTransfers, ownerAddressT, valueT)

				// prep to adjust power stats
				if result.minerDeletedWasAboveMinPower {
					totalDeletedMinersAboveMinPower++
				}
				totalDeletedMiners++
				deletedActorCount++

				// adjust miner escrow table and send funds around- first, get the amount
				tokenAmountInEscrow, err := escrowTable.Get(result.address)
				if err != nil {
					return err
				}
				if !tokenAmountInEscrow.GreaterThanEqual(big.Zero()) {
					return xerrors.Errorf("miner's balance in escrow was negative and we tried to send it to address %v", ownerAddressT)
				}
				// zero out miner balances from escrow table
				if err = escrowTable.MustSubtract(result.address, tokenAmountInEscrow); err != nil {
					return nil
				}
				// schedule that amount to go back to the owner
				addIfPresent(&balanceTransfers, ownerAddressT, tokenAmountInEscrow)
				balanceTransferMapGuard.Unlock()
			} else {
				if err := actorsOut.SetActor(result.address, &result.actor); err != nil {
					return err
				}
				resultCount++
			}
		}
		log.Log(rt.INFO, "Result writer wrote %d results to state tree and deleted %d actors after %v", resultCount, deletedActorCount, time.Since(startTime))
		return nil
	})

	if err := grp.Wait(); err != nil {
		return cid.Undef, err
	}

	// remove claims from these actors, reset power stats
	if err = MigratePowerActor(ctx, store, actorsIn, actorsOut,
		totalDeletedMiners, totalDeletedMinersAboveMinPower); err != nil {
		return cid.Undef, err
	}

	// write back new escrow table into market state
	newEscrowTableRoot, err := escrowTable.Root()
	if err != nil {
		return cid.Undef, err
	}
	marketState.EscrowTable = newEscrowTableRoot
	marketActor.Head, err = store.Put(ctx, &marketState)
	if err != nil {
		return cid.Undef, err
	}
	if actorsOut.SetActor(builtin6.StorageMarketActorAddr, marketActor) != nil {
		return cid.Undef, err
	}

	// doing balance increments for owners of the deleted miners with test state tree types
	// this is balances from the miners' balances in the tree and also from the escrow tables in the market actor.

	for addressT, valueT := range balanceTransfers { //nolint:nomaprange // this nolint is ok, this thing will be like 30 entries long max...
		// check and make sure this is positive... just as a fun invariant, haha

		incrementaddr := addressT
		actor, found, err := actorsOut.GetActor(addressT)
		if err != nil {
			return cid.Undef, err
		}
		// if you don't find the owner of the deleted miner, swap to sending funds to f099
		if !found {
			f099addr, err := address.NewFromString("f099")
			if err != nil {
				return cid.Undef, err
			}
			actor, found, err = actorsOut.GetActor(f099addr)
			incrementaddr = f099addr
			if err != nil {
				return cid.Undef, err
			}
			// if you don't find THAT one, you really messed up bad!
			if !found {
				return cid.Undef, xerrors.Errorf("could not find actor for the owner of the deleted miner, and then could not find f099 to send the funds to as a backup. something is very wrong here.")
			}
		}
		actor.Balance = big.Add(actor.Balance, valueT)
		err = actorsOut.SetActor(incrementaddr, actor)
		if err != nil {
			return cid.Undef, err
		}
	}

	elapsed := time.Since(startTime)
	rate := float64(doneCount) / elapsed.Seconds()
	log.Log(rt.INFO, "All %d done after %v (%.0f/s). Flushing state tree root.", doneCount, elapsed, rate)
	return actorsOut.Flush()
}

type actorMigrationInput struct {
	address    address.Address // actor's address
	balance    abi.TokenAmount // actor's balance
	head       cid.Cid         // actor's state head CID
	priorEpoch abi.ChainEpoch  // epoch of last state transition prior to migration
	cache      MigrationCache  // cache of existing cid -> cid migrations for this actor
}

type balanceTransferInfo struct {
	address address.Address
	value   big.Int
}

type actorMigrationResult struct {
	newCodeCID                            cid.Cid
	newHead                               cid.Cid
	minerTypeMigrationShouldDelete        bool
	minerDeletedWasAboveMinPower          bool
	minerTypeMigrationBalanceTransferInfo balanceTransferInfo
}

type actorMigration interface {
	// Loads an actor's state from an input store and writes new state to an output store.
	// Returns the new state head CID.
	migrateState(ctx context.Context, store cbor.IpldStore, input actorMigrationInput) (result *actorMigrationResult, err error)
	migratedCodeCID() cid.Cid
}

type migrationJob struct {
	address.Address
	states5.Actor
	actorMigration
	cache MigrationCache
}

type migrationJobResult struct {
	address                               address.Address
	actor                                 states6.Actor
	minerTypeMigrationShouldDelete        bool
	minerDeletedWasAboveMinPower          bool
	minerTypeMigrationBalanceTransferInfo balanceTransferInfo
}

func (job *migrationJob) run(ctx context.Context, store cbor.IpldStore, priorEpoch abi.ChainEpoch) (*migrationJobResult, error) {
	result, err := job.migrateState(ctx, store, actorMigrationInput{
		address:    job.Address,
		balance:    job.Actor.Balance,
		head:       job.Actor.Head,
		priorEpoch: priorEpoch,
		cache:      job.cache,
	})
	if err != nil {
		return nil, xerrors.Errorf("state migration failed for %s actor, addr %s: %w",
			builtin5.ActorNameByCode(job.Actor.Code), job.Address, err)
	}

	// Set up new actor record with the migrated state.
	// XXX: to test: add one of each type of miner, maybe add some sectors, make a complex enough state and check some invariants???
	// XXX: give some some fees, give some no fees, etc, etc, etc
	// XXX: what happens if someone sends these addresses some funds??? no good.
	// XXX: give some some fees, give some no fees, etc, etc, etc
	// XXX: https://github.com/filecoin-project/specs-actors/blob/0fa32a654d910960306a0567d69f8d2ac1e66c67/actors/migration/nv4/top.go#L228
	// XXX: https://github.com/filecoin-project/specs-actors/issues/1474#issuecomment-948948954
	return &migrationJobResult{
		minerTypeMigrationShouldDelete:        result.minerTypeMigrationShouldDelete,
		minerDeletedWasAboveMinPower:          result.minerDeletedWasAboveMinPower,
		minerTypeMigrationBalanceTransferInfo: result.minerTypeMigrationBalanceTransferInfo,
		address:                               job.Address, // Unchanged
		actor: states6.Actor{
			Code:       result.newCodeCID,
			Head:       result.newHead,
			CallSeqNum: job.Actor.CallSeqNum, // Unchanged
			Balance:    job.Actor.Balance,    // Unchanged
		},
	}, nil
}

// Migrator which preserves the head CID and provides a fixed result code CID.
type nilMigrator struct {
	OutCodeCID cid.Cid
}

func (n nilMigrator) migrateState(_ context.Context, _ cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	return &actorMigrationResult{
		newCodeCID:                            n.OutCodeCID,
		newHead:                               in.head,
		minerTypeMigrationShouldDelete:        false,
		minerDeletedWasAboveMinPower:          false,
		minerTypeMigrationBalanceTransferInfo: balanceTransferInfo{},
	}, nil
}

func (n nilMigrator) migratedCodeCID() cid.Cid {
	return n.OutCodeCID
}

type cachedMigrator struct {
	cache MigrationCache
	actorMigration
}

func (c cachedMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	newHead, err := c.cache.Load(ActorHeadKey(in.address, in.head), func() (cid.Cid, error) {
		result, err := c.actorMigration.migrateState(ctx, store, in)
		if err != nil {
			return cid.Undef, err
		}
		return result.newHead, nil
	})
	if err != nil {
		return nil, err
	}
	return &actorMigrationResult{
		newCodeCID:                            c.migratedCodeCID(),
		newHead:                               newHead,
		minerTypeMigrationShouldDelete:        false,
		minerDeletedWasAboveMinPower:          false,
		minerTypeMigrationBalanceTransferInfo: balanceTransferInfo{},
	}, nil
}
func cachedMigration(cache MigrationCache, m actorMigration) actorMigration {
	return cachedMigrator{
		actorMigration: m,
		cache:          cache,
	}
}
