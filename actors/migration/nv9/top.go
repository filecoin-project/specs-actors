package nv9

import (
	"context"
	"fmt"
	"sync"

	address "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	builtin2 "github.com/filecoin-project/specs-actors/v2/actors/builtin"
	states2 "github.com/filecoin-project/specs-actors/v2/actors/states"

	builtin3 "github.com/filecoin-project/specs-actors/v3/actors/builtin"
	states3 "github.com/filecoin-project/specs-actors/v3/actors/states"
	adt3 "github.com/filecoin-project/specs-actors/v3/actors/util/adt"
)

// Config parameterizes a state tree migration
type Config struct {
	MaxWorkers int
}

// Migrates the filecoin state tree starting from the global state tree and upgrading all actor state.
func MigrateStateTree(ctx context.Context, store cbor.IpldStore, actorsRootIn cid.Cid, priorEpoch abi.ChainEpoch, cfg Config) (cid.Cid, error) {
	if cfg.MaxWorkers <= 0 {
		return cid.Undef, xerrors.Errorf("invalid migration config with %d workers", cfg.MaxWorkers)
	}

	// Maps prior version code CIDs to migration functions.
	var migrations = map[cid.Cid]actorMigration{
		builtin2.AccountActorCodeID:          nilMigrator{builtin3.AccountActorCodeID},
		builtin2.CronActorCodeID:             nilMigrator{builtin3.CronActorCodeID},
		builtin2.InitActorCodeID:             initMigrator{},
		builtin2.MultisigActorCodeID:         multisigMigrator{},
		builtin2.PaymentChannelActorCodeID:   paychMigrator{},
		builtin2.RewardActorCodeID:           nilMigrator{builtin3.RewardActorCodeID},
		builtin2.StorageMarketActorCodeID:    marketMigrator{},
		builtin2.StorageMinerActorCodeID:     minerMigrator{},
		builtin2.StoragePowerActorCodeID:     powerMigrator{},
		builtin2.SystemActorCodeID:           nilMigrator{builtin3.SystemActorCodeID},
		builtin2.VerifiedRegistryActorCodeID: verifregMigrator{},
	}
	// Set of prior version code CIDs for actors to defer during iteration, for explicit migration afterwards.
	var deferredCodeIDs = map[cid.Cid]struct{}{
		// None
	}
	if len(migrations)+len(deferredCodeIDs) != 11 {
		panic(fmt.Sprintf("incomplete migration specification with %d code CIDs", len(migrations)))
	}

	// Load input and output state trees
	adtStore := adt3.WrapStore(ctx, store)
	actorsIn, err := states2.LoadTree(adtStore, actorsRootIn)
	if err != nil {
		return cid.Undef, err
	}
	actorsOut, err := states3.NewTree(adtStore)
	if err != nil {
		return cid.Undef, err
	}

	// Setup synchronization
	grp, ctx := errgroup.WithContext(ctx)
	jobCh := make(chan *migrationJob)
	jobResultCh := make(chan *migrationJobResult)

	// Iterate all actors in old state root to create migration jobs for each non-deferred actor.
	grp.Go(func() error {
		defer close(jobCh)
		return actorsIn.ForEach(func(addr address.Address, actorIn *states2.Actor) error {
			if _, ok := deferredCodeIDs[actorIn.Code]; ok {
				return nil // Deferred for explicit migration later.
			}
			nextInput := &migrationJob{
				Address:        addr,
				Actor:          *actorIn, // Must take a copy, the pointer is not stable.
				actorMigration: migrations[actorIn.Code],
			}
			select {
			case jobCh <- nextInput:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
	})

	// Worker threads run jobs.
	var workerWg sync.WaitGroup
	for i := 0; i < cfg.MaxWorkers; i++ {
		workerWg.Add(1)
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
			}
			return nil
		})
	}

	// Close output channel when workers are done.
	grp.Go(func() error {
		workerWg.Wait()
		close(jobResultCh)
		return nil
	})

	// Insert migrated records in output state tree and accumulators.
	grp.Go(func() error {
		for result := range jobResultCh {
			if err := actorsOut.SetActor(result.Address, &result.Actor); err != nil {
				return err
			}
		}
		return nil
	})

	if err := grp.Wait(); err != nil {
		return cid.Undef, err
	}

	// Perform any deferred migrations explicitly here.
	// Deferred migrations might depend on values accumulated through migration of other actors.

	return actorsOut.Flush()
}

type actorMigrationInput struct {
	address    address.Address // actor's address
	balance    abi.TokenAmount // actor's balance
	head       cid.Cid         // actor's state head CID
	priorEpoch abi.ChainEpoch  // epoch of last state transition prior to migration
}

type actorMigrationResult struct {
	newCodeCID cid.Cid
	newHead    cid.Cid
}

type actorMigration interface {
	// Loads an actor's state from an input store and writes new state to an output store.
	// Returns the new state head CID.
	migrateState(ctx context.Context, store cbor.IpldStore, input actorMigrationInput) (result *actorMigrationResult, err error)
}

type migrationJob struct {
	address.Address
	states2.Actor
	actorMigration
}
type migrationJobResult struct {
	address.Address
	states3.Actor
}

func (job *migrationJob) run(ctx context.Context, store cbor.IpldStore, priorEpoch abi.ChainEpoch) (*migrationJobResult, error) {
	result, err := job.migrateState(ctx, store, actorMigrationInput{
		address:    job.Address,
		balance:    job.Actor.Balance,
		head:       job.Actor.Head,
		priorEpoch: priorEpoch,
	})
	if err != nil {
		return nil, xerrors.Errorf("state migration failed for %s actor, addr %s: %w",
			builtin2.ActorNameByCode(job.Actor.Code), job.Address, err)
	}

	// Set up new actor record with the migrated state.
	return &migrationJobResult{
		job.Address, // Unchanged
		states3.Actor{
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
		newCodeCID: n.OutCodeCID,
		newHead:    in.head,
	}, nil
}
