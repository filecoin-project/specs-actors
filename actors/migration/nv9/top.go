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

type StateMigrationInput struct {
	address    address.Address // actor's address
	balance    abi.TokenAmount // actor's balance
	head       cid.Cid         // actor's state head CID
	priorEpoch abi.ChainEpoch  // epoch of last state transition prior to migration
}

type StateMigrationResult struct {
	NewCodeCID cid.Cid
	NewHead    cid.Cid
}

type StateMigration interface {
	// Loads an actor's state from an input store and writes new state to an output store.
	// Returns the new state head CID.
	MigrateState(ctx context.Context, store cbor.IpldStore, input StateMigrationInput) (result *StateMigrationResult, err error)
}

// Migrator which preserves the head CID and provides a fixed result code CID.
type nilMigrator struct {
	OutCodeCID cid.Cid
}

func (n nilMigrator) MigrateState(_ context.Context, _ cbor.IpldStore, in StateMigrationInput) (*StateMigrationResult, error) {
	return &StateMigrationResult{
		NewCodeCID: n.OutCodeCID,
		NewHead:    in.head,
	}, nil
}

// Migrates the filecoin state tree starting from the global state tree and upgrading all actor state.
func MigrateStateTree(ctx context.Context, store cbor.IpldStore, actorsRootIn cid.Cid, priorEpoch abi.ChainEpoch, cfg Config) (cid.Cid, error) {
	if cfg.MaxWorkers <= 0 {
		return cid.Undef, xerrors.Errorf("invalid migration config with %d workers", cfg.MaxWorkers)
	}

	// Maps prior version code CIDs to migration functions.
	var migrations = map[cid.Cid]StateMigration{
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
	inputCh := make(chan *migrationInput)
	resultCh := make(chan *migrationResult)

	// Iterate all actors in old state root to generate migration inputs for each non-deferred actor.
	grp.Go(func() error {
		defer close(inputCh)
		return actorsIn.ForEach(func(addr address.Address, actorIn *states2.Actor) error {
			if _, ok := deferredCodeIDs[actorIn.Code]; ok {
				return nil // Deferred for explicit migration later.
			}
			nextInput := &migrationInput{
				Address:        addr,
				Actor:          *actorIn, // Must take a copy, the pointer is not stable.
				StateMigration: migrations[actorIn.Code],
			}
			select {
			case inputCh <- nextInput:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
	})

	// Worker threads run migrations on inputs.
	var workerWg sync.WaitGroup
	for i := 0; i < cfg.MaxWorkers; i++ {
		workerWg.Add(1)
		grp.Go(func() error {
			defer workerWg.Done()
			for input := range inputCh {
				result, err := migrateOneActor(ctx, store, input, priorEpoch)
				if err != nil {
					return err
				}
				select {
				case resultCh <- result:
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
		close(resultCh)
		return nil
	})

	// Insert migrated records in output state tree and accumulators.
	grp.Go(func() error {
		for result := range resultCh {
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

type migrationInput struct {
	address.Address
	states2.Actor
	StateMigration
}
type migrationResult struct {
	address.Address
	states3.Actor
}

func migrateOneActor(ctx context.Context, store cbor.IpldStore, input *migrationInput, priorEpoch abi.ChainEpoch) (*migrationResult, error) {
	actorIn := input.Actor
	addr := input.Address
	result, err := input.MigrateState(ctx, store, StateMigrationInput{
		address:    addr,
		balance:    actorIn.Balance,
		head:       actorIn.Head,
		priorEpoch: priorEpoch,
	})
	if err != nil {
		return nil, xerrors.Errorf("state migration failed for %s actor, addr %s: %w", builtin2.ActorNameByCode(actorIn.Code), addr, err)
	}

	// Set up new actor record with the migrated state.
	return &migrationResult{
		addr, // Unchanged
		states3.Actor{
			Code:       result.NewCodeCID,
			Head:       result.NewHead,
			CallSeqNum: actorIn.CallSeqNum, // Unchanged
			Balance:    actorIn.Balance,    // Unchanged
		},
	}, nil
}
