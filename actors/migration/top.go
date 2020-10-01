package migration

import (
	"context"

	address "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	builtin0 "github.com/filecoin-project/specs-actors/actors/builtin"
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	power0 "github.com/filecoin-project/specs-actors/actors/builtin/power"
	states0 "github.com/filecoin-project/specs-actors/actors/states"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/states"
)

// Config parameterizes a state tree migration
type Config struct {
}

func DefaultConfig() Config {
	return Config{}
}

type PowerUpdates struct {
	claims map[address.Address]power0.Claim
	crons  map[abi.ChainEpoch][]power0.CronEvent
}

type MigrationInfo struct {
	address      address.Address // actor's address
	balance      abi.TokenAmount // actor's balance
	priorEpoch   abi.ChainEpoch  // epoch of last state transition prior to migration
	powerUpdates *PowerUpdates   // used to update power and add cron tasks to power actor state
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

	// Iterate all actors in old state root
	// Set new state root actors as we go
	err = actorsIn.ForEach(func(addr address.Address, actorIn *states.Actor) error {
		transfer, err := migrateOneActor(ctx, store, addr, *actorIn, actorsOut, priorEpoch, powerUpdates)
		if err != nil {
			return err
		}
		// Accumulate funds transfered from burnt to miners.
		transferFromBurnt = big.Add(transferFromBurnt, transfer)
		return nil
	})
	if err != nil {
		return cid.Undef, err
	}

	// Migrate Power actor
	pm := migrations[builtin0.StoragePowerActorCodeID].StateMigration.(*powerMigrator)
	pm.actorsIn = actorsIn
	powerActorIn, found, err := actorsIn.GetActor(builtin0.StoragePowerActorAddr)
	if err != nil {
		return cid.Undef, err
	}
	if !found {
		return cid.Undef, xerrors.Errorf("could not find power actor in state")
	}
	powerActorHeadOut, transfer, err := pm.MigrateState(ctx, store, powerActorIn.Head, MigrationInfo{
		address:      builtin0.StoragePowerActorAddr,
		balance:      powerActorIn.Balance,
		priorEpoch:   priorEpoch,
		powerUpdates: powerUpdates,
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
		address:      builtin0.VerifiedRegistryActorAddr,
		balance:      verifRegActorIn.Balance,
		priorEpoch:   priorEpoch,
		powerUpdates: powerUpdates,
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

func migrateOneActor(ctx context.Context, store cbor.IpldStore, addr address.Address, actorIn states.Actor, actorsOut *states.Tree,
	priorEpoch abi.ChainEpoch, powerUpdates *PowerUpdates,
) (big.Int, error) {
	// This will be migrated at the end
	if _, found := deferredMigrations[actorIn.Code]; found {
		return big.Zero(), nil
	}
	migration := migrations[actorIn.Code]
	codeOut := migration.OutCodeCID
	headOut, transfer, err := migration.StateMigration.MigrateState(ctx, store, actorIn.Head, MigrationInfo{
		address:      addr,
		balance:      actorIn.Balance,
		priorEpoch:   priorEpoch,
		powerUpdates: powerUpdates,
	})
	if err != nil {
		return big.Zero(), xerrors.Errorf("state migration error on %s actor at addr %s: %w", builtin.ActorNameByCode(codeOut), addr, err)
	}

	// set up new state root with the migrated state
	actorOut := states.Actor{
		Code:       codeOut,
		Head:       headOut,
		CallSeqNum: actorIn.CallSeqNum,
		Balance:    big.Add(actorIn.Balance, transfer),
	}
	err = actorsOut.SetActor(addr, &actorOut)
	if err != nil {
		return big.Zero(), err
	}
	return transfer, nil
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
