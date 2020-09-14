package migration

import (
	"context"

	address "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	builtin0 "github.com/filecoin-project/specs-actors/actors/builtin"
	states0 "github.com/filecoin-project/specs-actors/actors/states"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/states"
)

type StateMigration interface {
	// Loads an actor's state from an input store and writes new state to an output store.
	// Returns the new state head CID.
	MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid) (cid.Cid, error)
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
	builtin0.StorageMinerActorCodeID: ActorMigration{
		OutCodeCID:     builtin.StorageMinerActorCodeID,
		StateMigration: &minerMigrator{},
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
}

// A phoenix tracks the unburning of funds
type phoenix struct {
	burntBalance abi.TokenAmount
}

func (p phoenix) load(ctx context.Context, actorsIn *states0.Tree) error {
	burntFundsActor, found, err := actorsIn.GetActor(builtin0.BurntFundsActorAddr)
	if err != nil {
		return err
	}
	if !found {
		return xerrors.Errorf("burnt funds actor not in tree")
	}
	p.burntBalance = burntFundsActor.Balance
	return nil
}

func (p phoenix) transfer(amt abi.TokenAmount) error {
	p.burntBalance = big.Sub(p.burntBalance, amt)
	if p.burntBalance.LessThan(big.Zero()) {
		return xerrors.Errorf("migration programmer error burnt funds balance falls to %v, below zero", p.burntBalance)
	}
	return nil
}

func (p phoenix) flush(ctx context.Context, actorsIn *states0.Tree, actorsOut *states.Tree) error {
	burntFundsActor, found, err := actorsIn.GetActor(builtin0.BurntFundsActorAddr)
	if err != nil {
		return err
	}
	if !found {
		return xerrors.Errorf("burnt funds actor not in tree")
	}
	burntFundsActor.Balance = p.burntBalance
	return actorsOut.SetActor(builtin.BurntFundsActorAddr, burntFundsActor)
}

// Migrates the filecoin state tree starting from the global state tree and upgrading all actor state.
func MigrateStateTree(ctx context.Context, store cbor.IpldStore, stateRootIn cid.Cid) (cid.Cid, error) {
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

	// Extra setup
	// miner
	var p phoenix
	if err := p.load(ctx, actorsIn); err != nil {
		return cid.Undef, err
	}
	// power
	pm := migrations[builtin0.StoragePowerActorCodeID].StateMigration.(*powerMigrator)
	pm.actorsIn = actorsIn

	// Iterate all actors in old state root
	// Set new state root actors as we go
	err = actorsIn.ForEach(func(addr address.Address, actorIn *states.Actor) error {
		migration := migrations[actorIn.Code]

		// This will be migrated at the end
		if actorIn.Code == builtin0.VerifiedRegistryActorCodeID {
			return nil
		}
		if actorIn.Code == builtin0.StorageMinerActorCodeID {
			// setup migration fields
			mm := migration.StateMigration.(*minerMigrator)
			mm.MinerBalance = actorIn.Balance
			mm.Transfer = big.Zero()
		}
		headOut, err := migration.StateMigration.MigrateState(ctx, store, actorIn.Head)
		if err != nil {
			return xerrors.Errorf("state migration error on %s actor at addr %s: %w", builtin.ActorNameByCode(migration.OutCodeCID), addr, err)
		}

		// set up new state root with the migrated state
		actorOut := states.Actor{
			Code:       migration.OutCodeCID,
			Head:       headOut,
			CallSeqNum: actorIn.CallSeqNum,
			Balance:    actorIn.Balance,
		}

		if actorIn.Code == builtin0.StorageMinerActorCodeID {
			// propagate transfer to miner actor
			mm := migration.StateMigration.(*minerMigrator)
			if err := p.transfer(mm.Transfer); err != nil {
				return err
			}

			actorOut.Balance = big.Add(actorOut.Balance, mm.Transfer)
		}
		return actorsOut.SetActor(addr, &actorOut)
	})
	if err != nil {
		return cid.Undef, err
	}

	// // Migrate verified registry
	vm := migrations[builtin0.VerifiedRegistryActorCodeID].StateMigration.(*verifregMigrator)
	vm.actorsOut = actorsOut
	verifRegActorIn, found, err := actorsIn.GetActor(builtin0.VerifiedRegistryActorAddr)
	if err != nil {
		return cid.Undef, err
	}
	if !found {
		return cid.Undef, xerrors.Errorf("could not find verifreg actor in state")
	}
	verifRegHeadOut, err := vm.MigrateState(ctx, store, verifRegActorIn.Head)
	if err != nil {
		return cid.Undef, err
	}
	verifRegActorOut := states.Actor{
		Code:       builtin.VerifiedRegistryActorCodeID,
		Head:       verifRegHeadOut,
		CallSeqNum: verifRegActorIn.CallSeqNum,
		Balance:    verifRegActorIn.Balance,
	}
	if err := actorsOut.SetActor(builtin.VerifiedRegistryActorAddr, &verifRegActorOut); err != nil {
		return cid.Undef, err
	}

	// Track deductions to burntFunds actor's balance
	if err := p.flush(ctx, actorsIn, actorsOut); err != nil {
		return cid.Undef, err
	}

	return actorsOut.Flush()
}
