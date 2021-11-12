package nv14

import (
	"context"

	"github.com/filecoin-project/go-state-types/big"
	miner5 "github.com/filecoin-project/specs-actors/v5/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin"
	"github.com/filecoin-project/specs-actors/v6/actors/util/adt"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"
)

type minerMigrator struct{}

func (m minerMigrator) migratedCodeCID() cid.Cid {
	return builtin.StorageMinerActorCodeID
}

func (m minerMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	var inState miner5.State

	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}
	wrappedStore := adt.WrapStore(ctx, store)
	minerInfo, err := inState.GetInfo(wrappedStore)
	if err != nil {
		return nil, err
	}
	if IsTestPostProofType(minerInfo.WindowPoStProofType) {
		if inState.PreCommitDeposits.GreaterThan(big.Zero()) {
			return nil, xerrors.Errorf("test type miner has nonzero PreCommitDeposits at address %v.", in.address)
		}
		if inState.LockedFunds.GreaterThan(big.Zero()) {
			return nil, xerrors.Errorf("test type miner has nonzero LockedFunds at address %v.", in.address)
		}
		if inState.FeeDebt.GreaterThan(big.Zero()) {
			return nil, xerrors.Errorf("test type miner has nonzero FeeDebt at address %v.", in.address)
		}
		if inState.InitialPledge.GreaterThan(big.Zero()) {
			return nil, xerrors.Errorf("test type miner has nonzero InitialPledge at address %v.", in.address)
		}
		sectors, err := adt.AsArray(wrappedStore, inState.Sectors, builtin.DefaultHamtBitwidth)
		if err != nil {
			return nil, err
		}
		if sectors.Length() != 0 {
			return nil, xerrors.Errorf("test type miner has nonzero length of Sectors at address %v.", in.address)
		}
		precommittedSectors, err := adt.AsMap(wrappedStore, inState.PreCommittedSectors, builtin.DefaultHamtBitwidth)
		if err != nil {
			return nil, err
		}
		n, err := precommittedSectors.NumKeys()
		if err != nil {
			return nil, err
		}
		if n != 0 {
			return nil, xerrors.Errorf("test type miner has nonzero length of PrecommittedSectors at address %v.", in.address)
		}
		minPower, err := builtin.ConsensusMinerMinPower(minerInfo.WindowPoStProofType)
		if err != nil {
			return nil, err
		}
		deletedAboveMinPower := minPower.LessThanEqual(big.Zero())

		return &actorMigrationResult{
			newCodeCID:                            m.migratedCodeCID(),
			newHead:                               in.head,
			minerTypeMigrationShouldDelete:        true,
			minerDeletedWasAboveMinPower:          deletedAboveMinPower,
			minerTypeMigrationBalanceTransferInfo: balanceTransferInfo{address: minerInfo.Owner, value: in.balance},
		}, nil
	}

	return &actorMigrationResult{
		newCodeCID:                     m.migratedCodeCID(),
		newHead:                        in.head,
		minerTypeMigrationShouldDelete: false,
	}, nil
}
