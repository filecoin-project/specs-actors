package nv9

import (
	"context"

	miner2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	adt2 "github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	miner3 "github.com/filecoin-project/specs-actors/v3/actors/builtin/miner"

	"golang.org/x/xerrors"

	builtin3 "github.com/filecoin-project/specs-actors/v3/actors/builtin"
	adt3 "github.com/filecoin-project/specs-actors/v3/actors/util/adt"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
)

type minerMigrator struct{}

func (m minerMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, in StateMigrationInput) (*StateMigrationResult, error) {
	var inState miner2.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}

	preCommittedSectorsOut, err := migrateHAMTRaw(ctx, store, inState.PreCommittedSectors, adt3.DefaultHamtOptionsWithDefaultBitwidth)
	if err != nil {
		return nil, err
	}
	preCommittedSectorsExpiryOut, err := migrateAMTRaw(ctx, store, inState.PreCommittedSectorsExpiry, adt3.DefaultAmtOptions)
	if err != nil {
		return nil, err
	}
	sectorsOut, err := migrateAMTRaw(ctx, store, inState.Sectors, adt3.DefaultAmtOptions)
	if err != nil {
		return nil, err
	}

	deadlinesOut, err := m.migrateDeadlines(ctx, store, inState.Deadlines)
	if err != nil {
		return nil, err
	}

	outState := miner3.State{
		Info:                      inState.Info,
		PreCommitDeposits:         inState.PreCommitDeposits,
		LockedFunds:               inState.LockedFunds,
		VestingFunds:              inState.VestingFunds,
		FeeDebt:                   inState.FeeDebt,
		InitialPledge:             inState.InitialPledge,
		PreCommittedSectors:       preCommittedSectorsOut,
		PreCommittedSectorsExpiry: preCommittedSectorsExpiryOut,
		AllocatedSectors:          inState.AllocatedSectors,
		Sectors:                   sectorsOut,
		ProvingPeriodStart:        inState.ProvingPeriodStart,
		CurrentDeadline:           inState.CurrentDeadline,
		Deadlines:                 deadlinesOut,
		EarlyTerminations:         inState.EarlyTerminations,
	}
	newHead, err := store.Put(ctx, &outState)
	return &StateMigrationResult{
		NewCodeCID: builtin3.StorageMinerActorCodeID,
		NewHead:    newHead,
	}, err
}

func (m *minerMigrator) migrateDeadlines(ctx context.Context, store cbor.IpldStore, deadlines cid.Cid) (cid.Cid, error) {
	var inDeadlines miner2.Deadlines
	err := store.Get(ctx, deadlines, &inDeadlines)
	if err != nil {
		return cid.Undef, err
	}

	if miner2.WPoStPeriodDeadlines != miner3.WPoStPeriodDeadlines {
		return cid.Undef, xerrors.Errorf("unexpected WPoStPeriodDeadlines changed from %d to %d",
			miner2.WPoStPeriodDeadlines, miner3.WPoStPeriodDeadlines)
	}

	outDeadlines := miner3.Deadlines{Due: [miner3.WPoStPeriodDeadlines]cid.Cid{}}

	for i, c := range inDeadlines.Due {
		var inDeadline miner2.Deadline
		if err = store.Get(ctx, c, &inDeadline); err != nil {
			return cid.Undef, err
		}

		partitions, err := m.migratePartitions(ctx, store, inDeadline.Partitions)
		if err != nil {
			return cid.Undef, xerrors.Errorf("partitions: %w", err)
		}

		expirationEpochs, err := migrateAMTRaw(ctx, store, inDeadline.ExpirationsEpochs, adt3.DefaultAmtOptions)
		if err != nil {
			return cid.Undef, xerrors.Errorf("bitfield queue: %w", err)
		}

		outDeadline := miner3.Deadline{
			Partitions:        partitions,
			ExpirationsEpochs: expirationEpochs,
			PostSubmissions:   inDeadline.PostSubmissions,
			EarlyTerminations: inDeadline.EarlyTerminations,
			LiveSectors:       inDeadline.LiveSectors,
			TotalSectors:      inDeadline.TotalSectors,
			FaultyPower:       miner3.PowerPair(inDeadline.FaultyPower),
		}

		outDlCid, err := store.Put(ctx, &outDeadline)
		if err != nil {
			return cid.Undef, err
		}
		outDeadlines.Due[i] = outDlCid
	}

	return store.Put(ctx, &outDeadlines)
}

func (m *minerMigrator) migratePartitions(ctx context.Context, store cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// AMT[PartitionNumber]Partition
	inArray, err := adt2.AsArray(adt2.WrapStore(ctx, store), root)
	if err != nil {
		return cid.Undef, err
	}
	outArray, err := adt3.MakeEmptyArray(adt2.WrapStore(ctx, store))
	if err != nil {
		return cid.Undef, err
	}

	var inPartition miner2.Partition
	if err = inArray.ForEach(&inPartition, func(i int64) error {
		expirationEpochs, err := migrateAMTRaw(ctx, store, inPartition.ExpirationsEpochs, adt3.DefaultAmtOptions)
		if err != nil {
			return xerrors.Errorf("expiration queue: %w", err)
		}

		earlyTerminated, err := migrateAMTRaw(ctx, store, inPartition.EarlyTerminated, adt3.DefaultAmtOptions)
		if err != nil {
			return xerrors.Errorf("early termination queue: %w", err)
		}

		outPartition := miner3.Partition{
			Sectors:           inPartition.Sectors,
			Unproven:          inPartition.Unproven,
			Faults:            inPartition.Faults,
			Recoveries:        inPartition.Recoveries,
			Terminated:        inPartition.Terminated,
			ExpirationsEpochs: expirationEpochs,
			EarlyTerminated:   earlyTerminated,
			LivePower:         miner3.PowerPair(inPartition.LivePower),
			UnprovenPower:     miner3.PowerPair(inPartition.UnprovenPower),
			FaultyPower:       miner3.PowerPair(inPartition.FaultyPower),
			RecoveringPower:   miner3.PowerPair(inPartition.RecoveringPower),
		}

		return outArray.Set(uint64(i), &outPartition)
	}); err != nil {
		return cid.Undef, err
	}

	return outArray.Root()
}
