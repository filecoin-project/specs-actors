package nv15

import (
	"context"

	builtin7 "github.com/filecoin-project/specs-actors/v7/actors/builtin"
	"github.com/filecoin-project/specs-actors/v7/actors/util/adt"
	"golang.org/x/xerrors"

	miner6 "github.com/filecoin-project/specs-actors/v6/actors/builtin/miner"
	miner7 "github.com/filecoin-project/specs-actors/v7/actors/builtin/miner"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
)

type minerMigrator struct{}

func (m minerMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	var inState miner6.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}

	ctxStore := adt.WrapStore(ctx, store)
	sectorsOut, err := migrateSectors(ctx, ctxStore, inState.Sectors)
	if err != nil {
		return nil, xerrors.Errorf("failed to migrate sectors: %w", err)
	}

	deadlinesOut, err := m.migrateDeadlines(ctx, ctxStore, inState.Deadlines)
	if err != nil {
		return nil, xerrors.Errorf("failed to migrate deadlines: %w", err)
	}

	outState := miner7.State{
		Info:                       inState.Info,
		PreCommitDeposits:          inState.PreCommitDeposits,
		LockedFunds:                inState.LockedFunds,
		VestingFunds:               inState.VestingFunds,
		FeeDebt:                    inState.FeeDebt,
		InitialPledge:              inState.InitialPledge,
		PreCommittedSectors:        inState.PreCommittedSectors,
		PreCommittedSectorsCleanUp: inState.PreCommittedSectorsCleanUp,
		AllocatedSectors:           inState.AllocatedSectors,
		Sectors:                    sectorsOut,
		ProvingPeriodStart:         inState.ProvingPeriodStart,
		CurrentDeadline:            inState.CurrentDeadline,
		Deadlines:                  deadlinesOut,
		EarlyTerminations:          inState.EarlyTerminations,
		DeadlineCronActive:         inState.DeadlineCronActive,
	}

	newHead, err := store.Put(ctx, &outState)
	return &actorMigrationResult{
		newCodeCID: m.migratedCodeCID(),
		newHead:    newHead,
	}, err
}

func (m minerMigrator) migratedCodeCID() cid.Cid {
	return builtin7.StorageMinerActorCodeID
}

func migrateSectors(ctx context.Context, store adt.Store, inRoot cid.Cid) (cid.Cid, error) {
	inArray, err := adt.AsArray(store, inRoot, miner6.SectorsAmtBitwidth)
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to read sectors array: %w", err)
	}

	outArray, err := adt.MakeEmptyArray(store, miner7.SectorsAmtBitwidth)
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to construct new sectors array: %w", err)
	}

	var sectorInfo miner6.SectorOnChainInfo
	if err = inArray.ForEach(&sectorInfo, func(k int64) error {
		return outArray.Set(uint64(k), migrateSectorInfo(sectorInfo))
	}); err != nil {
		return cid.Undef, err
	}

	return outArray.Root()
}
func (m *minerMigrator) migrateDeadlines(ctx context.Context, store adt.Store, deadlines cid.Cid) (cid.Cid, error) {
	var inDeadlines miner6.Deadlines
	err := store.Get(store.Context(), deadlines, &inDeadlines)
	if err != nil {
		return cid.Undef, err
	}

	if miner6.WPoStPeriodDeadlines != miner7.WPoStPeriodDeadlines {
		return cid.Undef, xerrors.Errorf("unexpected WPoStPeriodDeadlines changed from %d to %d",
			miner6.WPoStPeriodDeadlines, miner7.WPoStPeriodDeadlines)
	}

	outDeadlines := miner7.Deadlines{Due: [miner7.WPoStPeriodDeadlines]cid.Cid{}}

	// Start from an empty template to zero-initialize new fields.
	deadlineTemplate, err := miner7.ConstructDeadline(adt.WrapStore(ctx, store))
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to construct new deadline template: %w", err)
	}

	err = inDeadlines.ForEach(store, func(dlIdx uint64, inDeadline *miner6.Deadline) error {
		outDeadline := *deadlineTemplate
		outDeadline.Partitions = inDeadline.Partitions
		outDeadline.ExpirationsEpochs = inDeadline.ExpirationsEpochs
		outDeadline.PartitionsPoSted = inDeadline.PartitionsPoSted
		outDeadline.EarlyTerminations = inDeadline.EarlyTerminations
		outDeadline.LiveSectors = inDeadline.LiveSectors
		outDeadline.TotalSectors = inDeadline.TotalSectors
		outDeadline.FaultyPower = inDeadline.FaultyPower
		outDlCid, err := store.Put(ctx, &outDeadline)
		if err != nil {
			return xerrors.Errorf("failed to put new deadline: %w", err)
		}

		outDeadlines.Due[dlIdx] = outDlCid

		return nil
	})

	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to construct new deadlines")
	}

	return store.Put(ctx, &outDeadlines)
}

func migrateSectorInfo(sectorInfo miner6.SectorOnChainInfo) *miner7.SectorOnChainInfo {
	return &miner7.SectorOnChainInfo{
		SectorNumber:          sectorInfo.SectorNumber,
		SealProof:             sectorInfo.SealProof,
		SealedCID:             sectorInfo.SealedCID,
		DealIDs:               sectorInfo.DealIDs,
		Activation:            sectorInfo.Activation,
		Expiration:            sectorInfo.Expiration,
		DealWeight:            sectorInfo.DealWeight,
		VerifiedDealWeight:    sectorInfo.VerifiedDealWeight,
		InitialPledge:         sectorInfo.InitialPledge,
		ExpectedDayReward:     sectorInfo.ExpectedDayReward,
		ExpectedStoragePledge: sectorInfo.ExpectedStoragePledge,
		ReplacedSectorAge:     sectorInfo.ReplacedSectorAge,
		ReplacedDayReward:     sectorInfo.ReplacedDayReward,
		SectorKeyCID:          nil,
	}
}
