package nv15

import (
	"context"

	"github.com/filecoin-project/go-address"
	miner6 "github.com/filecoin-project/specs-actors/v6/actors/builtin/miner"
	builtin7 "github.com/filecoin-project/specs-actors/v7/actors/builtin"
	miner7 "github.com/filecoin-project/specs-actors/v7/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v7/actors/util/adt"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"
)

type minerMigrator struct{}

func (m minerMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	var inState miner6.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, xerrors.Errorf("getting inState: %w", err)
	}

	outState := fromv6State(inState)
	ctxStore := adt.WrapStore(ctx, store)

	sectorsOut, err := migrateSectors(ctx, ctxStore, in.cache, in.address, inState.Sectors)
	if err != nil {
		return nil, xerrors.Errorf("failed to migrate sectors: %w", err)
	}

	outState.Sectors = sectorsOut

	deadlinesOut, err := migrateDeadlines(ctx, ctxStore, in.cache, inState.Deadlines, sectorsOut)
	if err != nil {
		return nil, xerrors.Errorf("failed to migrate deadlines: %w", err)
	}

	outState.Deadlines = deadlinesOut

	newHead, err := store.Put(ctx, &outState)
	if err != nil {
		return nil, xerrors.Errorf("failed to flush outState: %w", err)
	}

	return &actorMigrationResult{
		newCodeCID: m.migratedCodeCID(),
		newHead:    newHead,
	}, nil
}

func (m minerMigrator) migratedCodeCID() cid.Cid {
	return builtin7.StorageMinerActorCodeID
}

func migrateSectors(ctx context.Context, store adt.Store, cache MigrationCache, minerAddr address.Address, inRoot cid.Cid) (cid.Cid, error) {
	return cache.Load(SectorsAmtKey(inRoot), func() (cid.Cid, error) {
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

		outRoot, err := outArray.Root()
		if err != nil {
			return cid.Undef, xerrors.Errorf("error writing new sectors AMT: %w", err)
		}

		return outRoot, nil
	})
}
func migrateDeadlines(ctx context.Context, store adt.Store, cache MigrationCache, deadlines cid.Cid, sectors cid.Cid) (cid.Cid, error) {
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

	for i, c := range inDeadlines.Due {
		outDlCid, err := cache.Load(DeadlineKey(c), func() (cid.Cid, error) {
			var inDeadline miner6.Deadline
			if err = store.Get(ctx, c, &inDeadline); err != nil {
				return cid.Undef, err
			}

			outDeadline := miner7.Deadline{
				Partitions:                        inDeadline.Partitions,
				ExpirationsEpochs:                 inDeadline.ExpirationsEpochs,
				PartitionsPoSted:                  inDeadline.PartitionsPoSted,
				EarlyTerminations:                 inDeadline.EarlyTerminations,
				LiveSectors:                       inDeadline.LiveSectors,
				TotalSectors:                      inDeadline.TotalSectors,
				FaultyPower:                       miner7.PowerPair(inDeadline.FaultyPower),
				OptimisticPoStSubmissions:         inDeadline.OptimisticPoStSubmissions,
				SectorsSnapshot:                   sectors,
				PartitionsSnapshot:                inDeadline.PartitionsSnapshot,
				OptimisticPoStSubmissionsSnapshot: inDeadline.OptimisticPoStSubmissionsSnapshot,
			}

			return store.Put(ctx, &outDeadline)
		})

		if err != nil {
			return cid.Undef, err
		}

		outDeadlines.Due[i] = outDlCid
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

// copies over all fields except Sectors and Deadlines
func fromv6State(inState miner6.State) miner7.State {
	return miner7.State{
		Info:                       inState.Info,
		PreCommitDeposits:          inState.PreCommitDeposits,
		LockedFunds:                inState.LockedFunds,
		VestingFunds:               inState.VestingFunds,
		FeeDebt:                    inState.FeeDebt,
		InitialPledge:              inState.InitialPledge,
		PreCommittedSectors:        inState.PreCommittedSectors,
		PreCommittedSectorsCleanUp: inState.PreCommittedSectorsCleanUp,
		AllocatedSectors:           inState.AllocatedSectors,
		ProvingPeriodStart:         inState.ProvingPeriodStart,
		CurrentDeadline:            inState.CurrentDeadline,
		EarlyTerminations:          inState.EarlyTerminations,
		DeadlineCronActive:         inState.DeadlineCronActive,
	}
}
