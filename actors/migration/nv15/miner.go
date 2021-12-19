package nv15

import (
	"context"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-amt-ipld/v3"
	"github.com/filecoin-project/go-state-types/abi"
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

	deadlinesOut, err := m.migrateDeadlines(ctx, ctxStore, in.cache, inState.Deadlines, sectorsOut)
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

		okIn, prevInRoot, err := cache.Read(MinerPrevSectorsInKey(minerAddr))
		if err != nil {
			return cid.Undef, xerrors.Errorf("failed to get previous inRoot from cache: %w", err)
		}

		okOut, prevOutRoot, err := cache.Read(MinerPrevSectorsOutKey(minerAddr))
		if err != nil {
			return cid.Undef, xerrors.Errorf("failed to get previous outRoot from cache: %w", err)
		}

		if okIn != okOut {
			return cid.Undef, xerrors.Errorf("cannot have only one of the inRoot and outRoot in the cache")
		}

		var outArray *adt.Array
		// first time we're doing this, do all the work
		if !okIn {
			outArray, err = adt.MakeEmptyArray(store, miner7.SectorsAmtBitwidth)
			if err != nil {
				return cid.Undef, xerrors.Errorf("failed to construct new sectors array: %w", err)
			}

			var sectorInfo miner6.SectorOnChainInfo
			if err = inArray.ForEach(&sectorInfo, func(k int64) error {
				return outArray.Set(uint64(k), migrateSectorInfo(sectorInfo))
			}); err != nil {
				return cid.Undef, err
			}
		} else {

			// we have previous work, but the AMT has changed -- diff them
			diffs, err := amt.Diff(ctx, store, store, prevInRoot, inRoot, amt.UseTreeBitWidth(miner7.SectorsAmtBitwidth))
			if err != nil {
				return cid.Undef, xerrors.Errorf("failed to diff old and new Sector AMTs: %w", err)
			}

			inSectors, err := miner6.LoadSectors(store, inRoot)
			if err != nil {
				return cid.Undef, xerrors.Errorf("failed to load inSectors: %w", err)
			}

			prevOutSectors, err := miner7.LoadSectors(store, prevOutRoot)
			if err != nil {
				return cid.Undef, xerrors.Errorf("failed to load prevOutSectors: %w", err)
			}

			for _, change := range diffs {
				switch change.Type {
				case amt.Remove:
					if err := prevOutSectors.Delete(change.Key); err != nil {
						return cid.Undef, xerrors.Errorf("failed to delete sector from prevOutSectors: %w", err)
					}
				case amt.Add:
					fallthrough
				case amt.Modify:
					sectorNo := abi.SectorNumber(change.Key)
					info, found, err := inSectors.Get(sectorNo)
					if err != nil {
						return cid.Undef, xerrors.Errorf("failed to get sector %d in inSectors: %w", sectorNo, err)
					}

					if !found {
						return cid.Undef, xerrors.Errorf("didn't find sector %d in inSectors", sectorNo)
					}

					if err := prevOutSectors.Set(change.Key, migrateSectorInfo(*info)); err != nil {
						return cid.Undef, xerrors.Errorf("failed to set migrated sector %d in prevOutSectors", sectorNo)
					}
				}
			}

			outArray = prevOutSectors.Array
		}

		outRoot, err := outArray.Root()
		if err != nil {
			return cid.Undef, xerrors.Errorf("error writing new sectors AMT: %w", err)
		}

		if err = cache.Write(MinerPrevSectorsInKey(minerAddr), inRoot); err != nil {
			return cid.Undef, xerrors.Errorf("failed to write previous sectors in key to cache: %w", err)
		}

		if err = cache.Write(MinerPrevSectorsOutKey(minerAddr), outRoot); err != nil {
			return cid.Undef, xerrors.Errorf("failed to write previous sectors out key to cache: %w", err)
		}

		return outRoot, nil
	})
}
func (m *minerMigrator) migrateDeadlines(ctx context.Context, store adt.Store, cache MigrationCache, deadlines cid.Cid, sectors cid.Cid) (cid.Cid, error) {
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
