package nv16

import (
	"context"

	builtin8 "github.com/filecoin-project/specs-actors/v8/actors/builtin"
	"github.com/filecoin-project/specs-actors/v8/actors/util/adt"
	"golang.org/x/xerrors"

	miner7 "github.com/filecoin-project/specs-actors/v7/actors/builtin/miner"
	miner8 "github.com/filecoin-project/specs-actors/v8/actors/builtin/miner"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
)

type minerMigrator struct{}

func (m minerMigrator) migrateState(ctx context.Context, store cbor.IpldStore, in actorMigrationInput) (*actorMigrationResult, error) {
	var inState miner7.State
	if err := store.Get(ctx, in.head, &inState); err != nil {
		return nil, err
	}

	sectorsOut, err := migrateSectors(ctx, store, inState.Sectors)
	if err != nil {
		return nil, err
	}

	outState := miner8.State{
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
		Deadlines:                  inState.Deadlines,
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
	return builtin8.StorageMinerActorCodeID
}

func migrateSectors(ctx context.Context, store cbor.IpldStore, inRoot cid.Cid) (cid.Cid, error) {
	ctxStore := adt.WrapStore(ctx, store)
	inArray, err := adt.AsArray(ctxStore, inRoot, miner7.SectorsAmtBitwidth)
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to read sectors array: %w", err)
	}

	outArray, err := adt.MakeEmptyArray(ctxStore, miner8.SectorsAmtBitwidth)
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to construct new sectors array: %w", err)
	}

	var sectorInfo miner7.SectorOnChainInfo
	if err = inArray.ForEach(&sectorInfo, func(k int64) error {
		return outArray.Set(uint64(k), migrateSectorInfo(sectorInfo))
	}); err != nil {
		return cid.Undef, err
	}

	return outArray.Root()
}

func migrateSectorInfo(sectorInfo miner7.SectorOnChainInfo) *miner8.SectorOnChainInfo {
	return &miner8.SectorOnChainInfo{
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
		SectorKeyCID:          sectorInfo.SectorKeyCID,
	}
}
