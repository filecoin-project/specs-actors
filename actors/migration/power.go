package migration

import (
	"context"

	address "github.com/filecoin-project/go-address"
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	power0 "github.com/filecoin-project/specs-actors/actors/builtin/power"
	states0 "github.com/filecoin-project/specs-actors/actors/states"
	adt0 "github.com/filecoin-project/specs-actors/actors/util/adt"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	power2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	adt2 "github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	smoothing2 "github.com/filecoin-project/specs-actors/v2/actors/util/smoothing"
)

type powerMigrator struct {
	actorsIn *states0.Tree
}

func (m *powerMigrator) MigrateState(ctx context.Context, storeIn, storeOut cbor.IpldStore, head cid.Cid) (cid.Cid, error) {
	var inState power0.State
	if err := storeIn.Get(ctx, head, &inState); err != nil {
		return cid.Undef, err
	}

	cronEventsRoot, err := m.migrateCronEvents(ctx, storeIn, storeOut, inState.CronEventQueue)
	if err != nil {
		return cid.Undef, xerrors.Errorf("cron events: %w", err)
	}

	claimsRoot, err := m.migrateClaims(ctx, storeIn, storeOut, inState.Claims)
	if err != nil {
		return cid.Undef, xerrors.Errorf("claims: %w", err)
	}

	outState := power2.State{
		TotalRawBytePower:         inState.TotalRawBytePower,
		TotalBytesCommitted:       inState.TotalBytesCommitted,
		TotalQualityAdjPower:      inState.TotalQualityAdjPower,
		TotalQABytesCommitted:     inState.TotalQABytesCommitted,
		TotalPledgeCollateral:     inState.TotalPledgeCollateral,
		ThisEpochRawBytePower:     inState.ThisEpochRawBytePower,
		ThisEpochQualityAdjPower:  inState.ThisEpochQualityAdjPower,
		ThisEpochPledgeCollateral: inState.ThisEpochPledgeCollateral,
		ThisEpochQAPowerSmoothed:  smoothing2.FilterEstimate(*inState.ThisEpochQAPowerSmoothed),
		MinerCount:                inState.MinerCount,
		MinerAboveMinPowerCount:   inState.MinerAboveMinPowerCount,
		CronEventQueue:            cronEventsRoot,
		FirstCronEpoch:            inState.FirstCronEpoch,
		Claims:                    claimsRoot,
		ProofValidationBatch:      nil, // Set nil at the end of every epoch in cron handler
	}

	return storeOut.Put(ctx, &outState)
}

func (m *powerMigrator) migrateCronEvents(ctx context.Context, storeIn, storeOut cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// The HAMT has changed, but the value (an AMT[CronEvent] root) is identical.
	// The AMT queues may contain miner0.CronEventWorkerKeyChange, but these will be ignored by the miner
	// actor so are safe to leave behind.
	var _ = power0.CronEvent(power2.CronEvent{})

	return migrateHAMTRaw(ctx, storeIn, storeOut, root)
}

func (m *powerMigrator) migrateClaims(ctx context.Context, storeIn, storeOut cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	inMap, err := adt0.AsMap(adt0.WrapStore(ctx, storeIn), root)
	if err != nil {
		return cid.Undef, err
	}
	outMap := adt2.MakeEmptyMap(adt2.WrapStore(ctx, storeOut))

	var inClaim power0.Claim
	if err = inMap.ForEach(&inClaim, func(key string) error {
		// look up seal proof type from miner actor
		a, err := address.NewFromBytes([]byte(key))
		if err != nil {
			return err
		}
		minerActor, found, err := m.actorsIn.GetActor(address.Address(a))
		if err != nil {
			return err
		}
		if !found {
			return xerrors.Errorf("claim exists for miner %s but miner not in state tree", a)
		}
		var minerState miner0.State
		if err := storeIn.Get(ctx, minerActor.Head, &minerState); err != nil {
			return err
		}
		info, err := minerState.GetInfo(adt0.WrapStore(ctx, storeIn))
		if err != nil {
			return err
		}

		outClaim := power2.Claim{
			SealProofType:   info.SealProofType,
			RawBytePower:    inClaim.RawBytePower,
			QualityAdjPower: inClaim.QualityAdjPower,
		}
		return outMap.Put(StringKey(key), &outClaim)
	}); err != nil {
		return cid.Undef, err
	}

	return outMap.Root()
}
