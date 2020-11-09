package nv7

import (
	"context"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	power "github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v2/actors/migration/nv4"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type claimsSummary struct {
	committedRawBytes              abi.StoragePower
	committedQABytes               abi.StoragePower
	rawPower                       abi.StoragePower
	qaPower                        abi.StoragePower
	claimsWithSufficientPowerCount int64
}

type PowerMigrator struct{}

func (m PowerMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, info MigrationInfo) (*StateMigrationResult, error) {
	var inState power.State
	if err := store.Get(ctx, head, &inState); err != nil {
		return nil, err
	}

	outClaimsRoot, claimsSummary, err := m.MigrateAndComputeClaimsStats(ctx, store, inState.Claims)
	if err != nil {
		return nil, err
	}

	outState := power.State{
		TotalRawBytePower:         claimsSummary.rawPower,
		TotalBytesCommitted:       claimsSummary.committedRawBytes,
		TotalQualityAdjPower:      claimsSummary.qaPower,
		TotalQABytesCommitted:     claimsSummary.committedQABytes,
		TotalPledgeCollateral:     inState.TotalPledgeCollateral,
		ThisEpochRawBytePower:     inState.ThisEpochRawBytePower,
		ThisEpochQualityAdjPower:  inState.ThisEpochQualityAdjPower,
		ThisEpochPledgeCollateral: inState.ThisEpochPledgeCollateral,
		ThisEpochQAPowerSmoothed:  inState.ThisEpochQAPowerSmoothed,
		MinerCount:                inState.MinerCount,
		MinerAboveMinPowerCount:   claimsSummary.claimsWithSufficientPowerCount,
		CronEventQueue:            inState.CronEventQueue,
		FirstCronEpoch:            inState.FirstCronEpoch,
		Claims:                    outClaimsRoot,
		ProofValidationBatch:      inState.ProofValidationBatch,
	}

	newHead, err := store.Put(ctx, &outState)
	return &StateMigrationResult{
		NewHead:  newHead,
		Transfer: big.Zero(),
	}, err
}

func (a PowerMigrator) MigrateAndComputeClaimsStats(ctx context.Context, store cbor.IpldStore, claimsRoot cid.Cid) (cid.Cid, *claimsSummary, error) {
	claims, err := adt.AsMap(adt.WrapStore(ctx, store), claimsRoot)
	if err != nil {
		return cid.Undef, nil, err
	}

	outClaims := adt.MakeEmptyMap(adt.WrapStore(ctx, store))

	summary := &claimsSummary{
		committedRawBytes:              abi.NewStoragePower(0),
		committedQABytes:               abi.NewStoragePower(0),
		rawPower:                       abi.NewStoragePower(0),
		qaPower:                        abi.NewStoragePower(0),
		claimsWithSufficientPowerCount: 0,
	}
	var claim power.Claim
	err = claims.ForEach(&claim, func(key string) error {
		summary.committedRawBytes = big.Add(summary.committedRawBytes, claim.RawBytePower)
		summary.committedQABytes = big.Add(summary.committedQABytes, claim.QualityAdjPower)

		minPower, err := builtin.ConsensusMinerMinPower(claim.SealProofType)
		if err != nil {
			return err
		}

		if claim.RawBytePower.GreaterThanEqual(minPower) {
			summary.claimsWithSufficientPowerCount += 1
			summary.rawPower = big.Add(summary.rawPower, claim.RawBytePower)
			summary.qaPower = big.Add(summary.qaPower, claim.QualityAdjPower)
		}
		sealProofType := claim.SealProofType
		if sealProofType == abi.RegisteredSealProof_StackedDrg32GiBV1 {
			sealProofType = abi.RegisteredSealProof_StackedDrg32GiBV1_1
		} else if sealProofType == abi.RegisteredSealProof_StackedDrg64GiBV1 {
			sealProofType = abi.RegisteredSealProof_StackedDrg32GiBV1_1
		}
		outClaim := power.Claim{
			SealProofType:   sealProofType,
			RawBytePower:    claim.RawBytePower,
			QualityAdjPower: claim.QualityAdjPower,
		}
		return outClaims.Put(nv4.StringKey(key), &outClaim)
	})
	if err != nil {
		return cid.Undef, nil, err
	}
	outClaimsRoot, err := outClaims.Root()
	return outClaimsRoot, summary, err
}
