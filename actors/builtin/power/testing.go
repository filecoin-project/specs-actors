package power

import (
	"bytes"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type CronEventsByAddress map[address.Address][]miner.CronEventPayload
type ClaimsByAddress map[address.Address]Claim

type StateSummary struct {
	Crons  CronEventsByAddress
	Claims ClaimsByAddress
}

/*
TotalRawBytePower abi.StoragePower
// TotalBytesCommitted includes claims from miners below min power threshold
TotalBytesCommitted  abi.StoragePower
TotalQualityAdjPower abi.StoragePower
// TotalQABytesCommitted includes claims from miners below min power threshold
TotalQABytesCommitted abi.StoragePower
TotalPledgeCollateral abi.TokenAmount

// These fields are set once per epoch in the previous cron tick and used
// for consistent values across a single epoch's state transition.
ThisEpochRawBytePower     abi.StoragePower
ThisEpochQualityAdjPower  abi.StoragePower
ThisEpochPledgeCollateral abi.TokenAmount
ThisEpochQAPowerSmoothed  smoothing.FilterEstimate

MinerCount int64
// Number of miners having proven the minimum consensus power.
MinerAboveMinPowerCount int64

// A queue of events to be triggered by cron, indexed by epoch.
CronEventQueue cid.Cid // Multimap, (HAMT[ChainEpoch]AMT[CronEvent])

// First epoch in which a cron task may be stored.
// Cron will iterate every epoch between this and the current epoch inclusively to find tasks to execute.
FirstCronEpoch abi.ChainEpoch

// Claimed power for each miner.
Claims cid.Cid // Map, HAMT[address]Claim

ProofValidationBatch *cid.Cid // Multimap, (HAMT[Address]AMT[SealVerifyInfo])
*/

// Checks internal invariants of power state.
func CheckStateInvariants(st *State, store adt.Store) (*StateSummary, *builtin.MessageAccumulator, error) {
	acc := &builtin.MessageAccumulator{}

	// basic invariants around recorded power
	acc.Require(st.TotalRawBytePower.GreaterThanEqual(big.Zero()), "total raw power is negative %v", st.TotalRawBytePower)
	acc.Require(st.TotalQualityAdjPower.GreaterThanEqual(big.Zero()), "total qa power is negative %v", st.TotalQualityAdjPower)
	acc.Require(st.TotalBytesCommitted.GreaterThanEqual(big.Zero()), "total raw power committed is negative %v", st.TotalBytesCommitted)
	acc.Require(st.TotalQABytesCommitted.GreaterThanEqual(big.Zero()), "total qa power committed is negative %v", st.TotalQABytesCommitted)
	acc.Require(st.TotalRawBytePower.GreaterThanEqual(big.Zero()), "total raw power is negative %v", st.TotalRawBytePower)
	acc.Require(st.TotalRawBytePower.LessThanEqual(st.TotalQualityAdjPower),
		"total raw power %v is greater than total quality adjusted power %v", st.TotalRawBytePower, st.TotalQualityAdjPower)
	acc.Require(st.TotalBytesCommitted.LessThanEqual(st.TotalQABytesCommitted),
		"committed raw power %v is greater than committed quality adjusted power %v", st.TotalBytesCommitted, st.TotalQABytesCommitted)
	acc.Require(st.TotalRawBytePower.LessThanEqual(st.TotalBytesCommitted),
		"total raw power %v is greater than raw power committed %v", st.TotalRawBytePower, st.TotalBytesCommitted)
	acc.Require(st.TotalQualityAdjPower.LessThanEqual(st.TotalQABytesCommitted),
		"total qua power %v is greater than qa power committed %v", st.TotalQualityAdjPower, st.TotalQABytesCommitted)

	crons, msgs, err := CheckCronInvariants(st, store)
	acc.AddAll(msgs)
	if err != nil {
		return nil, acc, err
	}

	claims, msgs, err := CheckClaimInvariants(st, store)

	return &StateSummary{
		Crons:  crons,
		Claims: claims,
	}, acc, nil
}

func CheckCronInvariants(st *State, store adt.Store) (CronEventsByAddress, *builtin.MessageAccumulator, error) {
	acc := &builtin.MessageAccumulator{}

	queue, err := adt.AsMultimap(store, st.CronEventQueue)
	if err != nil {
		return nil, acc, err
	}

	byAddress := make(CronEventsByAddress)
	err = queue.ForAll(func(ekey string, arr *adt.Array) error {
		epoch, err := abi.ParseIntKey(ekey)
		acc.Require(err == nil, "non-int key in cron array")
		if err != nil {
			return nil // error noted above
		}

		acc.Require(abi.ChainEpoch(epoch) >= st.FirstCronEpoch, "cron event at epoch %d before FirstCronEpoch %d",
			epoch, st.FirstCronEpoch)

		var event CronEvent
		return arr.ForEach(&event, func(i int64) error {

			var payload miner.CronEventPayload
			err := payload.UnmarshalCBOR(bytes.NewReader(event.CallbackPayload))
			acc.Require(err == nil, "error unmarshalling miner cron event payload (might not be miner.CronEventPayload): %v", err)
			if err != nil {
				return nil // error noted above
			}

			existingPayloads, found := byAddress[event.MinerAddr]
			if found && payload.EventType == miner.CronEventProvingDeadline {
				for _, p := range existingPayloads {
					acc.Require(p.EventType != miner.CronEventProvingDeadline, "found duplicate miner proving cron for miner %v", event.MinerAddr)
				}
			}
			byAddress[event.MinerAddr] = append(existingPayloads, payload)

			return nil
		})
	})
	acc.Require(err != nil, "error attempting to read through power actor cron tasks: %v", err)

	return byAddress, acc, nil
}

func CheckClaimInvariants(st *State, store adt.Store) (ClaimsByAddress, *builtin.MessageAccumulator, error) {
	acc := &builtin.MessageAccumulator{}

	claims, err := adt.AsMap(store, st.Claims)
	if err != nil {
		return nil, acc, err
	}

	committedRawPower := abi.NewStoragePower(0)
	committedQAPower := abi.NewStoragePower(0)
	rawPower := abi.NewStoragePower(0)
	qaPower := abi.NewStoragePower(0)
	claimCount := int64(0)
	claimsWithSufficientPowerCount := int64(0)
	byAddress := make(ClaimsByAddress)
	var claim Claim
	err = claims.ForEach(&claim, func(key string) error {
		addr, err := address.NewFromBytes([]byte(key))
		if err != nil {
			return err
		}
		byAddress[addr] = claim
		claimCount += 1
		committedRawPower = big.Add(committedRawPower, claim.RawBytePower)
		committedQAPower = big.Add(committedQAPower, claim.QualityAdjPower)

		minPower, err := builtin.ConsensusMinerMinPower(claim.SealProofType)
		acc.Require(err != nil, "could not get consensus miner min power for miner %v: %w", addr, err)
		if err != nil {
			return nil // noted above
		}

		if claim.RawBytePower.GreaterThanEqual(minPower) {
			claimsWithSufficientPowerCount += 1
			rawPower = big.Add(rawPower, claim.RawBytePower)
			qaPower = big.Add(qaPower, claim.QualityAdjPower)
		}
		return nil
	})
	if err != nil {
		return nil, acc, err
	}

	acc.Require(committedRawPower.Equals(st.TotalBytesCommitted),
		"sum of raw power in claims %v does not match recorded bytes committed %v",
		committedRawPower, st.TotalBytesCommitted)
	acc.Require(committedQAPower.Equals(st.TotalQABytesCommitted),
		"sum of qa power in claims %v does not match recorded qa power committed %v",
		committedQAPower, st.TotalQABytesCommitted)

	acc.Require(claimsWithSufficientPowerCount == st.MinerAboveMinPowerCount,
		"claims with sufficient power %d does not match MinerAboveMinPowerCount %d",
		claimsWithSufficientPowerCount, st.MinerAboveMinPowerCount)

	if claimsWithSufficientPowerCount >= ConsensusMinerMinMiners {
		acc.Require(st.TotalRawBytePower.Equals(rawPower),
			"recorded raw power %v does not match raw power in claims %v", st.TotalRawBytePower, rawPower)
		acc.Require(st.TotalQualityAdjPower.Equals(qaPower),
			"recorded qa power %v does not match qa power in claims %v", st.TotalQualityAdjPower, qaPower)
	} else {
		acc.Require(st.TotalRawBytePower.Equals(st.TotalBytesCommitted),
			"below consensus min recorded raw power %v does not match committed bytes %v",
			st.TotalRawBytePower, st.TotalBytesCommitted)
		acc.Require(st.TotalQualityAdjPower.Equals(st.TotalQABytesCommitted),
			"below consensus min recorded qa power %v does not match qa committed bytes %v",
			st.TotalQualityAdjPower, st.TotalQABytesCommitted)
	}

	return byAddress, acc, nil
}
