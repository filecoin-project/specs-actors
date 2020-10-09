package power

import (
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type MinerCronEvent struct {
	Epoch   abi.ChainEpoch
	Payload []byte
}

type CronEventsByAddress map[address.Address][]MinerCronEvent
type ClaimsByAddress map[address.Address]Claim
type ProofsByAddress map[address.Address][]proof.SealVerifyInfo

type StateSummary struct {
	Crons  CronEventsByAddress
	Claims ClaimsByAddress
	Proofs ProofsByAddress
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

	acc.Require(st.TotalRawBytePower.LessThanEqual(st.TotalQualityAdjPower),
		"total raw power %v is greater than total quality adjusted power %v", st.TotalRawBytePower, st.TotalQualityAdjPower)
	acc.Require(st.TotalBytesCommitted.LessThanEqual(st.TotalQABytesCommitted),
		"committed raw power %v is greater than committed quality adjusted power %v", st.TotalBytesCommitted, st.TotalQABytesCommitted)
	acc.Require(st.TotalRawBytePower.LessThanEqual(st.TotalBytesCommitted),
		"total raw power %v is greater than raw power committed %v", st.TotalRawBytePower, st.TotalBytesCommitted)
	acc.Require(st.TotalQualityAdjPower.LessThanEqual(st.TotalQABytesCommitted),
		"total qua power %v is greater than qa power committed %v", st.TotalQualityAdjPower, st.TotalQABytesCommitted)

	crons, err := CheckCronInvariants(st, store, acc)
	if err != nil {
		return nil, acc, err
	}

	claims, err := CheckClaimInvariants(st, store, acc)
	if err != nil {
		return nil, acc, err
	}

	proofs, err := CheckProofValidationInvariants(st, store, claims, acc)
	if err != nil {
		return nil, acc, err
	}

	return &StateSummary{
		Crons:  crons,
		Claims: claims,
		Proofs: proofs,
	}, acc, nil
}

func CheckCronInvariants(st *State, store adt.Store, acc *builtin.MessageAccumulator) (CronEventsByAddress, error) {
	queue, err := adt.AsMultimap(store, st.CronEventQueue)
	if err != nil {
		return nil, err
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
			byAddress[event.MinerAddr] = append(byAddress[event.MinerAddr], MinerCronEvent{
				Epoch:   abi.ChainEpoch(epoch),
				Payload: event.CallbackPayload,
			})

			return nil
		})
	})
	acc.Require(err != nil, "error attempting to read through power actor cron tasks: %v", err)

	return byAddress, nil
}

func CheckClaimInvariants(st *State, store adt.Store, acc *builtin.MessageAccumulator) (ClaimsByAddress, error) {
	claims, err := adt.AsMap(store, st.Claims)
	if err != nil {
		return nil, err
	}

	committedRawPower := abi.NewStoragePower(0)
	committedQAPower := abi.NewStoragePower(0)
	rawPower := abi.NewStoragePower(0)
	qaPower := abi.NewStoragePower(0)
	claimsWithSufficientPowerCount := int64(0)
	byAddress := make(ClaimsByAddress)
	var claim Claim
	err = claims.ForEach(&claim, func(key string) error {
		addr, err := address.NewFromBytes([]byte(key))
		if err != nil {
			return err
		}
		byAddress[addr] = claim
		committedRawPower = big.Add(committedRawPower, claim.RawBytePower)
		committedQAPower = big.Add(committedQAPower, claim.QualityAdjPower)

		minPower, err := builtin.ConsensusMinerMinPower(claim.SealProofType)
		acc.Require(err != nil, "could not get consensus miner min power for miner %v: %v", addr, err)
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
		return nil, err
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

	return byAddress, nil
}

func CheckProofValidationInvariants(st *State, store adt.Store, claims ClaimsByAddress, acc *builtin.MessageAccumulator) (ProofsByAddress, error) {
	if st.ProofValidationBatch == nil {
		return nil, nil
	}

	queue, err := adt.AsMultimap(store, *st.ProofValidationBatch)
	if err != nil {
		return nil, err
	}

	proofs := make(ProofsByAddress)
	err = queue.ForAll(func(key string, arr *adt.Array) error {
		addr, err := address.NewFromBytes([]byte(key))
		if err != nil {
			return err
		}

		claim, found := claims[addr]
		acc.Require(found, "miner %v has proofs awaiting validation but no claim", addr)
		if !found {
			return nil
		}

		var info proof.SealVerifyInfo
		err = arr.ForEach(&info, func(i int64) error {
			acc.Require(claim.SealProofType == info.SealProof, "miner submitted proof with proof type %d different from claim %d",
				info.SealProof, claim.SealProofType)
			proofs[addr] = append(proofs[addr], info)
			return nil
		})
		if err != nil {
			return err
		}
		acc.Require(len(proofs[addr]) <= MaxMinerProveCommitsPerEpoch,
			"miner %v has submitted too many proofs (%d) for batch verification", addr, len(proofs[addr]))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return proofs, nil
}
