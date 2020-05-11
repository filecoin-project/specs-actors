package miner

import (
	"bytes"
	"encoding/binary"
	"fmt"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
	cbg "github.com/whyrusleeping/cbor-gen"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	market "github.com/filecoin-project/specs-actors/actors/builtin/market"
	power "github.com/filecoin-project/specs-actors/actors/builtin/power"
	crypto "github.com/filecoin-project/specs-actors/actors/crypto"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	. "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type Runtime = vmr.Runtime

type CronEventType int64

const (
	CronEventWorkerKeyChange CronEventType = iota
	CronEventPreCommitExpiry
	CronEventProvingPeriod
)

type CronEventPayload struct {
	EventType CronEventType
	Sectors   *abi.BitField
}

type Actor struct{}

func (a Actor) Exports() []interface{} {
	return []interface{}{
		builtin.MethodConstructor: a.Constructor,
		2:                         a.ControlAddresses,
		3:                         a.ChangeWorkerAddress,
		4:                         a.ChangePeerID,
		5:                         a.SubmitWindowedPoSt,
		6:                         a.PreCommitSector,
		7:                         a.ProveCommitSector,
		8:                         a.ExtendSectorExpiration,
		9:                         a.TerminateSectors,
		10:                        a.DeclareFaults,
		11:                        a.DeclareFaultsRecovered,
		12:                        a.OnDeferredCronEvent,
		13:                        a.CheckSectorProven,
		14:                        a.AddLockedFund,
		15:                        a.ReportConsensusFault,
		16:                        a.WithdrawBalance,
	}
}

var _ abi.Invokee = Actor{}

/////////////////
// Constructor //
/////////////////

// Storage miner actors are created exclusively by the storage power actor. In order to break a circular dependency
// between the two, the construction parameters are defined in the power actor.
type ConstructorParams = power.MinerConstructorParams

func (a Actor) Constructor(rt Runtime, params *ConstructorParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.InitActorAddr)

	// Sanity check that we've been given a valid peer ID
	_, err := peer.IDFromString(string(params.PeerId))
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalArgument, "invalid peer ID in parameters: %s", err)
	}

	owner := resolveOwnerAddress(rt, params.OwnerAddr)
	worker := resolveWorkerAddress(rt, params.WorkerAddr)

	emptyMap, err := adt.MakeEmptyMap(adt.AsStore(rt)).Root()
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to construct initial state: %v", err)
	}

	emptyArray, err := adt.MakeEmptyArray(adt.AsStore(rt)).Root()
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to construct initial state: %v", err)
	}

	emptyDeadlines := ConstructDeadlines()
	emptyDeadlinesCid := rt.Store().Put(emptyDeadlines)

	currEpoch := rt.CurrEpoch()
	offset, err := assignProvingPeriodOffset(rt.Message().Receiver(), currEpoch, rt.Syscalls().HashBlake2b)
	builtin.RequireNoErr(rt, err, exitcode.ErrSerialization, "failed to assign proving period offset")
	periodStart := nextProvingPeriodStart(currEpoch, offset)
	Assert(periodStart > currEpoch)

	state, err := ConstructState(emptyArray, emptyMap, emptyDeadlinesCid, owner, worker, params.PeerId, params.SealProofType, periodStart)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalArgument, "failed to construct state")
	rt.State().Create(state)

	// Register cron callback for epoch before the first proving period starts.
	enrollCronEvent(rt, periodStart-1, &CronEventPayload{
		EventType: CronEventProvingPeriod,
	})
	return nil
}

/////////////
// Control //
/////////////

type GetControlAddressesReturn struct {
	Owner  addr.Address
	Worker addr.Address
}

func (a Actor) ControlAddresses(rt Runtime, _ *adt.EmptyValue) *GetControlAddressesReturn {
	rt.ValidateImmediateCallerAcceptAny()
	var st State
	rt.State().Readonly(&st)
	return &GetControlAddressesReturn{
		Owner:  st.Info.Owner,
		Worker: st.Info.Worker,
	}
}

type ChangeWorkerAddressParams struct {
	NewWorker addr.Address
}

func (a Actor) ChangeWorkerAddress(rt Runtime, params *ChangeWorkerAddressParams) *adt.EmptyValue {
	var effectiveEpoch abi.ChainEpoch
	var st State
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Owner)

		worker := resolveWorkerAddress(rt, params.NewWorker)

		effectiveEpoch = rt.CurrEpoch() + WorkerKeyChangeDelay

		// This may replace another pending key change.
		st.Info.PendingWorkerKey = &WorkerKeyChange{
			NewWorker:   worker,
			EffectiveAt: effectiveEpoch,
		}
		return nil
	})

	cronPayload := CronEventPayload{
		EventType: CronEventWorkerKeyChange,
	}
	enrollCronEvent(rt, effectiveEpoch, &cronPayload)
	return nil
}

type ChangePeerIDParams struct {
	NewID peer.ID
}

func (a Actor) ChangePeerID(rt Runtime, params *ChangePeerIDParams) *adt.EmptyValue {
	var st State
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)
		st.Info.PeerId = params.NewID
		return nil
	})
	return nil
}

//////////////////
// WindowedPoSt //
//////////////////

// Information submitted by a miner to provide a Window PoSt.
type SubmitWindowedPoStParams struct {
	// The deadline index which the submission targets.
	Deadline uint64
	// The partition indices being proven.
	// Partitions are counted across all deadlines, such that all partition indices in the second deadline are greater
	// than the partition numbers in the first deadlines.
	Partitions []uint64
	// Parallel array of proofs corresponding to the partitions.
	Proofs []abi.PoStProof
	// Sectors skipped while proving that weren't already declared faulty
	Skipped abi.BitField
}

// Invoked by miner's worker address to submit their fallback post
func (a Actor) SubmitWindowedPoSt(rt Runtime, params *SubmitWindowedPoStParams) *adt.EmptyValue {
	if len(params.Partitions) != len(params.Proofs) {
		rt.Abortf(exitcode.ErrIllegalArgument, "proof count %d must match partition count %", len(params.Proofs), len(params.Partitions))
	}

	currEpoch := rt.CurrEpoch()
	store := adt.AsStore(rt)
	var st State
	var detectedFaultSectors []*SectorOnChainInfo
	var penalty abi.TokenAmount
	var recoveredSectors []*SectorOnChainInfo
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)

		partitionSize := st.Info.WindowPoStPartitionSectors
		submissionPartitionLimit := windowPoStMessagePartitionsMax(partitionSize)
		if uint64(len(params.Partitions)) > submissionPartitionLimit {
			rt.Abortf(exitcode.ErrIllegalArgument, "too many partitions %d, limit %d", len(params.Partitions), submissionPartitionLimit)
		}
		deadline := st.DeadlineInfo(currEpoch)
		if !deadline.PeriodStarted() {
			rt.Abortf(exitcode.ErrIllegalArgument, "proving period %d not yet open at %d", deadline.PeriodStart, currEpoch)
		}
		if deadline.PeriodElapsed() {
			// A cron event has not yet processed the previous proving period and established the next one.
			// This is possible in the first non-empty epoch of a proving period if there was an empty tipset on the
			// last epoch of the previous period.
			rt.Abortf(exitcode.ErrIllegalState, "proving period at %d elapsed, next one not yet opened", deadline.PeriodStart)
		}
		if params.Deadline != deadline.Index {
			rt.Abortf(exitcode.ErrIllegalArgument, "invalid deadline %d at epoch %d, expected %d",
				params.Deadline, currEpoch, deadline.Index)
		}

		// Verify locked funds are are at least the sum of sector initial pledges.
		// Note that this call does not actually compute recent vesting, so the reported locked funds may be
		// slightly higher than the true amount (i.e. slightly in the miner's favour).
		// Computing vesting here would be almost always redundant since vesting is quantized to ~daily units.
		// Vesting will be at most one proving period old if computed in the cron callback.
		verifyPledgeMeetsInitialRequirements(rt, &st)

		deadlines, err := st.LoadDeadlines(adt.AsStore(rt))
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deadlines")

		// Traverse earlier submissions and enact detected faults.
		// This isn't strictly necessary, but keeps the power table up to date eagerly and can force payment
		// of penalties if locked pledge drops too low.
		detectedFaultSectors, penalty = checkMissingPoStFaults(rt, &st, store, deadlines, deadline.PeriodStart, deadline.Index, currEpoch)

		// Work out which sectors are due in the declared partitions at this deadline.
		partitionsSectors, err := ComputePartitionsSectors(deadlines, partitionSize, deadline.Index, params.Partitions)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to compute partitions sectors at deadline %d, partitions %s",
			deadline.Index, params.Partitions)

		provenSectors, err := abi.BitFieldUnion(partitionsSectors...)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to union %d partitions of sectors", len(partitionsSectors))

		// TODO WPOST (follow-up): process Skipped as faults

		// Extract a fault set relevant to the sectors being submitted, for expansion into a map.
		declaredFaults, err := bitfield.IntersectBitField(provenSectors, st.Faults)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to intersect proof sectors with faults")

		declaredRecoveries, err := bitfield.IntersectBitField(declaredFaults, st.Recoveries)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to intersect recoveries with faults")

		expectedFaults, err := bitfield.SubtractBitField(declaredFaults, declaredRecoveries)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to subtract recoveries from faults")

		nonFaults, err := bitfield.SubtractBitField(provenSectors, expectedFaults)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to diff bitfields")

		empty, err := nonFaults.IsEmpty()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to check if bitfield was empty")
		if empty {
			rt.Abortf(exitcode.ErrIllegalArgument, "no non-faulty sectors in partitions %s", params.Partitions)
		}

		// Select a non-faulty sector as a substitute for faulty ones.
		goodSectorNo, err := nonFaults.First()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to get first good sector")

		// Load sector infos for proof
		sectorInfos, err := st.LoadSectorInfosWithFaultMask(store, provenSectors, expectedFaults, abi.SectorNumber(goodSectorNo))
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load sector infos")

		// Verify the proof.
		// A failed verification doesn't immediately cause a penalty; the miner can try again.
		verifyWindowedPost(rt, deadline.Challenge, sectorInfos, params.Proofs)

		// Record the successful submission
		postedPartitions := bitfield.NewFromSet(params.Partitions)
		contains, err := abi.BitFieldContainsAny(st.PostSubmissions, postedPartitions)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to intersect post partitions")
		if contains {
			rt.Abortf(exitcode.ErrIllegalArgument, "duplicate PoSt partition")
		}
		err = st.AddPoStSubmissions(postedPartitions)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to record submissions for partitions %s", params.Partitions)

		// If the PoSt was successful, the declared recoveries should be restored
		err = st.RemoveFaults(store, declaredRecoveries)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to remove recoveries from faults")

		err = st.RemoveRecoveries(declaredRecoveries)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to remove recoveries")

		// Load info for recovered sectors for recovery of power outside this state transaction.
		empty, err = declaredRecoveries.IsEmpty()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to check if bitfield was empty: %s")

		if !empty {
			sectorsByNumber := map[abi.SectorNumber]*SectorOnChainInfo{}
			for _, s := range sectorInfos {
				sectorsByNumber[s.Info.SectorNumber] = s
			}

			_ = declaredRecoveries.ForEach(func(i uint64) error {
				recoveredSectors = append(recoveredSectors, sectorsByNumber[abi.SectorNumber(i)])
				return nil
			})
		}
		return nil
	})

	// Remove power for new faults, and burn penalties.
	requestBeginFaults(rt, st.Info.SectorSize, detectedFaultSectors)
	burnFundsAndNotifyPledgeChange(rt, penalty)

	// Restore power for recovered sectors.
	if len(recoveredSectors) > 0 {
		requestEndFaults(rt, st.Info.SectorSize, recoveredSectors)
	}
	return nil
}

///////////////////////
// Sector Commitment //
///////////////////////

// Proposals must be posted on chain via sma.PublishStorageDeals before PreCommitSector.
// Optimization: PreCommitSector could contain a list of deals that are not published yet.
func (a Actor) PreCommitSector(rt Runtime, params *SectorPreCommitInfo) *adt.EmptyValue {
	if params.Expiration <= rt.CurrEpoch() {
		rt.Abortf(exitcode.ErrIllegalArgument, "sector expiration %v must be after now (%v)", params.Expiration, rt.CurrEpoch())
	}

	store := adt.AsStore(rt)
	var st State
	newlyVestedAmount := rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)
		if params.RegisteredProof != st.Info.SealProofType {
			rt.Abortf(exitcode.ErrIllegalArgument, "wrong proof type")
		}

		if _, found, err := st.GetPrecommittedSector(store, params.SectorNumber); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to check precommit %v: %v", params.SectorNumber, err)
		} else if found {
			rt.Abortf(exitcode.ErrIllegalArgument, "sector %v already precommitted", params.SectorNumber)
		}

		if found, err := st.HasSectorNo(store, params.SectorNumber); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to check sector %v: %v", params.SectorNumber, err)
		} else if found {
			rt.Abortf(exitcode.ErrIllegalArgument, "sector %v already committed", params.SectorNumber)
		}

		// Check expiry is exactly *the epoch before* the start of a proving period.
		periodOffset := st.ProvingPeriodStart % WPoStProvingPeriod
		expiryOffset := (params.Expiration + 1) % WPoStProvingPeriod
		if expiryOffset != periodOffset {
			rt.Abortf(exitcode.ErrIllegalArgument, "invalid expiration %d, must be immediately before proving period boundary %d mod %d",
				params.Expiration, periodOffset, WPoStProvingPeriod)
		}

		newlyVestedFund, err := st.UnlockVestedFunds(store, rt.CurrEpoch())
		availableBalance := st.GetAvailableBalance(rt.CurrentBalance())
		depositReq := precommitDeposit(st.GetSectorSize(), params.Expiration-rt.CurrEpoch())
		if availableBalance.LessThan(depositReq) {
			rt.Abortf(exitcode.ErrInsufficientFunds, "insufficient funds for pre-commit deposit: %v", depositReq)
		}

		st.AddPreCommitDeposit(depositReq)
		st.AssertBalanceInvariants(rt.CurrentBalance())

		err = st.PutPrecommittedSector(store, &SectorPreCommitOnChainInfo{
			Info:             *params,
			PreCommitDeposit: depositReq,
			PreCommitEpoch:   rt.CurrEpoch(),
		})
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to write pre-committed sector %v: %v", params.SectorNumber, err)
		}

		return newlyVestedFund
	}).(abi.TokenAmount)

	notifyPledgeChanged(rt, newlyVestedAmount.Neg())

	bf := abi.NewBitField()
	bf.Set(uint64(params.SectorNumber))

	// Request deferred Cron check for PreCommit expiry check.
	cronPayload := CronEventPayload{
		EventType: CronEventPreCommitExpiry,
		Sectors:   bf,
	}

	msd, ok := MaxSealDuration[params.RegisteredProof]
	if !ok {
		rt.Abortf(exitcode.ErrIllegalArgument, "no max seal duration set for proof type: %d", params.RegisteredProof)
	}

	expiryBound := rt.CurrEpoch() + msd + 1
	enrollCronEvent(rt, expiryBound, &cronPayload)

	return nil
}

type ProveCommitSectorParams struct {
	SectorNumber abi.SectorNumber
	Proof        []byte
}

func (a Actor) ProveCommitSector(rt Runtime, params *ProveCommitSectorParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerAcceptAny()

	store := adt.AsStore(rt)
	var st State
	rt.State().Readonly(&st)

	sectorNo := params.SectorNumber
	precommit, found, err := st.GetPrecommittedSector(store, sectorNo)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to get precommitted sector %v: %v", sectorNo, err)
	} else if !found {
		rt.Abortf(exitcode.ErrNotFound, "no precommitted sector %v", sectorNo)
	}

	msd, ok := MaxSealDuration[precommit.Info.RegisteredProof]
	if !ok {
		rt.Abortf(exitcode.ErrIllegalState, "no max seal duration for proof type: %d", precommit.Info.RegisteredProof)
	}
	proveCommitDue := precommit.PreCommitEpoch + msd
	if rt.CurrEpoch() > proveCommitDue {
		rt.Abortf(exitcode.ErrIllegalArgument, "commitment proof for %d too late at %d, due %d", sectorNo, rt.CurrEpoch(), proveCommitDue)
	}

	// will abort if seal invalid
	verifySeal(rt, &abi.OnChainSealVerifyInfo{
		SealedCID:        precommit.Info.SealedCID,
		InteractiveEpoch: precommit.PreCommitEpoch + PreCommitChallengeDelay,
		SealRandEpoch:    precommit.Info.SealRandEpoch,
		Proof:            params.Proof,
		DealIDs:          precommit.Info.DealIDs,
		SectorNumber:     precommit.Info.SectorNumber,
		RegisteredProof:  precommit.Info.RegisteredProof,
	})

	// Check (and activate) storage deals associated to sector. Abort if checks failed.
	// return DealWeight for the deal set in the sector
	var dealWeights market.VerifyDealsOnSectorProveCommitReturn
	ret, code := rt.Send(
		builtin.StorageMarketActorAddr,
		builtin.MethodsMarket.VerifyDealsOnSectorProveCommit,
		&market.VerifyDealsOnSectorProveCommitParams{
			DealIDs:      precommit.Info.DealIDs,
			SectorExpiry: precommit.Info.Expiration,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to verify deals and get deal weight")
	AssertNoError(ret.Into(&dealWeights))

	// Request power for activated sector.
	// Return initial pledge requirement.
	var initialPledge abi.TokenAmount
	ret, code = rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.OnSectorProveCommit,
		&power.OnSectorProveCommitParams{
			Weight: power.SectorStorageWeightDesc{
				SectorSize:         st.Info.SectorSize,
				DealWeight:         dealWeights.DealWeight,
				VerifiedDealWeight: dealWeights.VerifiedDealWeight,
				Duration:           precommit.Info.Expiration - rt.CurrEpoch(),
			},
		},
		big.Zero(),
	)
	builtin.RequireSuccess(rt, code, "failed to notify power actor")
	AssertNoError(ret.Into(&initialPledge))

	// Add sector and pledge lock-up to miner state
	newlyVestedAmount := rt.State().Transaction(&st, func() interface{} {
		newlyVestedFund, err := st.UnlockVestedFunds(store, rt.CurrEpoch())
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to vest new funds: %s", err)
		}

		// Unlock deposit for successful proof, make it available for lock-up as initial pledge.
		st.AddPreCommitDeposit(precommit.PreCommitDeposit.Neg())

		// Verify locked funds are are at least the sum of sector initial pledges.
		verifyPledgeMeetsInitialRequirements(rt, &st)

		// Lock up initial pledge for new sector.
		availableBalance := st.GetAvailableBalance(rt.CurrentBalance())
		if availableBalance.LessThan(initialPledge) {
			rt.Abortf(exitcode.ErrInsufficientFunds, "insufficient funds for initial pledge requirement %s, available: %s", initialPledge, availableBalance)
		}
		if err := st.AddLockedFunds(store, rt.CurrEpoch(), initialPledge, &PledgeVestingSpec); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to add pledge: %v", err)
		}
		st.AssertBalanceInvariants(rt.CurrentBalance())

		newSectorInfo := &SectorOnChainInfo{
			Info:               precommit.Info,
			ActivationEpoch:    rt.CurrEpoch(),
			DealWeight:         dealWeights.DealWeight,
			VerifiedDealWeight: dealWeights.VerifiedDealWeight,
		}

		if err := st.PutSector(store, newSectorInfo); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to prove commit: %v", err)
		}

		if err := st.DeletePrecommittedSector(store, sectorNo); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to delete precommit for sector %v: %v", sectorNo, err)
		}

		if err := st.AddSectorExpirations(store, precommit.Info.Expiration, uint64(sectorNo)); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to add new sector %v expiration: %v", sectorNo, err)
		}

		// Add to new sectors, a staging ground before scheduling to a deadline at end of proving period.
		if err := st.AddNewSectors(sectorNo); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to add new sector number %v: %v", sectorNo, err)
		}

		return newlyVestedFund
	}).(abi.TokenAmount)

	notifyPledgeChanged(rt, big.Sub(initialPledge, newlyVestedAmount))

	return nil
}

type CheckSectorProvenParams struct {
	SectorNumber abi.SectorNumber
}

func (a Actor) CheckSectorProven(rt Runtime, params *CheckSectorProvenParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerAcceptAny()

	var st State
	rt.State().Readonly(&st)
	store := adt.AsStore(rt)
	sectorNo := params.SectorNumber

	_, found, _ := st.GetSector(store, sectorNo)
	if !found {
		rt.Abortf(exitcode.ErrNotFound, "Sector hasn't been proven %v", sectorNo)
	}

	return nil
}

/////////////////////////
// Sector Modification //
/////////////////////////

type ExtendSectorExpirationParams struct {
	SectorNumber  abi.SectorNumber
	NewExpiration abi.ChainEpoch
}

func (a Actor) ExtendSectorExpiration(rt Runtime, params *ExtendSectorExpirationParams) *adt.EmptyValue {
	var st State
	rt.State().Readonly(&st)
	rt.ValidateImmediateCallerIs(st.Info.Worker)

	store := adt.AsStore(rt)
	sectorNo := params.SectorNumber
	sector, found, err := st.GetSector(store, sectorNo)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to load sector %v: %v", sectorNo, err)
	} else if !found {
		rt.Abortf(exitcode.ErrNotFound, "no such sector %v", sectorNo)
	}

	oldExpiration := sector.Info.Expiration
	storageWeightDescPrev := AsStorageWeightDesc(st.Info.SectorSize, sector)
	extensionLength := params.NewExpiration - oldExpiration
	if extensionLength < 0 {
		rt.Abortf(exitcode.ErrIllegalArgument, "cannot reduce sector expiration")
	}

	storageWeightDescNew := *storageWeightDescPrev
	storageWeightDescNew.Duration = storageWeightDescPrev.Duration + extensionLength

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.OnSectorModifyWeightDesc,
		&power.OnSectorModifyWeightDescParams{
			PrevWeight: *storageWeightDescPrev,
			NewWeight:  storageWeightDescNew,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to modify sector weight")

	// Store new sector expiry.
	rt.State().Transaction(&st, func() interface{} {
		sector.Info.Expiration = params.NewExpiration
		if err := st.PutSector(store, sector); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to update sector %v, %v", sectorNo, err)
		}
		return nil
	})

	return nil
}

type TerminateSectorsParams struct {
	Sectors *abi.BitField
}

func (a Actor) TerminateSectors(rt Runtime, params *TerminateSectorsParams) *adt.EmptyValue {
	var st State
	rt.State().Readonly(&st)
	rt.ValidateImmediateCallerIs(st.Info.Worker)

	// Note: this cannot terminate pre-committed but un-proven sectors.
	// They must be allowed to expire (and deposit burnt).
	terminateSectors(rt, params.Sectors, power.SectorTerminationManual)
	return nil
}

////////////
// Faults //
////////////

type DeclareFaultsParams struct {
	Faults []FaultDeclaration
}

type FaultDeclaration struct {
	Deadline uint64 // In range [0..WPoStPeriodDeadlines)
	Sectors  *abi.BitField
}

func (a Actor) DeclareFaults(rt Runtime, params *DeclareFaultsParams) *adt.EmptyValue {
	if uint64(len(params.Faults)) > WPoStPeriodDeadlines {
		rt.Abortf(exitcode.ErrIllegalArgument, "too many declarations %d, max %d", len(params.Faults), WPoStPeriodDeadlines)
	}

	currEpoch := rt.CurrEpoch()
	store := adt.AsStore(rt)
	var st State
	var declaredFaultSectors []*SectorOnChainInfo
	var detectedFaultSectors []*SectorOnChainInfo
	var penalty abi.TokenAmount

	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)

		currDeadline := st.DeadlineInfo(currEpoch)
		deadlines, err := st.LoadDeadlines(adt.AsStore(rt))
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deadlines")

		// Traverse earlier submissions and enact detected faults.
		// This is necessary to prevent the miner "declaring" a fault for a PoSt already missed.
		detectedFaultSectors, penalty = checkMissingPoStFaults(rt, &st, store, deadlines, currDeadline.PeriodStart, currDeadline.Index, currEpoch)

		var decaredSectors []*abi.BitField
		for _, decl := range params.Faults {
			targetDeadline, err := declarationDeadlineInfo(st.ProvingPeriodStart, decl.Deadline, currEpoch)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalArgument, "invalid fault declaration deadline")

			err = validateFRDeclaration(deadlines, targetDeadline, decl.Sectors)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalArgument, "invalid fault declaration")
			decaredSectors = append(decaredSectors, decl.Sectors)
		}

		allDeclared, err := abi.BitFieldUnion(decaredSectors...)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to union faults")

		// Split declarations into declarations of new faults, and retraction of declared recoveries.
		recoveries, err := bitfield.IntersectBitField(st.Recoveries, allDeclared)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to intersect sectors with recoveries")

		newFaults, err := bitfield.SubtractBitField(allDeclared, recoveries)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to subtract recoveries from sectors")

		empty, err := newFaults.IsEmpty()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to check if bitfield was empty")

		if !empty {
			// Check new fault are really new.
			contains, err := abi.BitFieldContainsAny(st.Faults, newFaults)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to intersect existing faults")
			if contains {
				// This could happen if attempting to declare a fault for a deadline that's already passed,
				// detected and added to Faults above.
				// The miner must for the fault detection at proving period end, or submit again omitting
				// sectors in deadlines that have passed.
				// Alternatively, we could subtract the just-detected faults from new faults.
				rt.Abortf(exitcode.ErrIllegalArgument, "attempted to re-declare fault")
			}

			// Add new faults to state and charge fee.
			// Note: this sets the fault epoch for all declarations to be the beginning of this proving period,
			// even if some sectors have already been proven in this period.
			// It would better to use the target deadline's proving period start (which may be the one subsequent
			// to the current).
			err = st.AddFaults(store, newFaults, st.ProvingPeriodStart)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to add faults")

			// Note: this charges a fee for all declarations, even if the sectors have already been proven
			// in this proving period. This discourages early declaration compared with waiting for
			// the proving period to roll over.
			// It would be better to charge a fee for this proving period only if the target deadline has
			// not already passed. If it _has_ already passed then either:
			// - the miner submitted PoSt successfully and should not be penalised more relative to
			//   submitting this declaration after the proving period rolls over, or
			// - the miner failed to submit PoSt and will be penalised at the proving period end
			// In either case, the miner will pay a fee for the subsequent proving period at the start
			// of that period, unless faults are recovered sooner.

			// Load info for sectors.
			declaredFaultSectors, err = st.LoadSectorInfos(store, newFaults)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load fault sectors")

			// Unlock penalty for declared faults.
			declaredPenalty, err := unlockPenalty(&st, store, currEpoch, declaredFaultSectors, pledgePenaltyForSectorDeclaredFault)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to charge fault fee")
			penalty = big.Add(penalty, declaredPenalty)
		}

		// Remove faulty recoveries
		empty, err = recoveries.IsEmpty()
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to check if bitfield was empty")

		if !empty {
			err = st.RemoveRecoveries(recoveries)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to remove recoveries")
		}
		return nil
	})

	// Remove power for new faulty sectors.
	requestBeginFaults(rt, st.Info.SectorSize, append(detectedFaultSectors, declaredFaultSectors...))
	burnFundsAndNotifyPledgeChange(rt, penalty)

	return nil
}

type DeclareFaultsRecoveredParams struct {
	Recoveries []RecoveryDeclaration
}

type RecoveryDeclaration struct {
	Deadline uint64 // In range [0..WPoStPeriodDeadlines)
	Sectors  *abi.BitField
}

func (a Actor) DeclareFaultsRecovered(rt Runtime, params *DeclareFaultsRecoveredParams) *adt.EmptyValue {
	if uint64(len(params.Recoveries)) > WPoStPeriodDeadlines {
		rt.Abortf(exitcode.ErrIllegalArgument, "too many declarations %d, max %d", len(params.Recoveries), WPoStPeriodDeadlines)
	}

	currEpoch := rt.CurrEpoch()
	var st State
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)

		deadlines, err := st.LoadDeadlines(adt.AsStore(rt))
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deadlines")

		var declaredSectors []*abi.BitField
		for _, decl := range params.Recoveries {
			targetDeadline, err := declarationDeadlineInfo(st.ProvingPeriodStart, decl.Deadline, currEpoch)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalArgument, "invalid recovery declaration deadline")

			err = validateFRDeclaration(deadlines, targetDeadline, decl.Sectors)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalArgument, "invalid recovery declaration")
			declaredSectors = append(declaredSectors, decl.Sectors)
		}

		allRecoveries, err := abi.BitFieldUnion(declaredSectors...)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to union recoveries")

		contains, err := abi.BitFieldContainsAll(st.Faults, allRecoveries)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to check recoveries are faulty")
		if !contains {
			rt.Abortf(exitcode.ErrIllegalArgument, "declared recoveries not currently faulty")
		}
		contains, err = abi.BitFieldContainsAny(st.Recoveries, allRecoveries)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to intersect new recoveries")
		if contains {
			rt.Abortf(exitcode.ErrIllegalArgument, "sector already declared recovered")
		}

		err = st.AddRecoveries(allRecoveries)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalArgument, "invalid recoveries")
		return nil
	})

	// Power is not restored yet, but when the recovered sectors are successfully PoSted.
	return nil
}

///////////////////////
// Pledge Collateral //
///////////////////////

// Locks up some amount of a the miner's unlocked balance (including any received alongside the invoking message).
func (a Actor) AddLockedFund(rt Runtime, amountToLock *abi.TokenAmount) *adt.EmptyValue {
	store := adt.AsStore(rt)
	var st State
	newlyVested := rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker, st.Info.Owner, builtin.RewardActorAddr)

		newlyVestedFund, err := st.UnlockVestedFunds(store, rt.CurrEpoch())
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to vest funds")

		availableBalance := st.GetAvailableBalance(rt.CurrentBalance())
		if availableBalance.LessThan(*amountToLock) {
			rt.Abortf(exitcode.ErrInsufficientFunds, "insufficient funds to lock, available: %v, requested: %v", availableBalance, *amountToLock)
		}

		if err := st.AddLockedFunds(store, rt.CurrEpoch(), *amountToLock, &PledgeVestingSpec); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to lock pledge: %v", err)
		}
		return newlyVestedFund
	}).(abi.TokenAmount)

	notifyPledgeChanged(rt, big.Sub(*amountToLock, newlyVested))
	return nil
}

type ReportConsensusFaultParams struct {
	BlockHeader1     []byte
	BlockHeader2     []byte
	BlockHeaderExtra []byte
}

func (a Actor) ReportConsensusFault(rt Runtime, params *ReportConsensusFaultParams) *adt.EmptyValue {
	// Note: only the first reporter of any fault is rewarded.
	// Subsequent invocations fail because the target miner has been removed.
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	reporter := rt.Message().Caller()

	fault, err := rt.Syscalls().VerifyConsensusFault(params.BlockHeader1, params.BlockHeader2, params.BlockHeaderExtra)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalArgument, "fault not verified: %s", err)
	}

	// Elapsed since the fault (i.e. since the higher of the two blocks)
	faultAge := rt.CurrEpoch() - fault.Epoch
	if faultAge <= 0 {
		rt.Abortf(exitcode.ErrIllegalArgument, "invalid fault epoch %v ahead of current %v", fault.Epoch, rt.CurrEpoch())
	}

	var st State
	rt.State().Readonly(&st)

	// Notify power actor with lock-up total being removed.
	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.OnConsensusFault,
		&st.LockedFunds,
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to notify power actor on consensus fault")

	// TODO: terminate deals with market actor, https://github.com/filecoin-project/specs-actors/issues/279

	// Reward reporter with a share of the miner's current balance.
	slasherReward := rewardForConsensusSlashReport(faultAge, rt.CurrentBalance())
	_, code = rt.Send(reporter, builtin.MethodSend, nil, slasherReward)
	builtin.RequireSuccess(rt, code, "failed to reward reporter")

	// Delete the actor and burn all remaining funds
	rt.DeleteActor(builtin.BurntFundsActorAddr)
	return nil
}

type WithdrawBalanceParams struct {
	AmountRequested abi.TokenAmount
}

func (a Actor) WithdrawBalance(rt Runtime, params *WithdrawBalanceParams) *adt.EmptyValue {
	var st State
	if params.AmountRequested.LessThan(big.Zero()) {
		rt.Abortf(exitcode.ErrIllegalArgument, "negative fund requested for withdrawal: %s", params.AmountRequested)
	}

	newlyVestedAmount := rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Owner)
		newlyVestedFund, err := st.UnlockVestedFunds(adt.AsStore(rt), rt.CurrEpoch())
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to vest fund: %v", err)
		}
		return newlyVestedFund
	}).(abi.TokenAmount)

	currBalance := rt.CurrentBalance()
	amountWithdrawn := big.Min(st.GetAvailableBalance(currBalance), params.AmountRequested)
	Assert(amountWithdrawn.LessThanEqual(currBalance))

	_, code := rt.Send(st.Info.Owner, builtin.MethodSend, nil, amountWithdrawn)
	builtin.RequireSuccess(rt, code, "failed to withdraw balance")

	pledgeDelta := newlyVestedAmount.Neg()
	notifyPledgeChanged(rt, pledgeDelta)

	st.AssertBalanceInvariants(rt.CurrentBalance())
	return nil
}

//////////
// Cron //
//////////

func (a Actor) OnDeferredCronEvent(rt Runtime, payload *CronEventPayload) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	switch payload.EventType {
	case CronEventProvingPeriod:
		handleProvingPeriod(rt)
	case CronEventPreCommitExpiry:
		checkPrecommitExpiry(rt, payload.Sectors)
	case CronEventWorkerKeyChange:
		commitWorkerKeyChange(rt)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////
// Utility functions & helpers
////////////////////////////////////////////////////////////////////////////////

// Invoked at the end of each proving period, at the end of the epoch before the next one starts.
func handleProvingPeriod(rt Runtime) {
	store := adt.AsStore(rt)
	var st State
	// Note: because the cron actor is not invoked on epochs with empty tipsets, the current epoch is not necessarily
	// exactly the final epoch of the period; it may be slightly later (i.e. in the subsequent period).
	// Further, this method is invoked once *before* the first proving period starts, after the actor is first
	// constructed; this is detected by !deadline.PeriodStarted().
	// Use deadline.PeriodEnd() rather than rt.CurrEpoch unless certain of the desired semantics.
	var deadline *DeadlineInfo
	{
		// Vest locked funds.
		// This happens first so that any subsequent penalties are taken from locked pledge, rather than free funds.
		newlyVestedAmount := rt.State().Transaction(&st, func() interface{} {
			newlyVestedFund, err := st.UnlockVestedFunds(store, rt.CurrEpoch())
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to vest funds")
			return newlyVestedFund
		}).(abi.TokenAmount)

		notifyPledgeChanged(rt, newlyVestedAmount.Neg())
	}

	{
		// Detect and penalize missing proofs.
		var detectedFaultSectors []*SectorOnChainInfo
		currEpoch := rt.CurrEpoch()
		penalty := big.Zero()
		rt.State().Transaction(&st, func() interface{} {
			deadline = st.DeadlineInfo(currEpoch)
			if deadline.PeriodStarted() { // Skip checking faults on the first, incomplete period.
				deadlines, err := st.LoadDeadlines(store)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deadlines")
				detectedFaultSectors, penalty = checkMissingPoStFaults(rt, &st, store, deadlines, deadline.PeriodStart, WPoStPeriodDeadlines, deadline.PeriodEnd())
			}
			return nil
		})

		// Remove power for new faults, and burn penalties.
		requestBeginFaults(rt, st.Info.SectorSize, detectedFaultSectors)
		burnFundsAndNotifyPledgeChange(rt, penalty)
	}

	{
		// Expire sectors that are due.
		var expiredSectors *abi.BitField
		var err error
		rt.State().Transaction(&st, func() interface{} {
			expiredSectors, err = popSectorExpirations(&st, store, deadline.PeriodEnd())
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load expired sectors")
			return nil
		})

		// Terminate expired sectors (sends messages to power and market actors).
		terminateSectors(rt, expiredSectors, power.SectorTerminationExpired)
	}

	{
		// Terminate sectors with faults that are too old, and pay fees for ongoing faults.
		var expiredFaults, ongoingFaults *abi.BitField
		var ongoingFaultPenalty abi.TokenAmount
		var err error
		rt.State().Transaction(&st, func() interface{} {
			expiredFaults, ongoingFaults, err = popExpiredFaults(&st, store, deadline.PeriodEnd()-FaultMaxAge)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load fault epochs")

			// Load info for ongoing faults.
			// TODO: this is potentially super expensive for a large miner with ongoing faults
			ongoingFaultInfos, err := st.LoadSectorInfos(store, ongoingFaults)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load fault sectors")

			// Unlock penalty for ongoing faults.
			ongoingFaultPenalty, err = unlockPenalty(&st, store, deadline.PeriodEnd(), ongoingFaultInfos, pledgePenaltyForSectorDeclaredFault)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to charge fault fee")
			return nil
		})

		terminateSectors(rt, expiredFaults, power.SectorTerminationFaulty)
		burnFundsAndNotifyPledgeChange(rt, ongoingFaultPenalty)
	}

	{
		// Establish new proving sets and clear proofs.
		rt.State().Transaction(&st, func() interface{} {
			deadlines, err := st.LoadDeadlines(store)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deadlines")

			// Assign new sectors to deadlines.
			newSectors, err := st.NewSectors.All(NewSectorsPerPeriodMax)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to expand new sectors")

			if len(newSectors) > 0 {
				assignmentSeed := rt.GetRandomness(crypto.DomainSeparationTag_WindowedPoStDeadlineAssignment, deadline.PeriodEnd()-1, nil)
				err = AssignNewSectors(deadlines, st.Info.WindowPoStPartitionSectors, newSectors, assignmentSeed)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to assign new sectors to deadlines")

				// Store updated deadline state.
				err = st.SaveDeadlines(store, deadlines)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to store new deadlines")

				st.NewSectors = abi.NewBitField()
			}

			// Reset PoSt submissions for next period.
			err = st.ClearPoStSubmissions()
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to clear PoSt submissions")

			// Set new proving period start.
			if deadline.PeriodStarted() {
				st.ProvingPeriodStart = st.ProvingPeriodStart + WPoStProvingPeriod
			}
			return nil
		})
	}

	// Schedule cron callback for next period
	nextPeriodEnd := st.ProvingPeriodStart + WPoStProvingPeriod - 1
	enrollCronEvent(rt, nextPeriodEnd, &CronEventPayload{
		EventType: CronEventProvingPeriod,
	})
}

// Detects faults from missing PoSt submissions that did not arrive.
func checkMissingPoStFaults(rt Runtime, st *State, store adt.Store, deadlines *Deadlines, periodStart abi.ChainEpoch, beforeDeadline uint64, currEpoch abi.ChainEpoch) ([]*SectorOnChainInfo, abi.TokenAmount) {
	detectedFaults, failedRecoveries, err := computeFaultsFromMissingPoSts(st, deadlines, beforeDeadline)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to compute detected faults")

	err = st.AddFaults(store, detectedFaults, periodStart)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to record new faults")

	err = st.RemoveRecoveries(failedRecoveries)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to record failed recoveries")

	// Load info for sectors.
	// TODO: this is potentially super expensive for a large miner failing to submit proofs.
	detectedFaultSectors, err := st.LoadSectorInfos(store, detectedFaults)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load fault sectors")
	failedRecoverySectors, err := st.LoadSectorInfos(store, failedRecoveries)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load failed recovery sectors")

	// Unlock sector penalty for all undeclared faults.
	penalty, err := unlockPenalty(st, store, currEpoch, append(detectedFaultSectors, failedRecoverySectors...), pledgePenaltyForSectorUndeclaredFault)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to charge sector penalty")
	return detectedFaultSectors, penalty
}

// Computes the sectors that were expected to be present in partitions of a PoSt submission but were not.
func computeFaultsFromMissingPoSts(st *State, deadlines *Deadlines, beforeDeadline uint64) (detectedFaults, failedRecoveries *abi.BitField, err error) {
	// TODO: Iterating this bitfield and keeping track of what partitions we're expecting could remove the
	// need to expand this into a potentially-giant map. But it's tricksy.
	partitionSize := st.Info.WindowPoStPartitionSectors
	submissions, err := st.PostSubmissions.AllMap(activePartitionsMax(partitionSize))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to expand submissions: %w", err)
	}

	deadlineFirstPartition := uint64(0)
	var fGroups, rGroups []*abi.BitField
	for dlIdx := uint64(0); dlIdx < beforeDeadline; dlIdx++ {
		dlPartCount, dlSectorCount, err := DeadlineCount(deadlines, partitionSize, dlIdx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to count deadline %d partitions: %w", dlIdx, err)
		}
		deadlineSectors := deadlines.Due[dlIdx]

		for dlPartIdx := uint64(0); dlPartIdx < dlPartCount; dlPartIdx++ {
			if !submissions[deadlineFirstPartition+dlPartIdx] {
				// No PoSt received in prior period.
				partFirstSectorIdx := dlPartIdx * partitionSize
				partSectorCount := min64(partitionSize, dlSectorCount-partFirstSectorIdx)

				partitionSectors, err := deadlineSectors.Slice(partFirstSectorIdx, partSectorCount)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to slice deadline %d partition %d sectors %d..%d: %w",
						dlIdx, dlPartIdx, partFirstSectorIdx, partFirstSectorIdx+dlPartCount, err)
				}

				// Record newly-faulty sectors.
				newFaults, err := bitfield.SubtractBitField(partitionSectors, st.Faults)
				fGroups = append(fGroups, newFaults)

				// Record failed recoveries.
				// By construction, these are already faulty and thus not in newFaults.
				failedRecovery, err := bitfield.IntersectBitField(partitionSectors, st.Recoveries)
				rGroups = append(rGroups, failedRecovery)
			}
		}

		deadlineFirstPartition += dlPartCount
	}
	detectedFaults, err = abi.BitFieldUnion(fGroups...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to union detected fault groups: %w", err)
	}
	failedRecoveries, err = abi.BitFieldUnion(rGroups...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to union failed recovery groups: %w", err)
	}
	return
}

// Removes and returns sector numbers that expire at or before an epoch.
func popSectorExpirations(st *State, store adt.Store, epoch abi.ChainEpoch) (*abi.BitField, error) {
	var expiredEpochs []abi.ChainEpoch
	var expiredSectors []*abi.BitField
	errDone := fmt.Errorf("done")
	err := st.ForEachSectorExpiration(store, func(expiry abi.ChainEpoch, sectors *abi.BitField) error {
		if expiry > epoch {
			return errDone
		}
		expiredSectors = append(expiredSectors, sectors)
		expiredEpochs = append(expiredEpochs, expiry)
		return nil
	})
	if err != nil && err != errDone {
		return nil, err
	}
	err = st.ClearSectorExpirations(store, expiredEpochs...)
	if err != nil {
		return nil, fmt.Errorf("failed to clear sector expirations %s: %w", expiredEpochs, err)
	}

	allExpiries, err := abi.BitFieldUnion(expiredSectors...)
	if err != nil {
		return nil, fmt.Errorf("failed to union expired sectors: %w", err)
	}
	return allExpiries, err
}

// Removes and returns sector numbers that were faulty at or before an epoch, and returns the sector
// numbers for other ongoing faults.
func popExpiredFaults(st *State, store adt.Store, latestTermination abi.ChainEpoch) (*abi.BitField, *abi.BitField, error) {
	var expiredEpochs []abi.ChainEpoch
	var expiredFaults []*abi.BitField
	var ongoingFaults []*abi.BitField
	errDone := fmt.Errorf("done")
	err := st.ForEachFaultEpoch(store, func(faultStart abi.ChainEpoch, faults *abi.BitField) error {
		if faultStart <= latestTermination {
			expiredFaults = append(expiredFaults, faults)
			expiredEpochs = append(expiredEpochs, faultStart)
		} else {
			ongoingFaults = append(ongoingFaults, faults)
		}
		return nil
	})
	if err != nil && err != errDone {
		return nil, nil, nil
	}
	err = st.ClearFaultEpochs(store, expiredEpochs...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to clear fault epochs %s: %w", expiredEpochs, err)
	}

	allExpiries, err := abi.BitFieldUnion(expiredFaults...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to union expired faults: %w", err)
	}
	allOngoing, err := abi.BitFieldUnion(ongoingFaults...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to union ongoing faults: %w", err)
	}

	return allExpiries, allOngoing, err
}

func checkPrecommitExpiry(rt Runtime, sectors *abi.BitField) {
	store := adt.AsStore(rt)
	var st State

	// initialize here to add together for all sectors and minimize calls across actors
	depositToBurn := abi.NewTokenAmount(0)
	rt.State().Transaction(&st, func() interface{} {
		err := sectors.ForEach(func(i uint64) error {
			sectorNo := abi.SectorNumber(i)
			sector, found, err := st.GetPrecommittedSector(store, sectorNo)
			if err != nil {
				return err
			}
			if !found {
				// already committed/deleted
				return nil
			}

			// delete sector
			err = st.DeletePrecommittedSector(store, sectorNo)
			if err != nil {
				return err
			}
			// increment deposit to burn
			depositToBurn = big.Add(depositToBurn, sector.PreCommitDeposit)
			return nil
		})
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to check precommit expiries")

		st.PreCommitDeposits = big.Sub(st.PreCommitDeposits, depositToBurn)
		Assert(st.PreCommitDeposits.GreaterThanEqual(big.Zero()))
		return nil
	})

	// This deposit was locked separately to pledge collateral so there's no pledge change here.
	burnFunds(rt, depositToBurn)
}

// TODO: red flag that this method is potentially super expensive
func terminateSectors(rt Runtime, sectorNos *abi.BitField, terminationType power.SectorTermination) {
	empty, err := sectorNos.IsEmpty()
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to count sectors")
	}
	if empty {
		return
	}

	store := adt.AsStore(rt)
	var st State

	var dealIDs []abi.DealID
	var allSectors []*SectorOnChainInfo
	var faultySectors []*SectorOnChainInfo
	var penalty abi.TokenAmount

	rt.State().Transaction(&st, func() interface{} {
		maxAllowedFaults, err := st.GetMaxAllowedFaults(store)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load fault max")

		// Narrow faults to just the set that are expiring, before expanding to a map.
		faults, err := bitfield.IntersectBitField(sectorNos, st.Faults)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load faults")

		faultsMap, err := faults.AllMap(maxAllowedFaults)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to expand faults")
		}

		err = sectorNos.ForEach(func(sectorNo uint64) error {
			sector, found, err := st.GetSector(store, abi.SectorNumber(sectorNo))
			if err != nil {
				return fmt.Errorf("failed to load sector %v: %w", sectorNo, err)
			}
			if !found {
				rt.Abortf(exitcode.ErrNotFound, "no sector %v", sectorNo)
			}

			dealIDs = append(dealIDs, sector.Info.DealIDs...)
			allSectors = append(allSectors, sector)

			_, fault := faultsMap[sectorNo]
			if fault {
				faultySectors = append(faultySectors, sector)
			}
			return nil
		})
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load sector metadata")

		deadlines, err := st.LoadDeadlines(store)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deadlines")

		err = removeTerminatedSectors(&st, store, deadlines, sectorNos)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to delete sectors: %v", err)
		}

		err = st.SaveDeadlines(store, deadlines)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to store new deadlines")

		if terminationType != power.SectorTerminationExpired {
			penalty, err = unlockPenalty(&st, store, rt.CurrEpoch(), allSectors, pledgePenaltyForSectorTermination)
		}
		return nil
	})

	// End any fault state before terminating sector power.
	// TODO: could we compress the three calls to power actor into one sector termination call?
	requestEndFaults(rt, st.Info.SectorSize, faultySectors)
	requestTerminateDeals(rt, dealIDs)
	requestTerminatePower(rt, terminationType, st.Info.SectorSize, allSectors)

	burnFundsAndNotifyPledgeChange(rt, penalty)
}

// Removes a group sectors from the sector set and its number from all sector collections in state.
func removeTerminatedSectors(st *State, store adt.Store, deadlines *Deadlines, sectors *abi.BitField) error {
	err := st.DeleteSectors(store, sectors)
	if err != nil {
		return err
	}
	err = st.RemoveNewSectors(sectors)
	if err != nil {
		return err
	}
	err = deadlines.RemoveFromAllDeadlines(sectors)
	if err != nil {
		return err
	}
	err = st.RemoveFaults(store, sectors)
	if err != nil {
		return err
	}
	err = st.RemoveRecoveries(sectors)
	return err
}

func enrollCronEvent(rt Runtime, eventEpoch abi.ChainEpoch, callbackPayload *CronEventPayload) {
	payload := new(bytes.Buffer)
	err := callbackPayload.MarshalCBOR(payload)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalArgument, "failed to serialize payload: %v", err)
	}
	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.EnrollCronEvent,
		&power.EnrollCronEventParams{
			EventEpoch: eventEpoch,
			Payload:    payload.Bytes(),
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to enroll cron event")
}

func requestBeginFaults(rt Runtime, sectorSize abi.SectorSize, sectors []*SectorOnChainInfo) {
	if len(sectors) == 0 {
		return
	}
	params := &power.OnFaultBeginParams{
		Weights: make([]power.SectorStorageWeightDesc, len(sectors)),
	}
	for i, s := range sectors {
		params.Weights[i] = *AsStorageWeightDesc(sectorSize, s)
	}

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.OnFaultBegin,
		params,
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to request faults %v", sectors)
}

func requestEndFaults(rt Runtime, sectorSize abi.SectorSize, sectors []*SectorOnChainInfo) {
	if len(sectors) == 0 {
		return
	}
	params := &power.OnFaultEndParams{
		Weights: make([]power.SectorStorageWeightDesc, len(sectors)),
	}
	for i, s := range sectors {
		params.Weights[i] = *AsStorageWeightDesc(sectorSize, s)
	}

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.OnFaultEnd,
		params,
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to request end faults %v", sectors)
}

func requestTerminateDeals(rt Runtime, dealIDs []abi.DealID) {
	if len(dealIDs) == 0 {
		return
	}
	_, code := rt.Send(
		builtin.StorageMarketActorAddr,
		builtin.MethodsMarket.OnMinerSectorsTerminate,
		&market.OnMinerSectorsTerminateParams{
			DealIDs: dealIDs,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to terminate deals %v, exit code %v", dealIDs, code)
}

func requestTerminateAllDeals(rt Runtime, st *State) {
	// TODO: red flag this is an ~unbounded computation.
	// Transform into an idempotent partial computation that can be progressed on each invocation.
	dealIds := []abi.DealID{}
	if err := st.ForEachSector(adt.AsStore(rt), func(sector *SectorOnChainInfo) {
		dealIds = append(dealIds, sector.Info.DealIDs...)
	}); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to traverse sectors for termination: %v", err)
	}

	requestTerminateDeals(rt, dealIds)
}

func requestTerminatePower(rt Runtime, terminationType power.SectorTermination, sectorSize abi.SectorSize, sectors []*SectorOnChainInfo) {
	if len(sectors) == 0 {
		return
	}
	params := &power.OnSectorTerminateParams{
		TerminationType: terminationType,
		Weights:         make([]power.SectorStorageWeightDesc, len(sectors)),
	}
	for i, s := range sectors {
		params.Weights[i] = *AsStorageWeightDesc(sectorSize, s)
	}

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.OnSectorTerminate,
		params,
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to terminate sector power type %v, sectors %v", terminationType, sectors)
}

func verifyWindowedPost(rt Runtime, challengeEpoch abi.ChainEpoch, sectors []*SectorOnChainInfo, proofs []abi.PoStProof) {
	minerActorID, err := addr.IDFromAddress(rt.Message().Receiver())
	AssertNoError(err) // Runtime always provides ID-addresses

	// Regenerate challenge randomness, which must match that generated for the proof.
	var addrBuf bytes.Buffer
	err = rt.Message().Receiver().MarshalCBOR(&addrBuf)
	AssertNoError(err)
	postRandomness := rt.GetRandomness(crypto.DomainSeparationTag_WindowedPoStChallengeSeed, challengeEpoch, addrBuf.Bytes())

	sectorProofInfo := make([]abi.SectorInfo, len(sectors))
	for i, s := range sectors {
		sectorProofInfo[i] = s.AsSectorInfo()
	}

	// Get public inputs
	pvInfo := abi.WindowPoStVerifyInfo{
		Randomness:        abi.PoStRandomness(postRandomness),
		Proofs:            proofs,
		ChallengedSectors: sectorProofInfo,
		Prover:            abi.ActorID(minerActorID),
	}

	// Verify the PoSt Proof
	if err := rt.Syscalls().VerifyPoSt(pvInfo); err != nil {
		rt.Abortf(exitcode.ErrIllegalArgument, "invalid PoSt %+v: %s", pvInfo, err)
	}
}

func verifySeal(rt Runtime, onChainInfo *abi.OnChainSealVerifyInfo) {
	if rt.CurrEpoch() <= onChainInfo.InteractiveEpoch {
		rt.Abortf(exitcode.ErrForbidden, "too early to prove sector")
	}

	// Check randomness.
	sealRandEarliest := rt.CurrEpoch() - ChainFinalityish - MaxSealDuration[onChainInfo.RegisteredProof]
	if onChainInfo.SealRandEpoch < sealRandEarliest {
		rt.Abortf(exitcode.ErrIllegalArgument, "seal epoch %v too old, expected >= %v", onChainInfo.SealRandEpoch, sealRandEarliest)
	}

	commD := requestUnsealedSectorCID(rt, onChainInfo.RegisteredProof, onChainInfo.DealIDs)

	minerActorID, err := addr.IDFromAddress(rt.Message().Receiver())
	AssertNoError(err) // Runtime always provides ID-addresses

	buf := new(bytes.Buffer)
	err = rt.Message().Receiver().MarshalCBOR(buf)
	AssertNoError(err)

	svInfoRandomness := rt.GetRandomness(crypto.DomainSeparationTag_SealRandomness, onChainInfo.SealRandEpoch, buf.Bytes())
	svInfoInteractiveRandomness := rt.GetRandomness(crypto.DomainSeparationTag_InteractiveSealChallengeSeed, onChainInfo.InteractiveEpoch, buf.Bytes())

	svInfo := abi.SealVerifyInfo{
		SectorID: abi.SectorID{
			Miner:  abi.ActorID(minerActorID),
			Number: onChainInfo.SectorNumber,
		},
		OnChain:               *onChainInfo,
		Randomness:            abi.SealRandomness(svInfoRandomness),
		InteractiveRandomness: abi.InteractiveSealRandomness(svInfoInteractiveRandomness),
		UnsealedCID:           commD,
	}
	if err := rt.Syscalls().VerifySeal(svInfo); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "invalid seal %+v: %s", svInfo, err)
	}
}

// Requests the storage market actor compute the unsealed sector CID from a sector's deals.
func requestUnsealedSectorCID(rt Runtime, proofType abi.RegisteredProof, dealIDs []abi.DealID) cid.Cid {
	var unsealedCID cbg.CborCid
	ret, code := rt.Send(
		builtin.StorageMarketActorAddr,
		builtin.MethodsMarket.ComputeDataCommitment,
		&market.ComputeDataCommitmentParams{
			SectorType: proofType,
			DealIDs:    dealIDs,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed request for unsealed sector CID for deals %v", dealIDs)
	AssertNoError(ret.Into(&unsealedCID))
	return cid.Cid(unsealedCID)
}

func commitWorkerKeyChange(rt Runtime) *adt.EmptyValue {
	var st State
	rt.State().Transaction(&st, func() interface{} {
		if st.Info.PendingWorkerKey == nil {
			rt.Abortf(exitcode.ErrIllegalState, "No pending key change.")
		}

		if st.Info.PendingWorkerKey.EffectiveAt > rt.CurrEpoch() {
			rt.Abortf(exitcode.ErrIllegalState, "Too early for key change. Current: %v, Change: %v)", rt.CurrEpoch(), st.Info.PendingWorkerKey.EffectiveAt)
		}

		st.Info.Worker = st.Info.PendingWorkerKey.NewWorker
		st.Info.PendingWorkerKey = nil

		return nil
	})
	return nil
}

// Verifies that the total locked balance exceeds the sum of sector initial pledges.
func verifyPledgeMeetsInitialRequirements(rt Runtime, st *State) {
	// TODO WPOST (follow-up): implement this
}

// Resolves an address to an ID address and verifies that it is address of an account or multisig actor.
func resolveOwnerAddress(rt Runtime, raw addr.Address) addr.Address {
	resolved, ok := rt.ResolveAddress(raw)
	if !ok {
		rt.Abortf(exitcode.ErrIllegalArgument, "unable to resolve address %v", raw)
	}
	Assert(resolved.Protocol() == addr.ID)

	ownerCode, ok := rt.GetActorCodeCID(resolved)
	if !ok {
		rt.Abortf(exitcode.ErrIllegalArgument, "no code for address %v", resolved)
	}
	if !builtin.IsPrincipal(ownerCode) {
		rt.Abortf(exitcode.ErrIllegalArgument, "owner actor type must be a principal, was %v", ownerCode)
	}
	return resolved
}

// Resolves an address to an ID address and verifies that it is address of an account actor with an associated BLS key.
// The worker must be BLS since the worker key will be used alongside a BLS-VRF.
func resolveWorkerAddress(rt Runtime, raw addr.Address) addr.Address {
	resolved, ok := rt.ResolveAddress(raw)
	if !ok {
		rt.Abortf(exitcode.ErrIllegalArgument, "unable to resolve address %v", raw)
	}
	Assert(resolved.Protocol() == addr.ID)

	ownerCode, ok := rt.GetActorCodeCID(resolved)
	if !ok {
		rt.Abortf(exitcode.ErrIllegalArgument, "no code for address %v", resolved)
	}
	if ownerCode != builtin.AccountActorCodeID {
		rt.Abortf(exitcode.ErrIllegalArgument, "worker actor type must be an account, was %v", ownerCode)
	}

	if raw.Protocol() != addr.BLS {
		ret, code := rt.Send(resolved, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero())
		builtin.RequireSuccess(rt, code, "failed to fetch account pubkey from %v", resolved)
		var pubkey addr.Address
		err := ret.Into(&pubkey)
		if err != nil {
			rt.Abortf(exitcode.ErrSerialization, "failed to deserialize address result: %v", ret)
		}
		if pubkey.Protocol() != addr.BLS {
			rt.Abortf(exitcode.ErrIllegalArgument, "worker account %v must have BLS pubkey, was %v", resolved, pubkey.Protocol())
		}
	}
	return resolved
}

func burnFundsAndNotifyPledgeChange(rt Runtime, amt abi.TokenAmount) {
	burnFunds(rt, amt)
	notifyPledgeChanged(rt, amt.Neg())
}

func burnFunds(rt Runtime, amt abi.TokenAmount) {
	if amt.GreaterThan(big.Zero()) {
		_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, amt)
		builtin.RequireSuccess(rt, code, "failed to burn funds")
	}
}

func notifyPledgeChanged(rt Runtime, pledgeDelta abi.TokenAmount) {
	if !pledgeDelta.IsZero() {
		_, code := rt.Send(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdatePledgeTotal, &pledgeDelta, big.Zero())
		builtin.RequireSuccess(rt, code, "failed to update total pledge")
	}
}

// Assigns proving period offset randomly in the range [0, WPoStProvingPeriod) by hashing
// the actor's address and current epoch.
func assignProvingPeriodOffset(myAddr addr.Address, currEpoch abi.ChainEpoch, hash func(data []byte) [32]byte) (abi.ChainEpoch, error) {
	offsetSeed := bytes.Buffer{}
	err := myAddr.MarshalCBOR(&offsetSeed)
	if err != nil {
		return 0, fmt.Errorf("failed to serialize address: %w", err)
	}

	err = binary.Write(&offsetSeed, binary.BigEndian, currEpoch)
	if err != nil {
		return 0, fmt.Errorf("failed to serialize epoch: %w", err)
	}

	digest := hash(offsetSeed.Bytes())
	var offset uint64
	err = binary.Read(bytes.NewBuffer(digest[:]), binary.BigEndian, &offset)
	if err != nil {
		return 0, fmt.Errorf("failed to interpret digest: %w", err)
	}

	offset = offset % uint64(WPoStProvingPeriod)
	return abi.ChainEpoch(offset), nil
}

// Computes the epoch at which a proving period should start such that it is greater than the current epoch, and
// has a defined offset from being an exact multiple of WPoStProvingPeriod.
// A miner is exempt from Winow PoSt until the first full proving period starts.
func nextProvingPeriodStart(currEpoch abi.ChainEpoch, offset abi.ChainEpoch) abi.ChainEpoch {
	currModulus := currEpoch % WPoStProvingPeriod
	var periodProgress abi.ChainEpoch // How far ahead is currEpoch from previous offset boundary.
	if currModulus >= offset {
		periodProgress = currModulus - offset
	} else {
		periodProgress = WPoStProvingPeriod - (offset - currModulus)
	}

	periodStart := currEpoch - periodProgress + WPoStProvingPeriod
	Assert(periodStart > currEpoch)
	return periodStart
}

// Computes deadline information for a fault or recovery declaration.
// If the deadline has not yet elapsed, the declaration is taken as being for the current proving period.
// If the deadline has elapsed, it's instead taken as being for the next proving period after the current epoch.
func declarationDeadlineInfo(periodStart abi.ChainEpoch, deadlineIdx uint64, currEpoch abi.ChainEpoch) (*DeadlineInfo, error) {
	if deadlineIdx >= WPoStPeriodDeadlines {
		return nil, fmt.Errorf("invalid deadline %d, must be < %d", deadlineIdx, WPoStPeriodDeadlines)
	}

	deadline := NewDeadlineInfo(periodStart, deadlineIdx, currEpoch)
	// While deadline is in the past, roll over to the next proving period..
	for deadline.HasElapsed() {
		deadline = NewDeadlineInfo(deadline.NextPeriodStart(), deadlineIdx, currEpoch)
	}
	return deadline, nil
}

// Checks that a fault or recovery declaration of sectors at a specific deadline is valid and not within
// the exclusion window for the deadline.
func validateFRDeclaration(deadlines *Deadlines, deadline *DeadlineInfo, declaredSectors *abi.BitField) error {
	if deadline.FaultCutoffPassed() {
		return fmt.Errorf("late fault or recovery declaration at %v", deadline)
	}

	// Check that the declared sectors are actually due at the deadline.
	deadlineSectors := deadlines.Due[deadline.Index]
	contains, err := abi.BitFieldContainsAll(deadlineSectors, declaredSectors)
	if err != nil {
		return fmt.Errorf("failed to check sectors at deadline: %w", err)
	}
	if !contains {
		return fmt.Errorf("sectors not all due at deadline %d", deadline.Index)
	}
	return nil
}

// Computes a fee for a collection of sectors and unlocks it from unvested funds (for burning).
// The fee computation is a parameter.
func unlockPenalty(st *State, store adt.Store, currEpoch abi.ChainEpoch, sectors []*SectorOnChainInfo,
	feeCalc func(info *SectorOnChainInfo) abi.TokenAmount) (abi.TokenAmount, error) {
	fee := big.Zero()
	for _, s := range sectors {
		fee = big.Add(fee, feeCalc(s))
	}
	return st.UnlockUnvestedFunds(store, currEpoch, fee)
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
