package miner

import (
	"bytes"
	"encoding/binary"
	"fmt"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	cid "github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"

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
	//CronEventProvingPeriod
	CronEventProvingDeadline
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
		17:                        a.ConfirmSectorProofsValid,
		18:                        a.ChangeMultiaddrs,
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

	_, ok := SupportedProofTypes[params.SealProofType]
	if !ok {
		rt.Abortf(exitcode.ErrIllegalArgument, "proof type %d not allowed for new miner actors", params.SealProofType)
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

	emptyDeadline := ConstructDeadline(emptyArray)
	emptyDeadlineCid := rt.Store().Put(emptyDeadline)

	emptyDeadlines := ConstructDeadlines(emptyDeadlineCid)
	emptyDeadlinesCid := rt.Store().Put(emptyDeadlines)

	currEpoch := rt.CurrEpoch()
	offset, err := assignProvingPeriodOffset(rt.Message().Receiver(), currEpoch, rt.Syscalls().HashBlake2b)
	builtin.RequireNoErr(rt, err, exitcode.ErrSerialization, "failed to assign proving period offset")
	periodStart := nextProvingPeriodStart(currEpoch, offset)
	Assert(periodStart > currEpoch)

	info, err := ConstructMinerInfo(owner, worker, params.PeerId, params.Multiaddrs, params.SealProofType)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalArgument, "failed to construct initial miner info")
	infoCid := rt.Store().Put(info)

	state, err := ConstructState(infoCid, periodStart, emptyArray, emptyMap, emptyDeadlinesCid)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalArgument, "failed to construct state")
	rt.State().Create(state)

	// Register first cron callback for epoch before the first proving period starts.
	enrollCronEvent(rt, periodStart-1, &CronEventPayload{
		EventType: CronEventProvingDeadline,
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
	info := getMinerInfo(rt, &st)
	return &GetControlAddressesReturn{
		Owner:  info.Owner,
		Worker: info.Worker,
	}
}

type ChangeWorkerAddressParams struct {
	NewWorker addr.Address
}

func (a Actor) ChangeWorkerAddress(rt Runtime, params *ChangeWorkerAddressParams) *adt.EmptyValue {
	var effectiveEpoch abi.ChainEpoch
	var st State
	rt.State().Transaction(&st, func() interface{} {
		info := getMinerInfo(rt, &st)

		rt.ValidateImmediateCallerIs(info.Owner)

		worker := resolveWorkerAddress(rt, params.NewWorker)

		effectiveEpoch = rt.CurrEpoch() + WorkerKeyChangeDelay

		// This may replace another pending key change.
		info.PendingWorkerKey = &WorkerKeyChange{
			NewWorker:   worker,
			EffectiveAt: effectiveEpoch,
		}
		err := st.SaveInfo(adt.AsStore(rt), info)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "could not save miner info")
		return nil
	})

	cronPayload := CronEventPayload{
		EventType: CronEventWorkerKeyChange,
	}
	enrollCronEvent(rt, effectiveEpoch, &cronPayload)
	return nil
}

type ChangePeerIDParams struct {
	NewID abi.PeerID
}

func (a Actor) ChangePeerID(rt Runtime, params *ChangePeerIDParams) *adt.EmptyValue {
	var st State
	rt.State().Transaction(&st, func() interface{} {
		info := getMinerInfo(rt, &st)

		rt.ValidateImmediateCallerIs(info.Worker)
		info.PeerId = params.NewID
		err := st.SaveInfo(adt.AsStore(rt), info)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "could not save miner info")
		return nil
	})
	return nil
}

type ChangeMultiaddrsParams struct {
	NewMultiaddrs []abi.Multiaddrs
}

func (a Actor) ChangeMultiaddrs(rt Runtime, params *ChangeMultiaddrsParams) *adt.EmptyValue {
	var st State
	rt.State().Transaction(&st, func() interface{} {
		info := getMinerInfo(rt, &st)
		rt.ValidateImmediateCallerIs(info.Worker)
		info.Multiaddrs = params.NewMultiaddrs
		err := st.SaveInfo(adt.AsStore(rt), info)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "could not save miner info")
		return nil
	})
	return nil
}

//////////////////
// WindowedPoSt //
//////////////////

type PoStPartition struct {
	// Partitions are numbered per-deadline, from zero.
	Index uint64
	// Sectors skipped while proving that weren't already declared faulty
	Skipped *abi.BitField
}

// Information submitted by a miner to provide a Window PoSt.
type SubmitWindowedPoStParams struct {
	// The deadline index which the submission targets.
	Deadline uint64
	// The partitions being proven.
	Partitions []PoStPartition
	// Array of proofs, one per distinct registered proof type present in the sectors being proven.
	// In the usual case of a single proof type, this array will always have a single element (independent of number of partitions).
	Proofs []abi.PoStProof
}

// Invoked by miner's worker address to submit their fallback post
func (a Actor) SubmitWindowedPoSt(rt Runtime, params *SubmitWindowedPoStParams) *adt.EmptyValue {
	currEpoch := rt.CurrEpoch()
	store := adt.AsStore(rt)
	var st State

	if params.Deadline > WPoStPeriodDeadlines {
		rt.Abortf(exitcode.ErrIllegalArgument, "invalid deadline %d of %d", params.Deadline, WPoStPeriodDeadlines)
	}
	// TODO: limit the length of proofs array https://github.com/filecoin-project/specs-actors/issues/416

	// Get the total power/reward. We need these to compute penalties.
	epochReward := requestCurrentEpochBlockReward(rt)
	pwrTotal := requestCurrentTotalPower(rt)

	newFaultPowerTotal := NewPowerPairZero()
	retractedRecoveryPowerTotal := NewPowerPairZero()
	recoveredPowerTotal := NewPowerPairZero()
	penalty := abi.NewTokenAmount(0)

	var info *MinerInfo
	rt.State().Transaction(&st, func() interface{} {
		info = getMinerInfo(rt, &st)
		rt.ValidateImmediateCallerIs(info.Worker)

		// Validate that the miner didn't try to prove too many partitions at once.
		partitionSize := info.WindowPoStPartitionSectors
		submissionPartitionLimit := windowPoStMessagePartitionsMax(partitionSize)
		if uint64(len(params.Partitions)) > submissionPartitionLimit {
			rt.Abortf(exitcode.ErrIllegalArgument, "too many partitions %d, limit %d", len(params.Partitions), submissionPartitionLimit)
		}

		// Load and check deadline.
		currDeadline := st.DeadlineInfo(currEpoch)
		deadlines, err := st.LoadDeadlines(adt.AsStore(rt))
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deadlines")

		// Double check that the current proving period has started. This should only happen if the cron actor wasn't invoked.
		// TODO deadline-cron: check instead that the prior deadline cron has executed.
		// Or remove this check now that sector deadline allocation is immediate?
		if !currDeadline.PeriodStarted() {
			rt.Abortf(exitcode.ErrIllegalState, "proving period %d not yet open at %d", currDeadline.PeriodStart, currEpoch)
		}

		// The miner may only submit a proof for the current deadline.
		if params.Deadline != currDeadline.Index {
			rt.Abortf(exitcode.ErrIllegalArgument, "invalid deadline %d at epoch %d, expected %d",
				params.Deadline, currEpoch, currDeadline.Index)
		}

		deadline, err := deadlines.LoadDeadline(store, params.Deadline)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deadline %d", params.Deadline)

		partitions, err := deadline.PartitionsArray(store)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deadline %d partitions", params.Deadline)

		faultExpiration := currDeadline.NextOpen() + FaultMaxAge

		partitionIdxs := make([]uint64, 0, len(params.Partitions))
		allSectors := make([]*abi.BitField, 0, len(params.Partitions))
		allFaults := make([]*abi.BitField, 0, len(params.Partitions))

		// Check and record skipped faults for each partition.
		// Accumulate sectors info for proof verification.
		for _, post := range params.Partitions {
			key := PartitionKey{params.Deadline, post.Index}
			alreadyProven, err := deadline.PostSubmissions.IsSet(post.Index)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to check if partition %v already posted", key)
			if alreadyProven {
				// Skip partitions already proven for this deadline.
				continue
			}

			var partition Partition
			found, err := partitions.Get(post.Index, &partition)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load partition %v", key)
			if !found {
				rt.Abortf(exitcode.ErrNotFound, "no such partition %v", key)
			}

			// Process new faults and accumulate new faulty power.
			newFaultPower, retractedRecoveryPower := processSkippedFaults(rt, &st, store, info.SectorSize, faultExpiration, &partition, post.Skipped)

			// Process recoveries, assuming the proof will be successful.
			// This will be rolled back if the method aborts with a failed proof.
			recoveredPower := processRecoveries(rt, &st, store, info.SectorSize, &partition)

			err = partitions.Set(post.Index, &partition)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to update partition %v", key)

			newFaultPowerTotal = newFaultPowerTotal.Add(newFaultPower)
			retractedRecoveryPowerTotal = retractedRecoveryPowerTotal.Add(retractedRecoveryPower)
			recoveredPowerTotal = recoveredPowerTotal.Add(recoveredPower)

			// At this point, the partition faults represents the expected faults for the proof, with new skipped
			// faults and recoveries taken into account.
			partitionIdxs = append(partitionIdxs, post.Index)
			allSectors = append(allSectors, partition.Sectors)
			allFaults = append(allFaults, partition.Faults)
		}

		// TODO minerstate: factor out a method for the miner node to calculate and load all sectors for proof,
		// with reference to faults, skipped etc

		// Load sector infos for proof, substituting a known-good sector for known-faulty sectors.
		// Note: this is slightly sub-optimal, loading info for the recovering sectors again after they were already
		// loaded above.
		sectorInfos, err := st.LoadSectorInfosForProof(store, allSectors, allFaults)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load proven sector info")

		// Skip verification if all sectors are faults.
		// We still need to allow this call to succeed so the miner can declare a whole partition as skipped.
		if len(sectorInfos) > 0 {
			// Verify the proof.
			// A failed verification doesn't immediately cause a penalty; the miner can try again.
			verifyWindowedPost(rt, currDeadline.Challenge, sectorInfos, params.Proofs)
		}

		// Penalize new skipped faults and retracted recoveries.
		// Faults declared here pay a higher fee than those declared earlier or ongoing.
		// Retracted recoveries pay a high fee, rather than no fee if declared "on time".
		// TODO deadline-cron: defer power loss and penalty to end of deadline
		// XXX at the deadline cron the faulty sectors will pay FF. Here we need to charge SP-FF (or defer the charge of SP)
		penalizedPower := newFaultPowerTotal.Add(retractedRecoveryPowerTotal)
		penaltyTarget := PledgePenaltyForUndeclaredFault(epochReward, pwrTotal.QualityAdjPower, penalizedPower.QA)
		penalty, err = st.UnlockUnvestedFunds(store, currEpoch, penaltyTarget)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to unlock penalty for %v", penalizedPower)

		// Record the successful submission
		postedPartitions := bitfield.NewFromSet(partitionIdxs)
		err = st.AddPoStSubmissions(postedPartitions)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to record submissions for deadline %d partitions %s",
			params.Deadline, postedPartitions)
		return nil
	})

	// Restore power for recovered sectors. Remove power for new faults.
	requestUpdatePower(rt, recoveredPowerTotal.Sub(newFaultPowerTotal))
	// Burn penalties.
	burnFundsAndNotifyPledgeChange(rt, penalty)
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
	if params.SealRandEpoch >= rt.CurrEpoch() {
		rt.Abortf(exitcode.ErrIllegalArgument, "seal challenge epoch %v must be before now %v", params.SealRandEpoch, rt.CurrEpoch())
	}
	challengeEarliest := sealChallengeEarliest(rt.CurrEpoch(), params.SealProof)
	if params.SealRandEpoch < challengeEarliest {
		// The subsequent commitment proof can't possibly be accepted because the seal challenge will be deemed
		// too old. Note that passing this check doesn't guarantee the proof will be soon enough, depending on
		// when it arrives.
		rt.Abortf(exitcode.ErrIllegalArgument, "seal challenge epoch %v too old, must be after %v", params.SealRandEpoch, challengeEarliest)
	}
	if params.ReplaceCapacity && len(params.DealIDs) == 0 {
		rt.Abortf(exitcode.ErrIllegalArgument, "cannot replace sector without committing deals")
	}

	// gather information from other actors
	epochReward := requestCurrentEpochBlockReward(rt)
	pwrTotal := requestCurrentTotalPower(rt)
	dealWeight := requestDealWeight(rt, params.DealIDs, rt.CurrEpoch(), params.Expiration)
	circulatingSupply := rt.TotalFilCircSupply()

	store := adt.AsStore(rt)
	var st State
	newlyVestedAmount := rt.State().Transaction(&st, func() interface{} {
		info := getMinerInfo(rt, &st)
		rt.ValidateImmediateCallerIs(info.Worker)
		if params.SealProof != info.SealProofType {
			rt.Abortf(exitcode.ErrIllegalArgument, "sector seal proof %v must match miner seal proof type %d", params.SealProof, info.SealProofType)
		}

		maxDealLimit := dealPerSectorLimit(info.SectorSize)
		if uint64(len(params.DealIDs)) > maxDealLimit {
			rt.Abortf(exitcode.ErrIllegalArgument, "too many deals for sector %d > %d", len(params.DealIDs), maxDealLimit)
		}

		_, preCommitFound, err := st.GetPrecommittedSector(store, params.SectorNumber)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to check pre-commit %v", params.SectorNumber)
		if preCommitFound {
			rt.Abortf(exitcode.ErrIllegalArgument, "sector %v already pre-committed", params.SectorNumber)
		}

		sectorFound, err := st.HasSectorNo(store, params.SectorNumber)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to check sector %v", params.SectorNumber)
		if sectorFound {
			rt.Abortf(exitcode.ErrIllegalArgument, "sector %v already committed", params.SectorNumber)
		}

		validateExpiration(rt, &st, rt.CurrEpoch(), params.Expiration, params.SealProof)

		depositMinimum := big.Zero()
		if params.ReplaceCapacity {
			replaceSector := validateReplaceSector(rt, &st, store, params)
			// Note the replaced sector's initial pledge as a lower bound for the new sector's deposit
			depositMinimum = replaceSector.InitialPledge
		}

		newlyVestedFund, err := st.UnlockVestedFunds(store, rt.CurrEpoch())
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to vest funds")
		availableBalance := st.GetAvailableBalance(rt.CurrentBalance())
		duration := params.Expiration - rt.CurrEpoch()

		sectorWeight := QAPowerForWeight(info.SectorSize, duration, dealWeight.DealWeight, dealWeight.VerifiedDealWeight)
		depositReq := big.Max(
			precommitDeposit(sectorWeight, pwrTotal.QualityAdjPower, pwrTotal.PledgeCollateral, epochReward, circulatingSupply),
			depositMinimum,
		)
		if availableBalance.LessThan(depositReq) {
			rt.Abortf(exitcode.ErrInsufficientFunds, "insufficient funds for pre-commit deposit: %v", depositReq)
		}

		st.AddPreCommitDeposit(depositReq)
		st.AssertBalanceInvariants(rt.CurrentBalance())

		if err := st.PutPrecommittedSector(store, &SectorPreCommitOnChainInfo{
			Info:               *params,
			PreCommitDeposit:   depositReq,
			PreCommitEpoch:     rt.CurrEpoch(),
			DealWeight:         dealWeight.DealWeight,
			VerifiedDealWeight: dealWeight.VerifiedDealWeight,
		}); err != nil {
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

	msd, ok := MaxSealDuration[params.SealProof]
	if !ok {
		rt.Abortf(exitcode.ErrIllegalArgument, "no max seal duration set for proof type: %d", params.SealProof)
	}

	// The +1 here is critical for the batch verification of proofs. Without it, if a proof arrived exactly on the
	// due epoch, ProveCommitSector would accept it, then the expiry event would remove it, and then
	// ConfirmSectorProofsValid would fail to find it.
	expiryBound := rt.CurrEpoch() + msd + 1
	enrollCronEvent(rt, expiryBound, &cronPayload)

	return nil
}

type ProveCommitSectorParams struct {
	SectorNumber abi.SectorNumber
	Proof        []byte
}

// Checks state of the corresponding sector pre-commitment, then schedules the proof to be verified in bulk
// by the power actor.
// If valid, the power actor will call ConfirmSectorProofsValid at the end of the same epoch as this message.
func (a Actor) ProveCommitSector(rt Runtime, params *ProveCommitSectorParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerAcceptAny()

	store := adt.AsStore(rt)
	var st State
	rt.State().Readonly(&st)

	// Verify locked funds are are at least the sum of sector initial pledges.
	// Note that this call does not actually compute recent vesting, so the reported locked funds may be
	// slightly higher than the true amount (i.e. slightly in the miner's favour).
	// Computing vesting here would be almost always redundant since vesting is quantized to ~daily units.
	// Vesting will be at most one proving period old if computed in the cron callback.
	verifyPledgeMeetsInitialRequirements(rt, &st)

	sectorNo := params.SectorNumber
	precommit, found, err := st.GetPrecommittedSector(store, sectorNo)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load pre-committed sector %v", sectorNo)
	if !found {
		rt.Abortf(exitcode.ErrNotFound, "no pre-committed sector %v", sectorNo)
	}

	msd, ok := MaxSealDuration[precommit.Info.SealProof]
	if !ok {
		rt.Abortf(exitcode.ErrIllegalState, "no max seal duration for proof type: %d", precommit.Info.SealProof)
	}
	proveCommitDue := precommit.PreCommitEpoch + msd
	if rt.CurrEpoch() > proveCommitDue {
		rt.Abortf(exitcode.ErrIllegalArgument, "commitment proof for %d too late at %d, due %d", sectorNo, rt.CurrEpoch(), proveCommitDue)
	}

	svi := getVerifyInfo(rt, &SealVerifyStuff{
		SealedCID:           precommit.Info.SealedCID,
		InteractiveEpoch:    precommit.PreCommitEpoch + PreCommitChallengeDelay,
		SealRandEpoch:       precommit.Info.SealRandEpoch,
		Proof:               params.Proof,
		DealIDs:             precommit.Info.DealIDs,
		SectorNumber:        precommit.Info.SectorNumber,
		RegisteredSealProof: precommit.Info.SealProof,
	})

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.SubmitPoRepForBulkVerify,
		svi,
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to submit proof for bulk verification")
	return nil
}

func (a Actor) ConfirmSectorProofsValid(rt Runtime, params *builtin.ConfirmSectorProofsParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	// 1. Activate deals, skipping pre-commits with invalid deals.
	//    - calls the market actor.
	// 2. Reschedule replacement sector expiration.
	//    - loads and saves sectors
	//    - loads and saves deadlines/partitions
	// 3. Add new sectors.
	//    - loads and saves sectors.
	//    - loads and saves deadlines/partitions
	//
	// Ideally, we'd combine some of these operations, but at least we have
	// a constant number of them.

	var st State
	rt.State().Readonly(&st)
	store := adt.AsStore(rt)
	info := getMinerInfo(rt, &st)

	//
	// Activate storage deals.
	//

	// This skips missing pre-commits.
	precommittedSectors, err := st.GetExistingPrecommittedSectors(store, params.Sectors...)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load pre-committed sectors")

	// Committed-capacity sectors licensed for early removal by new sectors being proven.
	var replaceSectorLocations []SectorLocation
	// Pre-commits for new sectors.
	var preCommits []*SectorPreCommitOnChainInfo
	for _, precommit := range precommittedSectors {
		// Check (and activate) storage deals associated to sector. Abort if checks failed.
		// TODO: we should batch these calls...
		// https://github.com/filecoin-project/specs-actors/issues/474
		_, code := rt.Send(
			builtin.StorageMarketActorAddr,
			builtin.MethodsMarket.ActivateDeals,
			&market.ActivateDealsParams{
				DealIDs:      precommit.Info.DealIDs,
				SectorExpiry: precommit.Info.Expiration,
			},
			abi.NewTokenAmount(0),
		)

		if code != exitcode.Ok {
			// TODO #564 log: "failed to activate deals on sector %d, dropping from prove commit set"
			continue
		}

		preCommits = append(preCommits, precommit)

		if precommit.Info.ReplaceCapacity {
			replaceSectorLocations = append(replaceSectorLocations, precommit.Info.ReplaceSector)
		}
	}

	// When all prove commits have failed abort early
	if len(preCommits) == 0 {
		rt.Abortf(exitcode.ErrIllegalArgument, "all prove commits failed to validate")
	}

	totalPledge := big.Zero()
	newSectors := make([]*SectorOnChainInfo, 0)
	newlyVestedAmount := rt.State().Transaction(&st, func() interface{} {
		// Schedule expiration for replaced sectors.
		err = st.RescheduleSectorExpirations(store, rt.CurrEpoch(), info.SectorSize, replaceSectorLocations)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to replace sector expirations")

		newSectorNos := make([]abi.SectorNumber, 0, len(preCommits))
		for _, precommit := range preCommits {
			// initial pledge is precommit deposit
			initialPledge := precommit.PreCommitDeposit
			totalPledge = big.Add(totalPledge, initialPledge)
			newSectorInfo := SectorOnChainInfo{
				SectorNumber:       precommit.Info.SectorNumber,
				SealProof:          precommit.Info.SealProof,
				SealedCID:          precommit.Info.SealedCID,
				DealIDs:            precommit.Info.DealIDs,
				Expiration:         precommit.Info.Expiration,
				Activation:         precommit.PreCommitEpoch,
				DealWeight:         precommit.DealWeight,
				VerifiedDealWeight: precommit.VerifiedDealWeight,
				InitialPledge:      initialPledge,
			}
			newSectors = append(newSectors, &newSectorInfo)
			newSectorNos = append(newSectorNos, newSectorInfo.SectorNumber)
		}

		err = st.PutSectors(store, newSectors...)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to put new sectors")

		err = st.DeletePrecommittedSectors(store, newSectorNos...)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to delete precommited sectors")

		err = st.AssignSectorsToDeadlines(store, rt.CurrEpoch(), newSectors...)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to assign new sectors to deadlines")

		// Add sector and pledge lock-up to miner state
		newlyVestedFund, err := st.UnlockVestedFunds(store, rt.CurrEpoch())
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to vest new funds: %s", err)
		}

		// Unlock deposit for successful proofs, make it available for lock-up as initial pledge.
		st.AddPreCommitDeposit(totalPledge.Neg())
		st.AddInitialPledgeRequirement(totalPledge)

		// Lock up initial pledge for new sectors.
		availableBalance := st.GetAvailableBalance(rt.CurrentBalance())
		if availableBalance.LessThan(totalPledge) {
			rt.Abortf(exitcode.ErrInsufficientFunds, "insufficient funds for aggregate initial pledge requirement %s, available: %s", totalPledge, availableBalance)
		}
		if err := st.AddLockedFunds(store, rt.CurrEpoch(), totalPledge, &PledgeVestingSpec); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to add aggregate pledge: %v", err)
		}
		st.AssertBalanceInvariants(rt.CurrentBalance())

		return newlyVestedFund
	}).(abi.TokenAmount)

	// Request power and pledge update for activated sector.
	requestUpdatePowerForSectors(rt, info.SectorSize, newSectors, nil)
	notifyPledgeChanged(rt, big.Sub(totalPledge, newlyVestedAmount))

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

	if _, found, err := st.GetSector(store, sectorNo); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to load proven sector %v", sectorNo)
	} else if !found {
		rt.Abortf(exitcode.ErrNotFound, "sector %v not proven", sectorNo)
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
	info := getMinerInfo(rt, &st)
	rt.ValidateImmediateCallerIs(info.Worker)

	store := adt.AsStore(rt)
	sectorNo := params.SectorNumber
	oldSector, found, err := st.GetSector(store, sectorNo)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load sector %v", sectorNo)
	if !found {
		rt.Abortf(exitcode.ErrNotFound, "no such sector %v", sectorNo)
	}
	if params.NewExpiration < oldSector.Expiration {
		rt.Abortf(exitcode.ErrIllegalArgument, "cannot reduce sector expiration to %d from %d",
			params.NewExpiration, oldSector.Expiration)
	}

	validateExpiration(rt, &st, oldSector.Activation, params.NewExpiration, oldSector.SealProof)

	newSector := *oldSector
	newSector.Expiration = params.NewExpiration
	qaPowerDelta := big.Sub(QAPowerForSector(info.SectorSize, &newSector), QAPowerForSector(info.SectorSize, oldSector))

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.UpdateClaimedPower,
		&power.UpdateClaimedPowerParams{
			RawByteDelta:         big.Zero(), // Sector size has not changed
			QualityAdjustedDelta: qaPowerDelta,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to modify sector weight")

	// Store new sector expiry.
	rt.State().Transaction(&st, func() interface{} {
		err = st.PutSectors(store, &newSector)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to update sector %v", sectorNo)

		// move expiration from old epoch to new
		err = st.RemoveSectorExpirations(store, oldSector)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to remove sector expiration %v at %d", sectorNo, oldSector.Expiration)
		err = st.AddSectorExpirations(store, &newSector)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to add sector expiration %v at %d", sectorNo, newSector.Expiration)
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
	info := getMinerInfo(rt, &st)
	rt.ValidateImmediateCallerIs(info.Worker)

	epochReward := requestCurrentEpochBlockReward(rt)
	pwrTotal := requestCurrentTotalPower(rt)
	// Note: this cannot terminate pre-committed but un-proven sectors.
	// They must be allowed to expire (and deposit burnt).
	terminateSectors(rt, params.Sectors, power.SectorTerminationManual, epochReward, pwrTotal.QualityAdjPower)
	return nil
}

////////////
// Faults //
////////////

type DeclareFaultsParams struct {
	Faults []FaultDeclaration
}

type FaultDeclaration struct {
	// The deadline to which the faulty sectors are assigned, in range [0..WPoStPeriodDeadlines)
	Deadline uint64
	// Partition index within the deadline containing the faulty sectors.
	Partition uint64
	// Sectors in the partition being declared faulty.
	Sectors *abi.BitField
}

func (a Actor) DeclareFaults(rt Runtime, params *DeclareFaultsParams) *adt.EmptyValue {
	if uint64(len(params.Faults)) > FaultMaxPartitions {
		rt.Abortf(exitcode.ErrIllegalArgument, "too many declarations %d, max %d", len(params.Faults), FaultMaxPartitions)
	}
	// TODO: limit the number of sectors declared at once https://github.com/filecoin-project/specs-actors/issues/416

	currEpoch := rt.CurrEpoch()
	store := adt.AsStore(rt)
	var st State
	var info *MinerInfo
	newFaultPowerTotal := NewPowerPairZero()
	rt.State().Transaction(&st, func() interface{} {
		info = getMinerInfo(rt, &st)
		rt.ValidateImmediateCallerIs(info.Worker)

		deadlines, err := st.LoadDeadlines(adt.AsStore(rt))
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deadlines")

		// Group declarations by deadline, and remember iteration order.
		declsByDeadline := map[uint64][]*FaultDeclaration{}
		var deadlinesToLoad []uint64

		for _, decl := range params.Faults {
			deadlinesToLoad = append(deadlinesToLoad, decl.Deadline)
			declsByDeadline[decl.Deadline] = append(declsByDeadline[decl.Deadline], &decl)
		}

		for _, dlIdx := range deadlinesToLoad {
			targetDeadline, err := declarationDeadlineInfo(st.ProvingPeriodStart, dlIdx, currEpoch)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalArgument, "invalid fault declaration deadline %d", dlIdx)
			err = validateFRDeclarationDeadline(targetDeadline)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalArgument, "failed fault declaration at deadline %d", dlIdx)

			deadline, err := deadlines.LoadDeadline(store, dlIdx)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deadline %d", dlIdx)

			partitions, err := deadline.PartitionsArray(store)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load partitions for deadline %d", dlIdx)

			// Record partitions with some fault, for subsequently indexing in the deadline.
			// Duplicate entries don't matter, they'll be stored in a bitfield (a set).
			partitionsWithFault := make([]uint64, 0, len(declsByDeadline))
			faultExpirationEpoch := targetDeadline.NextOpen() + FaultMaxAge

			for _, decl := range declsByDeadline[dlIdx] {
				key := PartitionKey{dlIdx, decl.Partition}
				var partition Partition
				found, err := partitions.Get(decl.Partition, &partition)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load partition %v", key)
				if !found {
					rt.Abortf(exitcode.ErrNotFound, "no such partition %v", key)
				}

				err = validateFRDeclarationPartition(key, &partition, decl.Sectors)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalArgument, "failed fault declaration for partition %v", key)

				// Split declarations into declarations of new faults, and retraction of declared recoveries.
				retractedRecoveries, err := bitfield.IntersectBitField(partition.Recoveries, decl.Sectors)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to intersect sectors with recoveries")

				newFaults, err := bitfield.SubtractBitField(decl.Sectors, retractedRecoveries)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to subtract recoveries from sectors")
				// Ignore any previously declared or detected faults
				newFaults, err = bitfield.SubtractBitField(newFaults, partition.Faults)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to subtract existing faults from faults")

				// Add new faults to state.
				empty, err := newFaults.IsEmpty()
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to check if bitfield was empty")
				if !empty {
					newFaultSectors, err := st.LoadSectorInfos(store, newFaults)
					builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load fault sectors")

					newFaultPower, err := partition.AddFaults(store, newFaults, newFaultSectors, faultExpirationEpoch, info.SectorSize)
					builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to add faults")

					newFaultPowerTotal = newFaultPowerTotal.Add(newFaultPower)
					partitionsWithFault = append(partitionsWithFault, decl.Partition)
				}

				// Remove faulty recoveries from state.
				empty, err = retractedRecoveries.IsEmpty()
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to check if bitfield was empty")
				if !empty {
					retractedRecoverySectors, err := st.LoadSectorInfos(store, retractedRecoveries)
					builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load recovery sectors")
					retractedRecoveryPower := PowerForSectors(info.SectorSize, retractedRecoverySectors)

					err = partition.RemoveRecoveries(retractedRecoveries, retractedRecoveryPower)
					builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to remove recoveries")
				}

				err = partitions.Set(decl.Partition, &partition)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to store partition %v", key)
			}
			deadline.Partitions, err = partitions.Root()
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to store deadline %d partitions root", dlIdx)

			err = deadline.AddFaultEpochPartitions(store, currEpoch, partitionsWithFault...)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to update fault epochs for deadline %d", dlIdx)

			err = deadlines.UpdateDeadline(store, dlIdx, deadline)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to store deadline %d partitions", dlIdx)
		}

		return nil
	})

	// Remove power for new faulty sectors.
	// TODO: defer power loss until the respective deadline https://github.com/filecoin-project/specs-actors/issues/414
	requestUpdatePower(rt, newFaultPowerTotal.Neg())

	// Payment of penalty for declared faults is deferred to the deadline cron.
	return nil
}

type DeclareFaultsRecoveredParams struct {
	Recoveries []RecoveryDeclaration
}

type RecoveryDeclaration struct {
	// The deadline to which the faulty sectors are assigned, in range [0..WPoStPeriodDeadlines)
	Deadline uint64
	// Partition index within the deadline containing the faulty sectors.
	Partition uint64
	// Sectors in the partition being declared faulty.
	Sectors *abi.BitField
}

func (a Actor) DeclareFaultsRecovered(rt Runtime, params *DeclareFaultsRecoveredParams) *adt.EmptyValue {
	if uint64(len(params.Recoveries)) > FaultMaxPartitions {
		rt.Abortf(exitcode.ErrIllegalArgument, "too many declarations %d, max %d", len(params.Recoveries), FaultMaxPartitions)
	}

	currEpoch := rt.CurrEpoch()
	store := adt.AsStore(rt)
	var st State
	rt.State().Transaction(&st, func() interface{} {
		info := getMinerInfo(rt, &st)
		rt.ValidateImmediateCallerIs(info.Worker)

		deadlines, err := st.LoadDeadlines(adt.AsStore(rt))
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deadlines")

		// Group declarations by deadline, and remember iteration order.
		declsByDeadline := map[uint64][]*RecoveryDeclaration{}
		var deadlinesToLoad []uint64

		for _, decl := range params.Recoveries {
			deadlinesToLoad = append(deadlinesToLoad, decl.Deadline)
			declsByDeadline[decl.Deadline] = append(declsByDeadline[decl.Deadline], &decl)
		}

		for _, dlIdx := range deadlinesToLoad {
			targetDeadline, err := declarationDeadlineInfo(st.ProvingPeriodStart, dlIdx, currEpoch)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalArgument, "invalid recovery declaration deadline %d", dlIdx)
			err = validateFRDeclarationDeadline(targetDeadline)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalArgument, "failed recovery declaration at deadline %d", dlIdx)

			deadline, err := deadlines.LoadDeadline(store, dlIdx)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deadline %d", dlIdx)

			partitions, err := deadline.PartitionsArray(store)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load partitions for deadline %d", dlIdx)

			for _, decl := range declsByDeadline[dlIdx] {
				key := PartitionKey{dlIdx, decl.Partition}
				var partition Partition
				found, err := partitions.Get(decl.Partition, &partition)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load partition %v", key)
				if !found {
					rt.Abortf(exitcode.ErrNotFound, "no such partition %v", key)
				}

				err = validateFRDeclarationPartition(key, &partition, decl.Sectors)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalArgument, "failed recovery declaration for partition %v", key)

				// Ignore sectors not faulty or already declared recovered
				recoveries, err := bitfield.IntersectBitField(decl.Sectors, partition.Faults)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to intersect recoveries with faults")
				recoveries, err = bitfield.SubtractBitField(recoveries, partition.Recoveries)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to subtract existing recoveries")

				// Record the new recoveries for processing at Window PoSt or deadline cron.
				recoverySectors, err := st.LoadSectorInfos(store, recoveries)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load recovery sectors")
				recoveryPower := PowerForSectors(info.SectorSize, recoverySectors)

				err = partition.AddRecoveries(recoveries, recoveryPower)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to add recoveries")
			}
		}
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
		info := getMinerInfo(rt, &st)
		rt.ValidateImmediateCallerIs(info.Worker, info.Owner, builtin.RewardActorAddr)

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

	// Reward reporter with a share of the miner's current balance.
	slasherReward := RewardForConsensusSlashReport(faultAge, rt.CurrentBalance())
	_, code := rt.Send(reporter, builtin.MethodSend, nil, slasherReward)
	builtin.RequireSuccess(rt, code, "failed to reward reporter")

	var st State
	rt.State().Readonly(&st)

	// Notify power actor with lock-up total being removed.
	_, code = rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.OnConsensusFault,
		&st.LockedFunds,
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to notify power actor on consensus fault")

	// close deals and burn funds
	terminateMiner(rt)

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
	var info *MinerInfo
	newlyVestedAmount := rt.State().Transaction(&st, func() interface{} {
		info = getMinerInfo(rt, &st)
		rt.ValidateImmediateCallerIs(info.Owner)
		newlyVestedFund, err := st.UnlockVestedFunds(adt.AsStore(rt), rt.CurrEpoch())
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to vest fund: %v", err)
		}

		// Verify locked funds are are at least the sum of sector initial pledges after vesting.
		// TODO: simplify this just to refuse to vest if pledge requirement is unmet https://github.com/filecoin-project/specs-actors/issues/537
		verifyPledgeMeetsInitialRequirements(rt, &st)

		return newlyVestedFund
	}).(abi.TokenAmount)

	currBalance := rt.CurrentBalance()
	amountWithdrawn := big.Min(st.GetAvailableBalance(currBalance), params.AmountRequested)
	Assert(amountWithdrawn.LessThanEqual(currBalance))

	_, code := rt.Send(info.Owner, builtin.MethodSend, nil, amountWithdrawn)
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
	//case CronEventProvingPeriod:
	//	handleProvingPeriod(rt)
	case CronEventProvingDeadline:
		handleProvingDeadline(rt)
	case CronEventPreCommitExpiry:
		if payload.Sectors != nil {
			checkPrecommitExpiry(rt, payload.Sectors)
		}
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
	{
		// Establish new proving sets and clear proofs.
		rt.State().Transaction(&st, func() interface{} {
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

func handleProvingDeadline(rt Runtime) {
	currEpoch := rt.CurrEpoch()
	store := adt.AsStore(rt)

	epochReward := requestCurrentEpochBlockReward(rt)
	pwrTotal := requestCurrentTotalPower(rt)

	powerDelta := PowerPair{big.Zero(), big.Zero()}
	newlyVested := big.Zero()
	penalty := abi.NewTokenAmount(0)
	var st State
	rt.State().Transaction(&st, func() interface{} {
		var err error
		info := getMinerInfo(rt, &st)

		{
			// Vest locked funds.
			// This happens first so that any subsequent penalties are taken from locked pledge, rather than free funds.
			newlyVested, err = st.UnlockVestedFunds(store, rt.CurrEpoch())
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to vest funds")
		}

		// Note: because the cron actor is not invoked on epochs with empty tipsets, the current epoch is not necessarily
		// exactly the final epoch of the deadline; it may be slightly later (i.e. in the subsequent deadline/period).
		// Further, this method is invoked once *before* the first proving period starts, after the actor is first
		// constructed; this is detected by !dlInfo.PeriodStarted().
		// Use dlInfo.PeriodEnd() rather than rt.CurrEpoch unless certain of the desired semantics.
		dlInfo := st.DeadlineInfo(currEpoch)
		if !dlInfo.PeriodStarted() {
			return nil // Skip checking faults on the first, incomplete period.
		}
		deadlines, err := st.LoadDeadlines(store)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deadlines")
		deadline, err := deadlines.LoadDeadline(store, dlInfo.Index)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deadline %d", dlInfo.Index)
		partitions, err := deadline.PartitionsArray(store)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load partitions for deadline %d", dlInfo.Index)

		{
			// Detect and penalize missing proofs.
			faultExpiration := dlInfo.NextOpen() + FaultMaxAge
			penalizePowerTotal := big.Zero()

			for i := uint64(0); i < partitions.Length(); i++ {
				key := PartitionKey{dlInfo.Index, i}
				proven, err := deadline.PostSubmissions.IsSet(i)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to check submission for %v", key)
				if proven {
					continue
				}

				var partition Partition
				found, err := partitions.Get(i, &partition)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load partitions %v", key)
				if !found {
					rt.Abortf(exitcode.ErrIllegalState, "no partition %v", key)
				}

				newFaultPower, failedRecoveryPower, err := partition.RecordMissedPost(store, faultExpiration)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to record missed PoSt for %v", key)

				// Failed recoveries attract a penalty, but not repeated subtraction of the power.
				powerDelta = powerDelta.Sub(newFaultPower)
				penalizePowerTotal = big.Sum(penalizePowerTotal, newFaultPower.QA, failedRecoveryPower.QA)

				// Save new partition state.
				err = partitions.Set(i, &partition)
				builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to update partition %v", key)
			}

			// Unlock sector penalty for all undeclared faults.
			penaltyTarget := PledgePenaltyForLateUndeclaredFault(epochReward, pwrTotal.QualityAdjPower, penalizePowerTotal)
			penalty, err = st.UnlockUnvestedFunds(store, currEpoch, penaltyTarget)

			// Reset PoSt submissions.
			deadline.PostSubmissions = abi.NewBitField()
		}
		{
			// Terminate sectors with faults that are too old, and pay fees for ongoing faults.
			// Note that this must happen before expirations are processed in order to ensure that sectors faulty
			// at expiration pay their final penalty.

			deadline.PopExpiredFaults()

			expiredFaults := abi.NewBitField()
			ongoingFaults := abi.NewBitField()
			ongoingFaultPenalty := abi.NewTokenAmount(0)
			expiredFaults, ongoingFaults, err = popExpiredFaults(&st, store, deadline.PeriodEnd()-FaultMaxAge)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load fault epochs")

			// Load info for ongoing faults.
			// TODO: this is potentially super expensive for a large miner with ongoing faults
			// https://github.com/filecoin-project/specs-actors/issues/411
			ongoingFaultInfos, err := st.LoadSectorInfos(store, ongoingFaults)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load fault sectors")

			// Unlock penalty for ongoing faults.
			ongoingFaultPenalty, err = unlockDeclaredFaultPenalty(&st, store, info.SectorSize, deadline.PeriodEnd(), epochReward, pwrTotal.QualityAdjPower, ongoingFaultInfos)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to charge fault fee")

			// FIXME outside txn
			terminateSectors(rt, expiredFaults, power.SectorTerminationFaulty, epochReward, pwrTotal.QualityAdjPower)
			burnFundsAndNotifyPledgeChange(rt, ongoingFaultPenalty)
		}
		{
			// Expire sectors that are due.
			// XXX: Assuming that this updates all the deadline and partition state for the expiring sectors,
			// marking them terminated, subtracting power etc.
			// XXX: This assumes that the power recorded with the expiration queue entries excludes
			// already-faulty power. Fault detection/declaration/recovery needs to update the expiration queue.
			expired, err := deadline.PopExpiredSectors(store, dlInfo.Close)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load expired sectors")

			// XXX: the deals are not terminated yet, that is left for a defrag. Since the sector is
			// terminating healthily, this just results in the miner's collateral being locked up.
			// Maybe we should have a deal termination queue to do this work, but spread out.

			// Record power reduction
			powerDelta = powerDelta.Sub(expired.ActivePower)

			// The expired sectors' pledge is not released until defrag.
		}

		// Save new deadline state.
		err = deadlines.UpdateDeadline(store, dlInfo.Index, deadline)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to update deadline %v", dlInfo.Index)

		// TODO: bump current deadline index forward
		return nil
	})

	// Remove power for new faults, and burn penalties.
	requestUpdatePower(rt, powerDelta)
	burnFunds(rt, penalty)
	notifyPledgeChanged(rt, big.Add(newlyVested, penalty).Neg())
}

// Discovers how skipped faults declared during post intersect with existing faults and recoveries, then unlocks funds
// and returns the penalty to be paid and the sector infos needed to deduct power.
// - Skipped faults that are not in the current deadline will trigger an error.
// - Skipped faults that have previously been marked recovered, will be penalized as a retracted recovery but will not
// result in a change in power (the power has already been removed).
// - Skipped faults that are already declared, but not recovered will be ignored.
// - The rest will be penalized as undeclared faults and have their power removed.
// FIXME update comment
func processSkippedFaults(rt Runtime, st *State, store adt.Store, ssize abi.SectorSize, faultExpiration abi.ChainEpoch,
	partition *Partition, skipped *bitfield.BitField) (newFaultPower, retractedRecoveryPower PowerPair) {
	empty, err := skipped.IsEmpty()
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalArgument, "failed to check if skipped sectors is empty")
	if empty {
		return NewPowerPairZero(), NewPowerPairZero()
	}

	// Check that the declared sectors are actually in the partition.
	contains, err := abi.BitFieldContainsAll(partition.Sectors, skipped)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to check if skipped faults are in partition")
	if !contains {
		rt.Abortf(exitcode.ErrIllegalArgument, "skipped faults contains sectors outside partition")
	}

	// Find all skipped faults that have been labeled recovered
	retractedRecoveries, err := bitfield.IntersectBitField(partition.Recoveries, skipped)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to intersect sectors with recoveries")
	retractedRecoverySectors, err := st.LoadSectorInfos(store, retractedRecoveries)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load sectors")
	retractedRecoveryPower = PowerForSectors(ssize, retractedRecoverySectors)

	// Ignore skipped faults that are already faults
	newFaults, err := bitfield.SubtractBitField(skipped, partition.Faults)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to subtract existing faults from skipped")
	newFaultSectors, err := st.LoadSectorInfos(store, newFaults)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load sectors")

	// Record new faults
	newFaultPower, err = partition.AddFaults(store, newFaults, newFaultSectors, faultExpiration, ssize)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to add skipped faults")

	// Remove faulty recoveries
	err = partition.RemoveRecoveries(retractedRecoveries, retractedRecoveryPower)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to remove recoveries")

	return newFaultPower, retractedRecoveryPower
}

func processRecoveries(rt Runtime, st *State, store adt.Store, ssize abi.SectorSize, partition *Partition) PowerPair {
	recoveredSectors, err := st.LoadSectorInfos(store, partition.Recoveries)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load recovered sectors")

	recoveredPower, err := partition.RemoveFaults(store, partition.Recoveries, recoveredSectors, ssize)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to remove recoveries from faults")

	err = partition.RemoveRecoveries(partition.Recoveries, recoveredPower)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to remove recoveries")

	return recoveredPower
}

// Check expiry is exactly *the epoch before* the start of a proving period.
func validateExpiration(rt Runtime, st *State, activation, expiration abi.ChainEpoch, sealProof abi.RegisteredSealProof) {
	// expiration cannot exceed MaxSectorExpirationExtension from now
	if expiration > rt.CurrEpoch()+MaxSectorExpirationExtension {
		rt.Abortf(exitcode.ErrIllegalArgument, "invalid expiration %d, cannot be more than %d past current epoch %d",
			expiration, MaxSectorExpirationExtension, rt.CurrEpoch())
	}

	// total sector lifetime cannot exceed SectorMaximumLifetime for the sector's seal proof
	if expiration-activation > sealProof.SectorMaximumLifetime() {
		rt.Abortf(exitcode.ErrIllegalArgument, "invalid expiration %d, total sector lifetime (%d) cannot exceed %d after activation %d",
			expiration, expiration-activation, sealProof.SectorMaximumLifetime(), activation)
	}

	// ensure expiration is one epoch before a proving period boundary
	periodOffset := st.ProvingPeriodStart % WPoStProvingPeriod
	expiryOffset := (expiration + 1) % WPoStProvingPeriod
	if expiryOffset != periodOffset {
		rt.Abortf(exitcode.ErrIllegalArgument, "invalid expiration %d, must be immediately before proving period boundary %d mod %d",
			expiration, periodOffset, WPoStProvingPeriod)
	}
}

func validateReplaceSector(rt Runtime, st *State, store adt.Store, params *SectorPreCommitInfo) *SectorOnChainInfo {
	replaceSector, found, err := st.GetSector(store, params.ReplaceSector.SectorNumber)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load sector %v", params.SectorNumber)
	if !found {
		rt.Abortf(exitcode.ErrNotFound, "no such sector %v to replace", params.ReplaceSector.SectorNumber)
	}

	if len(replaceSector.DealIDs) > 0 {
		rt.Abortf(exitcode.ErrIllegalArgument, "cannot replace sector %v which has deals", params.ReplaceSector.SectorNumber)
	}
	if params.SealProof != replaceSector.SealProof {
		rt.Abortf(exitcode.ErrIllegalArgument, "cannot replace sector %v seal proof %v with seal proof %v",
			params.ReplaceSector.SectorNumber, replaceSector.SealProof, params.SealProof)
	}
	if params.Expiration < replaceSector.Expiration {
		rt.Abortf(exitcode.ErrIllegalArgument, "cannot replace sector %v expiration %v with sooner expiration %v",
			params.ReplaceSector.SectorNumber, replaceSector.Expiration, params.Expiration)
	}

	status, err := st.SectorStatus(store, params.ReplaceSector)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to check sector health %v", params.ReplaceSector)

	switch status {
	case SectorNotFound:
		rt.Abortf(exitcode.ErrIllegalArgument, "sector %d not found at %d:%d (deadline:partition)",
			params.ReplaceSector.SectorNumber, params.ReplaceSector.Deadline, params.ReplaceSector.Partition,
		)
	case SectorFaulty:
		rt.Abortf(exitcode.ErrIllegalArgument, "cannot replace faulty sector %d", params.ReplaceSector.SectorNumber)
	case SectorTerminated:
		rt.Abortf(exitcode.ErrIllegalArgument, "cannot replace terminated sector %d", params.ReplaceSector.SectorNumber)
	case SectorHealthy:
		// pass
	default:
		// TODO: Just panic?
		rt.Abortf(exitcode.ErrIllegalState, "unexpected sector status %d", status)
	}

	return replaceSector
}

// scheduleReplaceSectorsExpiration re-schedules sector expirations at the end
// of the current proving period.
//
// This function also updates the Expiration field of the passed-in sector
// slice, in-place.
func scheduleReplaceSectorsExpiration(rt Runtime, st *State, store adt.Store, replaceSectors []*SectorOnChainInfo) error {
	if len(replaceSectors) == 0 {
		return nil
	}

	// Mark replaced sectors for on-time expiration at the end of the current proving period.
	// They can't be removed right now because they may yet be challenged for Window PoSt in this period,
	// and the deadline assignments can't be changed mid-period.
	// If their initial pledge hasn't finished vesting yet, it just continues vesting (like other termination paths).
	// Note that a sector's weight and power were calculated from its lifetime when the sector was first
	// committed, but are not recalculated here. We only get away with this because we know the replaced sector
	// has no deals and so its power does not vary with lifetime.
	// That's a very brittle constraint, and would be much better with two-phase termination (where we could
	// deduct power immediately).
	// See https://github.com/filecoin-project/specs-actors/issues/535
	deadlineInfo := st.DeadlineInfo(rt.CurrEpoch())
	newExpiration := deadlineInfo.PeriodEnd()

	if err := st.RemoveSectorExpirations(store, replaceSectors...); err != nil {
		return xerrors.Errorf("when removing expirations for replacement: %w", err)
	}

	for _, sector := range replaceSectors {
		sector.Expiration = newExpiration
	}

	if err := st.PutSectors(store, replaceSectors...); err != nil {
		return xerrors.Errorf("when updating sector expirations: %w", err)
	}

	if err := st.AddSectorExpirations(store, replaceSectors...); err != nil {
		return xerrors.Errorf("when replacing sector expirations: %w", err)
	}
	return nil
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

	allExpiries, err := bitfield.MultiMerge(expiredFaults...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to union expired faults: %w", err)
	}
	allOngoing, err := bitfield.MultiMerge(ongoingFaults...)
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
		var sectorNos []abi.SectorNumber
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

			// mark it for deletion
			sectorNos = append(sectorNos, sectorNo)

			// increment deposit to burn
			depositToBurn = big.Add(depositToBurn, sector.PreCommitDeposit)
			return nil
		})
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to check pre-commit expiries")

		// Actually delete it.
		if len(sectorNos) > 0 {
			err = st.DeletePrecommittedSectors(store, sectorNos...)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to delete pre-commits")
		}

		st.PreCommitDeposits = big.Sub(st.PreCommitDeposits, depositToBurn)
		Assert(st.PreCommitDeposits.GreaterThanEqual(big.Zero()))
		return nil
	})

	// This deposit was locked separately to pledge collateral so there's no pledge change here.
	burnFunds(rt, depositToBurn)
}

// TODO: red flag that this method is potentially super expensive
// https://github.com/filecoin-project/specs-actors/issues/483
func terminateSectors(rt Runtime, sectorNos *abi.BitField, terminationType power.SectorTermination, epochReward abi.TokenAmount, currentTotalPower abi.StoragePower) {
	currentEpoch := rt.CurrEpoch()
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
	var terminatedSectors []*SectorOnChainInfo
	var faultySectors []*SectorOnChainInfo
	penalty := abi.NewTokenAmount(0)
	var info *MinerInfo
	rt.State().Transaction(&st, func() interface{} {
		info = getMinerInfo(rt, &st)
		maxAllowedFaults, err := st.GetMaxAllowedFaults(store)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load fault max")

		// Narrow faults to just the set that are expiring, before expanding to a map.
		faults, err := bitfield.IntersectBitField(sectorNos, st.Faults)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load faults")

		faultsMap, err := faults.AllMap(maxAllowedFaults)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to expand faults")
		}

		// sectors we're not terminating because they either don't exist or have expired.
		var notTerminatedSectorNos []uint64
		pledgeRequirementToRemove := abi.NewTokenAmount(0)
		err = sectorNos.ForEach(func(sectorNo uint64) error {
			sector, found, err := st.GetSector(store, abi.SectorNumber(sectorNo))
			if err != nil {
				return fmt.Errorf("failed to load sector %v: %w", sectorNo, err)
			}

			// If the sector wasn't found, skip termination. It may
			// have already expired, have been terminated due to a
			// faults, etc.
			if !found {
				notTerminatedSectorNos = append(notTerminatedSectorNos, sectorNo)
				return nil
			}

			// If we're terminating the sector because it expired,
			// make sure it has actually expired.
			//
			// We can hit this case if the sector was changed
			// somehow but the termination wasn't removed from the
			// SectorExpirations queue.
			//
			// TODO: This should not be possible. We should consider
			// failing in this case.
			if terminationType == power.SectorTerminationExpired && currentEpoch < sector.Expiration {
				notTerminatedSectorNos = append(notTerminatedSectorNos, sectorNo)
				return nil
			}

			dealIDs = append(dealIDs, sector.DealIDs...)
			terminatedSectors = append(terminatedSectors, sector)

			_, fault := faultsMap[sectorNo]
			if fault {
				faultySectors = append(faultySectors, sector)
			}

			pledgeRequirementToRemove = big.Add(pledgeRequirementToRemove, sector.InitialPledge)
			return nil
		})
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load sector metadata")

		terminatedSectorNos, err := bitfield.SubtractBitField(sectorNos, bitfield.NewFromSet(notTerminatedSectorNos))
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to subtract skipped sector set")

		// lower initial pledge requirement
		st.AddInitialPledgeRequirement(pledgeRequirementToRemove.Neg())

		deadlines, err := st.LoadDeadlines(store)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load deadlines")

		err = removeTerminatedSectors(&st, store, deadlines, terminatedSectorNos)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to delete sectors: %v", err)
		}

		err = st.SaveDeadlines(store, deadlines)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to store new deadlines")

		if terminationType != power.SectorTerminationExpired {

			// Remove scheduled expirations.
			err = st.RemoveSectorExpirations(store, terminatedSectors...)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to remove expirations for terminated sectors")

			// Unlock penalty.
			penalty, err = unlockTerminationPenalty(&st, store, info.SectorSize, rt.CurrEpoch(), epochReward, currentTotalPower, terminatedSectors)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "failed to unlock penalty %s", err)
			}
		}
		return nil
	})

	// TODO: could we compress the multiple calls to power actor into one sector termination call?
	// https://github.com/filecoin-project/specs-actors/issues/478
	// Compute the power delta as if recovering all the currently-faulty sectors before terminating all of them.
	requestUpdatePowerForSectors(rt, info.SectorSize, faultySectors, terminatedSectors)
	requestTerminateDeals(rt, dealIDs)

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

func requestUpdatePowerForSectors(rt Runtime, sectorSize abi.SectorSize, sectorsAdded, sectorsRemoved []*SectorOnChainInfo) (delta PowerPair) {
	if len(sectorsAdded)+len(sectorsRemoved) == 0 {
		return
	}
	addPower := PowerForSectors(sectorSize, sectorsAdded)
	remPower := PowerForSectors(sectorSize, sectorsRemoved)
	delta = addPower.Sub(remPower)
	requestUpdatePower(rt, delta)
	return delta
}

func requestUpdatePower(rt Runtime, delta PowerPair) {
	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.UpdateClaimedPower,
		&power.UpdateClaimedPowerParams{
			RawByteDelta:         delta.Raw,
			QualityAdjustedDelta: delta.QA,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to update power with %v", delta)
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

func requestTerminateAllDeals(rt Runtime, st *State) { //nolint:deadcode,unused
	// TODO: red flag this is an ~unbounded computation.
	// Transform into an idempotent partial computation that can be progressed on each invocation.
	// https://github.com/filecoin-project/specs-actors/issues/483
	dealIds := []abi.DealID{}
	if err := st.ForEachSector(adt.AsStore(rt), func(sector *SectorOnChainInfo) {
		dealIds = append(dealIds, sector.DealIDs...)
	}); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to traverse sectors for termination: %v", err)
	}

	requestTerminateDeals(rt, dealIds)
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

// SealVerifyParams is the structure of information that must be sent with a
// message to commit a sector. Most of this information is not needed in the
// state tree but will be verified in sm.CommitSector. See SealCommitment for
// data stored on the state tree for each sector.
type SealVerifyStuff struct {
	SealedCID        cid.Cid        // CommR
	InteractiveEpoch abi.ChainEpoch // Used to derive the interactive PoRep challenge.
	abi.RegisteredSealProof
	Proof   []byte
	DealIDs []abi.DealID
	abi.SectorNumber
	SealRandEpoch abi.ChainEpoch // Used to tie the seal to a chain.
}

func getVerifyInfo(rt Runtime, params *SealVerifyStuff) *abi.SealVerifyInfo {
	if rt.CurrEpoch() <= params.InteractiveEpoch {
		rt.Abortf(exitcode.ErrForbidden, "too early to prove sector")
	}

	// Check randomness.
	challengeEarliest := sealChallengeEarliest(rt.CurrEpoch(), params.RegisteredSealProof)
	if params.SealRandEpoch < challengeEarliest {
		rt.Abortf(exitcode.ErrIllegalArgument, "seal epoch %v too old, expected >= %v", params.SealRandEpoch, challengeEarliest)
	}

	commD := requestUnsealedSectorCID(rt, params.RegisteredSealProof, params.DealIDs)

	minerActorID, err := addr.IDFromAddress(rt.Message().Receiver())
	AssertNoError(err) // Runtime always provides ID-addresses

	buf := new(bytes.Buffer)
	err = rt.Message().Receiver().MarshalCBOR(buf)
	AssertNoError(err)

	svInfoRandomness := rt.GetRandomness(crypto.DomainSeparationTag_SealRandomness, params.SealRandEpoch, buf.Bytes())
	svInfoInteractiveRandomness := rt.GetRandomness(crypto.DomainSeparationTag_InteractiveSealChallengeSeed, params.InteractiveEpoch, buf.Bytes())

	return &abi.SealVerifyInfo{
		SealProof: params.RegisteredSealProof,
		SectorID: abi.SectorID{
			Miner:  abi.ActorID(minerActorID),
			Number: params.SectorNumber,
		},
		DealIDs:               params.DealIDs,
		InteractiveRandomness: abi.InteractiveSealRandomness(svInfoInteractiveRandomness),
		Proof:                 params.Proof,
		Randomness:            abi.SealRandomness(svInfoRandomness),
		SealedCID:             params.SealedCID,
		UnsealedCID:           commD,
	}
}

// Closes down this miner by erasing its power, terminating all its deals and burning its funds
func terminateMiner(rt Runtime) {
	var st State
	rt.State().Readonly(&st)

	requestTerminateAllDeals(rt, &st)

	// Delete the actor and burn all remaining funds
	rt.DeleteActor(builtin.BurntFundsActorAddr)
}

// Requests the storage market actor compute the unsealed sector CID from a sector's deals.
func requestUnsealedSectorCID(rt Runtime, proofType abi.RegisteredSealProof, dealIDs []abi.DealID) cid.Cid {
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
	var unsealedCID cbg.CborCid
	AssertNoError(ret.Into(&unsealedCID))
	return cid.Cid(unsealedCID)
}

func requestDealWeight(rt Runtime, dealIDs []abi.DealID, sectorStart, sectorExpiry abi.ChainEpoch) market.VerifyDealsForActivationReturn {
	var dealWeights market.VerifyDealsForActivationReturn
	ret, code := rt.Send(
		builtin.StorageMarketActorAddr,
		builtin.MethodsMarket.VerifyDealsForActivation,
		&market.VerifyDealsForActivationParams{
			DealIDs:      dealIDs,
			SectorStart:  sectorStart,
			SectorExpiry: sectorExpiry,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to verify deals and get deal weight")
	AssertNoError(ret.Into(&dealWeights))
	return dealWeights

}

func commitWorkerKeyChange(rt Runtime) *adt.EmptyValue {
	var st State
	rt.State().Transaction(&st, func() interface{} {
		info := getMinerInfo(rt, &st)
		if info.PendingWorkerKey == nil {
			rt.Abortf(exitcode.ErrIllegalState, "No pending key change.")
		}

		if info.PendingWorkerKey.EffectiveAt > rt.CurrEpoch() {
			rt.Abortf(exitcode.ErrIllegalState, "Too early for key change. Current: %v, Change: %v)", rt.CurrEpoch(), info.PendingWorkerKey.EffectiveAt)
		}

		info.Worker = info.PendingWorkerKey.NewWorker
		info.PendingWorkerKey = nil
		err := st.SaveInfo(adt.AsStore(rt), info)
		builtin.RequireNoErr(rt, err, exitcode.ErrSerialization, "failed to save miner info")

		return nil
	})
	return nil
}

// Requests the current epoch target block reward from the reward actor.
func requestCurrentEpochBlockReward(rt Runtime) abi.TokenAmount {
	rwret, code := rt.Send(builtin.RewardActorAddr, builtin.MethodsReward.ThisEpochReward, nil, big.Zero())
	builtin.RequireSuccess(rt, code, "failed to check epoch reward")
	epochReward := abi.NewTokenAmount(0)
	err := rwret.Into(&epochReward)
	builtin.RequireNoErr(rt, err, exitcode.ErrSerialization, "failed to unmarshal epoch reward value")
	return epochReward
}

// Requests the current network total power and pledge from the power actor.
func requestCurrentTotalPower(rt Runtime) *power.CurrentTotalPowerReturn {
	pwret, code := rt.Send(builtin.StoragePowerActorAddr, builtin.MethodsPower.CurrentTotalPower, nil, big.Zero())
	builtin.RequireSuccess(rt, code, "failed to check current power")
	var pwr power.CurrentTotalPowerReturn
	err := pwret.Into(&pwr)
	builtin.RequireNoErr(rt, err, exitcode.ErrSerialization, "failed to unmarshal power total value")
	return &pwr
}

// Verifies that the total locked balance exceeds the sum of sector initial pledges.
func verifyPledgeMeetsInitialRequirements(rt Runtime, st *State) {
	if st.LockedFunds.LessThan(st.InitialPledgeRequirement) {
		rt.Abortf(exitcode.ErrInsufficientFunds, "locked funds insufficient to cover initial pledges (%v < %v)",
			st.LockedFunds, st.InitialPledgeRequirement)
	}
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

// Checks that a fault or recovery declaration at a specific deadline is outside the exclusion window for the deadline.
func validateFRDeclarationDeadline(deadline *DeadlineInfo) error {
	if deadline.FaultCutoffPassed() {
		return fmt.Errorf("late fault or recovery declaration at %v", deadline)
	}
	return nil
}

// Validates that a fault or recovery declaration for a partition is valid.
func validateFRDeclarationPartition(key PartitionKey, partition *Partition, sectors *abi.BitField) error {
	// Check that the declared sectors are actually assigned to the partition.
	contains, err := abi.BitFieldContainsAll(partition.Sectors, sectors)
	if err != nil {
		return fmt.Errorf("failed to check sectors for %v: %w", key, err)
	}
	if !contains {
		return fmt.Errorf("sectors not all due at %v", key)
	}
	return nil
}

// qaPowerForSectors sums the quality adjusted power of all sectors
func qaPowerForSectors(sectorSize abi.SectorSize, sectors []*SectorOnChainInfo) abi.StoragePower {
	power := big.Zero()
	for _, s := range sectors {
		power = big.Add(power, QAPowerForSector(sectorSize, s))
	}
	return power
}

// XXX minerstate: deprecate these, compute the power first
func unlockDeclaredFaultPenalty(st *State, store adt.Store, sectorSize abi.SectorSize, currEpoch abi.ChainEpoch, epochTargetReward abi.TokenAmount, networkQAPower abi.StoragePower, sectors []*SectorOnChainInfo) (abi.TokenAmount, error) {
	totalQAPower := qaPowerForSectors(sectorSize, sectors)
	fee := PledgePenaltyForDeclaredFault(epochTargetReward, networkQAPower, totalQAPower)
	return st.UnlockUnvestedFunds(store, currEpoch, fee)
}

func unlockTerminationPenalty(st *State, store adt.Store, sectorSize abi.SectorSize, currEpoch abi.ChainEpoch, epochTargetReward abi.TokenAmount, networkQAPower abi.StoragePower, sectors []*SectorOnChainInfo) (abi.TokenAmount, error) {
	totalFee := big.Zero()
	for _, s := range sectors {
		sectorPower := QAPowerForSector(sectorSize, s)
		fee := PledgePenaltyForTermination(s.InitialPledge, currEpoch-s.Activation, epochTargetReward, networkQAPower, sectorPower)
		totalFee = big.Add(fee, totalFee)
	}
	return st.UnlockUnvestedFunds(store, currEpoch, totalFee)
}

func PowerForSector(sectorSize abi.SectorSize, sector *SectorOnChainInfo) PowerPair {
	return PowerPair{
		Raw: big.NewIntUnsigned(uint64(sectorSize)),
		QA:  QAPowerForSector(sectorSize, sector),
	}
}

// Returns the sum of the raw byte and quality-adjusted power for sectors.
func PowerForSectors(ssize abi.SectorSize, sectors []*SectorOnChainInfo) PowerPair {
	return PowerPair{
		Raw: big.Mul(big.NewIntUnsigned(uint64(ssize)), big.NewIntUnsigned(uint64(len(sectors)))),
		QA:  qaPowerForSectors(ssize, sectors),
	}
}

// The oldest seal challenge epoch that will be accepted in the current epoch.
func sealChallengeEarliest(currEpoch abi.ChainEpoch, proof abi.RegisteredSealProof) abi.ChainEpoch {
	return currEpoch - ChainFinality - MaxSealDuration[proof]
}

func getMinerInfo(rt Runtime, st *State) *MinerInfo {
	info, err := st.GetInfo(adt.AsStore(rt))
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "could not read miner info")
	return info
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minEpoch(a, b abi.ChainEpoch) abi.ChainEpoch {
	if a < b {
		return a
	}
	return b
}
