package miner

import (
	"bytes"
	"fmt"

	addr "github.com/filecoin-project/go-address"
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

const epochUndefined = abi.ChainEpoch(-1)

type CronEventType int64

const (
	//CronEventWindowedPoStExpiration CronEventType = iota
	CronEventWorkerKeyChange CronEventType = iota
	CronEventPreCommitExpiry
	//CronEventSectorExpiry
	CronEventProvingPeriod
	//CronEventTempFault
)

type CronEventPayload struct {
	EventType       CronEventType
	Sectors         *abi.BitField
	RegisteredProof abi.RegisteredProof // Used for PreCommitExpiry verification}
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
		14:                        a.AwardReward,
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

	var emptyDeadlines Deadlines
	emptyDeadlinesCid := rt.Store().Put(&emptyDeadlines)

	// TODO WPOST: assign proving period boundary using chain randomness
	// TODO WPOST: register cron callback on proving period boundary

	state := ConstructState(emptyArray, emptyMap, owner, worker, params.PeerId, params.SectorSize, emptyDeadlinesCid)
	rt.State().Create(state)
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
	a.enrollCronEvent(rt, effectiveEpoch, &cronPayload)
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
	if len(params.Partitions) > WPoStPartitionsMax {
		rt.Abortf(exitcode.ErrIllegalArgument, "too many partitions %d, max %d", len(params.Partitions), WPoStPartitionsMax)
	}
	if len(params.Partitions) != len(params.Proofs) {
		rt.Abortf(exitcode.ErrIllegalArgument, "proof count %d must match partition count %", len(params.Proofs), len(params.Partitions))
	}

	store := adt.AsStore(rt)
	var st State
	var recoveredSectorInfos []*SectorOnChainInfo
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)

		provingPeriodStart, hasStarted := st.ProvingPeriodStart(rt.CurrEpoch())
		if !hasStarted {
			// A miner is exempt from PoSt until the first full proving period begins after chain genesis.
			rt.Abortf(exitcode.ErrIllegalState, "invalid proving period: %v", provingPeriodStart)
		}

		// Every epoch is during some deadline's challenge window.
		// Rather than require it in the parameters, compute it from the current epoch.
		// If the submission was intended for a different window, the partitions won't match and it will be rejected.
		deadlineIndex, challengeEpoch := ComputeCurrentDeadline(provingPeriodStart, rt.CurrEpoch())

		// Verify locked funds are are at least the sum of sector initial pledges.
		// Note that this call does not actually compute recent vesting, so the reported locked funds may be
		// slightly higher than the true amount (i.e. slightly in the miner's favour).
		// Computing vesting here would be almost always redundant since vesting is quantized to ~daily units.
		// Vesting will be at most one proving period old if computed in the cron callback.
		verifyPledgeMeetsInitialRequirements(rt, &st)

		// TODO WPOST: traverse earlier submissions and enact detected faults

		deadlines := LoadDeadlines(rt, &st)

		// Work out which sectors are due in the declared partitions at this deadline.
		partitionsSectors, err := st.ComputePartitionsSectors(deadlines, deadlineIndex, params.Partitions)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to compute partitions sectors at deadline %d, partitions %s: %w",
				deadlineIndex, params.Partitions, err)
		}

		provenSectors, err := abi.BitFieldUnion(partitionsSectors...)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to union %d partitions of sectors: %v", len(partitionsSectors), err)
		}

		// TODO WPOST (follow-up): process Skipped as faults

		// Extract a fault set relevant to the sectors being submitted, for expansion into a map.
		declaredFaults, err := abi.BitFieldIntersection(provenSectors, st.Faults)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to intersect proof sectors with faults: %v", err)
		}
		declaredRecoveries, err := abi.BitFieldIntersection(declaredFaults, st.Recoveries)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to intersect recoveries with faults: %v", err)
		}
		expectedFaults, err := abi.BitFieldDifference(declaredFaults, declaredRecoveries)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to subtract recoveries from faults: %v", err)
		}

		nonFaults, err := abi.BitFieldDifference(provenSectors, expectedFaults)
		if abi.BitFieldEmpty(nonFaults) {
			rt.Abortf(exitcode.ErrIllegalArgument, "no non-faulty sectors in partitions %s", params.Partitions)
		}

		// Select a non-faulty sector as a substitute for faulty ones.
		goodSectorNo, err := abi.BitFieldFirst(nonFaults)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to get first good sector: %v", err)
		}

		// Load sector infos for proof
		sectorInfos, err := st.LoadSectorInfosWithFaultMask(store, provenSectors, expectedFaults, abi.SectorNumber(goodSectorNo))
		rt.Abortf(exitcode.ErrIllegalState, "failed to load sector infos: %v", err)

		// Verify the proof.
		// A failed verification doesn't immediately cause a penalty; the miner can try again.
		a.verifyWindowedPost(rt, challengeEpoch, sectorInfos, params.Proofs)

		// Record the successful submission
		err = st.AddPoStSubmissions(params.Partitions)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to record submissions for partitions %s: %v", params.Partitions, err)
		}

		// If the PoSt was successful, the declared recoveries should be restored
		err = st.RemoveFaults(store, declaredRecoveries)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to remove recoveries from faults: %v", err)
		}
		err = st.RemoveRecoveries(declaredRecoveries)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to remove recoveries: %v", err)
		}

		// Load info for recovered sectors for recovery of power outside this state transaction.
		if !abi.BitFieldEmpty(declaredRecoveries) {
			sectorsByNumber := map[abi.SectorNumber]*SectorOnChainInfo{}
			for _, s := range sectorInfos {
				sectorsByNumber[s.Info.SectorNumber] = s
			}

			_ = abi.BitFieldForEach(declaredRecoveries, func(i uint64) error {
				recoveredSectorInfos = append(recoveredSectorInfos, sectorsByNumber[abi.SectorNumber(i)])
				return nil
			})
		}
		return nil
	})

	// Restore power for recovered sectors.
	if len(recoveredSectorInfos) > 0 {
		a.requestEndFaults(rt, st.Info.SectorSize, recoveredSectorInfos)
	}
	return nil
}

///////////////////////
// Sector Commitment //
///////////////////////

type PreCommitSectorParams struct {
	Info SectorPreCommitInfo
}

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
		if found, err := st.HasSectorNo(store, params.SectorNumber); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to check sector %v: %v", params.SectorNumber, err)
		} else if found {
			rt.Abortf(exitcode.ErrIllegalArgument, "sector %v already committed", params.SectorNumber)
		}

		// Check expiry is on proving period boundary
		expiryMod := params.Expiration % WPoStProvingPeriod
		if expiryMod != st.ProvingPeriodBoundary {
			rt.Abortf(exitcode.ErrIllegalArgument, "invalid expiration %d, must be on proving period boundary %d mod %d",
				params.Expiration, st.ProvingPeriodBoundary, WPoStProvingPeriod)
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

	pledgeDelta := newlyVestedAmount.Neg()
	notifyPledgeChanged(rt, &pledgeDelta)

	bf := abi.NewBitField()
	bf.Set(uint64(params.SectorNumber))

	// Request deferred Cron check for PreCommit expiry check.
	cronPayload := CronEventPayload{
		EventType:       CronEventPreCommitExpiry,
		Sectors:         &bf,
		RegisteredProof: params.RegisteredProof,
	}

	msd, ok := MaxSealDuration[params.RegisteredProof]
	if !ok {
		rt.Abortf(exitcode.ErrIllegalArgument, "no max seal duration set for proof type: %d", params.RegisteredProof)
	}

	expiryBound := rt.CurrEpoch() + msd + 1
	a.enrollCronEvent(rt, expiryBound, &cronPayload)

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
		rt.Abortf(exitcode.ErrIllegalState, "No max seal duration for proof type: %d", precommit.Info.RegisteredProof)
	}
	if rt.CurrEpoch() > precommit.PreCommitEpoch+msd {
		rt.Abortf(exitcode.ErrIllegalArgument, "Invalid ProveCommitSector epoch (cur=%d, pcepoch=%d)", rt.CurrEpoch(), precommit.PreCommitEpoch)
	}

	// will abort if seal invalid
	a.verifySeal(rt, &abi.OnChainSealVerifyInfo{
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
	var dealWeight abi.DealWeight
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
	AssertNoError(ret.Into(&dealWeight))

	// Request power for activated sector.
	// Return initial pledge requirement.
	var initialPledge abi.TokenAmount
	ret, code = rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.OnSectorProveCommit,
		&power.OnSectorProveCommitParams{
			Weight: power.SectorStorageWeightDesc{
				SectorSize: st.Info.SectorSize,
				DealWeight: dealWeight,
				Duration:   precommit.Info.Expiration - rt.CurrEpoch(),
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
		if err = st.AddLockedFunds(store, rt.CurrEpoch(), initialPledge, &PledgeVestingSpec); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to add pledge: %v", err)
		}
		st.AssertBalanceInvariants(rt.CurrentBalance())

		newSectorInfo := &SectorOnChainInfo{
			Info:            precommit.Info,
			ActivationEpoch: rt.CurrEpoch(),
			DealWeight:      dealWeight,
		}

		if err = st.PutSector(store, newSectorInfo); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to prove commit: %v", err)
		}

		if err = st.DeletePrecommittedSector(store, sectorNo); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to delete precommit for sector %v: %v", sectorNo, err)
		}

		if err = st.AddSectorExpirations(store, precommit.Info.Expiration, sectorNo); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to add new sector %v expiration: %v", sectorNo, err)
		}

		// Add to new sectors, a staging ground before scheduling to a deadline at end of proving period.
		if err = st.AddNewSectors(sectorNo); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to add new sector number %v: %v", sectorNo, err)
		}

		return newlyVestedFund
	}).(abi.TokenAmount)

	pledgeDelta := big.Sub(initialPledge, newlyVestedAmount)
	notifyPledgeChanged(rt, &pledgeDelta)

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

	ret, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.OnSectorModifyWeightDesc,
		&power.OnSectorModifyWeightDescParams{
			PrevWeight: *storageWeightDescPrev,
			NewWeight:  storageWeightDescNew,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to modify sector weight")
	var newInitialPledge abi.TokenAmount
	AssertNoError(ret.Into(&newInitialPledge))

	newlyVestedAmount := rt.State().Transaction(&st, func() interface{} {
		newlyVestedFund, err := st.UnlockVestedFunds(store, rt.CurrEpoch())
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to vest funds: %s", err)
		}

		// Lock up a new initial pledge. This locks a whole new amount because the pledge provided when the sector
		// was first committed may have vested.
		availableBalance := st.GetAvailableBalance(rt.CurrentBalance())
		if availableBalance.LessThan(newInitialPledge) {
			rt.Abortf(exitcode.ErrInsufficientFunds, "not enough funds for new initial pledge requirement %s, available: %s", newInitialPledge, availableBalance)
		}
		if err = st.AddLockedFunds(store, rt.CurrEpoch(), newInitialPledge, &PledgeVestingSpec); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to add pledge: %v", err)
		}

		// Store new sector expiry.
		sector.Info.Expiration = params.NewExpiration
		if err = st.PutSector(store, sector); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to update sector %v, %v", sectorNo, err)
		}

		// Update sector expiration queue.
		err = st.RemoveSectorExpirations(store, oldExpiration, sectorNo)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to remove sector %d from expiry %d", sectorNo, oldExpiration)
		err = st.AddSectorExpirations(store, params.NewExpiration, sectorNo)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to add sector %d to expiry %d", sectorNo, params.NewExpiration)

		return newlyVestedFund
	}).(abi.TokenAmount)

	pledgeDelta := big.Sub(newInitialPledge, newlyVestedAmount)
	notifyPledgeChanged(rt, &pledgeDelta)
	return nil
}

type TerminateSectorsParams struct {
	Sectors *abi.BitField
}

func (a Actor) TerminateSectors(rt Runtime, params *TerminateSectorsParams) *adt.EmptyValue {
	var st State
	rt.State().Readonly(&st)
	rt.ValidateImmediateCallerIs(st.Info.Worker)

	sectorNos := bitfieldToSectorNos(rt, params.Sectors)

	// Note: this cannot terminate pre-committed but un-proven sectors.
	// They must be allowed to expire (and deposit burnt).
	a.terminateSectors(rt, sectorNos, power.SectorTerminationManual)
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
	Sectors  abi.BitField
}

func (a Actor) DeclareFaults(rt Runtime, params *DeclareFaultsParams) *adt.EmptyValue {
	currEpoch := rt.CurrEpoch()
	store := adt.AsStore(rt)
	var st State
	var newFaultySectors []*SectorOnChainInfo
	var penalty abi.TokenAmount
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)

		// The proving period start may be negative for low epochs, but all the arithmetic should work out
		// correctly in order to declare faults for an upcoming deadline or the next period.
		provingPeriodStart, _ := st.ProvingPeriodStart(currEpoch)
		deadlines := LoadDeadlines(rt, &st)

		var decaredSectors []abi.BitField
		for _, decl := range params.Faults {
			err := validateFaultDeclaration(currEpoch, provingPeriodStart, deadlines, decl.Deadline, decl.Sectors)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalArgument, "invalid fault declaration")
			decaredSectors = append(decaredSectors, decl.Sectors)
		}

		allFaults, err := abi.BitFieldUnion(decaredSectors...)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to union faults")

		// Split declarations into declarations of new faults, and retraction of declared recoveries.
		recoveries, err := abi.BitFieldIntersection(st.Recoveries, allFaults)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to intersect sectors with recoveries: %s", err)
		}
		newFaults, err := abi.BitFieldDifference(allFaults, recoveries)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to subtract recoveries from sectors: %s", err)
		}

		if !abi.BitFieldEmpty(newFaults) {
			// Add new faults to state and charge fee.
			err = st.AddFaults(store, newFaults, provingPeriodStart)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to add faults")

			// Load info for sectors.
			newFaultySectors, err = st.LoadSectorInfos(store, newFaults)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load fault sectors")

			// Calculate and unlock fee.
			penalty, err = unlockFaultFee(store, &st, currEpoch, newFaultySectors)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to charge fault fee")
		}

		// Remove faulty recoveries
		if !abi.BitFieldEmpty(recoveries) {
			err = st.RemoveRecoveries(recoveries)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to remove recoveries")
		}
		return nil
	})

	burnFundsAndNotifyPledgeChange(rt, penalty)

	// Remove power for new faulty sectors.
	a.requestBeginFaults(rt, st.Info.SectorSize, newFaultySectors)
	return nil
}

type DeclareFaultsRecoveredParams struct {
	Recoveries []RecoveryDeclaration
}

type RecoveryDeclaration struct {
	Deadline uint64 // In range [0..WPoStPeriodDeadlines)
	Sectors  abi.BitField
}

func (a Actor) DeclareFaultsRecovered(rt Runtime, params *DeclareFaultsRecoveredParams) *adt.EmptyValue {
	currEpoch := rt.CurrEpoch()
	var st State
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)

		provingPeriodStart, _ := st.ProvingPeriodStart(currEpoch)
		deadlines := LoadDeadlines(rt, &st)

		var declaredSectors []abi.BitField
		for _, decl := range params.Recoveries {
			err := validateFaultDeclaration(currEpoch, provingPeriodStart, deadlines, decl.Deadline, decl.Sectors)
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

func (a Actor) AwardReward(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.RewardActorAddr)
	pledgeAmount := rt.Message().ValueReceived()

	var st State
	rt.State().Transaction(&st, func() interface{} {
		if err := st.AddLockedFunds(adt.AsStore(rt), rt.CurrEpoch(), pledgeAmount, &PledgeVestingSpec); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to add pledge: %v", err)
		}
		return nil
	})

	notifyPledgeChanged(rt, &pledgeAmount)
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
	currEpoch := rt.CurrEpoch()
	earliest := currEpoch - ConsensusFaultReportingWindow

	fault, err := rt.Syscalls().VerifyConsensusFault(params.BlockHeader1, params.BlockHeader2, params.BlockHeaderExtra, earliest)
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
	notifyPledgeChanged(rt, &pledgeDelta)

	st.AssertBalanceInvariants(rt.CurrentBalance())
	return nil
}

//////////
// Cron //
//////////

func (a Actor) OnDeferredCronEvent(rt Runtime, payload *CronEventPayload) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	switch payload.EventType {
	case CronEventWorkerKeyChange:
		a.commitWorkerKeyChange(rt)
	case CronEventPreCommitExpiry:
		a.checkPrecommitExpiry(rt, payload.Sectors, payload.RegisteredProof)
	case CronEventProvingPeriod:
		a.handleProvingPeriod(rt)
	}

	return nil
}

func (a Actor) handleProvingPeriod(rt Runtime) {
	// TODO WPOST
	// - process funds vesting
	// - traverse post submission, detect missing posts
	// - traverse fault epochs, charge fees, terminate too-long faulty sectors
	// - expire sectors DO THIS SECOND
	// - assign new sectors to deadlines DO THIS FIRST
	// - [DONE] clear post submissions

	//store := adt.AsStore(rt)
	var st State

	rt.State().Transaction(&st, func() interface{} {

		// Reset PoSt submissions for next period.
		if err := st.ClearPoStSubmissions(); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to clear PoSt submissions: %v", err)
		}
		return nil
	})
}

////////////////////////////////////////////////////////////////////////////////
// Method utility functions
////////////////////////////////////////////////////////////////////////////////

func (a Actor) checkPrecommitExpiry(rt Runtime, sectors *abi.BitField, regProof abi.RegisteredProof) {
	store := adt.AsStore(rt)
	var st State

	sectorNos := bitfieldToSectorNos(rt, sectors)

	// initialize here to add together for all sectors and minimize calls across actors
	depositToBurn := abi.NewTokenAmount(0)
	rt.State().Transaction(&st, func() interface{} {
		for _, sectorNo := range sectorNos {
			sector, found, err := st.GetPrecommittedSector(store, sectorNo)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "failed to load sector %v: %v", sectorNo, err)
			}
			if !found || rt.CurrEpoch()-sector.PreCommitEpoch <= MaxSealDuration[regProof] {
				// already deleted or not yet expired
				return nil
			}

			// delete sector
			err = st.DeletePrecommittedSector(store, sectorNo)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "failed to delete precommit %v: %v", sectorNo, err)
			}
			// increment deposit to burn
			depositToBurn = big.Add(depositToBurn, sector.PreCommitDeposit)
		}

		st.PreCommitDeposits = big.Sub(st.PreCommitDeposits, depositToBurn)
		Assert(st.PreCommitDeposits.GreaterThanEqual(big.Zero()))
		return nil
	})

	// This deposit was locked separately to pledge collateral so there's no pledge change here.
	burnFunds(rt, depositToBurn)
}

// TODO: red flag that this method is potentially super expensive
func (a Actor) terminateSectors(rt Runtime, sectorNos []abi.SectorNumber, terminationType power.SectorTermination) {
	store := adt.AsStore(rt)
	var st State

	var dealIDs []abi.DealID
	var allSectors []*SectorOnChainInfo
	var faultedSectors []*SectorOnChainInfo
	var penalty abi.TokenAmount

	rt.State().Transaction(&st, func() interface{} {
		maxAllowedFaults, err := st.GetMaxAllowedFaults(store)

		if err != nil {
			rt.Abortf(exitcode.SysErrInternal, "failed to get number of sectors")
		}
		faultsMap, err := st.Faults.AllMap(maxAllowedFaults)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "too many faults")
		}

		for _, sectorNo := range sectorNos {
			sector, found, err := st.GetSector(store, sectorNo)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "failed to check sector %v: %v", sectorNo, err)
			}
			if !found {
				rt.Abortf(exitcode.ErrNotFound, "no sector %v", sectorNo)
			}

			dealIDs = append(dealIDs, sector.Info.DealIDs...)
			allSectors = append(allSectors, sector)

			_, fault := faultsMap[uint64(sectorNo)]
			if fault {
				faultedSectors = append(faultedSectors, sector)
			}

			err = st.DeleteSector(store, sectorNo)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "failed to delete sector: %v", err)
			}
		}

		if terminationType != power.SectorTerminationExpired {
			penalty, err = unlockEarlyTerminationFee(store, &st, rt.CurrEpoch(), allSectors)
		}

		return nil
	})

	// End any fault state before terminating sector power.
	// TODO: could we compress the three calls to power actor into one sector termination call?
	if len(faultedSectors) > 0 {
		a.requestEndFaults(rt, st.Info.SectorSize, faultedSectors)
	}

	a.requestTerminateDeals(rt, dealIDs)
	a.requestTerminatePower(rt, terminationType, st.Info.SectorSize, allSectors)

	burnFundsAndNotifyPledgeChange(rt, penalty)
}

func (a Actor) enrollCronEvent(rt Runtime, eventEpoch abi.ChainEpoch, callbackPayload *CronEventPayload) {
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

func (a Actor) requestBeginFaults(rt Runtime, sectorSize abi.SectorSize, sectors []*SectorOnChainInfo) {
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

func (a Actor) requestEndFaults(rt Runtime, sectorSize abi.SectorSize, sectors []*SectorOnChainInfo) {
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

func (a Actor) requestTerminateDeals(rt Runtime, dealIDs []abi.DealID) {
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

func (a Actor) requestTerminateAllDeals(rt Runtime, st *State) {
	// TODO: this is an unbounded computation. Transform into an idempotent partial computation that can be
	// progressed on each invocation.
	dealIds := []abi.DealID{}
	if err := st.ForEachSector(adt.AsStore(rt), func(sector *SectorOnChainInfo) {
		dealIds = append(dealIds, sector.Info.DealIDs...)
	}); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to traverse sectors for termination: %v", err)
	}

	a.requestTerminateDeals(rt, dealIds)
}

func (a Actor) requestTerminatePower(rt Runtime, terminationType power.SectorTermination, sectorSize abi.SectorSize, sectors []*SectorOnChainInfo) {
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

func (a Actor) verifyWindowedPost(rt Runtime, challengeEpoch abi.ChainEpoch, sectors []*SectorOnChainInfo, proofs []abi.PoStProof) {
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
	if err = rt.Syscalls().VerifyPoSt(pvInfo); err != nil {
		rt.Abortf(exitcode.ErrIllegalArgument, "invalid PoSt %+v: %s", pvInfo, err)
	}
}

func (a Actor) verifySeal(rt Runtime, onChainInfo *abi.OnChainSealVerifyInfo) {

	if rt.CurrEpoch() <= onChainInfo.InteractiveEpoch {
		rt.Abortf(exitcode.ErrForbidden, "too early to prove sector")
	}

	// Check randomness.
	sealRandEarliest := rt.CurrEpoch() - ChainFinalityish - MaxSealDuration[onChainInfo.RegisteredProof]
	if onChainInfo.SealRandEpoch < sealRandEarliest {
		rt.Abortf(exitcode.ErrIllegalArgument, "seal epoch %v too old, expected >= %v", onChainInfo.SealRandEpoch, sealRandEarliest)
	}

	commD := a.requestUnsealedSectorCID(rt, onChainInfo.RegisteredProof, onChainInfo.DealIDs)

	minerActorID, err := addr.IDFromAddress(rt.Message().Receiver())
	AssertNoError(err) // Runtime always provides ID-addresses

	svInfoRandomness := rt.GetRandomness(crypto.DomainSeparationTag_SealRandomness, onChainInfo.SealRandEpoch, nil)
	svInfoInteractiveRandomness := rt.GetRandomness(crypto.DomainSeparationTag_InteractiveSealChallengeSeed, onChainInfo.InteractiveEpoch, nil)

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
	if err = rt.Syscalls().VerifySeal(svInfo); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "invalid seal %+v: %s", svInfo, err)
	}
}

// Requests the storage market actor compute the unsealed sector CID from a sector's deals.
func (a Actor) requestUnsealedSectorCID(rt Runtime, st abi.RegisteredProof, dealIDs []abi.DealID) cid.Cid {
	var unsealedCID cbg.CborCid
	ret, code := rt.Send(
		builtin.StorageMarketActorAddr,
		builtin.MethodsMarket.ComputeDataCommitment,
		&market.ComputeDataCommitmentParams{
			SectorType: st,
			DealIDs:    dealIDs,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed request for unsealed sector CID for deals %v", dealIDs)
	AssertNoError(ret.Into(&unsealedCID))
	return cid.Cid(unsealedCID)
}

func (a Actor) commitWorkerKeyChange(rt Runtime) *adt.EmptyValue {
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

//
// Helpers
//

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
	pledgeDelta := amt.Neg()
	notifyPledgeChanged(rt, &pledgeDelta)
}

func burnFunds(rt Runtime, amt abi.TokenAmount) {
	if amt.GreaterThan(big.Zero()) {
		_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, amt)
		builtin.RequireSuccess(rt, code, "failed to burn funds")
	}
}

func notifyPledgeChanged(rt Runtime, pledgeDelta *abi.TokenAmount) {
	if !pledgeDelta.IsZero() {
		_, code := rt.Send(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdatePledgeTotal, pledgeDelta, big.Zero())
		builtin.RequireSuccess(rt, code, "failed to update total pledge")
	}
}

// Checks that a fault or recovery declaration of sectors at a specific deadline is valid and not within
// the exclusion window for the deadline.
func validateFaultDeclaration(currEpoch, periodStart abi.ChainEpoch, deadlines *Deadlines, deadlineIdx uint64, sectors abi.BitField) error {
	if deadlineIdx >= WPoStPeriodDeadlines {
		return fmt.Errorf("invalid deadline %d, must be < %d", deadlineIdx, WPoStPeriodDeadlines)
	}

	// Check if this declaration is before the fault declaration cutoff for this deadline.
	deadlineEpoch, faultCutoff := ComputeFaultDeclarationCutoff(currEpoch, periodStart, deadlineIdx)
	if currEpoch > faultCutoff {
		return fmt.Errorf("invalid fault declaration for deadline %d (epoch %d) in epoch %d, cutoff %d",
			deadlineIdx, deadlineEpoch, currEpoch, faultCutoff)
	}

	// Check that the declared sectors are actually due at the deadline.
	deadlineSectors := deadlines.Due[deadlineIdx]
	contains, err := abi.BitFieldContainsAll(deadlineSectors, sectors)
	if err != nil {
		return fmt.Errorf("failed to check sectors at deadline: %w", err)
	}
	if !contains {
		return fmt.Errorf("sectors not all due at deadline %d", deadlineIdx)
	}
	return nil
}

// Computes the current deadline index and challenge epoch for that deadline.
func ComputeCurrentDeadline(provingPeriodStart, currEpoch abi.ChainEpoch) (deadlineIdx uint64, challengeEpoch abi.ChainEpoch) {
	challengeEpoch = quantizeDown(currEpoch, WPoStChallengeWindow)
	AssertMsg(challengeEpoch >= provingPeriodStart, "invalid challenge epoch %d, proving period %d", challengeEpoch, provingPeriodStart)
	deadlineIdx = uint64((challengeEpoch - provingPeriodStart) / WPoStChallengeWindow)
	return deadlineIdx, challengeEpoch
}

// Computes the last epoch that a fault declaration may be submitted for a deadline in the current proving period.
// If the deadline has already passed in this period, the cutoff is for the subsequent period instead.
func ComputeFaultDeclarationCutoff(currEpoch, periodStart abi.ChainEpoch, deadlineIdx uint64) (deadlineEpoch, faultCutoff abi.ChainEpoch) {
	deadlineEpoch = periodStart + abi.ChainEpoch(deadlineIdx+1)*WPoStChallengeWindow
	if currEpoch > deadlineEpoch {
		// Deadline has already passed in this period, so the declaration targets the next period instead.
		deadlineEpoch += WPoStProvingPeriod
	}
	AssertMsg(deadlineEpoch >= currEpoch, "invalid deadline epoch calculation")

	faultCutoff = deadlineEpoch - (WPoStChallengeWindow + FaultDeclarationCutoff)
	return
}

// Computes the "fault fee" for a collection of sectors and unlocks it from unvested funds (for burning).
func unlockFaultFee(store adt.Store, st *State, currEpoch abi.ChainEpoch, sectors []*SectorOnChainInfo) (abi.TokenAmount, error) {
	fee := big.Zero()
	for _, s := range sectors {
		fee = big.Add(fee, pledgePenaltyForSectorFault(s))
	}
	return st.UnlockUnvestedFunds(store, currEpoch, fee)
}

// Computes the "termination fee" for a collection of sectors and unlocks it from unvested funds (for burning).
func unlockEarlyTerminationFee(store adt.Store, st *State, currEpoch abi.ChainEpoch, sectors []*SectorOnChainInfo) (abi.TokenAmount, error) {
	fee := big.Zero()
	for _, s := range sectors {
		fee = big.Add(fee, pledgePenaltyForSectorTermination(s))
	}
	return st.UnlockUnvestedFunds(store, currEpoch, fee)
}

func LoadDeadlines(rt Runtime, st *State) *Deadlines {
	var deadlines Deadlines
	ok := rt.Store().Get(st.Deadlines, &deadlines)
	if !ok {
		rt.Abortf(exitcode.ErrIllegalState, "failed to load deadlines as %s", st.Deadlines)
	}
	return &deadlines
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
