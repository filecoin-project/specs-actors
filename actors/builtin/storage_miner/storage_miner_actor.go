package storage_miner

import (
	"bytes"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	storage_market "github.com/filecoin-project/specs-actors/actors/builtin/storage_market"
	storage_power "github.com/filecoin-project/specs-actors/actors/builtin/storage_power"
	crypto "github.com/filecoin-project/specs-actors/actors/crypto"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	autil "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type Runtime = vmr.Runtime

var Assert = autil.Assert
var AssertNoError = autil.AssertNoError

const epochUndefined = abi.ChainEpoch(-1)

type Actor struct{}

type CronEventType int64

const (
	CronEventType_Miner_SurpriseExpiration = CronEventType(1)
	CronEventType_Miner_WorkerKeyChange    = CronEventType(2)
	CronEventType_Miner_PreCommitExpiry    = CronEventType(3)
	CronEventType_Miner_SectorExpiry       = CronEventType(4)
	CronEventType_Miner_TempFault          = CronEventType(5)
)

type CronEventPayload struct {
	EventType CronEventType
	Sectors   []abi.SectorNumber // Empty for global events, such as SurprisePoSt expiration.
}

/////////////////
// Constructor //
/////////////////

// Storage miner actors are created exclusively by the storage power actor. In order to break a circular dependency
// between the two, the construction parameters are defined in the power actor.
type ConstructorParams = storage_power.MinerConstructorParams

func (a *Actor) Constructor(rt Runtime, params *ConstructorParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	// TODO: fix this, check that the account actor at the other end of this address has a BLS key.
	if params.WorkerAddr.Protocol() != addr.BLS {
		rt.Abort(exitcode.ErrIllegalArgument, "Worker Key must be BLS.")
	}

	var st State
	rt.State().Transaction(&st, func() interface{} {
		state, err := ConstructState(adt.AsStore(rt), params.OwnerAddr, params.WorkerAddr, params.PeerId, params.SectorSize)
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to construct initial state: %v", err)
		}
		return state
	})
	return &adt.EmptyValue{}
}

///////////////////////
// Worker Key Change //
///////////////////////

type StageWorkerKeyChangeParams struct {
	newKey addr.Address
}

func (a *Actor) StageWorkerKeyChange(rt Runtime, params *StageWorkerKeyChangeParams) *adt.EmptyValue {
	var st State
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Owner)

		// must be BLS since the worker key will be used alongside a BLS-VRF
		// Specifically, this check isn't quite right
		// TODO: check that the account actor at the other end of this address has a BLS key.
		if params.newKey.Protocol() != addr.BLS {
			rt.Abort(exitcode.ErrIllegalArgument, "Worker Key must be BLS.")
		}

		keyChange := WorkerKeyChange{
			NewWorker:   params.newKey,
			EffectiveAt: rt.CurrEpoch() + WorkerKeyChangeDelay,
		}

		// note that this may replace another pending key change
		st.Info.PendingWorkerKey = keyChange

		cronPayload := CronEventPayload{
			EventType: CronEventType_Miner_WorkerKeyChange,
			Sectors:   nil,
		}
		a.enrollCronEvent(rt, rt.CurrEpoch()+keyChange.EffectiveAt, &cronPayload)

		return nil
	})
	return &adt.EmptyValue{}
}

func (a *Actor) _rtCommitWorkerKeyChange(rt Runtime) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.CronActorAddr)

	var st State
	rt.State().Transaction(&st, func() interface{} {
		if (st.Info.PendingWorkerKey == WorkerKeyChange{}) {
			rt.Abort(exitcode.ErrIllegalState, "No pending key change.")
		}

		if st.Info.PendingWorkerKey.EffectiveAt > rt.CurrEpoch() {
			rt.Abort(exitcode.ErrIllegalState, "Too early for key change. Current: %v, Change: %v)", rt.CurrEpoch(), st.Info.PendingWorkerKey.EffectiveAt)
		}

		st.Info.Worker = st.Info.PendingWorkerKey.NewWorker
		st.Info.PendingWorkerKey = WorkerKeyChange{}

		return nil
	})
	return &adt.EmptyValue{}
}

//////////////////
// SurprisePoSt //
//////////////////

// Called by Actor to notify StorageMiner of SurprisePoSt Challenge.
func (a *Actor) OnSurprisePoStChallenge(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	var st State
	challenged := rt.State().Transaction(&st, func() interface{} {
		// If already challenged, do not challenge again.
		// Failed PoSt will automatically reset the state to not-challenged.
		if st.PoStState.isChallenged() {
			return false
		}

		var curRecBuf bytes.Buffer
		err := rt.CurrReceiver().MarshalCBOR(&curRecBuf)
		autil.AssertNoError(err)

		st.PoStState = PoStState{
			LastSuccessfulPoSt:     st.PoStState.LastSuccessfulPoSt,
			SurpriseChallengeEpoch: rt.CurrEpoch(),
			NumConsecutiveFailures: st.PoStState.NumConsecutiveFailures,
		}
		return true
	}).(bool)

	if challenged {
		// Request deferred Cron check for SurprisePoSt challenge expiry.
		cronPayload := CronEventPayload{
			EventType: CronEventType_Miner_SurpriseExpiration,
			Sectors:   nil,
		}
		surpriseDuration := storage_power.SurprisePostChallengeDuration
		a.enrollCronEvent(rt, rt.CurrEpoch()+surpriseDuration, &cronPayload)
	}
	return &adt.EmptyValue{}
}

type SubmitSurprisePoStResponseParams struct {
	onChainInfo abi.OnChainSurprisePoStVerifyInfo
}

// Invoked by miner's worker address to submit a response to a pending SurprisePoSt challenge.
func (a *Actor) SubmitSurprisePoStResponse(rt Runtime, params *SubmitSurprisePoStResponseParams) *adt.EmptyValue {
	var st State
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)
		if !st.PoStState.isChallenged() {
			rt.Abort(exitcode.ErrIllegalState, "Not currently challenged")
		}
		a.verifySurprisePost(rt, &st, &params.onChainInfo)

		st.PoStState = PoStState{
			LastSuccessfulPoSt:     rt.CurrEpoch(),
			SurpriseChallengeEpoch: epochUndefined,
			NumConsecutiveFailures: 0,
		}
		return nil
	})

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.Method_StoragePowerActor_OnMinerSurprisePoStSuccess,
		&adt.EmptyValue{},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to notify storage power actor")
	return &adt.EmptyValue{}
}

// Called by Actor.
func (a *Actor) OnDeleteMiner(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)
	minerAddr := rt.CurrReceiver()
	rt.DeleteActor(minerAddr)
	return &adt.EmptyValue{}
}

//////////////////
// ElectionPoSt //
//////////////////

// Called by the VM interpreter once an ElectionPoSt has been verified.
func (a *Actor) OnVerifiedElectionPoSt(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	if rt.ToplevelBlockWinner() != rt.CurrReceiver() {
		rt.Abort(exitcode.ErrForbidden, "receiver must be miner of this block")
	}

	var st State
	rt.State().Transaction(&st, func() interface{} {
		updateSuccessEpoch := st.PoStState.isPoStOk()

		// Advance the timestamp of the most recent PoSt success, provided the miner has not faulted
		// in normal state. (Cannot do this if miner has missed a SurprisePoSt.)
		if updateSuccessEpoch {
			st.PoStState = PoStState{
				LastSuccessfulPoSt:     rt.CurrEpoch(),
				SurpriseChallengeEpoch: st.PoStState.SurpriseChallengeEpoch, // expected to be undef because PoStState is OK
				NumConsecutiveFailures: st.PoStState.NumConsecutiveFailures, // expected to be 0
			}
		}
		return nil
	})
	return &adt.EmptyValue{}
}

///////////////////////
// Sector Commitment //
///////////////////////

type PreCommitSectorParams struct {
	info SectorPreCommitInfo
}

// Proposals must be posted on chain via sma.PublishStorageDeals before PreCommitSector.
// Optimization: PreCommitSector could contain a list of deals that are not published yet.
func (a *Actor) PreCommitSector(rt Runtime, params *PreCommitSectorParams) *adt.EmptyValue {
	var st State
	rt.State().Readonly(&st)
	rt.ValidateImmediateCallerIs(st.Info.Worker)

	store := adt.AsStore(rt)
	if found, err := st.hasSectorNo(store, params.info.SectorNumber); err != nil {
		rt.Abort(exitcode.ErrIllegalState, "failed to check sector %v: %v", params.info.SectorNumber, err)
	} else if found {
		rt.Abort(exitcode.ErrIllegalArgument, "sector %v already committed", params.info.SectorNumber)
	}

	depositReq := precommitDeposit(st.getSectorSize(), params.info.Expiration-rt.CurrEpoch())
	confirmPaymentAndRefundChange(rt, depositReq)

	// TODO HS Check on valid SealEpoch

	rt.State().Transaction(&st, func() interface{} {
		err := st.putPrecommittedSector(store, &SectorPreCommitOnChainInfo{
			Info:             params.info,
			PreCommitDeposit: depositReq,
			PreCommitEpoch:   rt.CurrEpoch(),
		})
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to write pre-committed sector %v: %v", params.info.SectorNumber, err)
		}
		return nil
	})

	if params.info.Expiration <= rt.CurrEpoch() {
		rt.Abort(exitcode.ErrIllegalArgument, "sector expiration %v must be after now (%v)", params.info.Expiration, rt.CurrEpoch())
	}

	// Request deferred Cron check for PreCommit expiry check.
	cronPayload := CronEventPayload{
		EventType: CronEventType_Miner_PreCommitExpiry,
		Sectors:   []abi.SectorNumber{params.info.SectorNumber},
	}
	expiryBound := rt.CurrEpoch() + PoRepMaxDelay + 1
	a.enrollCronEvent(rt, expiryBound, &cronPayload)

	return &adt.EmptyValue{}
}

type ProveCommitSectorParams struct {
	SectorNumber abi.SectorNumber
	Proof        abi.SealProof
}

func (a *Actor) ProveCommitSector(rt Runtime, params *ProveCommitSectorParams) *adt.EmptyValue {
	sectorNo := params.SectorNumber
	store := adt.AsStore(rt)

	var st State
	rt.State().Readonly(&st)
	rt.ValidateImmediateCallerIs(st.Info.Worker)

	precommit, found, err := st.getPrecommittedSector(store, sectorNo)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "failed to get precommitted sector %v: %v", sectorNo, err)
	} else if !found {
		rt.Abort(exitcode.ErrNotFound, "no precommitted sector %v", sectorNo)
	}

	if rt.CurrEpoch() > precommit.PreCommitEpoch+PoRepMaxDelay || rt.CurrEpoch() < precommit.PreCommitEpoch+PoRepMinDelay {
		rt.Abort(exitcode.ErrIllegalArgument, "Invalid ProveCommitSector epoch")
	}

	autil.TODO()
	// TODO HS: How are SealEpoch, InteractiveEpoch determined (and intended to be used)?
	// Presumably they cannot be derived from the SectorProveCommitInfo provided by an untrusted party.

	a.verifySeal(rt, st.Info.SectorSize, &abi.OnChainSealVerifyInfo{
		SealedCID:    precommit.Info.SealedCID,
		SealEpoch:    precommit.Info.SealEpoch,
		Proof:        params.Proof,
		DealIDs:      precommit.Info.DealIDs,
		SectorNumber: precommit.Info.SectorNumber,
	})

	// Check (and activate) storage deals associated to sector. Abort if checks failed.
	// return DealWeight for the deal set in the sector
	var dealWeight abi.DealWeight
	ret, code := rt.Send(
		builtin.StorageMarketActorAddr,
		builtin.StorageMarketMethods.VerifyDealsOnSectorProveCommit,
		&storage_market.VerifyDealsOnSectorProveCommitParams{
			DealIDs:      precommit.Info.DealIDs,
			SectorExpiry: precommit.Info.Expiration,
		},
		abi.NewTokenAmount(0),
	)

	builtin.RequireSuccess(rt, code, "failed to verify deals and get deal weight")
	autil.AssertNoError(ret.Into(&dealWeight))

	var storageWeightDesc *storage_power.SectorStorageWeightDesc
	rt.State().Transaction(&st, func() interface{} {
		newSectorInfo := &SectorOnChainInfo{
			Info:            precommit.Info,
			ActivationEpoch: rt.CurrEpoch(),
			DealWeight:      dealWeight,
		}
		if err = st.putSector(adt.AsStore(rt), newSectorInfo); err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to prove commit: %v", err)
		}

		if err = st.deletePrecommitttedSector(store, sectorNo); err != nil {
			rt.Abort(exitcode.ErrIllegalState, "faile to delete proven precommit for sector %v: %v", sectorNo, err)
		}

		st.ProvingSet = st.ComputeProvingSet()
		storageWeightDesc = asStorageWeightDesc(st.Info.SectorSize, newSectorInfo)
		return nil
	})

	// Request deferred callback for sector expiry.
	cronPayload := CronEventPayload{
		EventType: CronEventType_Miner_SectorExpiry,
		Sectors:   []abi.SectorNumber{sectorNo},
	}
	a.enrollCronEvent(rt, precommit.Info.Expiration, &cronPayload)

	// Request power for activated sector.
	_, code = rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.Method_StoragePowerActor_OnSectorProveCommit,
		&storage_power.OnSectorProveCommitParams{
			Weight: *storageWeightDesc,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to notify power actor")

	// Return PreCommit deposit to worker upon successful ProveCommit.
	_, code = rt.Send(st.Info.Worker, builtin.MethodSend, nil, precommit.PreCommitDeposit)
	builtin.RequireSuccess(rt, code, "failed to send funds")
	return &adt.EmptyValue{}
}

/////////////////////////
// Sector Modification //
/////////////////////////

type ExtendSectorExpirationParams struct {
	sectorNumber  abi.SectorNumber
	newExpiration abi.ChainEpoch
}

func (a *Actor) ExtendSectorExpiration(rt Runtime, params *ExtendSectorExpirationParams) *adt.EmptyValue {
	sectorNo := params.sectorNumber

	store := adt.AsStore(rt)
	var storageWeightDescPrev *storage_power.SectorStorageWeightDesc
	var extensionLength abi.ChainEpoch
	var st State
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)
		var err error

		sector, found, err := st.getSector(store, sectorNo)
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to load sector %v: %v", sectorNo, err)
		} else if !found {
			rt.Abort(exitcode.ErrNotFound, "no such sector %v", sectorNo)
		}

		storageWeightDescPrev = asStorageWeightDesc(st.Info.SectorSize, sector)

		extensionLength = params.newExpiration - sector.Info.Expiration
		if extensionLength < 0 {
			rt.Abort(exitcode.ErrIllegalArgument, "cannot reduce sector expiration")
		}

		sector.Info.Expiration = params.newExpiration
		if err = st.putSector(store, sector); err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to update sector %v, %v", sectorNo, err)
		}
		return nil
	})

	storageWeightDescNew := *storageWeightDescPrev
	storageWeightDescNew.Duration = storageWeightDescPrev.Duration + extensionLength

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.Method_StoragePowerActor_OnSectorModifyWeightDesc,
		&storage_power.OnSectorModifyWeightDescParams{
			PrevWeight: *storageWeightDescPrev,
			NewWeight:  storageWeightDescNew,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to modify sector weight")
	return &adt.EmptyValue{}
}

type TerminateSectorsParams struct {
	sectorNumbers []abi.SectorNumber
}

func (a *Actor) TerminateSectors(rt Runtime, params *TerminateSectorsParams) *adt.EmptyValue {
	var st State
	rt.State().Readonly(&st)
	rt.ValidateImmediateCallerIs(st.Info.Worker)

	for _, sectorNumber := range params.sectorNumbers {
		a.terminateSector(rt, sectorNumber, storage_power.SectorTerminationManual)
	}

	return &adt.EmptyValue{}
}

////////////
// Faults //
////////////

type DeclareTemporaryFaultsParams struct {
	sectorNumbers []abi.SectorNumber
	duration      abi.ChainEpoch
}

func (a *Actor) DeclareTemporaryFaults(rt Runtime, params DeclareTemporaryFaultsParams) *adt.EmptyValue {
	if params.duration <= abi.ChainEpoch(0) {
		rt.Abort(exitcode.ErrIllegalArgument, "non-positive fault duration %v", params.duration)
	}

	effectiveEpoch := rt.CurrEpoch() + DeclaredFaultEffectiveDelay
	var st State
	requiredFee := rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)

		store := adt.AsStore(rt)
		storageWeightDescs := []*storage_power.SectorStorageWeightDesc{}
		for _, sectorNumber := range params.sectorNumbers {
			sector, found, err := st.getSector(store, sectorNumber)
			if err != nil {
				rt.Abort(exitcode.ErrIllegalState, "failed to load sector %v: %v", sectorNumber, err)
			}
			fault, err := st.IsSectorInTemporaryFault(store, sectorNumber)
			if err != nil {
				rt.Abort(exitcode.ErrIllegalState, "failed to check sector fault %v: %v", sectorNumber, err)
			}
			if !found || fault {
				continue
			}

			storageWeightDescs = append(storageWeightDescs, asStorageWeightDesc(st.Info.SectorSize, sector))

			sector.DeclaredFaultEpoch = effectiveEpoch
			sector.DeclaredFaultDuration = params.duration
			if err = st.putSector(store, sector); err != nil {
				rt.Abort(exitcode.ErrIllegalState, "failed to update sector %v: %v", sectorNumber, err)
			}
		}
		return temporaryFaultFee(storageWeightDescs, params.duration)
	}).(abi.TokenAmount)

	// Burn the fee, refund any change.
	confirmPaymentAndRefundChange(rt, requiredFee)
	_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, requiredFee)
	builtin.RequireSuccess(rt, code, "failed to burn fee")

	// Request deferred Cron invocation to update temporary fault state.
	cronPayload := CronEventPayload{
		EventType: CronEventType_Miner_TempFault,
		Sectors:   params.sectorNumbers,
	}
	// schedule cron event to start marking temp fault at BeginEpoch
	a.enrollCronEvent(rt, effectiveEpoch, &cronPayload)
	// schedule cron event to end marking temp fault at EndEpoch
	a.enrollCronEvent(rt, effectiveEpoch+params.duration, &cronPayload)
	return &adt.EmptyValue{}
}

//////////
// Cron //
//////////

type OnDeferredCronEventParams struct {
	callbackPayload []byte
}

func (a *Actor) OnDeferredCronEvent(rt Runtime, params *OnDeferredCronEventParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	var payload CronEventPayload
	if err := payload.UnmarshalCBOR(bytes.NewReader(params.callbackPayload)); err != nil {
		rt.Abort(exitcode.ErrIllegalArgument, "failed to deserialize event payload")
	}

	if payload.EventType == CronEventType_Miner_TempFault {
		for _, sectorNumber := range payload.Sectors {
			a._rtCheckTemporaryFaultEvents(rt, sectorNumber)
		}
	}

	if payload.EventType == CronEventType_Miner_PreCommitExpiry {
		for _, sectorNumber := range payload.Sectors {
			a.checkPrecommitExpiry(rt, sectorNumber)
		}
	}

	if payload.EventType == CronEventType_Miner_SectorExpiry {
		for _, sectorNumber := range payload.Sectors {
			a.checkSectorExpiry(rt, sectorNumber)
		}
	}

	if payload.EventType == CronEventType_Miner_SurpriseExpiration {
		a.checkPoStProvingPeriodExpiration(rt)
	}

	if payload.EventType == CronEventType_Miner_WorkerKeyChange {
		a._rtCommitWorkerKeyChange(rt)
	}

	return &adt.EmptyValue{}
}

////////////////////////////////////////////////////////////////////////////////
// Method utility functions
////////////////////////////////////////////////////////////////////////////////

func (a *Actor) _rtCheckTemporaryFaultEvents(rt Runtime, sectorNumber abi.SectorNumber) {
	store := adt.AsStore(rt)

	var st State
	rt.State().Readonly(&st)
	sector, found, err := st.getSector(store, sectorNumber)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "error loading sector %v: %v", sectorNumber, err)
	} else if !found {
		return
	}
	weight, err := st.GetStorageWeightDescForSector(store, sectorNumber)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "failed to get weight for sector %v: %v", sectorNumber, err)
	}
	fault, err := st.IsSectorInTemporaryFault(store, sectorNumber)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "failed to check fault for sector %v: %v", sectorNumber, err)
	}

	if !fault && rt.CurrEpoch() >= sector.DeclaredFaultEpoch {
		_, code := rt.Send(
			builtin.StoragePowerActorAddr,
			builtin.Method_StoragePowerActor_OnSectorTemporaryFaultEffectiveBegin,
			&storage_power.OnSectorTemporaryFaultEffectiveBeginParams{
				Weight: *weight,
			},
			abi.NewTokenAmount(0),
		)
		builtin.RequireSuccess(rt, code, "failed to begin fault")

		rt.State().Transaction(&st, func() interface{} {
			st.FaultSet.Set(uint64(sectorNumber))
			return nil
		})
	}

	if fault && rt.CurrEpoch() >= sector.DeclaredFaultEpoch+sector.DeclaredFaultDuration {
		sector.DeclaredFaultEpoch = epochUndefined
		sector.DeclaredFaultDuration = epochUndefined

		_, code := rt.Send(
			builtin.StoragePowerActorAddr,
			builtin.Method_StoragePowerActor_OnSectorTemporaryFaultEffectiveEnd,
			&storage_power.OnSectorTemporaryFaultEffectiveEndParams{
				Weight: *weight,
			},
			abi.NewTokenAmount(0),
		)
		builtin.RequireSuccess(rt, code, "failed to end fault")

		rt.State().Transaction(&st, func() interface{} {
			if err = st.FaultSet.Unset(uint64(sectorNumber)); err != nil {
				rt.Abort(exitcode.ErrIllegalState, "failed to unset fault for %v: %v", sectorNumber, err)
			}
			return nil
		})
	}

	rt.State().Transaction(&st, func() interface{} {
		if err = st.putSector(store, sector); err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to update sector %v: %v", sectorNumber, err)
		}
		return nil
	})
}

func (a *Actor) checkPrecommitExpiry(rt Runtime, sectorNo abi.SectorNumber) {
	var st State
	rt.State().Readonly(&st)
	store := adt.AsStore(rt)
	sector, found, err := st.getPrecommittedSector(store, sectorNo)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "failed to load sector %v: %v", sectorNo, err)
	} else if !found {
		return
	}

	if rt.CurrEpoch()-sector.PreCommitEpoch > PoRepMaxDelay {
		rt.State().Transaction(&st, func() interface{} {
			err = st.deletePrecommitttedSector(store, sectorNo)
			if err != nil {
				rt.Abort(exitcode.ErrIllegalState, "failed to delete precommit %v: %v", sectorNo, err)
			}
			return nil
		})
		_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, sector.PreCommitDeposit)
		builtin.RequireSuccess(rt, code, "failed to burn funds")
	}
	return
}

func (a *Actor) checkSectorExpiry(rt Runtime, sectorNumber abi.SectorNumber) {
	var st State
	rt.State().Readonly(&st)
	sector, found, err := st.getSector(adt.AsStore(rt), sectorNumber)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "failed to load sector %v: %v", sectorNumber, err)
	} else if !found {
		return
	}

	// Note: the following test may be false, if sector expiration has been extended by the worker
	// in the interim after the Cron request was enrolled.
	if rt.CurrEpoch() >= sector.Info.Expiration {
		a.terminateSector(rt, sectorNumber, storage_power.SectorTerminationExpired)
	}
	return
}

func (a *Actor) terminateSector(rt Runtime, sectorNumber abi.SectorNumber, terminationType storage_power.SectorTermination) {
	store := adt.AsStore(rt)
	var st State

	var dealIDs []abi.DealID
	var weight *storage_power.SectorStorageWeightDesc
	var fault bool
	rt.State().Transaction(&st, func() interface{} {
		sector, found, err := st.getSector(store, sectorNumber)
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to check sector %v: %v", sectorNumber, err)
		}
		Assert(found)

		dealIDs = sector.Info.DealIDs
		weight = asStorageWeightDesc(st.Info.SectorSize, sector)
		fault, err = st.IsSectorInTemporaryFault(store, sectorNumber)
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to check fault for sector %v: %v", sectorNumber, err)
		}

		err = st.deleteSector(store, sectorNumber)
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to delete sector: %v", err)
		}
		return nil
	})

	// End any fault state before terminating sector power.
	if fault {
		a.requestEndFault(rt, weight)
	}

	a.requestTerminateDeals(rt, dealIDs)
	a.requestTerminatePower(rt, terminationType, weight)
}

func (a *Actor) checkPoStProvingPeriodExpiration(rt Runtime) {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	var st State
	expired := rt.State().Transaction(&st, func() interface{} {
		if !st.PoStState.isChallenged() {
			return false // Already exited challenged state successfully prior to expiry.
		}

		window := storage_power.SurprisePostChallengeDuration
		if rt.CurrEpoch() < st.PoStState.SurpriseChallengeEpoch+window {
			// Challenge not yet expired.
			return false
		}

		// Increment count of consecutive failures.
		st.PoStState = PoStState{
			LastSuccessfulPoSt:     st.PoStState.LastSuccessfulPoSt,
			SurpriseChallengeEpoch: epochUndefined,
			NumConsecutiveFailures: st.PoStState.NumConsecutiveFailures + 1,
		}
		return true
	}).(bool)

	if !expired {
		return
	}

	// Period has expired.
	// Terminate deals...
	if st.PoStState.NumConsecutiveFailures > storage_power.SurprisePostFailureLimit {
		a.requestTerminateAllDeals(rt, &st)
	}

	// ... and pay penalty (possibly terminating and deleting the miner).
	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.Method_StoragePowerActor_OnMinerSurprisePoStFailure,
		&storage_power.OnMinerSurprisePoStFailureParams{
			NumConsecutiveFailures: st.PoStState.NumConsecutiveFailures,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to notify power actor")
}

func (a *Actor) enrollCronEvent(rt Runtime, eventEpoch abi.ChainEpoch, callbackPayload *CronEventPayload) {
	var payload []byte
	err := callbackPayload.MarshalCBOR(bytes.NewBuffer(payload))
	if err != nil {
		rt.Abort(exitcode.ErrIllegalArgument, "failed to serialize payload: %v", err)
	}
	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.Method_StoragePowerActor_OnMinerEnrollCronEvent,
		&storage_power.EnrollCronEventParams{
			EventEpoch: eventEpoch,
			Payload:    payload,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to enroll cron event")
}

func (a *Actor) requestEndFault(rt Runtime, weight *storage_power.SectorStorageWeightDesc) {
	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.Method_StoragePowerActor_OnSectorTemporaryFaultEffectiveEnd,
		&storage_power.OnSectorTemporaryFaultEffectiveEndParams{
			Weight: *weight,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to end fault weight %v", weight)
}

func (a *Actor) requestTerminateDeals(rt Runtime, dealIDs []abi.DealID) {
	_, code := rt.Send(
		builtin.StorageMarketActorAddr,
		builtin.StorageMarketMethods.OnMinerSectorsTerminate,
		&storage_market.OnMinerSectorsTerminateParams{
			DealIDs: dealIDs,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to terminate deals %v, exit code %v", dealIDs, code)
}

func (a *Actor) requestTerminateAllDeals(rt Runtime, st *State) {
	// TODO: this is an unbounded computation. Transform into an idempotent partial computation that can be
	// progressed on each invocation.
	dealIds := []abi.DealID{}
	if err := st.forEachSector(adt.AsStore(rt), func(sector *SectorOnChainInfo) {
		dealIds = append(dealIds, sector.Info.DealIDs...)
	}); err != nil {
		rt.Abort(exitcode.ErrIllegalState, "failed to traverse sectors for termination: %v", err)
	}

	a.requestTerminateDeals(rt, dealIds)
}

func (a *Actor) requestTerminatePower(rt Runtime, terminationType storage_power.SectorTermination, weight *storage_power.SectorStorageWeightDesc) {
	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.Method_StoragePowerActor_OnSectorTerminate,
		&storage_power.OnSectorTerminateParams{
			TerminationType: terminationType,
			Weight:          *weight,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to terminate sector power type %v, weight %v", terminationType, weight)
}

func (a *Actor) verifySurprisePost(rt Runtime, st *State, onChainInfo *abi.OnChainSurprisePoStVerifyInfo) {
	Assert(st.PoStState.isChallenged())
	sectorSize := st.Info.SectorSize
	challengeEpoch := st.PoStState.SurpriseChallengeEpoch

	// verify no duplicate tickets
	challengeIndices := make(map[int64]bool)
	for _, tix := range onChainInfo.Candidates {
		if _, ok := challengeIndices[tix.ChallengeIndex]; ok {
			rt.Abort(exitcode.ErrIllegalArgument, "Invalid Surprise PoSt. Duplicate ticket included.")
		}
		challengeIndices[tix.ChallengeIndex] = true
	}

	// verify appropriate number of tickets is present
	if int64(len(onChainInfo.Candidates)) != NumSurprisePoStSectors {
		rt.Abort(exitcode.ErrIllegalArgument, "Invalid Surprise PoSt. Too few tickets included.")
	}

	randomnessK := rt.GetRandomness(challengeEpoch - PoStLookback)
	// regenerate randomness used. The PoSt Verification below will fail if
	// the same was not used to generate the proof
	postRandomness := crypto.DeriveRandWithMinerAddr(crypto.DomainSeparationTag_SurprisePoStChallengeSeed, randomnessK, rt.CurrReceiver())

	// Get public inputs

	pvInfo := abi.PoStVerifyInfo{
		Candidates: onChainInfo.Candidates,
		Proofs:     onChainInfo.Proofs,
		Randomness: abi.PoStRandomness(postRandomness),
		// EligibleSectors_: FIXME: verification needs these.
	}

	// Verify the PoSt Proof
	if !rt.Syscalls().VerifyPoSt(sectorSize, pvInfo) {
		rt.Abort(exitcode.ErrIllegalArgument, "invalid PoSt %+v", pvInfo)
	}
}

func (a *Actor) verifySeal(rt Runtime, sectorSize abi.SectorSize, onChainInfo *abi.OnChainSealVerifyInfo) {
	// Check randomness.
	sealEarliest := rt.CurrEpoch() - ChainFinalityish - MaxSealDuration[abi.RegisteredProof_WinStackedDRG32GiBSeal]
	if onChainInfo.SealEpoch < sealEarliest {
		rt.Abort(exitcode.ErrIllegalArgument, "seal epoch %v too old, expected >= %v", onChainInfo.SealEpoch, sealEarliest)
	}

	commD := a.requestUnsealedSectorCID(rt, sectorSize, onChainInfo.DealIDs)

	minerActorID, err := addr.IDFromAddress(rt.CurrReceiver())
	autil.AssertNoError(err) // Runtime always provides ID-addresses

	svInfoRandomness := rt.GetRandomness(onChainInfo.SealEpoch)
	svInfoInteractiveRandomness := rt.GetRandomness(onChainInfo.InteractiveEpoch)

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
	if !rt.Syscalls().VerifySeal(sectorSize, svInfo) {
		rt.Abort(exitcode.ErrIllegalState, "invalid seal %+v", svInfo)
	}
}

// Requests the storage market actor compute the unsealed sector CID from a sector's deals.
func (a *Actor) requestUnsealedSectorCID(rt Runtime, sectorSize abi.SectorSize, dealIDs []abi.DealID) cid.Cid {
	var unsealedCID cbg.CborCid
	ret, code := rt.Send(
		builtin.StorageMarketActorAddr,
		builtin.StorageMarketMethods.ComputeDataCommitment,
		&storage_market.ComputeDataCommitmentParams{
			SectorSize: sectorSize,
			DealIDs:    dealIDs,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed request for unsealed sector CID for deals %v", dealIDs)
	autil.AssertNoError(ret.Into(&unsealedCID))
	return cid.Cid(unsealedCID)
}

func confirmPaymentAndRefundChange(rt vmr.Runtime, expected abi.TokenAmount) {
	if rt.ValueReceived().LessThan(expected) {
		rt.Abort(exitcode.ErrInsufficientFunds, "insufficient funds received, expected %v", expected)
	}

	if rt.ValueReceived().GreaterThan(expected) {
		_, code := rt.Send(rt.ImmediateCaller(), builtin.MethodSend, nil, big.Sub(rt.ValueReceived(), expected))
		builtin.RequireSuccess(rt, code, "failed to transfer refund")
	}
}
