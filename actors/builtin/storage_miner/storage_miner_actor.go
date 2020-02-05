package storage_miner

import (
	"bytes"

	addr "github.com/filecoin-project/go-address"
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
	cid "github.com/ipfs/go-cid"
	errors "github.com/pkg/errors"
	cbg "github.com/whyrusleeping/cbor-gen"
)

type Runtime = vmr.Runtime

var Assert = autil.Assert
var AssertNoError = autil.AssertNoError

const epochUndefined = abi.ChainEpoch(-1)

type StorageMinerActor struct{}

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

func (a *StorageMinerActor) Constructor(rt Runtime, params *ConstructorParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	// TODO: fix this, check that the account actor at the other end of this address has a BLS key.
	if params.WorkerAddr.Protocol() != addr.BLS {
		rt.Abort(exitcode.ErrIllegalArgument, "Worker Key must be BLS.")
	}

	var st StorageMinerActorState
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

func (a *StorageMinerActor) StageWorkerKeyChange(rt Runtime, params *StageWorkerKeyChangeParams) *adt.EmptyValue {
	var st StorageMinerActorState
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

func (a *StorageMinerActor) _rtCommitWorkerKeyChange(rt Runtime) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.CronActorAddr)

	var st StorageMinerActorState
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

// Called by StoragePowerActor to notify StorageMiner of SurprisePoSt Challenge.
func (a *StorageMinerActor) OnSurprisePoStChallenge(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	var st StorageMinerActorState
	challenged := rt.State().Transaction(&st, func() interface{} {
		// If already challenged, do not challenge again.
		// Failed PoSt will automatically reset the state to not-challenged.
		if st.PoStState.isChallenged() {
			return false
		}

		var curRecBuf bytes.Buffer
		err := rt.CurrReceiver().MarshalCBOR(&curRecBuf)
		autil.AssertNoError(err)

		st.PoStState = MinerPoStState{
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
func (a *StorageMinerActor) SubmitSurprisePoStResponse(rt Runtime, params *SubmitSurprisePoStResponseParams) *adt.EmptyValue {
	var st StorageMinerActorState
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)
		if !st.PoStState.isChallenged() {
			rt.Abort(exitcode.ErrIllegalState, "Not currently challenged")
		}
		a.verifySurprisePost(rt, &st, &params.onChainInfo)

		st.PoStState = MinerPoStState{
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

// Called by StoragePowerActor.
func (a *StorageMinerActor) OnDeleteMiner(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)
	minerAddr := rt.CurrReceiver()
	rt.DeleteActor(minerAddr)
	return &adt.EmptyValue{}
}

//////////////////
// ElectionPoSt //
//////////////////

// Called by the VM interpreter once an ElectionPoSt has been verified.
func (a *StorageMinerActor) OnVerifiedElectionPoSt(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	if rt.ToplevelBlockWinner() != rt.CurrReceiver() {
		rt.Abort(exitcode.ErrForbidden, "receiver must be miner of this block")
	}

	var st StorageMinerActorState
	rt.State().Transaction(&st, func() interface{} {
		updateSuccessEpoch := st.PoStState.isPoStOk()

		// Advance the timestamp of the most recent PoSt success, provided the miner has not faulted
		// in normal state. (Cannot do this if miner has missed a SurprisePoSt.)
		if updateSuccessEpoch {
			st.PoStState = MinerPoStState{
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

// Deals must be posted on chain via sma.PublishStorageDeals before PreCommitSector.
// Optimization: PreCommitSector could contain a list of deals that are not published yet.
func (a *StorageMinerActor) PreCommitSector(rt Runtime, params *PreCommitSectorParams) *adt.EmptyValue {
	var st StorageMinerActorState
	rt.State().Readonly(&st)
	rt.ValidateImmediateCallerIs(st.Info.Worker)

	store := adt.AsStore(rt)
	if found, err := st.hasSectorNo(store, params.info.SectorNumber); err != nil {
		rt.Abort(exitcode.ErrIllegalState, "failed to check sector %v: %v", params.info.SectorNumber, err)
	} else if found {
		rt.Abort(exitcode.ErrIllegalArgument, "sector %v already committed", params.info.SectorNumber)
	}

	depositReq := precommitDeposit(st.getSectorSize(), params.info.Expiration-rt.CurrEpoch())
	confirmFundsReceiptOrAbort_RefundRemainder(rt, depositReq)

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

func (a *StorageMinerActor) ProveCommitSector(rt Runtime, params *ProveCommitSectorParams) *adt.EmptyValue {
	sectorNo := params.SectorNumber
	store := adt.AsStore(rt)

	var st StorageMinerActorState
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

	a._rtVerifySealOrAbort(rt, &abi.OnChainSealVerifyInfo{
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

	rt.State().Transaction(&st, func() interface{} {
		if err = st.putSector(adt.AsStore(rt), &SectorOnChainInfo{
			Info:            precommit.Info,
			ActivationEpoch: rt.CurrEpoch(),
			DealWeight:      dealWeight,
		}); err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to prove commit: %v", err)
		}

		if err = st.deletePrecommitttedSector(store, sectorNo); err != nil {
			rt.Abort(exitcode.ErrIllegalState, "faile to delete proven precommit for sector %v: %v", sectorNo, err)
		}

		st.ProvingSet = st.ComputeProvingSet()
		return nil
	})

	// Request deferred Cron check for sector expiry.
	cronPayload := CronEventPayload{
		EventType: CronEventType_Miner_SectorExpiry,
		Sectors:   []abi.SectorNumber{sectorNo},
	}
	a.enrollCronEvent(rt, precommit.Info.Expiration, &cronPayload)

	// Notify SPA to update power associated to newly activated sector.
	storageWeightDesc, err := a._rtGetStorageWeightDescForSector(rt, sectorNo)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "failed to get weight for sector %v: %v", sectorNo, err)
	}
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

func (a *StorageMinerActor) ExtendSectorExpiration(rt Runtime, params *ExtendSectorExpirationParams) *adt.EmptyValue {
	sectorNo := params.sectorNumber
	storageWeightDescPrev, err := a._rtGetStorageWeightDescForSector(rt, sectorNo)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "failed to load sector weight %v: %v", sectorNo, err)
	}

	store := adt.AsStore(rt)
	var extensionLength abi.ChainEpoch
	var st StorageMinerActorState
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)

		sector, found, err := st.getSector(store, sectorNo)
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to load sector %v: %v", sectorNo, err)
		} else if !found {
			rt.Abort(exitcode.ErrNotFound, "no such sector %v", sectorNo)
		}

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

	storageWeightDescNew := storageWeightDescPrev
	storageWeightDescNew.Duration = storageWeightDescPrev.Duration + extensionLength

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.Method_StoragePowerActor_OnSectorModifyWeightDesc,
		&storage_power.OnSectorModifyWeightDescParams{
			PrevWeight: *storageWeightDescPrev,
			NewWeight:  *storageWeightDescNew,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to modify sector weight")
	return &adt.EmptyValue{}
}

type TerminateSectorsParams struct {
	sectorNumbers []abi.SectorNumber
}

func (a *StorageMinerActor) TerminateSectors(rt Runtime, params *TerminateSectorsParams) *adt.EmptyValue {
	var st StorageMinerActorState
	rt.State().Readonly(&st)
	rt.ValidateImmediateCallerIs(st.Info.Worker)

	for _, sectorNumber := range params.sectorNumbers {
		a._rtTerminateSector(rt, sectorNumber, storage_power.SectorTerminationManual)
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

func (a *StorageMinerActor) DeclareTemporaryFaults(rt Runtime, params DeclareTemporaryFaultsParams) *adt.EmptyValue {
	if params.duration <= abi.ChainEpoch(0) {
		rt.Abort(exitcode.ErrIllegalArgument, "non-positive fault duration %v", params.duration)
	}

	storageWeightDescs, err := a._rtGetStorageWeightDescsForSectors(rt, params.sectorNumbers)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "failed to load sector weights %v: %v", params.sectorNumbers, err)
	}

	requiredFee := temporaryFaultFee(storageWeightDescs, params.duration)

	confirmFundsReceiptOrAbort_RefundRemainder(rt, requiredFee)
	_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, requiredFee)
	builtin.RequireSuccess(rt, code, "failed to burn fee")

	effectiveBeginEpoch := rt.CurrEpoch() + DeclaredFaultEffectiveDelay
	effectiveEndEpoch := effectiveBeginEpoch + params.duration

	store := adt.AsStore(rt)
	var st StorageMinerActorState
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)

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

			sector.DeclaredFaultEpoch = rt.CurrEpoch()
			sector.DeclaredFaultDuration = params.duration
			if err = st.putSector(store, sector); err != nil {
				rt.Abort(exitcode.ErrIllegalState, "failed to update sector %v: %v", sectorNumber, err)
			}
		}
		return nil
	})

	// Request deferred Cron invocation to update temporary fault state.
	cronPayload := CronEventPayload{
		EventType: CronEventType_Miner_TempFault,
		Sectors:   params.sectorNumbers,
	}
	// schedule cron event to start marking temp fault at BeginEpoch
	a.enrollCronEvent(rt, effectiveBeginEpoch, &cronPayload)
	// schedule cron event to end marking temp fault at EndEpoch
	a.enrollCronEvent(rt, effectiveEndEpoch, &cronPayload)
	return &adt.EmptyValue{}
}

//////////
// Cron //
//////////

type OnDeferredCronEventParams struct {
	callbackPayload []byte
}

func (a *StorageMinerActor) OnDeferredCronEvent(rt Runtime, params *OnDeferredCronEventParams) *adt.EmptyValue {
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
			a._rtCheckPreCommitExpiry(rt, sectorNumber)
		}
	}

	if payload.EventType == CronEventType_Miner_SectorExpiry {
		for _, sectorNumber := range payload.Sectors {
			a._rtCheckSectorExpiry(rt, sectorNumber)
		}
	}

	if payload.EventType == CronEventType_Miner_SurpriseExpiration {
		a._rtCheckSurprisePoStExpiry(rt)
	}

	if payload.EventType == CronEventType_Miner_WorkerKeyChange {
		a._rtCommitWorkerKeyChange(rt)
	}

	return &adt.EmptyValue{}
}

////////////////////////////////////////////////////////////////////////////////
// Method utility functions
////////////////////////////////////////////////////////////////////////////////

func (a *StorageMinerActor) _rtCheckTemporaryFaultEvents(rt Runtime, sectorNumber abi.SectorNumber) {
	store := adt.AsStore(rt)

	var st StorageMinerActorState
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

	if !fault && rt.CurrEpoch() >= sector.EffectiveFaultBeginEpoch() {
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

	if fault && rt.CurrEpoch() >= sector.EffectiveFaultEndEpoch() {
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

func (a *StorageMinerActor) _rtCheckPreCommitExpiry(rt Runtime, sectorNo abi.SectorNumber) {
	var st StorageMinerActorState
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

func (a *StorageMinerActor) _rtCheckSectorExpiry(rt Runtime, sectorNumber abi.SectorNumber) {
	var st StorageMinerActorState
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
		a._rtTerminateSector(rt, sectorNumber, storage_power.SectorTerminationExpired)
	}
	return
}

func (a *StorageMinerActor) _rtTerminateSector(rt Runtime, sectorNumber abi.SectorNumber, terminationType storage_power.SectorTermination) {
	store := adt.AsStore(rt)
	var st StorageMinerActorState
	rt.State().Readonly(&st)

	sector, found, err := st.getSector(store, sectorNumber)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "failed to check sector %v: %v", sectorNumber, err)
	}
	Assert(found)

	weight, err := st.GetStorageWeightDescForSector(store, sectorNumber)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "failed to check weight for sector %v: %v", sectorNumber, err)
	}
	fault, err := st.IsSectorInTemporaryFault(store, sectorNumber)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "failed to check fault for sector %v: %v", sectorNumber, err)
	}

	if fault {
		// To avoid boundary-case errors in power accounting, make sure we explicitly end
		// the temporary fault state first, before terminating the sector.
		_, code := rt.Send(
			builtin.StoragePowerActorAddr,
			builtin.Method_StoragePowerActor_OnSectorTemporaryFaultEffectiveEnd,
			&storage_power.OnSectorTemporaryFaultEffectiveEndParams{
				Weight: *weight,
			},
			abi.NewTokenAmount(0),
		)
		builtin.RequireSuccess(rt, code, "failed to end fault")
	}

	{
		_, code := rt.Send(
			builtin.StorageMarketActorAddr,
			builtin.StorageMarketMethods.OnMinerSectorsTerminate,
			&storage_market.OnMinerSectorsTerminateParams{
				DealIDs: sector.Info.DealIDs,
			},
			abi.NewTokenAmount(0),
		)
		builtin.RequireSuccess(rt, code, "failed to terminate sector with market actor")
	}

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.Method_StoragePowerActor_OnSectorTerminate,
		&storage_power.OnSectorTerminateParams{
			TerminationType: terminationType,
			Weight:          *weight,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to terminate sector with power actor")

	a._rtDeleteSectorEntry(rt, sectorNumber)
}

func (a *StorageMinerActor) _rtCheckSurprisePoStExpiry(rt Runtime) {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	var st StorageMinerActorState
	rt.State().Readonly(&st)

	if !st.PoStState.isChallenged() {
		// Already exited challenged state successfully prior to expiry.
		return
	}

	window := storage_power.SurprisePostChallengeDuration
	if rt.CurrEpoch() < st.PoStState.SurpriseChallengeEpoch+window {
		// Challenge not yet expired.
		return
	}

	numConsecutiveFailures := st.PoStState.NumConsecutiveFailures + 1

	// This comparison with the failure limit should happen only in the power actor, not here.
	if numConsecutiveFailures > storage_power.SurprisePostFailureLimit {
		// Terminate all sectors, notify power and market actors to terminate
		// associated storage deals, and reset miner's PoSt state to OK.
		if err := a._rtNotifyMarketForTerminatedSectors(rt); err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to notify market for terminated sectors: %v", err)
		}
	} else {
		// Increment count of consecutive failures, and continue.
		rt.State().Transaction(&st, func() interface{} {
			st.PoStState = MinerPoStState{
				LastSuccessfulPoSt:     st.PoStState.LastSuccessfulPoSt,
				SurpriseChallengeEpoch: epochUndefined,
				NumConsecutiveFailures: numConsecutiveFailures,
			}
			return nil
		})
	}

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.Method_StoragePowerActor_OnMinerSurprisePoStFailure,
		&storage_power.OnMinerSurprisePoStFailureParams{
			NumConsecutiveFailures: numConsecutiveFailures,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to notify power actor")
}

func (a *StorageMinerActor) enrollCronEvent(rt Runtime, eventEpoch abi.ChainEpoch, callbackPayload *CronEventPayload) {
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

func (a *StorageMinerActor) _rtDeleteSectorEntry(rt Runtime, sectorNo abi.SectorNumber) {
	var st StorageMinerActorState
	rt.State().Transaction(&st, func() interface{} {
		err := st.deleteSector(adt.AsStore(rt), sectorNo)
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to delete sector: %v", err)
		}
		return nil
	})
}

func (a *StorageMinerActor) _rtGetStorageWeightDescForSector(rt Runtime, sectorNo abi.SectorNumber) (*storage_power.SectorStorageWeightDesc, error) {
	var st StorageMinerActorState
	rt.State().Readonly(&st)
	return st.GetStorageWeightDescForSector(adt.AsStore(rt), sectorNo)
}

func (a *StorageMinerActor) _rtGetStorageWeightDescsForSectors(rt Runtime, sectorNos []abi.SectorNumber) ([]*storage_power.SectorStorageWeightDesc, error) {
	var st StorageMinerActorState
	rt.State().Readonly(&st)

	store := adt.AsStore(rt)
	ret := []*storage_power.SectorStorageWeightDesc{}
	for _, sectorNo := range sectorNos {
		weight, err := st.GetStorageWeightDescForSector(store, sectorNo)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to load weight for sector %v", sectorNo)
		}
		ret = append(ret, weight)
	}
	return ret, nil
}

func (a *StorageMinerActor) _rtNotifyMarketForTerminatedSectors(rt Runtime) error {
	// TODO: this is an unbounded computation. Transform into an idempotent partial computation that can be
	// progressed on each invocation.

	var st StorageMinerActorState
	rt.State().Readonly(&st)
	dealIds := []abi.DealID{}
	err := st.forEachSector(adt.AsStore(rt), func(sector *SectorOnChainInfo) {
		dealIds = append(dealIds, sector.Info.DealIDs...)
	})
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "failed to traverse sectors for termination: %v", err)
	}

	_, code := rt.Send(
		builtin.StorageMarketActorAddr,
		builtin.StorageMarketMethods.OnMinerSectorsTerminate,
		&storage_market.OnMinerSectorsTerminateParams{
			DealIDs: dealIds,
		},
		abi.NewTokenAmount(0),
	)
	if code != exitcode.Ok {
		return errors.Errorf("failed to terminate sectors deals %v, exit code %v", dealIds, code)
	}
	return nil
}

func (a *StorageMinerActor) verifySurprisePost(rt Runtime, st *StorageMinerActorState, onChainInfo *abi.OnChainSurprisePoStVerifyInfo) {
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
	isVerified := rt.Syscalls().VerifyPoSt(sectorSize, pvInfo)

	if !isVerified {
		rt.Abort(exitcode.ErrIllegalArgument, "Surprise PoSt failed to verify")
	}
}

func (a *StorageMinerActor) _rtVerifySealOrAbort(rt Runtime, onChainInfo *abi.OnChainSealVerifyInfo) {
	var st StorageMinerActorState
	rt.State().Readonly(&st)
	info := st.Info
	sectorSize := info.SectorSize

	// if IsValidAtCommitEpoch(onChainInfo.RegisteredProof, rt.CurrEpoch()) // Ensure proof type is valid at current epoch.
	// Check randomness.
	if onChainInfo.SealEpoch < (rt.CurrEpoch() - ChainFinalityish - MaxSealDuration[abi.RegisteredProof_WinStackedDRG32GiBSeal]) {
		rt.Abort(exitcode.ErrIllegalArgument, "Seal references ticket from invalid epoch")
	}

	var unsealedCID cbg.CborCid
	ret, code := rt.Send(
		builtin.StorageMarketActorAddr,
		builtin.StorageMarketMethods.ComputeDataCommitment,
		&storage_market.ComputeDataCommitmentParams{
			SectorSize: info.SectorSize,
			DealIDs:    onChainInfo.DealIDs,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to fetch piece info")
	autil.AssertNoError(ret.Into(&unsealedCID))

	minerActorID, err := addr.IDFromAddress(rt.CurrReceiver())
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "receiver must be ID address")
	}

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
		UnsealedCID:           cid.Cid(unsealedCID),
	}

	isVerified := rt.Syscalls().VerifySeal(sectorSize, svInfo)

	if !isVerified {
		rt.Abort(exitcode.ErrIllegalState, "Sector seal failed to verify")
	}
}

func confirmFundsReceiptOrAbort_RefundRemainder(rt vmr.Runtime, fundsRequired abi.TokenAmount) {
	if rt.ValueReceived().LessThan(fundsRequired) {
		rt.Abort(exitcode.ErrInsufficientFunds, "Insufficient funds received accompanying message")
	}

	if rt.ValueReceived().GreaterThan(fundsRequired) {
		_, code := rt.Send(rt.ImmediateCaller(), builtin.MethodSend, nil, big.Sub(rt.ValueReceived(), fundsRequired))
		builtin.RequireSuccess(rt, code, "failed to transfer refund")
	}
}
