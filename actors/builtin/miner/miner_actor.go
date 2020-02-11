package miner

import (
	"bytes"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
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
	CronEventType_Miner_SurpriseExpiration CronEventType = iota
	CronEventType_Miner_WorkerKeyChange
	CronEventType_Miner_PreCommitExpiry
	CronEventType_Miner_SectorExpiry
	CronEventType_Miner_TempFault
)

type CronEventPayload struct {
	EventType CronEventType
	// TODO: replace sectors with a bitfield
	Sectors []abi.SectorNumber // Empty for global events, such as SurprisePoSt expiration.
}

type Actor struct{}

func (a Actor) Exports() []interface{} {
	return []interface{}{
		builtin.MethodConstructor: a.Constructor,
		2:                         a.ControlAddresses,
		3:                         a.ChangeWorkerAddress,
		4:                         a.OnSurprisePoStChallenge,
		5:                         a.SubmitSurprisePoStResponse,
		6:                         a.OnDeleteMiner,
		7:                         a.OnVerifiedElectionPoSt,
		8:                         a.PreCommitSector,
		9:                         a.ProveCommitSector,
		10:                        a.ExtendSectorExpiration,
		11:                        a.TerminateSectors,
		12:                        a.DeclareTemporaryFaults,
		13:                        a.OnDeferredCronEvent,
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
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	// TODO: fix this, check that the account actor at the other end of this address has a BLS key.
	if params.WorkerAddr.Protocol() != addr.BLS {
		rt.Abortf(exitcode.ErrIllegalArgument, "Worker Key must be BLS.")
	}

	var st State
	rt.State().Transaction(&st, func() interface{} {
		state, err := ConstructState(adt.AsStore(rt), params.OwnerAddr, params.WorkerAddr, params.PeerId, params.SectorSize)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to construct initial state: %v", err)
		}
		return state
	})
	return &adt.EmptyValue{}
}

/////////////
// Control //
/////////////

type GetControlAddressesReturn struct {
	Owner  addr.Address
	Worker addr.Address
}

func (a Actor) ControlAddresses(rt Runtime, _ *adt.EmptyValue) *GetControlAddressesReturn {
	var st State
	rt.State().Readonly(&st)
	return &GetControlAddressesReturn{
		Owner:  st.Info.Owner,
		Worker: st.Info.Worker,
	}
}

type ChangeWorkerAddressParams struct {
	newKey addr.Address
}

func (a Actor) ChangeWorkerAddress(rt Runtime, params *ChangeWorkerAddressParams) *adt.EmptyValue {
	var effectiveEpoch abi.ChainEpoch
	var st State
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Owner)

		// must be BLS since the worker key will be used alongside a BLS-VRF
		// Specifically, this check isn't quite right
		// TODO: check that the account actor at the other end of this address has a BLS key.
		if params.newKey.Protocol() != addr.BLS {
			rt.Abortf(exitcode.ErrIllegalArgument, "Worker Key must be BLS.")
		}

		effectiveEpoch = rt.CurrEpoch() + WorkerKeyChangeDelay

		// This may replace another pending key change.
		st.Info.PendingWorkerKey = WorkerKeyChange{
			NewWorker:   params.newKey,
			EffectiveAt: effectiveEpoch,
		}
		return nil
	})

	cronPayload := CronEventPayload{
		EventType: CronEventType_Miner_WorkerKeyChange,
		Sectors:   nil,
	}
	a.enrollCronEvent(rt, effectiveEpoch, &cronPayload)
	return &adt.EmptyValue{}
}

//////////////////
// SurprisePoSt //
//////////////////

// Called by Actor to notify StorageMiner of SurprisePoSt Challenge.
func (a Actor) OnSurprisePoStChallenge(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	var st State
	challenged := rt.State().Transaction(&st, func() interface{} {
		// If already challenged, do not challenge again.
		// Failed PoSt will automatically reset the state to not-challenged.
		if st.PoStState.isChallenged() {
			return false
		}

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
		surpriseDuration := power.SurprisePostChallengeDuration
		a.enrollCronEvent(rt, rt.CurrEpoch()+surpriseDuration, &cronPayload)
	}
	return &adt.EmptyValue{}
}

type SubmitSurprisePoStResponseParams struct {
	onChainInfo abi.OnChainSurprisePoStVerifyInfo
}

// Invoked by miner's worker address to submit a response to a pending SurprisePoSt challenge.
func (a Actor) SubmitSurprisePoStResponse(rt Runtime, params *SubmitSurprisePoStResponseParams) *adt.EmptyValue {
	var st State
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)
		if !st.PoStState.isChallenged() {
			rt.Abortf(exitcode.ErrIllegalState, "Not currently challenged")
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
		builtin.MethodsPower.OnMinerSurprisePoStSuccess,
		&adt.EmptyValue{},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to notify storage power actor")
	return &adt.EmptyValue{}
}

// Called by Actor.
func (a Actor) OnDeleteMiner(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)
	rt.DeleteActor()
	return &adt.EmptyValue{}
}

//////////////////
// ElectionPoSt //
//////////////////

// Called by the VM interpreter once an ElectionPoSt has been verified.
func (a Actor) OnVerifiedElectionPoSt(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	if rt.Message().BlockMiner() != rt.Message().Receiver() {
		rt.Abortf(exitcode.ErrForbidden, "receiver must be miner of this block")
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

// Proposals must be posted on chain via sma.PublishStorageDeals before PreCommitSector.
// Optimization: PreCommitSector could contain a list of deals that are not published yet.
func (a Actor) PreCommitSector(rt Runtime, params *SectorPreCommitInfo) *adt.EmptyValue {
	var st State
	rt.State().Readonly(&st)
	rt.ValidateImmediateCallerIs(st.Info.Worker)

	store := adt.AsStore(rt)
	if found, err := st.hasSectorNo(store, params.SectorNumber); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to check sector %v: %v", params.SectorNumber, err)
	} else if found {
		rt.Abortf(exitcode.ErrIllegalArgument, "sector %v already committed", params.SectorNumber)
	}

	depositReq := precommitDeposit(st.getSectorSize(), params.Expiration-rt.CurrEpoch())
	confirmPaymentAndRefundChange(rt, depositReq)

	// TODO HS Check on valid SealEpoch

	rt.State().Transaction(&st, func() interface{} {
		err := st.putPrecommittedSector(store, &SectorPreCommitOnChainInfo{
			Info:             *params,
			PreCommitDeposit: depositReq,
			PreCommitEpoch:   rt.CurrEpoch(),
		})
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to write pre-committed sector %v: %v", params.SectorNumber, err)
		}
		return nil
	})

	if params.Expiration <= rt.CurrEpoch() {
		rt.Abortf(exitcode.ErrIllegalArgument, "sector expiration %v must be after now (%v)", params.Expiration, rt.CurrEpoch())
	}

	// Request deferred Cron check for PreCommit expiry check.
	cronPayload := CronEventPayload{
		EventType: CronEventType_Miner_PreCommitExpiry,
		Sectors:   []abi.SectorNumber{params.SectorNumber},
	}
	expiryBound := rt.CurrEpoch() + PoRepMaxDelay + 1
	a.enrollCronEvent(rt, expiryBound, &cronPayload)

	return &adt.EmptyValue{}
}

type ProveCommitSectorParams struct {
	SectorNumber abi.SectorNumber
	Proof        abi.SealProof
}

func (a Actor) ProveCommitSector(rt Runtime, params *ProveCommitSectorParams) *adt.EmptyValue {
	sectorNo := params.SectorNumber
	store := adt.AsStore(rt)

	var st State
	rt.State().Readonly(&st)
	rt.ValidateImmediateCallerIs(st.Info.Worker)

	precommit, found, err := st.getPrecommittedSector(store, sectorNo)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to get precommitted sector %v: %v", sectorNo, err)
	} else if !found {
		rt.Abortf(exitcode.ErrNotFound, "no precommitted sector %v", sectorNo)
	}

	if rt.CurrEpoch() > precommit.PreCommitEpoch+PoRepMaxDelay || rt.CurrEpoch() < precommit.PreCommitEpoch+PoRepMinDelay {
		rt.Abortf(exitcode.ErrIllegalArgument, "Invalid ProveCommitSector epoch")
	}

	TODO()
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
	var pledgeRequirement abi.TokenAmount
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
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to notify power actor")
	AssertNoError(ret.Into(&pledgeRequirement))

	rt.State().Transaction(&st, func() interface{} {
		newSectorInfo := &SectorOnChainInfo{
			Info:              precommit.Info,
			ActivationEpoch:   rt.CurrEpoch(),
			DealWeight:        dealWeight,
			PledgeRequirement: pledgeRequirement,
		}
		if err = st.putSector(adt.AsStore(rt), newSectorInfo); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to prove commit: %v", err)
		}

		if err = st.deletePrecommittedSector(store, sectorNo); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to delete precommit for sector %v: %v", sectorNo, err)
		}

		st.ProvingSet = st.ComputeProvingSet()
		return nil
	})

	// Request deferred callback for sector expiry.
	cronPayload := CronEventPayload{
		EventType: CronEventType_Miner_SectorExpiry,
		Sectors:   []abi.SectorNumber{sectorNo},
	}
	a.enrollCronEvent(rt, precommit.Info.Expiration, &cronPayload)

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

func (a Actor) ExtendSectorExpiration(rt Runtime, params *ExtendSectorExpirationParams) *adt.EmptyValue {
	var st State
	rt.State().Readonly(&st)
	rt.ValidateImmediateCallerIs(st.Info.Worker)

	store := adt.AsStore(rt)
	sectorNo := params.sectorNumber
	sector, found, err := st.getSector(store, sectorNo)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to load sector %v: %v", sectorNo, err)
	} else if !found {
		rt.Abortf(exitcode.ErrNotFound, "no such sector %v", sectorNo)
	}

	storageWeightDescPrev := asStorageWeightDesc(st.Info.SectorSize, sector)
	pledgePrev := sector.PledgeRequirement

	extensionLength := params.newExpiration - sector.Info.Expiration
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
			PrevPledge: pledgePrev,
			NewWeight:  storageWeightDescNew,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to modify sector weight")
	var newPledgeRequirement abi.TokenAmount
	AssertNoError(ret.Into(&newPledgeRequirement))

	rt.State().Transaction(&st, func() interface{} {
		sector.Info.Expiration = params.newExpiration
		sector.PledgeRequirement = newPledgeRequirement
		if err = st.putSector(store, sector); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to update sector %v, %v", sectorNo, err)
		}
		return nil
	})
	return &adt.EmptyValue{}
}

type TerminateSectorsParams struct {
	sectorNumbers []abi.SectorNumber
}

func (a Actor) TerminateSectors(rt Runtime, params *TerminateSectorsParams) *adt.EmptyValue {
	var st State
	rt.State().Readonly(&st)
	rt.ValidateImmediateCallerIs(st.Info.Worker)

	// Note: this cannot terminate pre-committed but un-proven sectors.
	// They must be allowed to expire (and deposit burnt).
	a.terminateSectors(rt, params.sectorNumbers, power.SectorTerminationManual)

	return &adt.EmptyValue{}
}

////////////
// Faults //
////////////

type DeclareTemporaryFaultsParams struct {
	sectorNumbers []abi.SectorNumber
	duration      abi.ChainEpoch
}

func (a Actor) DeclareTemporaryFaults(rt Runtime, params DeclareTemporaryFaultsParams) *adt.EmptyValue {
	if params.duration <= abi.ChainEpoch(0) {
		rt.Abortf(exitcode.ErrIllegalArgument, "non-positive fault duration %v", params.duration)
	}

	effectiveEpoch := rt.CurrEpoch() + DeclaredFaultEffectiveDelay
	var st State
	requiredFee := rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)

		store := adt.AsStore(rt)
		storageWeightDescs := []*power.SectorStorageWeightDesc{}
		for _, sectorNumber := range params.sectorNumbers {
			sector, found, err := st.getSector(store, sectorNumber)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "failed to load sector %v: %v", sectorNumber, err)
			}
			fault, err := st.FaultSet.Has(uint64(sectorNumber))
			AssertNoError(err)
			Assert(fault == (sector.DeclaredFaultEpoch != epochUndefined))
			Assert(fault == (sector.DeclaredFaultDuration != epochUndefined))
			if !found || fault {
				continue // Ignore declaration for missing or already-faulted sector.
			}

			storageWeightDescs = append(storageWeightDescs, asStorageWeightDesc(st.Info.SectorSize, sector))

			sector.DeclaredFaultEpoch = effectiveEpoch
			sector.DeclaredFaultDuration = params.duration
			if err = st.putSector(store, sector); err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "failed to update sector %v: %v", sectorNumber, err)
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

func (a Actor) OnDeferredCronEvent(rt Runtime, params *OnDeferredCronEventParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	var payload CronEventPayload
	if err := payload.UnmarshalCBOR(bytes.NewReader(params.callbackPayload)); err != nil {
		rt.Abortf(exitcode.ErrIllegalArgument, "failed to deserialize event payload")
	}

	if payload.EventType == CronEventType_Miner_TempFault {
		a.checkTemporaryFaultEvents(rt, payload.Sectors)
	}

	if payload.EventType == CronEventType_Miner_PreCommitExpiry {
		a.checkPrecommitExpiry(rt, payload.Sectors)
	}

	if payload.EventType == CronEventType_Miner_SectorExpiry {
		a.checkSectorExpiry(rt, payload.Sectors)
	}

	if payload.EventType == CronEventType_Miner_SurpriseExpiration {
		a.checkPoStProvingPeriodExpiration(rt)
	}

	if payload.EventType == CronEventType_Miner_WorkerKeyChange {
		a.commitWorkerKeyChange(rt)
	}

	return &adt.EmptyValue{}
}

////////////////////////////////////////////////////////////////////////////////
// Method utility functions
////////////////////////////////////////////////////////////////////////////////

func (a Actor) checkTemporaryFaultEvents(rt Runtime, sectorNos []abi.SectorNumber) {
	store := adt.AsStore(rt)

	var beginFaults []*power.SectorStorageWeightDesc
	var endFaults []*power.SectorStorageWeightDesc
	beginFaultPledge := abi.NewTokenAmount(0)
	endFaultPledge := abi.NewTokenAmount(0)
	var st State
	rt.State().Transaction(&st, func() interface{} {
		for _, sectorNo := range sectorNos {
			sector, found, err := st.getSector(store, sectorNo)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "failed to load sector %v: %v", sectorNo, err)
			} else if !found {
				continue // Sector has been terminated
			}

			hasFault, err := st.FaultSet.Has(uint64(sectorNo))
			AssertNoError(err)
			Assert(hasFault == (sector.DeclaredFaultEpoch != epochUndefined))
			Assert(hasFault == (sector.DeclaredFaultDuration != epochUndefined))

			if !hasFault && rt.CurrEpoch() >= sector.DeclaredFaultEpoch {
				beginFaults = append(beginFaults, asStorageWeightDesc(st.Info.SectorSize, sector))
				beginFaultPledge = big.Add(beginFaultPledge, sector.PledgeRequirement)
				st.FaultSet.Set(uint64(sectorNo))
			}
			if hasFault && rt.CurrEpoch() >= sector.DeclaredFaultEpoch+sector.DeclaredFaultDuration {
				sector.DeclaredFaultEpoch = epochUndefined
				sector.DeclaredFaultDuration = epochUndefined
				endFaults = append(endFaults, asStorageWeightDesc(st.Info.SectorSize, sector))
				endFaultPledge = big.Add(endFaultPledge, sector.PledgeRequirement)
				if err = st.FaultSet.Unset(uint64(sectorNo)); err != nil {
					rt.Abortf(exitcode.ErrIllegalState, "failed to unset fault for %v: %v", sectorNo, err)
				}
				if err = st.putSector(store, sector); err != nil {
					rt.Abortf(exitcode.ErrIllegalState, "failed to update sector %v: %v", sectorNo, err)
				}
			}
		}
		return nil
	})

	if len(beginFaults) > 0 {
		a.requestBeginFaults(rt, beginFaults, beginFaultPledge)
	}

	if len(endFaults) > 0 {
		a.requestEndFaults(rt, endFaults, endFaultPledge)
	}
}

func (a Actor) checkPrecommitExpiry(rt Runtime, sectorNos []abi.SectorNumber) {
	store := adt.AsStore(rt)
	var st State

	depositToBurn := abi.NewTokenAmount(0)
	rt.State().Transaction(&st, func() interface{} {
		for _, sectorNo := range sectorNos {
			sector, found, err := st.getPrecommittedSector(store, sectorNo)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "failed to load sector %v: %v", sectorNo, err)
			}
			if found && rt.CurrEpoch()-sector.PreCommitEpoch > PoRepMaxDelay {
				err = st.deletePrecommittedSector(store, sectorNo)
				if err != nil {
					rt.Abortf(exitcode.ErrIllegalState, "failed to delete precommit %v: %v", sectorNo, err)
				}
				depositToBurn = big.Add(depositToBurn, sector.PreCommitDeposit)
			}
			// Else sector has been terminated.
		}
		return nil
	})

	_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, depositToBurn)
	builtin.RequireSuccess(rt, code, "failed to burn funds")
	return
}

func (a Actor) checkSectorExpiry(rt Runtime, sectorNos []abi.SectorNumber) {
	var st State
	rt.State().Readonly(&st)
	toTerminate := []abi.SectorNumber{}
	for _, sectorNo := range sectorNos {
		sector, found, err := st.getSector(adt.AsStore(rt), sectorNo)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to load sector %v: %v", sectorNo, err)
		}
		if found && rt.CurrEpoch() >= sector.Info.Expiration {
			toTerminate = append(toTerminate, sectorNo)
		}
		// Else sector has been terminated or extended.
	}

	a.terminateSectors(rt, toTerminate, power.SectorTerminationExpired)
	return
}

func (a Actor) terminateSectors(rt Runtime, sectorNos []abi.SectorNumber, terminationType power.SectorTermination) {
	store := adt.AsStore(rt)
	var st State

	var dealIDs []abi.DealID
	var allWeights []*power.SectorStorageWeightDesc
	allPledge := abi.NewTokenAmount(0)
	var faultedWeights []*power.SectorStorageWeightDesc
	faultPledge := abi.NewTokenAmount(0)
	rt.State().Transaction(&st, func() interface{} {
		for _, sectorNo := range sectorNos {
			sector, found, err := st.getSector(store, sectorNo)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "failed to check sector %v: %v", sectorNo, err)
			}
			if !found {
				rt.Abortf(exitcode.ErrNotFound, "no sector %v", sectorNo)
			}

			dealIDs = append(dealIDs, sector.Info.DealIDs...)
			weight := asStorageWeightDesc(st.Info.SectorSize, sector)
			allWeights = append(allWeights, weight)
			allPledge = big.Add(allPledge, sector.PledgeRequirement)

			fault, err := st.FaultSet.Has(uint64(sectorNo))
			AssertNoError(err)
			Assert(fault == (sector.DeclaredFaultEpoch != epochUndefined))
			Assert(fault == (sector.DeclaredFaultDuration != epochUndefined))
			if fault {
				faultedWeights = append(faultedWeights, weight)
				faultPledge = big.Add(faultPledge, sector.PledgeRequirement)
			}

			err = st.deleteSector(store, sectorNo)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "failed to delete sector: %v", err)
			}
		}
		return nil
	})

	// End any fault state before terminating sector power.
	if len(faultedWeights) > 0 {
		a.requestEndFaults(rt, faultedWeights, faultPledge)
	}

	a.requestTerminateDeals(rt, dealIDs)
	a.requestTerminatePower(rt, terminationType, allWeights, allPledge)
}

func (a Actor) checkPoStProvingPeriodExpiration(rt Runtime) {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	var st State
	expired := rt.State().Transaction(&st, func() interface{} {
		if !st.PoStState.isChallenged() {
			return false // Already exited challenged state successfully prior to expiry.
		}

		window := power.SurprisePostChallengeDuration
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
	if st.PoStState.NumConsecutiveFailures > power.SurprisePostFailureLimit {
		a.requestTerminateAllDeals(rt, &st)
	}

	// ... and pay penalty (possibly terminating and deleting the miner).
	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.OnMinerSurprisePoStFailure,
		&power.OnMinerSurprisePoStFailureParams{
			NumConsecutiveFailures: st.PoStState.NumConsecutiveFailures,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to notify power actor")
}

func (a Actor) enrollCronEvent(rt Runtime, eventEpoch abi.ChainEpoch, callbackPayload *CronEventPayload) {
	var payload []byte
	err := callbackPayload.MarshalCBOR(bytes.NewBuffer(payload))
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalArgument, "failed to serialize payload: %v", err)
	}
	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.EnrollCronEvent,
		&power.EnrollCronEventParams{
			EventEpoch: eventEpoch,
			Payload:    payload,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to enroll cron event")
}

func (a Actor) requestBeginFaults(rt Runtime, weights []*power.SectorStorageWeightDesc, pledge abi.TokenAmount) {
	params := &power.OnSectorTemporaryFaultEffectiveBeginParams{
		Weights: make([]power.SectorStorageWeightDesc, len(weights)),
		Pledge:  pledge,
	}
	for i, w := range weights {
		params.Weights[i] = *w
	}

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.OnSectorTemporaryFaultEffectiveBegin,
		params,
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to request faults %v", weights)
}

func (a Actor) requestEndFaults(rt Runtime, weights []*power.SectorStorageWeightDesc, pledge abi.TokenAmount) {
	params := &power.OnSectorTemporaryFaultEffectiveEndParams{
		Weights: make([]power.SectorStorageWeightDesc, len(weights)),
		Pledge:  pledge,
	}
	for i, w := range weights {
		params.Weights[i] = *w
	}

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.OnSectorTemporaryFaultEffectiveEnd,
		params,
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to request end faults %v", weights)
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
	if err := st.forEachSector(adt.AsStore(rt), func(sector *SectorOnChainInfo) {
		dealIds = append(dealIds, sector.Info.DealIDs...)
	}); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to traverse sectors for termination: %v", err)
	}

	a.requestTerminateDeals(rt, dealIds)
}

func (a Actor) requestTerminatePower(rt Runtime, terminationType power.SectorTermination,
	weights []*power.SectorStorageWeightDesc, pledge abi.TokenAmount) {
	params := &power.OnSectorTerminateParams{
		TerminationType: terminationType,
		Weights:         make([]power.SectorStorageWeightDesc, len(weights)),
		Pledge:          pledge,
	}
	for i, w := range weights {
		params.Weights[i] = *w
	}

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.OnSectorTerminate,
		params,
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to terminate sector power type %v, weights %v", terminationType, weights)
}

func (a Actor) verifySurprisePost(rt Runtime, st *State, onChainInfo *abi.OnChainSurprisePoStVerifyInfo) {
	Assert(st.PoStState.isChallenged())
	sectorSize := st.Info.SectorSize
	challengeEpoch := st.PoStState.SurpriseChallengeEpoch

	// verify no duplicate tickets
	challengeIndices := make(map[int64]bool)
	for _, tix := range onChainInfo.Candidates {
		if _, ok := challengeIndices[tix.ChallengeIndex]; ok {
			rt.Abortf(exitcode.ErrIllegalArgument, "Invalid Surprise PoSt. Duplicate ticket included.")
		}
		challengeIndices[tix.ChallengeIndex] = true
	}

	// verify appropriate number of tickets is present
	if int64(len(onChainInfo.Candidates)) != NumSurprisePoStSectors {
		rt.Abortf(exitcode.ErrIllegalArgument, "Invalid Surprise PoSt. Too few tickets included.")
	}

	randomnessK := rt.GetRandomness(challengeEpoch - PoStLookback)
	// regenerate randomness used. The PoSt Verification below will fail if
	// the same was not used to generate the proof
	postRandomness := crypto.DeriveRandWithMinerAddr(crypto.DomainSeparationTag_SurprisePoStChallengeSeed, randomnessK, rt.Message().Receiver())

	// Get public inputs

	pvInfo := abi.PoStVerifyInfo{
		Candidates: onChainInfo.Candidates,
		Proofs:     onChainInfo.Proofs,
		Randomness: abi.PoStRandomness(postRandomness),
		// EligibleSectors_: FIXME: verification needs these.
	}

	// Verify the PoSt Proof
	if !rt.Syscalls().VerifyPoSt(sectorSize, pvInfo) {
		rt.Abortf(exitcode.ErrIllegalArgument, "invalid PoSt %+v", pvInfo)
	}
}

func (a Actor) verifySeal(rt Runtime, sectorSize abi.SectorSize, onChainInfo *abi.OnChainSealVerifyInfo) {
	// Check randomness.
	sealEarliest := rt.CurrEpoch() - ChainFinalityish - MaxSealDuration[abi.RegisteredProof_WinStackedDRG32GiBSeal]
	if onChainInfo.SealEpoch < sealEarliest {
		rt.Abortf(exitcode.ErrIllegalArgument, "seal epoch %v too old, expected >= %v", onChainInfo.SealEpoch, sealEarliest)
	}

	commD := a.requestUnsealedSectorCID(rt, sectorSize, onChainInfo.DealIDs)

	minerActorID, err := addr.IDFromAddress(rt.Message().Receiver())
	AssertNoError(err) // Runtime always provides ID-addresses

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
		rt.Abortf(exitcode.ErrIllegalState, "invalid seal %+v", svInfo)
	}
}

// Requests the storage market actor compute the unsealed sector CID from a sector's deals.
func (a Actor) requestUnsealedSectorCID(rt Runtime, sectorSize abi.SectorSize, dealIDs []abi.DealID) cid.Cid {
	var unsealedCID cbg.CborCid
	ret, code := rt.Send(
		builtin.StorageMarketActorAddr,
		builtin.MethodsMarket.ComputeDataCommitment,
		&market.ComputeDataCommitmentParams{
			SectorSize: sectorSize,
			DealIDs:    dealIDs,
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed request for unsealed sector CID for deals %v", dealIDs)
	AssertNoError(ret.Into(&unsealedCID))
	return cid.Cid(unsealedCID)
}

func (a Actor) commitWorkerKeyChange(rt Runtime) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.CronActorAddr)

	var st State
	rt.State().Transaction(&st, func() interface{} {
		if (st.Info.PendingWorkerKey == WorkerKeyChange{}) {
			rt.Abortf(exitcode.ErrIllegalState, "No pending key change.")
		}

		if st.Info.PendingWorkerKey.EffectiveAt > rt.CurrEpoch() {
			rt.Abortf(exitcode.ErrIllegalState, "Too early for key change. Current: %v, Change: %v)", rt.CurrEpoch(), st.Info.PendingWorkerKey.EffectiveAt)
		}

		st.Info.Worker = st.Info.PendingWorkerKey.NewWorker
		st.Info.PendingWorkerKey = WorkerKeyChange{}

		return nil
	})
	return &adt.EmptyValue{}
}

func confirmPaymentAndRefundChange(rt vmr.Runtime, expected abi.TokenAmount) {
	if rt.Message().ValueReceived().LessThan(expected) {
		rt.Abortf(exitcode.ErrInsufficientFunds, "insufficient funds received, expected %v", expected)
	}

	if rt.Message().ValueReceived().GreaterThan(expected) {
		_, code := rt.Send(rt.Message().Caller(), builtin.MethodSend, nil, big.Sub(rt.Message().ValueReceived(), expected))
		builtin.RequireSuccess(rt, code, "failed to transfer refund")
	}
}
