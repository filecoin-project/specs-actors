package miner

import (
	"bytes"

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
	cronEventWindowedPoStExpiration CronEventType = iota
	cronEventWorkerKeyChange
	cronEventPreCommitExpiry
	cronEventSectorExpiry
	cronEventTempFault
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
		6:                         a.OnDeleteMiner,
		7:                         a.PreCommitSector,
		8:                         a.ProveCommitSector,
		9:                         a.ExtendSectorExpiration,
		10:                        a.TerminateSectors,
		11:                        a.DeclareTemporaryFaults,
		12:                        a.OnDeferredCronEvent,
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
	NewKey addr.Address
}

func (a Actor) ChangeWorkerAddress(rt Runtime, params *ChangeWorkerAddressParams) *adt.EmptyValue {
	var effectiveEpoch abi.ChainEpoch
	var st State
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Owner)

		// must be BLS since the worker key will be used alongside a BLS-VRF
		// Specifically, this check isn't quite right
		// TODO: check that the account actor at the other end of this address has a BLS key.
		if params.NewKey.Protocol() != addr.BLS {
			rt.Abortf(exitcode.ErrIllegalArgument, "Worker Key must be BLS.")
		}

		effectiveEpoch = rt.CurrEpoch() + WorkerKeyChangeDelay

		// This may replace another pending key change.
		st.Info.PendingWorkerKey = WorkerKeyChange{
			NewWorker:   params.NewKey,
			EffectiveAt: effectiveEpoch,
		}
		return nil
	})

	cronPayload := CronEventPayload{
		EventType: cronEventWorkerKeyChange,
		Sectors:   nil,
	}
	a.enrollCronEvent(rt, effectiveEpoch, &cronPayload)
	return &adt.EmptyValue{}
}

type ChangePeerIDParams struct {
	NewID peer.ID
}

func (a Actor) ChangePeerID(rt Runtime, params *ChangePeerIDParams) *adt.EmptyValue {
	var st State
	rt.State().Transaction(&st, func() interface{} {
		st.Info.PeerId = params.NewID
		return nil
	})
	return &adt.EmptyValue{}
}

//////////////////
// WindowedPoSt //
//////////////////

// Invoked by miner's worker address to submit their fallback post
func (a Actor) SubmitWindowedPoSt(rt Runtime, params *abi.OnChainPoStVerifyInfo) *adt.EmptyValue {
	var st State
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)

		if rt.CurrEpoch() > st.PoStState.ProvingPeriodStart+power.WindowedPostChallengeDuration {
			rt.Abortf(exitcode.ErrIllegalState, "PoSt submission too late")
		}

		if rt.CurrEpoch() <= st.PoStState.ProvingPeriodStart {
			rt.Abortf(exitcode.ErrIllegalState, "Not currently in submission window for PoSt")
		}

		// A failed verification doesn't necessarily immediately cause a penalty
		// The miner has until the end of the window to submit a good proof
		a.verifyWindowedPost(rt, &st, params)

		// increment proving period start
		st.PoStState = PoStState{
			ProvingPeriodStart:     st.PoStState.ProvingPeriodStart + ProvingPeriod,
			NumConsecutiveFailures: 0,
		}

		// reset provingSet to include all sectors (were not included during challenge period)
		st.ProvingSet = st.Sectors

		return nil
	})

	// if PoSt is valid, notify the power actor to remove detected faults
	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.OnMinerWindowedPoStSuccess,
		nil,
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to call OnMinerWindowedPoStSuccess in Power Actor")

	return &adt.EmptyValue{}
}

// Called by Actor.
func (a Actor) OnDeleteMiner(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)
	rt.DeleteActor()
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
	if found, err := st.HasSectorNo(store, params.SectorNumber); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to check sector %v: %v", params.SectorNumber, err)
	} else if found {
		rt.Abortf(exitcode.ErrIllegalArgument, "sector %v already committed", params.SectorNumber)
	}

	depositReq := precommitDeposit(st.GetSectorSize(), params.Expiration-rt.CurrEpoch())
	confirmPaymentAndRefundChange(rt, depositReq)

	// TODO HS Check on valid SealEpoch

	rt.State().Transaction(&st, func() interface{} {
		err := st.PutPrecommittedSector(store, &SectorPreCommitOnChainInfo{
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

	bf := abi.NewBitField()
	bf.Set(uint64(params.SectorNumber))

	// Request deferred Cron check for PreCommit expiry check.
	cronPayload := CronEventPayload{
		EventType: cronEventPreCommitExpiry,
		Sectors:   &bf,
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

	precommit, found, err := st.GetPrecommittedSector(store, sectorNo)
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

	// will abort if seal invalid
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
	// Returns relevant pledge requirement.
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

	// add sector to miner state
	rt.State().Transaction(&st, func() interface{} {
		newSectorInfo := &SectorOnChainInfo{
			Info:              precommit.Info,
			ActivationEpoch:   rt.CurrEpoch(),
			DealWeight:        dealWeight,
			PledgeRequirement: pledgeRequirement,
		}

		if err = st.PutSector(store, newSectorInfo); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to prove commit: %v", err)
		}

		if err = st.DeletePrecommittedSector(store, sectorNo); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to delete precommit for sector %v: %v", sectorNo, err)
		}

		// if first sector, set proving period start at next period
		len, err := adt.AsArray(store, st.Sectors).Length()
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to check miner sectors sizes: %v", err)
		}
		if len == 1 {
			st.PoStState.ProvingPeriodStart = rt.CurrEpoch() + ProvingPeriod
		}

		// Do not update proving set during challenge window
		if !st.InChallengeWindow(rt) {
			st.ProvingSet = st.Sectors
		}
		return nil
	})

	bf := abi.NewBitField()
	bf.Set(uint64(sectorNo))

	// Request deferred callback for sector expiry.
	cronPayload := CronEventPayload{
		EventType: cronEventSectorExpiry,
		Sectors:   &bf,
	}
	a.enrollCronEvent(rt, precommit.Info.Expiration, &cronPayload)

	// If first sector
	len, err := adt.AsArray(store, st.Sectors).Length()
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to check miner sectors sizes: %v", err)
	}
	if len == 1 {
		// enroll expiration check
		a.enrollCronEvent(rt, st.PoStState.ProvingPeriodStart+power.WindowedPostChallengeDuration, &CronEventPayload{
			EventType: cronEventWindowedPoStExpiration,
		})
	}

	// Return PreCommit deposit to worker upon successful ProveCommit.
	_, code = rt.Send(st.Info.Worker, builtin.MethodSend, nil, precommit.PreCommitDeposit)
	builtin.RequireSuccess(rt, code, "failed to send funds")
	return &adt.EmptyValue{}
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

	storageWeightDescPrev := asStorageWeightDesc(st.Info.SectorSize, sector)
	pledgePrev := sector.PledgeRequirement

	extensionLength := params.NewExpiration - sector.Info.Expiration
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
		sector.Info.Expiration = params.NewExpiration
		sector.PledgeRequirement = newPledgeRequirement
		if err = st.PutSector(store, sector); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to update sector %v, %v", sectorNo, err)
		}
		return nil
	})
	return &adt.EmptyValue{}
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
	return &adt.EmptyValue{}
}

////////////
// Faults //
////////////

type DeclareTemporaryFaultsParams struct {
	SectorNumbers abi.BitField
	Duration      abi.ChainEpoch
}

func (a Actor) DeclareTemporaryFaults(rt Runtime, params DeclareTemporaryFaultsParams) *adt.EmptyValue {
	if params.Duration <= abi.ChainEpoch(0) {
		rt.Abortf(exitcode.ErrIllegalArgument, "non-positive fault Duration %v", params.Duration)
	}

	effectiveEpoch := rt.CurrEpoch() + DeclaredFaultEffectiveDelay
	var st State
	requiredFee := rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)

		store := adt.AsStore(rt)
		storageWeightDescs := []*power.SectorStorageWeightDesc{}
		sectors, err := params.SectorNumbers.All(MaxFaultsCount)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalArgument, "failed to enumerate faulted sector list")
		}

		for _, sectorNumber := range sectors {
			sector, found, err := st.GetSector(store, abi.SectorNumber(sectorNumber))
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
			sector.DeclaredFaultDuration = params.Duration
			if err = st.PutSector(store, sector); err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "failed to update sector %v: %v", sectorNumber, err)
			}
		}
		return temporaryFaultFee(storageWeightDescs, params.Duration)
	}).(abi.TokenAmount)

	// Burn the fee, refund any change.
	confirmPaymentAndRefundChange(rt, requiredFee)
	_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, requiredFee)
	builtin.RequireSuccess(rt, code, "failed to burn fee")

	// Request deferred Cron invocation to update temporary fault state.
	// TODO: cant we just lazily clean this up?
	cronPayload := CronEventPayload{
		EventType: cronEventTempFault,
		Sectors:   &params.SectorNumbers,
	}
	// schedule cron event to start marking temp fault at BeginEpoch
	a.enrollCronEvent(rt, effectiveEpoch, &cronPayload)
	// schedule cron event to end marking temp fault at EndEpoch
	a.enrollCronEvent(rt, effectiveEpoch+params.Duration, &cronPayload)
	return &adt.EmptyValue{}
}

//////////
// Cron //
//////////

type OnDeferredCronEventParams struct {
	CallbackPayload []byte
}

func (a Actor) OnDeferredCronEvent(rt Runtime, params *OnDeferredCronEventParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	var payload CronEventPayload
	if err := payload.UnmarshalCBOR(bytes.NewReader(params.CallbackPayload)); err != nil {
		rt.Abortf(exitcode.ErrIllegalArgument, "failed to deserialize event payload")
	}

	if payload.EventType == cronEventTempFault {
		a.checkTemporaryFaultEvents(rt, payload.Sectors)
	}

	if payload.EventType == cronEventPreCommitExpiry {
		a.checkPrecommitExpiry(rt, payload.Sectors)
	}

	if payload.EventType == cronEventSectorExpiry {
		a.checkSectorExpiry(rt, payload.Sectors)
	}

	if payload.EventType == cronEventWindowedPoStExpiration {
		a.checkPoStProvingPeriodExpiration(rt)
	}

	if payload.EventType == cronEventWorkerKeyChange {
		a.commitWorkerKeyChange(rt)
	}

	return &adt.EmptyValue{}
}

////////////////////////////////////////////////////////////////////////////////
// Method utility functions
////////////////////////////////////////////////////////////////////////////////

func (a Actor) checkTemporaryFaultEvents(rt Runtime, sectors *abi.BitField) {
	store := adt.AsStore(rt)

	var beginFaults []*power.SectorStorageWeightDesc
	var endFaults []*power.SectorStorageWeightDesc
	beginFaultPledge := abi.NewTokenAmount(0)
	endFaultPledge := abi.NewTokenAmount(0)
	var st State

	sectorNos := bitfieldToSectorNos(rt, sectors)

	rt.State().Transaction(&st, func() interface{} {
		for _, sectorNo := range sectorNos {
			sector, found, err := st.GetSector(store, sectorNo)
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
				if err = st.PutSector(store, sector); err != nil {
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

func (a Actor) checkPrecommitExpiry(rt Runtime, sectors *abi.BitField) {
	store := adt.AsStore(rt)
	var st State

	sectorNos := bitfieldToSectorNos(rt, sectors)

	depositToBurn := abi.NewTokenAmount(0)
	rt.State().Transaction(&st, func() interface{} {
		for _, sectorNo := range sectorNos {
			sector, found, err := st.GetPrecommittedSector(store, sectorNo)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "failed to load sector %v: %v", sectorNo, err)
			}
			if found && rt.CurrEpoch()-sector.PreCommitEpoch > PoRepMaxDelay {
				err = st.DeletePrecommittedSector(store, sectorNo)
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

func (a Actor) checkSectorExpiry(rt Runtime, sectors *abi.BitField) {
	sectorNos := bitfieldToSectorNos(rt, sectors)

	var st State
	rt.State().Readonly(&st)
	toTerminate := []abi.SectorNumber{}
	for _, sectorNo := range sectorNos {
		sector, found, err := st.GetSector(adt.AsStore(rt), sectorNo)
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

// TODO: red flag that this method is potentially super expensive
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
			sector, found, err := st.GetSector(store, sectorNo)
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

			err = st.DeleteSector(store, sectorNo)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "failed to delete sector: %v", err)
			}

			if !st.InChallengeWindow(rt) {
				st.ProvingSet = st.Sectors
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

		window := power.WindowedPostChallengeDuration
		if rt.CurrEpoch() < st.PoStState.ProvingPeriodStart+window {
			// NB: We don't expect this to be possible, need to guarantee with tests
			rt.Abortf(exitcode.ErrIllegalState, "should not be able to check post proving period expiration when not inside window")
		}

		// Increment count of consecutive failures and provingPeriodStart.
		st.PoStState = PoStState{
			ProvingPeriodStart:     st.PoStState.ProvingPeriodStart + ProvingPeriod,
			NumConsecutiveFailures: st.PoStState.NumConsecutiveFailures + 1,
		}
		return true
	}).(bool)

	if !expired {
		return
	}

	// Period has expired.
	// Terminate deals...
	if st.PoStState.NumConsecutiveFailures > power.WindowedPostFailureLimit {
		a.requestTerminateAllDeals(rt, &st)
	}

	// ... and pay penalty (possibly terminating and deleting the miner).
	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.OnMinerWindowedPoStFailure,
		&power.OnMinerWindowedPoStFailureParams{
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
	if err := st.ForEachSector(adt.AsStore(rt), func(sector *SectorOnChainInfo) {
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

func (a Actor) verifyWindowedPost(rt Runtime, st *State, onChainInfo *abi.OnChainPoStVerifyInfo) {
	sectorSize := st.Info.SectorSize

	// TODO: verifying no duplicates here seems wrong, we should be verifying
	// that exactly what we expect is passed in (this isnt election post)

	// verify no duplicate tickets
	challengeIndices := make(map[int64]bool)
	for _, tix := range onChainInfo.Candidates {
		if _, ok := challengeIndices[tix.ChallengeIndex]; ok {
			rt.Abortf(exitcode.ErrIllegalArgument, "Invalid Windowed PoSt. Duplicate ticket included.")
		}
		challengeIndices[tix.ChallengeIndex] = true
	}

	// verify appropriate number of tickets is present
	if int64(len(onChainInfo.Candidates)) != NumWindowedPoStSectors {
		rt.Abortf(exitcode.ErrIllegalArgument, "Invalid Windowed PoSt. Too few tickets included.")
	}

	// regenerate randomness used. The PoSt Verification below will fail if
	// the same was not used to generate the proof

	store := adt.AsStore(rt)
	provingSet, err := st.ComputeProvingSet(store)

	var sectorInfos []abi.SectorInfo
	var ssinfo SectorOnChainInfo
	err := provingSet.ForEach(&ssinfo, func(i int64) error {

		// TODO: faults!!!!
		sectorInfos = append(sectorInfos, abi.SectorInfo{
			SealedCID:    ssinfo.Info.SealedCID,
			SectorNumber: ssinfo.Info.SectorNumber,
		})
		return nil
	})
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to enumerate proving set: %v", err)
	}

	var addrBuf bytes.Buffer
	err = rt.Message().Receiver().MarshalCBOR(&addrBuf)
	AssertNoError(err)
	postRandomness := rt.GetRandomness(crypto.DomainSeparationTag_WindowedPoStChallengeSeed, st.PoStState.ProvingPeriodStart, addrBuf.Bytes())

	// Get public inputs
	pvInfo := abi.PoStVerifyInfo{
		Candidates:      onChainInfo.Candidates,
		Proofs:          onChainInfo.Proofs,
		Randomness:      abi.PoStRandomness(postRandomness),
		EligibleSectors: sectorInfos,
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

	svInfoRandomness := rt.GetRandomness(crypto.DomainSeparationTag_SealRandomness, onChainInfo.SealEpoch, nil)
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
