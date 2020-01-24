package storage_miner

import (
	"bytes"

	big "github.com/filecoin-project/specs-actors/actors/abi/big"

	addr "github.com/filecoin-project/go-address"
	peer "github.com/libp2p/go-libp2p-core/peer"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	storage_market "github.com/filecoin-project/specs-actors/actors/builtin/storage_market"
	crypto "github.com/filecoin-project/specs-actors/actors/crypto"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	indices "github.com/filecoin-project/specs-actors/actors/runtime/indices"
	serde "github.com/filecoin-project/specs-actors/actors/serde"
	autil "github.com/filecoin-project/specs-actors/actors/util"
)

type SectorStorageWeightDesc = autil.SectorStorageWeightDesc
type SectorTerminationType = autil.SectorTermination

type Runtime = vmr.Runtime

var Assert = autil.Assert
var IMPL_FINISH = autil.IMPL_FINISH
var TODO = autil.TODO

const epochUndefined = abi.ChainEpoch(-1)

type StorageMinerActor struct{}

//////////////////
// SurprisePoSt //
//////////////////

// Called by StoragePowerActor to notify StorageMiner of SurprisePoSt Challenge.
func (a *StorageMinerActor) OnSurprisePoStChallenge(rt Runtime) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	var st StorageMinerActorState
	challenged := rt.State().Transaction(&st, func() interface{} {
		// If already challenged, do not challenge again.
		// Failed PoSt will automatically reset the state to not-challenged.
		if st.PoStState.Is_Challenged() {
			return false
		}
		// Do not challenge if the last successful PoSt was recent enough.
		noChallengePeriod := indices.StorageMining_PoStNoChallengePeriod()
		if st.PoStState.LastSuccessfulPoSt >= rt.CurrEpoch()-noChallengePeriod {
			return false
		}

		var curRecBuf bytes.Buffer
		err := rt.CurrReceiver().MarshalCBOR(&curRecBuf)
		autil.AssertNoError(err)

		randomnessK := rt.GetRandomness(rt.CurrEpoch() - indices.StorageMining_SpcLookbackPoSt())
		challengedSectorsRandomness := crypto.DeriveRandWithMinerAddr(crypto.DomainSeparationTag_SurprisePoStSampleSectors, randomnessK, rt.CurrReceiver())

		challengedSectors := _surprisePoStSampleChallengedSectors(
			challengedSectorsRandomness,
			SectorNumberSetHAMT_Items(st.ProvingSet),
		)

		st.PoStState = MinerPoStState{
			LastSuccessfulPoSt:     st.PoStState.LastSuccessfulPoSt,
			SurpriseChallengeEpoch: rt.CurrEpoch(),
			ChallengedSectors:      challengedSectors,
			NumConsecutiveFailures: st.PoStState.NumConsecutiveFailures,
		}
		return true
	}).(bool)

	if challenged {
		// Request deferred Cron check for SurprisePoSt challenge expiry.
		provingPeriod := indices.StorageMining_SurprisePoStProvingPeriod()
		a._rtEnrollCronEvent(rt, rt.CurrEpoch()+provingPeriod, []abi.SectorNumber{})
	}
	return &vmr.EmptyReturn{}
}

// Invoked by miner's worker address to submit a response to a pending SurprisePoSt challenge.
func (a *StorageMinerActor) SubmitSurprisePoStResponse(rt Runtime, onChainInfo abi.OnChainSurprisePoStVerifyInfo) *vmr.EmptyReturn {
	var st StorageMinerActorState
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)
		if !st.PoStState.Is_Challenged() {
			rt.AbortStateMsg("Not currently challenged")
		}
		a.verifySurprisePost(rt, &st, &onChainInfo)

		st.PoStState = MinerPoStState{
			LastSuccessfulPoSt:     rt.CurrEpoch(),
			SurpriseChallengeEpoch: epochUndefined,
			ChallengedSectors:      nil,
			NumConsecutiveFailures: 0,
		}
		return nil
	})

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.Method_StoragePowerActor_OnMinerSurprisePoStSuccess,
		nil,
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to notify storage power actor")
	return &vmr.EmptyReturn{}
}

// Called by StoragePowerActor.
func (a *StorageMinerActor) OnDeleteMiner(rt Runtime) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)
	minerAddr := rt.CurrReceiver()
	rt.DeleteActor(minerAddr)
	return &vmr.EmptyReturn{}
}

//////////////////
// ElectionPoSt //
//////////////////

// Called by the VM interpreter once an ElectionPoSt has been verified.
func (a *StorageMinerActor) OnVerifiedElectionPoSt(rt Runtime) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	if rt.ToplevelBlockWinner() != rt.CurrReceiver() {
		rt.Abort(exitcode.ErrForbidden, "receiver must be miner of this block")
	}

	var st StorageMinerActorState
	rt.State().Transaction(&st, func() interface{} {
		updateSuccessEpoch := st.PoStState.Is_OK()

		// Advance the timestamp of the most recent PoSt success, provided the miner is currently
		// in normal state. (Cannot do this if SurprisePoSt mechanism already underway.)
		if updateSuccessEpoch {
			st.PoStState = MinerPoStState{
				LastSuccessfulPoSt:     rt.CurrEpoch(),
				SurpriseChallengeEpoch: st.PoStState.SurpriseChallengeEpoch, // expected to be undef because PoStState is OK
				ChallengedSectors:      st.PoStState.ChallengedSectors,      // expected to be empty
				NumConsecutiveFailures: st.PoStState.NumConsecutiveFailures, // expected to be 0
			}
		}
		return nil
	})
	return &vmr.EmptyReturn{}
}

///////////////////////
// Sector Commitment //
///////////////////////

// Deals must be posted on chain via sma.PublishStorageDeals before PreCommitSector.
// Optimization: PreCommitSector could contain a list of deals that are not published yet.
func (a *StorageMinerActor) PreCommitSector(rt Runtime, info SectorPreCommitInfo) *vmr.EmptyReturn {
	var st StorageMinerActorState
	rt.State().Readonly(&st)
	rt.ValidateImmediateCallerIs(st.Info.Worker)
	if _, found := st.Sectors[info.SectorNumber]; found {
		rt.AbortStateMsg("Sector number already exists in table")
	}

	cidx := rt.CurrIndices()
	depositReq := cidx.StorageMining_PreCommitDeposit(st.Info.SectorSize, info.Expiration)
	builtin.RT_ConfirmFundsReceiptOrAbort_RefundRemainder(rt, depositReq)

	// Verify deals with StorageMarketActor; abort if this fails.
	// (Note: committed-capacity sectors contain no deals, so in that case verification will pass trivially.)
	_, code := rt.Send(
		builtin.StorageMarketActorAddr,
		builtin.Method_StorageMarketActor_OnMinerSectorPreCommit_VerifyDealsOrAbort,
		serde.MustSerializeParams(
			info.DealIDs,
			info,
		),
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to verify deals")

	rt.State().Transaction(&st, func() interface{} {
		st.Sectors[info.SectorNumber] = SectorOnChainInfo{
			State:            PreCommit,
			Info:             info,
			PreCommitDeposit: depositReq,
			PreCommitEpoch:   rt.CurrEpoch(),
			ActivationEpoch:  epochUndefined,
			DealWeight:       big.NewInt(-1),
		}
		return nil
	})

	// Request deferred Cron check for PreCommit expiry check.
	expiryBound := rt.CurrEpoch() + indices.StorageMining_MaxProveCommitSectorEpoch() + 1
	a._rtEnrollCronEvent(rt, expiryBound, []abi.SectorNumber{info.SectorNumber})

	if info.Expiration <= rt.CurrEpoch() {
		rt.Abort(exitcode.ErrIllegalArgument, "PreCommit sector must expire (%v) after now (%v)", info.Expiration, rt.CurrEpoch())
	}

	a._rtEnrollCronEvent(rt, info.Expiration, []abi.SectorNumber{info.SectorNumber})
	return &vmr.EmptyReturn{}
}

func (a *StorageMinerActor) ProveCommitSector(rt Runtime, info SectorProveCommitInfo) *vmr.EmptyReturn {
	var st StorageMinerActorState
	rt.State().Readonly(&st)
	workerAddr := st.Info.Worker
	rt.ValidateImmediateCallerIs(workerAddr)

	preCommitSector, found := st.Sectors[info.SectorNumber]
	if !found {
		rt.Abort(exitcode.ErrNotFound, "no such sector %v", info.SectorNumber)
	}
	if preCommitSector.State != PreCommit {
		rt.Abort(exitcode.ErrIllegalArgument, "invalid sector state %v", preCommitSector.State)
	}

	if rt.CurrEpoch() > preCommitSector.PreCommitEpoch+indices.StorageMining_MaxProveCommitSectorEpoch() || rt.CurrEpoch() < preCommitSector.PreCommitEpoch+indices.StorageMining_MinProveCommitSectorEpoch() {
		rt.AbortStateMsg("Invalid ProveCommitSector epoch")
	}

	TODO()
	// TODO: How are SealEpoch, InteractiveEpoch determined (and intended to be used)?
	// Presumably they cannot be derived from the SectorProveCommitInfo provided by an untrusted party.

	a._rtVerifySealOrAbort(rt, &abi.OnChainSealVerifyInfo{
		SealedCID:        preCommitSector.Info.SealedCID,
		SealEpoch:        preCommitSector.Info.SealEpoch,
		InteractiveEpoch: info.InteractiveEpoch,
		RegisteredProof:  info.RegisteredProof,
		Proof:            info.Proof,
		DealIDs:          preCommitSector.Info.DealIDs,
		SectorNumber:     preCommitSector.Info.SectorNumber,
	})

	// Check (and activate) storage deals associated to sector. Abort if checks failed.
	_, code := rt.Send(
		builtin.StorageMarketActorAddr,
		builtin.Method_StorageMarketActor_OnMinerSectorProveCommit_VerifyDealsOrAbort,
		serde.MustSerializeParams(
			preCommitSector.Info.DealIDs,
			info,
		),
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to verify deals")

	var dealset storage_market.GetWeightForDealSetReturn
	ret, code := rt.Send(
		builtin.StorageMarketActorAddr,
		builtin.Method_StorageMarketActor_GetWeightForDealSet,
		serde.MustSerializeParams(
			preCommitSector.Info.DealIDs,
		),
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to get deal set weight")
	autil.AssertNoError(ret.Into(dealset))

	rt.State().Transaction(&st, func() interface{} {
		st.Sectors[info.SectorNumber] = SectorOnChainInfo{
			State:           Active,
			Info:            preCommitSector.Info,
			PreCommitEpoch:  preCommitSector.PreCommitEpoch,
			ActivationEpoch: rt.CurrEpoch(),
			DealWeight:      dealset.Weight,
		}

		st.ProvingSet[info.SectorNumber] = true
		return nil
	})

	// Request deferred Cron check for sector expiry.
	a._rtEnrollCronEvent(
		rt, preCommitSector.Info.Expiration, []abi.SectorNumber{info.SectorNumber})

	// Notify SPA to update power associated to newly activated sector.
	storageWeightDesc := a._rtGetStorageWeightDescForSector(rt, info.SectorNumber)
	_, code = rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.Method_StoragePowerActor_OnSectorProveCommit,
		serde.MustSerializeParams(
			storageWeightDesc,
		),
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to notify power actor")

	// Return PreCommit deposit to worker upon successful ProveCommit.
	_, code = rt.Send(workerAddr, builtin.MethodSend, nil, preCommitSector.PreCommitDeposit)
	builtin.RequireSuccess(rt, code, "failed to send funds")
	return &vmr.EmptyReturn{}
}

/////////////////////////
// Sector Modification //
/////////////////////////

func (a *StorageMinerActor) ExtendSectorExpiration(rt Runtime, sectorNumber abi.SectorNumber, newExpiration abi.ChainEpoch) *vmr.EmptyReturn {
	storageWeightDescPrev := a._rtGetStorageWeightDescForSector(rt, sectorNumber)

	var extensionLength abi.ChainEpoch
	var st StorageMinerActorState
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)

		sectorInfo, found := st.Sectors[sectorNumber]
		if !found {
			rt.AbortStateMsg("Sector not found")
		}

		extensionLength = newExpiration - sectorInfo.Info.Expiration
		if extensionLength < 0 {
			rt.AbortStateMsg("Cannot reduce sector expiration")
		}

		sectorInfo.Info.Expiration = newExpiration
		st.Sectors[sectorNumber] = sectorInfo
		return nil
	})

	storageWeightDescNew := storageWeightDescPrev
	storageWeightDescNew.Duration = storageWeightDescPrev.Duration + extensionLength

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.Method_StoragePowerActor_OnSectorModifyWeightDesc,
		serde.MustSerializeParams(
			storageWeightDescPrev,
			storageWeightDescNew,
		),
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to modify sector weight")
	return &vmr.EmptyReturn{}
}

func (a *StorageMinerActor) TerminateSectors(rt Runtime, sectorNumbers []abi.SectorNumber) *vmr.EmptyReturn {
	var st StorageMinerActorState
	rt.State().Readonly(&st)
	rt.ValidateImmediateCallerIs(st.Info.Worker)

	for _, sectorNumber := range sectorNumbers {
		a._rtTerminateSector(rt, sectorNumber, autil.UserTermination)
	}

	return &vmr.EmptyReturn{}
}

////////////
// Faults //
////////////

func (a *StorageMinerActor) DeclareTemporaryFaults(rt Runtime, sectorNumbers []abi.SectorNumber, duration abi.ChainEpoch) *vmr.EmptyReturn {
	if duration <= abi.ChainEpoch(0) {
		rt.Abort(exitcode.ErrIllegalArgument, "non-positive fault duration %v", duration)
	}

	storageWeightDescs := a._rtGetStorageWeightDescsForSectors(rt, sectorNumbers)
	cidx := rt.CurrIndices()
	requiredFee := cidx.StorageMining_TemporaryFaultFee(storageWeightDescs, duration)

	builtin.RT_ConfirmFundsReceiptOrAbort_RefundRemainder(rt, requiredFee)
	_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, requiredFee)
	builtin.RequireSuccess(rt, code, "failed to burn fee")

	effectiveBeginEpoch := rt.CurrEpoch() + indices.StorageMining_DeclaredFaultEffectiveDelay()
	effectiveEndEpoch := effectiveBeginEpoch + duration

	var st StorageMinerActorState
	rt.State().Transaction(&st, func() interface{} {
		rt.ValidateImmediateCallerIs(st.Info.Worker)

		for _, sectorNumber := range sectorNumbers {
			sectorInfo, found := st.Sectors[sectorNumber]
			if !found || sectorInfo.State != Active {
				continue
			}

			sectorInfo.State = TemporaryFault
			sectorInfo.DeclaredFaultEpoch = rt.CurrEpoch()
			sectorInfo.DeclaredFaultDuration = duration
			st.Sectors[sectorNumber] = sectorInfo
		}
		return nil
	})

	// Request deferred Cron invocation to update temporary fault state.
	a._rtEnrollCronEvent(rt, effectiveBeginEpoch, sectorNumbers)
	a._rtEnrollCronEvent(rt, effectiveEndEpoch, sectorNumbers)
	return &vmr.EmptyReturn{}
}

//////////
// Cron //
//////////

func (a *StorageMinerActor) OnDeferredCronEvent(rt Runtime, sectorNumbers []abi.SectorNumber) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	for _, sectorNumber := range sectorNumbers {
		a._rtCheckTemporaryFaultEvents(rt, sectorNumber)
		a._rtCheckSectorExpiry(rt, sectorNumber)
	}

	a._rtCheckSurprisePoStExpiry(rt)
	return &vmr.EmptyReturn{}
}

/////////////////
// Constructor //
/////////////////

func (a *StorageMinerActor) Constructor(rt Runtime, ownerAddr addr.Address, workerAddr addr.Address, sectorSize abi.SectorSize, peerId peer.ID) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)
	var st StorageMinerActorState
	rt.State().Transaction(&st, func() interface{} {
		st.Sectors = SectorsAMT_Empty()
		st.PoStState = MinerPoStState{
			LastSuccessfulPoSt:     epochUndefined,
			SurpriseChallengeEpoch: epochUndefined,
			ChallengedSectors:      nil,
			NumConsecutiveFailures: 0,
		}
		st.ProvingSet = SectorNumberSetHAMT_Empty()
		st.Info = MinerInfo_New(ownerAddr, workerAddr, sectorSize, peerId)
		return nil
	})
	return &vmr.EmptyReturn{}
}

////////////////////////////////////////////////////////////////////////////////
// Method utility functions
////////////////////////////////////////////////////////////////////////////////

func (a *StorageMinerActor) _rtCheckTemporaryFaultEvents(rt Runtime, sectorNumber abi.SectorNumber) {
	var st StorageMinerActorState
	rt.State().Readonly(&st)
	checkSector, found := st.Sectors[sectorNumber]

	if !found {
		return
	}

	storageWeightDesc := a._rtGetStorageWeightDescForSector(rt, sectorNumber)

	if checkSector.State == Active && rt.CurrEpoch() == checkSector.EffectiveFaultBeginEpoch() {
		checkSector.State = TemporaryFault

		_, code := rt.Send(
			builtin.StoragePowerActorAddr,
			builtin.Method_StoragePowerActor_OnSectorTemporaryFaultEffectiveBegin,
			serde.MustSerializeParams(
				storageWeightDesc,
			),
			abi.NewTokenAmount(0),
		)
		builtin.RequireSuccess(rt, code, "failed to begin fault")

		delete(st.ProvingSet, sectorNumber) // FIXME state mutation outside of transaction
	}

	if checkSector.Is_TemporaryFault() && rt.CurrEpoch() == checkSector.EffectiveFaultEndEpoch() {
		checkSector.State = Active
		checkSector.DeclaredFaultEpoch = epochUndefined
		checkSector.DeclaredFaultDuration = epochUndefined

		_, code := rt.Send(
			builtin.StoragePowerActorAddr,
			builtin.Method_StoragePowerActor_OnSectorTemporaryFaultEffectiveEnd,
			serde.MustSerializeParams(
				storageWeightDesc,
			),
			abi.NewTokenAmount(0),
		)
		builtin.RequireSuccess(rt, code, "failed to end fault")

		st.ProvingSet[sectorNumber] = true // FIXME state mutation outside of transaction
	}

	rt.State().Transaction(&st, func() interface{} {
		st.Sectors[sectorNumber] = checkSector
		return nil
	})
}

func (a *StorageMinerActor) _rtCheckSectorExpiry(rt Runtime, sectorNumber abi.SectorNumber) {
	var st StorageMinerActorState
	rt.State().Readonly(&st)
	checkSector, found := st.Sectors[sectorNumber]

	if !found {
		return
	}

	if checkSector.State == PreCommit {
		if rt.CurrEpoch()-checkSector.PreCommitEpoch > indices.StorageMining_MaxProveCommitSectorEpoch() {
			a._rtDeleteSectorEntry(rt, sectorNumber)
			_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, checkSector.PreCommitDeposit)
			builtin.RequireSuccess(rt, code, "failed to burn funds")
		}
		return
	}

	// Note: the following test may be false, if sector expiration has been extended by the worker
	// in the interim after the Cron request was enrolled.
	if rt.CurrEpoch() >= checkSector.Info.Expiration {
		a._rtTerminateSector(rt, sectorNumber, autil.NormalExpiration)
	}
}

func (a *StorageMinerActor) _rtTerminateSector(rt Runtime, sectorNumber abi.SectorNumber, terminationType SectorTerminationType) {
	var st StorageMinerActorState
	rt.State().Readonly(&st)
	checkSector, found := st.Sectors[sectorNumber]
	Assert(found)

	storageWeightDesc := a._rtGetStorageWeightDescForSector(rt, sectorNumber)

	if checkSector.State == TemporaryFault {
		// To avoid boundary-case errors in power accounting, make sure we explicitly end
		// the temporary fault state first, before terminating the sector.
		_, code := rt.Send(
			builtin.StoragePowerActorAddr,
			builtin.Method_StoragePowerActor_OnSectorTemporaryFaultEffectiveEnd,
			serde.MustSerializeParams(
				storageWeightDesc,
			),
			abi.NewTokenAmount(0),
		)
		builtin.RequireSuccess(rt, code, "failed to end fault")
	}

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.Method_StoragePowerActor_OnSectorTerminate,
		serde.MustSerializeParams(
			storageWeightDesc,
			terminationType,
		),
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to terminate sector with power actor")

	a._rtDeleteSectorEntry(rt, sectorNumber)
	delete(st.ProvingSet, sectorNumber)
}

func (a *StorageMinerActor) _rtCheckSurprisePoStExpiry(rt Runtime) {
	rt.ValidateImmediateCallerIs(builtin.StoragePowerActorAddr)

	var st StorageMinerActorState
	rt.State().Readonly(&st)

	if !st.PoStState.Is_Challenged() {
		// Already exited challenged state successfully prior to expiry.
		return
	}

	provingPeriod := indices.StorageMining_SurprisePoStProvingPeriod()
	if rt.CurrEpoch() < st.PoStState.SurpriseChallengeEpoch+provingPeriod {
		// Challenge not yet expired.
		return
	}

	numConsecutiveFailures := st.PoStState.NumConsecutiveFailures + 1

	if numConsecutiveFailures > indices.StoragePower_SurprisePoStMaxConsecutiveFailures() {
		// Terminate all sectors, notify power and market actors to terminate
		// associated storage deals, and reset miner's PoSt state to OK.
		terminatedSectors := []abi.SectorNumber{}
		for sectorNumber := range st.Sectors {
			terminatedSectors = append(terminatedSectors, sectorNumber)
		}
		a._rtNotifyMarketForTerminatedSectors(rt, terminatedSectors)
	} else {
		// Increment count of consecutive failures, and continue.
		rt.State().Transaction(&st, func() interface{} {
			st.PoStState = MinerPoStState{
				LastSuccessfulPoSt:     st.PoStState.LastSuccessfulPoSt,
				SurpriseChallengeEpoch: epochUndefined,
				ChallengedSectors:      nil,
				NumConsecutiveFailures: numConsecutiveFailures,
			}
			return nil
		})
	}

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.Method_StoragePowerActor_OnMinerSurprisePoStFailure,
		serde.MustSerializeParams(
			numConsecutiveFailures,
		),
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to notify power actor")
}

func (a *StorageMinerActor) _rtEnrollCronEvent(
	rt Runtime, eventEpoch abi.ChainEpoch, sectorNumbers []abi.SectorNumber) {

	_, code := rt.Send(
		builtin.StoragePowerActorAddr,
		builtin.Method_StoragePowerActor_OnMinerEnrollCronEvent,
		serde.MustSerializeParams(
			eventEpoch,
			sectorNumbers,
		),
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to enroll cron event")
}

func (a *StorageMinerActor) _rtDeleteSectorEntry(rt Runtime, sectorNumber abi.SectorNumber) {
	var st StorageMinerActorState
	rt.State().Transaction(&st, func() interface{} {
		delete(st.Sectors, sectorNumber)
		return nil
	})
}

func (a *StorageMinerActor) _rtGetStorageWeightDescForSector(rt Runtime, sectorNumber abi.SectorNumber) autil.SectorStorageWeightDesc {
	var st StorageMinerActorState
	rt.State().Readonly(&st)
	return st._getStorageWeightDescForSector(sectorNumber)
}

func (a *StorageMinerActor) _rtGetStorageWeightDescsForSectors(rt Runtime, sectorNumbers []abi.SectorNumber) []autil.SectorStorageWeightDesc {
	var st StorageMinerActorState
	rt.State().Readonly(&st)
	return st._getStorageWeightDescsForSectors(sectorNumbers)
}

func (a *StorageMinerActor) _rtNotifyMarketForTerminatedSectors(rt Runtime, sectorNumbers []abi.SectorNumber) {
	var st StorageMinerActorState
	rt.State().Readonly(&st)
	dealIDItems := []abi.DealID{}
	for _, sectorNo := range sectorNumbers {
		dealIDItems = append(dealIDItems, st._getSectorDealIDsAssert(sectorNo).Items...)
	}
	dealIDs := &abi.DealIDs{Items: dealIDItems}

	_, code := rt.Send(
		builtin.StorageMarketActorAddr,
		builtin.Method_StorageMarketActor_OnMinerSectorsTerminate,
		serde.MustSerializeParams(
			dealIDs,
		),
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to terminate sectors")
}

func (a *StorageMinerActor) verifySurprisePost(rt Runtime, st *StorageMinerActorState, onChainInfo *abi.OnChainSurprisePoStVerifyInfo) {
	Assert(st.PoStState.Is_Challenged())
	sectorSize := st.Info.SectorSize
	challengeEpoch := st.PoStState.SurpriseChallengeEpoch
	challengedSectors := st.PoStState.ChallengedSectors

	// verify no duplicate tickets
	challengeIndices := make(map[int64]bool)
	for _, tix := range onChainInfo.Candidates {
		if _, ok := challengeIndices[tix.ChallengeIndex]; ok {
			rt.AbortStateMsg("Invalid Surprise PoSt. Duplicate ticket included.")
		}
		challengeIndices[tix.ChallengeIndex] = true
	}

	TODO(challengedSectors)
	// TODO: Determine what should be the acceptance criterion for sector numbers
	// proven in SurprisePoSt proofs.
	//
	// Previous note:
	// Verify the partialTicket values
	// if !a._rtVerifySurprisePoStMeetsTargetReq(rt) {
	// 	rt.AbortStateMsg("Invalid Surprise PoSt. Tickets do not meet target.")
	// }

	randomnessK := rt.GetRandomness(challengeEpoch - indices.StorageMining_SpcLookbackPoSt())
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
		rt.AbortStateMsg("Surprise PoSt failed to verify")
	}
}

func (a *StorageMinerActor) _rtVerifySealOrAbort(rt Runtime, onChainInfo *abi.OnChainSealVerifyInfo) {
	var st StorageMinerActorState
	rt.State().Readonly(&st)
	info := st.Info
	sectorSize := info.SectorSize

	// if IsValidAtCommitEpoch(onChainInfo.RegisteredProof, rt.CurrEpoch()) // Ensure proof type is valid at current epoch.
	// Check randomness.
	if onChainInfo.SealEpoch < (rt.CurrEpoch() - indices.StorageMining_Finality() - indices.StorageMining_MaxSealTime32GiBWinStackedSDR()) {
		rt.AbortStateMsg("Seal references ticket from invalid epoch")
	}

	var infos storage_market.GetPieceInfosForDealIDsReturn
	ret, code := rt.Send(
		builtin.StorageMarketActorAddr,
		builtin.Method_StorageMarketActor_GetPieceInfosForDealIDs,
		serde.MustSerializeParams(
			sectorSize,
			onChainInfo.DealIDs,
		),
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to fetch piece info")
	autil.AssertNoError(ret.Into(infos))

	// Unless we enforce a minimum padding amount, this totalPieceSize calculation can be removed.
	// Leaving for now until that decision is entirely finalized.
	var totalPieceSize int64
	for _, pieceInfo := range infos.Pieces {
		pieceSize := pieceInfo.Size
		totalPieceSize += pieceSize
	}

	unsealedCID, err := rt.Syscalls().ComputeUnsealedSectorCID(sectorSize, infos.Pieces)
	if err != nil {
		rt.AbortStateMsg("invalid sector piece infos")
	}

	minerActorID, err := addr.IDFromAddress(rt.CurrReceiver())
	if err != nil {
		rt.AbortStateMsg("receiver must be ID address")
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
		UnsealedCID:           unsealedCID,
	}

	isVerified := rt.Syscalls().VerifySeal(sectorSize, svInfo)

	if !isVerified {
		rt.AbortStateMsg("Sector seal failed to verify")
	}
}

func getSectorNums(m map[abi.SectorNumber]SectorOnChainInfo) []abi.SectorNumber {
	var l []abi.SectorNumber
	for i := range m {
		l = append(l, i)
	}
	return l
}

func _surprisePoStSampleChallengedSectors(sampleRandomness abi.Randomness, provingSet []abi.SectorNumber) []abi.SectorNumber {
	TODO()
	panic("")
}
