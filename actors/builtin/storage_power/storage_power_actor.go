package storage_power

import (
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	"math"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	initact "github.com/filecoin-project/specs-actors/actors/builtin/init"
	crypto "github.com/filecoin-project/specs-actors/actors/crypto"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	indices "github.com/filecoin-project/specs-actors/actors/runtime/indices"
	serde "github.com/filecoin-project/specs-actors/actors/serde"
	autil "github.com/filecoin-project/specs-actors/actors/util"
)

type ConsensusFaultType int

const (
	UncommittedPowerFault ConsensusFaultType = 0
	DoubleForkMiningFault ConsensusFaultType = 1
	ParentGrindingFault   ConsensusFaultType = 2
	TimeOffsetMiningFault ConsensusFaultType = 3
)

type StoragePowerActor struct{}

func (a *StoragePowerActor) State(rt Runtime) (vmr.ActorStateHandle, StoragePowerActorState) {
	h := rt.AcquireState()
	stateCID := cid.Cid(h.Take())
	var state StoragePowerActorState
	if !rt.IpldGet(stateCID, &state) {
		rt.Abort(exitcode.ErrPlaceholder, "state not found")
	}
	return h, state
}

////////////////////////////////////////////////////////////////////////////////
// Actor methods
////////////////////////////////////////////////////////////////////////////////

func (a *StoragePowerActor) AddBalance(rt Runtime, minerAddr addr.Address) *vmr.EmptyReturn {
	vmr.RT_MinerEntry_ValidateCaller_DetermineFundsLocation(rt, minerAddr, vmr.MinerEntrySpec_MinerOnly)

	msgValue := rt.ValueReceived()

	h, st := a.State(rt)
	newTable, ok := autil.BalanceTable_WithAdd(st.EscrowTable, minerAddr, msgValue)
	if !ok {
		rt.AbortStateMsg("Escrow operation failed")
	}
	st.EscrowTable = newTable
	UpdateRelease(rt, h, st)
	return &vmr.EmptyReturn{}
}

func (a *StoragePowerActor) WithdrawBalance(rt Runtime, minerAddr addr.Address, amountRequested abi.TokenAmount) *vmr.EmptyReturn {
	if amountRequested.LessThan(big.Zero()) {
		rt.Abort(exitcode.ErrIllegalArgument, "negative withdrawal %v", amountRequested)
	}

	recipientAddr := vmr.RT_MinerEntry_ValidateCaller_DetermineFundsLocation(rt, minerAddr, vmr.MinerEntrySpec_MinerOnly)

	minBalanceMaintainRequired := a._rtGetPledgeCollateralReqForMinerOrAbort(rt, minerAddr)

	h, st := a.State(rt)
	newTable, amountExtracted, ok := autil.BalanceTable_WithExtractPartial(
		st.EscrowTable, minerAddr, amountRequested, minBalanceMaintainRequired)
	if !ok {
		rt.AbortStateMsg("Escrow operation failed")
	}
	st.EscrowTable = newTable
	UpdateRelease(rt, h, st)

	_, code := rt.Send(recipientAddr, builtin.MethodSend, nil, amountExtracted)
	vmr.RequireSuccess(rt, code, "failed to send funds")
	return &vmr.EmptyReturn{}
}

type CreateMinerReturn struct {
	IDAddress     addr.Address // The canonical ID-based address for the actor.
	RobustAddress addr.Address // A mre expensive but re-org-safe address for the newly created actor.
}

func (a *StoragePowerActor) CreateMiner(rt Runtime, workerAddr addr.Address, sectorSize abi.SectorSize, peerId peer.ID) *CreateMinerReturn {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	ownerAddr := rt.ImmediateCaller()

	ret, code := rt.Send(
		builtin.InitActorAddr,
		builtin.Method_InitActor_Exec,
		serde.MustSerializeParams(
			builtin.StorageMinerActorCodeID,
			ownerAddr,
			workerAddr,
			sectorSize,
			peerId,
		),
		abi.NewTokenAmount(0),
	)
	vmr.RequireSuccess(rt, code, "failed to init new actor")
	var addresses initact.ExecReturn
	autil.AssertNoError(ret.Into(addresses))

	h, st := a.State(rt)
	newTable, ok := autil.BalanceTable_WithNewAddressEntry(st.EscrowTable, addresses.IDAddress, rt.ValueReceived())
	Assert(ok)
	st.EscrowTable = newTable
	st.PowerTable[addresses.IDAddress] = abi.StoragePower(0)
	st.ClaimedPower[addresses.IDAddress] = abi.StoragePower(0)
	st.NominalPower[addresses.IDAddress] = abi.StoragePower(0)
	UpdateRelease(rt, h, st)

	return &CreateMinerReturn{
		IDAddress:     addresses.IDAddress,
		RobustAddress: addresses.RobustAddress,
	}
}

func (a *StoragePowerActor) DeleteMiner(rt Runtime, minerAddr addr.Address) *vmr.EmptyReturn {
	h, st := a.State(rt)

	minerPledgeBalance, ok := autil.BalanceTable_GetEntry(st.EscrowTable, minerAddr)
	if !ok {
		rt.Abort(exitcode.ErrNotFound, "no such miner %v", minerAddr)
	}

	if minerPledgeBalance.GreaterThan(abi.NewTokenAmount(0)) {
		rt.AbortStateMsg("Deletion requested for miner with pledge balance still remaining")
	}

	minerPower, ok := st.PowerTable[minerAddr]
	Assert(ok)
	if minerPower > 0 {
		rt.AbortStateMsg("Deletion requested for miner with power still remaining")
	}

	Release(rt, h, st)

	ownerAddr, workerAddr := vmr.RT_GetMinerAccountsAssert(rt, minerAddr)
	rt.ValidateImmediateCallerIs(ownerAddr, workerAddr)

	a._rtDeleteMinerActor(rt, minerAddr)
	return &vmr.EmptyReturn{}
}

func (a *StoragePowerActor) OnSectorProveCommit(rt Runtime, storageWeightDesc SectorStorageWeightDesc) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	a._rtAddPowerForSector(rt, rt.ImmediateCaller(), storageWeightDesc)
	return &vmr.EmptyReturn{}
}

func (a *StoragePowerActor) OnSectorTerminate(
	rt Runtime, storageWeightDesc SectorStorageWeightDesc, terminationType SectorTerminationType) *vmr.EmptyReturn {

	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.ImmediateCaller()
	a._rtDeductClaimedPowerForSectorAssert(rt, minerAddr, storageWeightDesc)

	if terminationType != autil.NormalExpiration {
		cidx := rt.CurrIndices()
		amountToSlash := cidx.StoragePower_PledgeSlashForSectorTermination(storageWeightDesc, terminationType)
		a._rtSlashPledgeCollateral(rt, minerAddr, amountToSlash)
	}
	return &vmr.EmptyReturn{}
}

func (a *StoragePowerActor) OnSectorTemporaryFaultEffectiveBegin(rt Runtime, storageWeightDesc SectorStorageWeightDesc) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	a._rtDeductClaimedPowerForSectorAssert(rt, rt.ImmediateCaller(), storageWeightDesc)
	return &vmr.EmptyReturn{}
}

func (a *StoragePowerActor) OnSectorTemporaryFaultEffectiveEnd(rt Runtime, storageWeightDesc SectorStorageWeightDesc) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	a._rtAddPowerForSector(rt, rt.ImmediateCaller(), storageWeightDesc)
	return &vmr.EmptyReturn{}
}

func (a *StoragePowerActor) OnSectorModifyWeightDesc(
	rt Runtime, storageWeightDescPrev SectorStorageWeightDesc, storageWeightDescNew SectorStorageWeightDesc) *vmr.EmptyReturn {

	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	a._rtDeductClaimedPowerForSectorAssert(rt, rt.ImmediateCaller(), storageWeightDescPrev)
	a._rtAddPowerForSector(rt, rt.ImmediateCaller(), storageWeightDescNew)
	return &vmr.EmptyReturn{}
}

func (a *StoragePowerActor) OnMinerSurprisePoStSuccess(rt Runtime) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.ImmediateCaller()

	h, st := a.State(rt)
	delete(st.PoStDetectedFaultMiners, minerAddr)
	st._updatePowerEntriesFromClaimedPower(minerAddr)
	UpdateRelease(rt, h, st)
	return &vmr.EmptyReturn{}
}

func (a *StoragePowerActor) OnMinerSurprisePoStFailure(rt Runtime, numConsecutiveFailures int64) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.ImmediateCaller()

	h, st := a.State(rt)

	st.PoStDetectedFaultMiners[minerAddr] = true
	st._updatePowerEntriesFromClaimedPower(minerAddr)

	minerClaimedPower, ok := st.ClaimedPower[minerAddr]
	Assert(ok)

	UpdateRelease(rt, h, st)

	if numConsecutiveFailures > indices.StoragePower_SurprisePoStMaxConsecutiveFailures() {
		a._rtDeleteMinerActor(rt, minerAddr)
	} else {
		cidx := rt.CurrIndices()
		amountToSlash := cidx.StoragePower_PledgeSlashForSurprisePoStFailure(minerClaimedPower, numConsecutiveFailures)
		a._rtSlashPledgeCollateral(rt, minerAddr, amountToSlash)
	}
	return &vmr.EmptyReturn{}
}

func (a *StoragePowerActor) OnMinerEnrollCronEvent(rt Runtime, eventEpoch abi.ChainEpoch, sectorNumbers []abi.SectorNumber) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.ImmediateCaller()
	minerEvent := autil.MinerEvent{
		MinerAddr: minerAddr,
		Sectors:   sectorNumbers,
	}

	h, st := a.State(rt)
	if _, found := st.CachedDeferredCronEvents[eventEpoch]; !found {
		st.CachedDeferredCronEvents[eventEpoch] = autil.MinerEventSetHAMT_Empty()
	}
	st.CachedDeferredCronEvents[eventEpoch] = append(st.CachedDeferredCronEvents[eventEpoch], minerEvent)
	UpdateRelease(rt, h, st)
	return &vmr.EmptyReturn{}
}

func (a *StoragePowerActor) ReportVerifiedConsensusFault(rt Runtime, slasheeAddr addr.Address, faultEpoch abi.ChainEpoch, faultType ConsensusFaultType) *vmr.EmptyReturn {
	TODO()
	panic("")
	// TODO: The semantics here are quite delicate:
	//
	// - (proof []block.Block) can't be validated in isolation; we must query the runtime to confirm
	//   that at least one of the blocks provided actually appeared in the current chain.
	// - We must prevent duplicate slashes on the same offense, taking into account that the blocks
	//   may appear in different orders.
	// - We must determine how to reward multiple reporters of the same fault within a single epoch.
	//
	// Deferring to followup after these security/mechanism design questions have been resolved.
	// Previous notes:
	//
	// validation checks to be done in runtime before calling this method
	// - there should be exactly two block headers in proof
	// - both blocks are mined by the same miner
	// - first block is of the same or lower block height as the second block
	//
	// Use EC's IsValidConsensusFault method to validate the proof

	// this method assumes that ConsensusFault has been checked in runtime
	slasherAddr := rt.ImmediateCaller()
	h, st := a.State(rt)

	claimedPower, powerOk := st.ClaimedPower[slasheeAddr]
	if !powerOk {
		rt.Abort(exitcode.ErrIllegalArgument, "spa.ReportConsensusFault: miner already slashed")
	}
	Assert(claimedPower > 0)

	currPledge, pledgeOk := st._getCurrPledgeForMiner(slasheeAddr)
	if !pledgeOk {
		rt.Abort(exitcode.ErrIllegalArgument, "spa.ReportConsensusFault: miner has no pledge")
	}
	Assert(currPledge.GreaterThan(big.Zero()))

	// elapsed epoch from the latter block which committed the fault
	elapsedEpoch := rt.CurrEpoch() - faultEpoch
	if elapsedEpoch <= 0 {
		rt.Abort(exitcode.ErrIllegalArgument, "spa.ReportConsensusFault: invalid block")
	}

	collateralToSlash := st._getPledgeSlashForConsensusFault(currPledge, faultType)
	slasherReward := _getConsensusFaultSlasherReward(elapsedEpoch, collateralToSlash)

	// request slasherReward to be deducted from EscrowTable
	amountToSlasher := st._slashPledgeCollateral(slasherAddr, slasherReward)
	Assert(slasherReward == amountToSlasher)

	UpdateRelease(rt, h, st)

	// reward slasher
	_, code := rt.Send(slasherAddr, builtin.MethodSend, nil, amountToSlasher)
	vmr.RequireSuccess(rt, code, "failed to reward slasher")

	// burn the rest of pledge collateral
	// delete miner from power table
	a._rtDeleteMinerActor(rt, slasheeAddr)
	return &vmr.EmptyReturn{}
}

// Called by Cron.
func (a *StoragePowerActor) OnEpochTickEnd(rt Runtime) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerIs(builtin.CronActorAddr)

	a._rtInitiateNewSurprisePoStChallenges(rt)
	a._rtProcessDeferredCronEvents(rt)
	return &vmr.EmptyReturn{}
}

func (a *StoragePowerActor) Constructor(rt Runtime) *vmr.EmptyReturn {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	h := rt.AcquireState()

	st := &StoragePowerActorState{
		TotalNetworkPower:        abi.StoragePower(0),
		PowerTable:               PowerTableHAMT_Empty(),
		EscrowTable:              autil.BalanceTableHAMT_Empty(),
		CachedDeferredCronEvents: MinerEventsHAMT_Empty(),
		PoStDetectedFaultMiners:  autil.MinerSetHAMT_Empty(),
		ClaimedPower:             PowerTableHAMT_Empty(),
		NominalPower:             PowerTableHAMT_Empty(),
		NumMinersMeetingMinPower: 0,
	}

	UpdateRelease(rt, h, *st)
	return &vmr.EmptyReturn{}
}

////////////////////////////////////////////////////////////////////////////////
// Method utility functions
////////////////////////////////////////////////////////////////////////////////

func (a *StoragePowerActor) _rtAddPowerForSector(rt Runtime, minerAddr addr.Address, storageWeightDesc SectorStorageWeightDesc) {
	h, st := a.State(rt)
	st._addClaimedPowerForSector(minerAddr, storageWeightDesc)
	UpdateRelease(rt, h, st)
}

func (a *StoragePowerActor) _rtDeductClaimedPowerForSectorAssert(rt Runtime, minerAddr addr.Address, storageWeightDesc SectorStorageWeightDesc) {
	h, st := a.State(rt)
	st._deductClaimedPowerForSectorAssert(minerAddr, storageWeightDesc)
	UpdateRelease(rt, h, st)
}

func (a *StoragePowerActor) _rtInitiateNewSurprisePoStChallenges(rt Runtime) {
	provingPeriod := indices.StorageMining_SurprisePoStProvingPeriod()

	h, st := a.State(rt)

	// sample the actor addresses
	minerSelectionSeed := rt.GetRandomness(rt.CurrEpoch())
	randomness := crypto.DeriveRandWithEpoch(crypto.DomainSeparationTag_SurprisePoStSelectMiners, minerSelectionSeed, int(rt.CurrEpoch()))

	IMPL_FINISH() // BigInt arithmetic (not floating-point)
	challengeCount := math.Ceil(float64(len(st.PowerTable)) / float64(provingPeriod))
	surprisedMiners := st._selectMinersToSurprise(int(challengeCount), randomness)

	UpdateRelease(rt, h, st)

	for _, addr := range surprisedMiners {
		_, code := rt.Send(
			addr,
			builtin.Method_StorageMinerActor_OnSurprisePoStChallenge,
			nil,
			abi.NewTokenAmount(0),
		)
		vmr.RequireSuccess(rt, code, "failed to challenge miner")
	}
}

func (a *StoragePowerActor) _rtProcessDeferredCronEvents(rt Runtime) {
	epoch := rt.CurrEpoch()

	h, st := a.State(rt)
	minerEvents, found := st.CachedDeferredCronEvents[epoch]
	if !found {
		return
	}
	delete(st.CachedDeferredCronEvents, epoch)
	UpdateRelease(rt, h, st)

	minerEventsRetain := []autil.MinerEvent{}
	for _, minerEvent := range minerEvents {
		if _, found := st.PowerTable[minerEvent.MinerAddr]; found {
			minerEventsRetain = append(minerEventsRetain, minerEvent)
		}
	}

	for _, minerEvent := range minerEventsRetain {
		_, code := rt.Send(
			minerEvent.MinerAddr,
			builtin.Method_StorageMinerActor_OnDeferredCronEvent,
			serde.MustSerializeParams(
				minerEvent.Sectors,
			),
			abi.NewTokenAmount(0),
		)
		vmr.RequireSuccess(rt, code, "failed to defer cron event")
	}
}

func (a *StoragePowerActor) _rtGetPledgeCollateralReqForMinerOrAbort(rt Runtime, minerAddr addr.Address) abi.TokenAmount {
	h, st := a.State(rt)
	minerNominalPower, found := st.NominalPower[minerAddr]
	if !found {
		rt.Abort(exitcode.ErrNotFound, "no miner %v", minerAddr)
	}
	Release(rt, h, st)
	cidx := rt.CurrIndices()
	return cidx.PledgeCollateralReq(minerNominalPower)
}

func (a *StoragePowerActor) _rtSlashPledgeCollateral(rt Runtime, minerAddr addr.Address, amountToSlash abi.TokenAmount) {
	h, st := a.State(rt)
	amountSlashed := st._slashPledgeCollateral(minerAddr, amountToSlash)
	UpdateRelease(rt, h, st)

	_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, amountSlashed)
	vmr.RequireSuccess(rt, code, "failed to burn funds")
}

func (a *StoragePowerActor) _rtDeleteMinerActor(rt Runtime, minerAddr addr.Address) {
	h, st := a.State(rt)

	delete(st.PowerTable, minerAddr)
	delete(st.ClaimedPower, minerAddr)
	delete(st.NominalPower, minerAddr)
	delete(st.PoStDetectedFaultMiners, minerAddr)

	newTable, amountSlashed, ok := autil.BalanceTable_WithExtractAll(st.EscrowTable, minerAddr)
	Assert(ok)
	newTable, ok = autil.BalanceTable_WithDeletedAddressEntry(newTable, minerAddr)
	Assert(ok)
	st.EscrowTable = newTable

	UpdateRelease(rt, h, st)

	_, code := rt.Send(
		minerAddr,
		builtin.Method_StorageMinerActor_OnDeleteMiner,
		serde.MustSerializeParams(),
		abi.NewTokenAmount(0),
	)
	vmr.RequireSuccess(rt, code, "failed to delete miner actor")

	_, code = rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, amountSlashed)
	vmr.RequireSuccess(rt, code, "failed to burn funds")
}
