package storage_power

import (
	"fmt"
	"math"

	addr "github.com/filecoin-project/go-address"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/pkg/errors"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	initact "github.com/filecoin-project/specs-actors/actors/builtin/init"
	crypto "github.com/filecoin-project/specs-actors/actors/crypto"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	indices "github.com/filecoin-project/specs-actors/actors/runtime/indices"
	serde "github.com/filecoin-project/specs-actors/actors/serde"
	autil "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type Runtime = vmr.Runtime

var Assert = autil.Assert

type ConsensusFaultType int

const (
	//UncommittedPowerFault ConsensusFaultType = 0
	DoubleForkMiningFault ConsensusFaultType = 1
	ParentGrindingFault   ConsensusFaultType = 2
	TimeOffsetMiningFault ConsensusFaultType = 3
)

type StoragePowerActor struct{}

////////////////////////////////////////////////////////////////////////////////
// Actor methods
////////////////////////////////////////////////////////////////////////////////

func (a *StoragePowerActor) Constructor(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)

	rt.State().Construct(func() vmr.CBORMarshaler {
		st, err := ConstructState(adt.AsStore(rt))
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to create empty map: %v", err)
		}
		return st
	})
	return &adt.EmptyValue{}
}

type AddBalanceParams struct {
	miner addr.Address
}

func (a *StoragePowerActor) AddBalance(rt Runtime, params *AddBalanceParams) *adt.EmptyValue {
	builtin.RT_MinerEntry_ValidateCaller_DetermineFundsLocation(rt, params.miner, builtin.MinerEntrySpec_MinerOnly)
	var err error
	var st StoragePowerActorState
	rt.State().Transaction(&st, func() interface{} {
		err = st.addMinerPledge(adt.AsStore(rt), params.miner, rt.ValueReceived())
		abortIfError(rt, err, "failed to add pledge balance")
		return nil
	})
	return &adt.EmptyValue{}
}

type WithdrawBalanceParams struct {
	miner     addr.Address
	requested abi.TokenAmount
}

func (a *StoragePowerActor) WithdrawBalance(rt Runtime, params *WithdrawBalanceParams) *adt.EmptyValue {
	if params.requested.LessThan(big.Zero()) {
		rt.Abort(exitcode.ErrIllegalArgument, "negative withdrawal %v", params.requested)
	}

	recipientAddr := builtin.RT_MinerEntry_ValidateCaller_DetermineFundsLocation(rt, params.miner, builtin.MinerEntrySpec_MinerOnly)

	var amountExtracted abi.TokenAmount
	var st StoragePowerActorState
	rt.State().Transaction(&st, func() interface{} {
		minCollateralRequired, err := a.getPledgeCollateralReqForMiner(rt, &st, params.miner)
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "Failed to get required pledge collateral required for miner %v: %v", params.miner, err)
		}

		subtracted, err := st.subtractMinerPledge(adt.AsStore(rt), params.miner, params.requested, minCollateralRequired)
		abortIfError(rt, err, "failed to subtract pledge balance")
		amountExtracted = subtracted
		return nil
	})

	_, code := rt.Send(recipientAddr, builtin.MethodSend, nil, amountExtracted)
	builtin.RequireSuccess(rt, code, "failed to send funds")
	return &adt.EmptyValue{}
}

type CreateMinerParams struct {
	worker     addr.Address
	sectorSize abi.SectorSize
	peer       peer.ID
}

type CreateMinerReturn struct {
	IDAddress     addr.Address // The canonical ID-based address for the actor.
	RobustAddress addr.Address // A mre expensive but re-org-safe address for the newly created actor.
}

func (a *StoragePowerActor) CreateMiner(rt Runtime, params *CreateMinerParams) *CreateMinerReturn {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	ownerAddr := rt.ImmediateCaller()

	ret, code := rt.Send(
		builtin.InitActorAddr,
		builtin.Method_InitActor_Exec,
		serde.MustSerializeParams(
			builtin.StorageMinerActorCodeID,
			ownerAddr,
			params.worker,
			params.sectorSize,
			params.peer,
		),
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to init new actor")
	var addresses initact.ExecReturn
	autil.AssertNoError(ret.Into(&addresses))

	var st StoragePowerActorState
	rt.State().Transaction(&st, func() interface{} {
		store := adt.AsStore(rt)
		err := st.setMinerPledge(store, addresses.IDAddress, rt.ValueReceived())
		abortIfError(rt, err, "failed to set pledge balance")
		st.PowerTable, err = putStoragePower(store, st.PowerTable, addresses.IDAddress, abi.NewStoragePower(0))
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to put power in power table while creating miner: %v", err)
		}
		st.ClaimedPower, err = putStoragePower(store, st.ClaimedPower, addresses.IDAddress, abi.NewStoragePower(0))
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to put power in claimed table while creating miner: %v", err)
		}
		st.NominalPower, err = putStoragePower(store, st.NominalPower, addresses.IDAddress, abi.NewStoragePower(0))
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to put power in nominal power table while creating miner: %v", err)
		}
		st.MinerCount += 1
		return nil
	})
	return &CreateMinerReturn{
		IDAddress:     addresses.IDAddress,
		RobustAddress: addresses.RobustAddress,
	}
}

type DeleteMinerParams struct {
	miner addr.Address
}

func (a *StoragePowerActor) DeleteMiner(rt Runtime, params *DeleteMinerParams) *adt.EmptyValue {
	var st StoragePowerActorState
	rt.State().Readonly(&st)

	balance, err := st.getMinerPledge(adt.AsStore(rt), params.miner)
	abortIfError(rt, err, "failed to get pledge balance for deletion")

	if balance.GreaterThan(abi.NewTokenAmount(0)) {
		rt.Abort(exitcode.ErrForbidden, "deletion requested for miner %v with pledge balance %v", params.miner, balance)
	}

	minerPower, found, err := getStoragePower(adt.AsStore(rt), st.PowerTable, params.miner)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "Failed to get miner power from power table for deletion request: %v", err)
	}
	if !found {
		rt.Abort(exitcode.ErrIllegalState, "Failed to find miner power in power table for deletion request")
	}
	if minerPower.GreaterThan(big.Zero()) {
		rt.Abort(exitcode.ErrIllegalState, "Deletion requested for miner with power still remaining")
	}

	ownerAddr, workerAddr := builtin.RT_GetMinerAccountsAssert(rt, params.miner)
	rt.ValidateImmediateCallerIs(ownerAddr, workerAddr)

	err = a.deleteMinerActor(rt, params.miner)
	abortIfError(rt, err, "failed to delete miner %v", params.miner)
	return &adt.EmptyValue{}
}

type OnSectorProveCommitParams struct {
	weight autil.SectorStorageWeightDesc
}

func (a *StoragePowerActor) OnSectorProveCommit(rt Runtime, params *OnSectorProveCommitParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	if err := a.addPowerForSector(rt, rt.ImmediateCaller(), params.weight); err != nil {
		rt.Abort(exitcode.ErrIllegalState, "Failed to add power for sector: %v", err)
	}
	return &adt.EmptyValue{}
}

type OnSectorTerminateParams struct {
	terminationType builtin.SectorTermination
	weight          autil.SectorStorageWeightDesc
}

func (a *StoragePowerActor) OnSectorTerminate(rt Runtime, params *OnSectorTerminateParams) *adt.EmptyValue {

	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.ImmediateCaller()
	if err := a.deductClaimedPowerForSector(rt, minerAddr, params.weight); err != nil {
		rt.Abort(exitcode.ErrIllegalState, "Failed to deduct claimed power for sector: %v", err)
	}

	if params.terminationType != builtin.NormalExpiration {
		amountToSlash := pledgePenaltyForSectorTermination(params.weight, params.terminationType)
		a.slashPledgeCollateral(rt, minerAddr, amountToSlash)
	}
	return &adt.EmptyValue{}
}

type OnSectorTemporaryFaultEffectiveBegin struct {
	weight autil.SectorStorageWeightDesc
}

func (a *StoragePowerActor) OnSectorTemporaryFaultEffectiveBegin(rt Runtime, params *OnSectorTemporaryFaultEffectiveBegin) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	if err := a.deductClaimedPowerForSector(rt, rt.ImmediateCaller(), params.weight); err != nil {
		rt.Abort(exitcode.ErrIllegalState, "Failed to deduct claimed power for sector: %v", err)
	}
	return &adt.EmptyValue{}
}

type OnSectorTemporaryFaultEffectiveEnd struct {
	weight autil.SectorStorageWeightDesc
}

func (a *StoragePowerActor) OnSectorTemporaryFaultEffectiveEnd(rt Runtime, params *OnSectorTemporaryFaultEffectiveEnd) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	if err := a.addPowerForSector(rt, rt.ImmediateCaller(), params.weight); err != nil {
		rt.Abort(exitcode.ErrIllegalState, "Failed to add power for sector: %v", err)
	}
	return &adt.EmptyValue{}
}

type OnSectorModifyWeightDesc struct {
	prevWeight autil.SectorStorageWeightDesc
	newWeight  autil.SectorStorageWeightDesc
}

func (a *StoragePowerActor) OnSectorModifyWeightDesc(rt Runtime, params *OnSectorModifyWeightDesc) *adt.EmptyValue {

	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	if err := a.deductClaimedPowerForSector(rt, rt.ImmediateCaller(), params.prevWeight); err != nil {
		rt.Abort(exitcode.ErrIllegalState, "Failed to deduct claimed power for sector: %v", err)
	}
	if err := a.addPowerForSector(rt, rt.ImmediateCaller(), params.newWeight); err != nil {
		rt.Abort(exitcode.ErrIllegalState, "Failed to add power for sector: %v", err)
	}
	return &adt.EmptyValue{}
}

func (a *StoragePowerActor) OnMinerSurprisePoStSuccess(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.ImmediateCaller()

	var st StoragePowerActorState
	rt.State().Transaction(&st, func() interface{} {
		if err := st.deleteFault(adt.AsStore(rt), minerAddr); err != nil {
			rt.Abort(exitcode.ErrIllegalState, "Failed to delete miner fault: %v", err)
		}
		if err := st.updatePowerEntriesFromClaimed(adt.AsStore(rt), minerAddr); err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to update miners claimed power table on surprise PoSt success: %v", err)
		}
		return nil
	})
	return &adt.EmptyValue{}
}

type OnMinerSurprisePoStFailure struct {
	numConsecutiveFailures int64
}

func (a *StoragePowerActor) OnMinerSurprisePoStFailure(rt Runtime, params *OnMinerSurprisePoStFailure) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.ImmediateCaller()

	var minerClaimedPower abi.StoragePower
	var st StoragePowerActorState
	rt.State().Transaction(&st, func() interface{} {
		if err := st.putFault(adt.AsStore(rt), minerAddr); err != nil {
			rt.Abort(exitcode.ErrIllegalState, "Failed to put miner fault: %v", err)
		}
		if err := st.updatePowerEntriesFromClaimed(adt.AsStore(rt), minerAddr); err != nil {
			rt.Abort(exitcode.ErrIllegalState, "Failed to update power entries for claimed power: %v", err)
		}

		var found bool
		var err error
		minerClaimedPower, found, err = getStoragePower(adt.AsStore(rt), st.ClaimedPower, minerAddr)
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "Failed to get miner power from claimed power table for surprise PoSt failure: %v", err)
		}
		if !found {
			rt.Abort(exitcode.ErrIllegalState, "Failed to find miner power in claimed power table for surprise PoSt failure")
		}
		return nil
	})

	if params.numConsecutiveFailures > indices.StoragePower_SurprisePoStMaxConsecutiveFailures() {
		err := a.deleteMinerActor(rt, minerAddr)
		abortIfError(rt, err, "failed to delete failed miner %v", minerAddr)
	} else {
		amountToSlash := pledgePenaltyForSurprisePoStFailure(minerClaimedPower, params.numConsecutiveFailures)
		a.slashPledgeCollateral(rt, minerAddr, amountToSlash)
	}
	return &adt.EmptyValue{}
}

type OnMinerEnrollCronEvent struct {
	eventEpoch abi.ChainEpoch
	payload    []byte
}

func (a *StoragePowerActor) OnMinerEnrollCronEvent(rt Runtime, params *OnMinerEnrollCronEvent) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.ImmediateCaller()
	minerEvent := CronEvent{
		MinerAddr:       minerAddr,
		CallbackPayload: params.payload,
	}

	var st StoragePowerActorState
	rt.State().Transaction(&st, func() interface{} {
		if _, found := st.CronEventQueue[params.eventEpoch]; !found {
			st.CronEventQueue[params.eventEpoch] = []CronEvent{}
		}
		st.CronEventQueue[params.eventEpoch] = append(st.CronEventQueue[params.eventEpoch], minerEvent)
		return nil
	})
	return &adt.EmptyValue{}
}

type ReportConsensusFaultParams struct {
	blockHeader1 []byte
	blockHeader2 []byte
	target       addr.Address
	faultEpoch   abi.ChainEpoch
	faultType    ConsensusFaultType
}

func (a *StoragePowerActor) ReportConsensusFault(rt Runtime, params *ReportConsensusFaultParams) *adt.EmptyValue {
	// TODO: jz, zx determine how to reward multiple reporters of the same fault within a single epoch.

	isValidConsensusFault := rt.Syscalls().VerifyConsensusFault(params.blockHeader1, params.blockHeader2)
	if !isValidConsensusFault {
		rt.Abort(exitcode.ErrIllegalArgument, "spa.ReportConsensusFault: unverified consensus fault")
	}

	reporter := rt.ImmediateCaller()
	var st StoragePowerActorState
	reward := rt.State().Transaction(&st, func() interface{} {
		store := adt.AsStore(rt)
		claimedPower, powerOk, err := getStoragePower(store, st.ClaimedPower, params.target)
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "spa.ReportConsensusFault failed to read claimed power for fault: %v", err)
		}
		if !powerOk {
			rt.Abort(exitcode.ErrIllegalArgument, "spa.ReportConsensusFault: miner already slashed")
		}
		Assert(claimedPower.GreaterThanEqual(big.Zero()))

		currPledge, err := st.getMinerPledge(store, params.target)
		abortIfError(rt, err, "failed to get miner pledge")
		Assert(currPledge.GreaterThanEqual(big.Zero()))

		// elapsed epoch from the latter block which committed the fault
		elapsedEpoch := rt.CurrEpoch() - params.faultEpoch
		if elapsedEpoch <= 0 {
			rt.Abort(exitcode.ErrIllegalArgument, "spa.ReportConsensusFault: invalid block")
		}

		collateralToSlash := pledgePenaltyForConsensusFault(currPledge, params.faultType)
		targetReward := rewardForConsensusSlashReport(elapsedEpoch, collateralToSlash)

		availableReward, err := st.subtractMinerPledge(store, params.target, targetReward, big.Zero())
		abortIfError(rt, err, "failed to subtract pledge for reward")
		return availableReward
	}).(abi.TokenAmount)

	// reward reporter
	_, code := rt.Send(reporter, builtin.MethodSend, nil, reward)
	builtin.RequireSuccess(rt, code, "failed to reward reporter")

	// burn the rest of pledge collateral
	// delete miner from power table
	err := a.deleteMinerActor(rt, params.target)
	abortIfError(rt, err, "failed to remove slashed miner %v", params.target)
	return &adt.EmptyValue{}
}

// Called by Cron.
func (a *StoragePowerActor) OnEpochTickEnd(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.CronActorAddr)

	if err := a.initiateNewSurprisePoStChallenges(rt); err != nil {
		rt.Abort(exitcode.ErrIllegalState, "Failed to initiate new surprise PoSt challenges: %v", err)
	}
	if err := a.processDeferredCronEvents(rt); err != nil {
		rt.Abort(exitcode.ErrIllegalState, "Failed to process deferred cron events: %v", err)
	}
	return &adt.EmptyValue{}
}

////////////////////////////////////////////////////////////////////////////////
// Method utility functions
////////////////////////////////////////////////////////////////////////////////

func (a *StoragePowerActor) addPowerForSector(rt Runtime, minerAddr addr.Address, weight autil.SectorStorageWeightDesc) error {
	var st StoragePowerActorState
	var txErr error
	rt.State().Transaction(&st, func() interface{} {
		if err := st.addClaimedPowerForSector(adt.AsStore(rt), minerAddr, weight); err != nil {
			txErr = errors.Wrap(err, "failed to add power power for sector")
		}
		return nil
	})
	return txErr
}

func (a *StoragePowerActor) deductClaimedPowerForSector(rt Runtime, minerAddr addr.Address, weight autil.SectorStorageWeightDesc) error {
	var st StoragePowerActorState
	var txErr error
	rt.State().Transaction(&st, func() interface{} {
		if err := st.deductClaimedPowerForSector(adt.AsStore(rt), minerAddr, weight); err != nil {
			txErr = errors.Wrap(err, "failed to deducted claimed power for sector")
		}
		return nil
	})
	return txErr
}

func (a *StoragePowerActor) initiateNewSurprisePoStChallenges(rt Runtime) error {
	provingPeriod := indices.StorageMining_SurprisePoStProvingPeriod()
	var surprisedMiners []addr.Address
	var st StoragePowerActorState
	var txErr error
	rt.State().Transaction(&st, func() interface{} {
		var err error
		// sample the actor addresses
		minerSelectionSeed := rt.GetRandomness(rt.CurrEpoch())
		randomness := crypto.DeriveRandWithEpoch(crypto.DomainSeparationTag_SurprisePoStSelectMiners, minerSelectionSeed, int(rt.CurrEpoch()))

		autil.IMPL_FINISH() // BigInt arithmetic (not floating-point)
		challengeCount := math.Ceil(float64(st.MinerCount) / float64(provingPeriod))
		surprisedMiners, err = st.selectMinersToSurprise(adt.AsStore(rt), int(challengeCount), randomness)
		if err != nil {
			txErr = errors.Wrap(err, "failed to select miner to surprise")
		}
		return nil
	})

	if txErr != nil {
		return txErr
	}

	for _, address := range surprisedMiners {
		_, code := rt.Send(
			address,
			builtin.Method_StorageMinerActor_OnSurprisePoStChallenge,
			nil,
			abi.NewTokenAmount(0),
		)
		builtin.RequireSuccess(rt, code, "failed to challenge miner")
	}
	return nil
}

func (a *StoragePowerActor) processDeferredCronEvents(rt Runtime) error {
	epoch := rt.CurrEpoch()

	var epochEvents []CronEvent
	var st StoragePowerActorState
	rt.State().Transaction(&st, func() interface{} {
		epochEvents, _ = st.CronEventQueue[epoch]
		delete(st.CronEventQueue, epoch)
		return nil
	})

	validEvents := []CronEvent{}
	for _, minerEvent := range epochEvents {
		if _, found, err := getStoragePower(adt.AsStore(rt), st.PowerTable, minerEvent.MinerAddr); err != nil {
			return errors.Wrap(err, "Failed to get miner power from power table while processing cron events")
		} else if found {
			validEvents = append(validEvents, minerEvent)
		}
	}

	for _, event := range validEvents {
		_, code := rt.Send(
			event.MinerAddr,
			builtin.Method_StorageMinerActor_OnDeferredCronEvent,
			serde.MustSerializeParams(
				event.CallbackPayload,
			),
			abi.NewTokenAmount(0),
		)
		builtin.RequireSuccess(rt, code, "failed to defer cron event")
	}
	return nil
}

func (a *StoragePowerActor) getPledgeCollateralReqForMiner(rt Runtime, st *StoragePowerActorState, minerAddr addr.Address) (abi.TokenAmount, error) {
	minerNominalPower, found, err := getStoragePower(adt.AsStore(rt), st.NominalPower, minerAddr)
	if err != nil {
		return abi.NewTokenAmount(0), errors.Wrap(err, "Failed to get miner power from nominal power table")
	}
	if !found {
		return abi.NewTokenAmount(0), errors.Errorf("no miner %v", minerAddr)
	}
	return rt.CurrIndices().PledgeCollateralReq(minerNominalPower), nil
}

func (a *StoragePowerActor) slashPledgeCollateral(rt Runtime, minerAddr addr.Address, amountToSlash abi.TokenAmount) {
	var st StoragePowerActorState
	amountSlashed := rt.State().Transaction(&st, func() interface{} {
		subtracted, err := st.subtractMinerPledge(adt.AsStore(rt), minerAddr, amountToSlash, big.Zero())
		abortIfError(rt, err, "failed to subtract collateral for slash")
		return subtracted
	}).(abi.TokenAmount)

	_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, amountSlashed)
	builtin.RequireSuccess(rt, code, "failed to burn funds")
}

func (a *StoragePowerActor) deleteMinerActor(rt Runtime, miner addr.Address) error {
	var st StoragePowerActorState
	var txErr error
	amountSlashed := rt.State().Transaction(&st, func() interface{} {
		var err error
		st.PowerTable, err = deleteStoragePower(adt.AsStore(rt), st.PowerTable, miner)
		if err != nil {
			txErr = errors.Wrapf(err, "failed to delete %v from storage power table", miner)
			return big.Zero()
		}
		st.ClaimedPower, err = deleteStoragePower(adt.AsStore(rt), st.ClaimedPower, miner)
		if err != nil {
			txErr = errors.Wrapf(err, "failed to delete %v from claimed power table", miner)
			return big.Zero()
		}
		st.NominalPower, err = deleteStoragePower(adt.AsStore(rt), st.NominalPower, miner)
		if err != nil {
			txErr = errors.Wrapf(err, "failed to delete %v from nominal power table", miner)
			return big.Zero()
		}
		st.MinerCount -= 1
		if err = st.deleteFault(adt.AsStore(rt), miner); err != nil {
			return err
		}

		table := adt.AsBalanceTable(adt.AsStore(rt), st.EscrowTable)
		balance, err := table.Remove(miner)
		if err != nil {
			txErr = errors.Wrapf(err, "failed to delete pledge balance entry for %v", miner)
			return big.Zero()
		}
		st.EscrowTable = table.Root()
		return balance
	}).(abi.TokenAmount)

	if txErr != nil {
		return txErr
	}

	_, code := rt.Send(
		miner,
		builtin.Method_StorageMinerActor_OnDeleteMiner,
		serde.MustSerializeParams(),
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to delete miner actor")

	_, code = rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, amountSlashed)
	builtin.RequireSuccess(rt, code, "failed to burn funds")

	return nil
}

func abortIfError(rt Runtime, err error, msg string, args ...interface{}) {
	if err != nil {
		code := exitcode.ErrIllegalState
		if _, ok := err.(adt.ErrNotFound); ok {
			code = exitcode.ErrNotFound
		}
		fmtmst := fmt.Sprintf(msg, args...)
		rt.Abort(code, "%s: %v", fmtmst, err)
	}
}
