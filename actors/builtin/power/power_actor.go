package power

import (
	"bytes"
	"fmt"

	addr "github.com/filecoin-project/go-address"
	peer "github.com/libp2p/go-libp2p-core/peer"
	errors "github.com/pkg/errors"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	initact "github.com/filecoin-project/specs-actors/actors/builtin/init"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	. "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type Runtime = vmr.Runtime

type SectorTermination int64

const (
	SectorTerminationExpired SectorTermination = iota // Implicit termination after all deals expire
	SectorTerminationManual                           // Unscheduled explicit termination by the miner
)

type Actor struct{}

func (a Actor) Exports() []interface{} {
	return []interface{}{
		builtin.MethodConstructor: a.Constructor,
		2:                         a.CreateMiner,
		3:                         a.DeleteMiner,
		4:                         a.OnSectorProveCommit,
		5:                         a.OnSectorTerminate,
		6:                         a.OnSectorTemporaryFaultEffectiveBegin,
		7:                         a.OnSectorTemporaryFaultEffectiveEnd,
		8:                         a.OnSectorModifyWeightDesc,
		9:                         a.OnMinerWindowedPoStSuccess,
		10:                        a.OnMinerWindowedPoStFailure,
		11:                        a.EnrollCronEvent,
		12:                        a.ReportConsensusFault,
		13:                        a.OnEpochTickEnd,
		14:                        a.AddPledgeCollateral,
	}
}

var _ abi.Invokee = Actor{}

// Storage miner actor constructor params are defined here so the power actor can send them to the init actor
// to instantiate miners.
type MinerConstructorParams struct {
	OwnerAddr  addr.Address
	WorkerAddr addr.Address
	SectorSize abi.SectorSize
	PeerId     peer.ID
}

type SectorStorageWeightDesc struct {
	SectorSize abi.SectorSize
	Duration   abi.ChainEpoch
	DealWeight abi.DealWeight
}

////////////////////////////////////////////////////////////////////////////////
// Actor methods
////////////////////////////////////////////////////////////////////////////////

func (a Actor) Constructor(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)

	emptyMap, err := adt.MakeEmptyMap(adt.AsStore(rt))
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to create storage power state: %v", err)
	}

	st := ConstructState(emptyMap.Root())
	rt.State().Create(st)
	return nil
}

type CreateMinerParams struct {
	Owner      addr.Address
	Worker     addr.Address
	SectorSize abi.SectorSize
	Peer       peer.ID
}

type CreateMinerReturn struct {
	IDAddress     addr.Address // The canonical ID-based address for the actor.
	RobustAddress addr.Address // A mre expensive but re-org-safe address for the newly created actor.
}

func (a Actor) CreateMiner(rt Runtime, params *CreateMinerParams) *CreateMinerReturn {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)

	ctorParams := MinerConstructorParams{
		OwnerAddr:  params.Owner,
		WorkerAddr: params.Worker,
		SectorSize: params.SectorSize,
		PeerId:     params.Peer,
	}
	ctorParamBuf := new(bytes.Buffer)
	err := ctorParams.MarshalCBOR(ctorParamBuf)
	if err != nil {
		rt.Abortf(exitcode.ErrPlaceholder, "failed to serialize miner constructor params %v: %v", ctorParams, err)
	}
	ret, code := rt.Send(
		builtin.InitActorAddr,
		builtin.MethodsInit.Exec,
		&initact.ExecParams{
			CodeCID:           builtin.StorageMinerActorCodeID,
			ConstructorParams: ctorParamBuf.Bytes(),
		},
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to init new actor")
	var addresses initact.ExecReturn
	err = ret.Into(&addresses)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "unmarshaling exec return value: %v", err)
	}

	var st State
	rt.State().Transaction(&st, func() interface{} {
		store := adt.AsStore(rt)
		// err = st.setMinerBalance(store, addresses.IDAddress, rt.Message().ValueReceived())
		// abortIfError(rt, err, "failed to set pledge balance")
		err = st.setClaim(store, addresses.IDAddress, &Claim{abi.NewStoragePower(0), abi.NewStoragePower(0)})
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to put power in claimed table while creating miner: %v", err)
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
	Miner addr.Address
}

func (a Actor) DeleteMiner(rt Runtime, params *DeleteMinerParams) *adt.EmptyValue {
	nominal, ok := rt.ResolveAddress(params.Miner)
	if !ok {
		rt.Abortf(exitcode.ErrIllegalArgument, "failed to resolve address %v", params.Miner)
	}

	var st State
	rt.State().Readonly(&st)

	// balance, err := st.getMinerBalance(adt.AsStore(rt), nominal)
	// abortIfError(rt, err, "failed to get pledge balance for deletion")

	// if balance.GreaterThan(abi.NewTokenAmount(0)) {
	// 	rt.Abortf(exitcode.ErrForbidden, "deletion requested for miner %v with pledge balance %v", nominal, balance)
	// }

	claim, found, err := st.getClaim(adt.AsStore(rt), nominal)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to load miner claim for deletion: %v", err)
	}
	if !found {
		rt.Abortf(exitcode.ErrIllegalState, "failed to find miner %v claim for deletion", nominal)
	}
	if claim.RawBytePower.GreaterThan(big.Zero()) {
		rt.Abortf(exitcode.ErrIllegalState, "deletion requested for miner %v with raw byte power %v", nominal, claim.RawBytePower)
	}
	if claim.QualityAdjPower.GreaterThan(big.Zero()) {
		rt.Abortf(exitcode.ErrIllegalState, "deletion requested for miner %v with quality adjusted power %v", nominal, claim.QualityAdjPower)
	}

	ownerAddr, workerAddr := builtin.RequestMinerControlAddrs(rt, nominal)
	rt.ValidateImmediateCallerIs(ownerAddr, workerAddr)

	err = a.deleteMinerActor(rt, nominal)
	abortIfError(rt, err, "failed to delete miner %v", nominal)
	return nil
}

type OnSectorProveCommitParams struct {
	Weight SectorStorageWeightDesc
}

// Returns the computed pledge collateral requirement, which is now committed.
func (a Actor) OnSectorProveCommit(rt Runtime, params *OnSectorProveCommitParams) *big.Int {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	var pledgeReq abi.TokenAmount
	var st State

	rwret, code := rt.Send(builtin.RewardActorAddr, builtin.MethodsReward.LastPerEpochReward, nil, big.Zero())
	builtin.RequireSuccess(rt, code, "failed to check epoch reward")
	var epochReward abi.TokenAmount
	if err := rwret.Into(&epochReward); err != nil {
		rt.Abortf(exitcode.SysErrInternal, "failed to unmarshal epoch reward value: %s", err)
	}

	rt.State().Transaction(&st, func() interface{} {
		rbpower := big.NewIntUnsigned(uint64(params.Weight.SectorSize))
		qapower := QAPowerForWeight(&params.Weight)

		pledgeReq = InitialPledgeForWeight(qapower, st.TotalQualityAdjPower, rt.TotalFilCircSupply(), st.TotalPledgeCollateral, epochReward)
		err := st.AddToClaim(adt.AsStore(rt), rt.Message().Caller(), rbpower, qapower)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "Failed to add power for sector: %v", err)
		}

		st.TotalPledgeCollateral = big.Add(st.TotalPledgeCollateral, pledgeReq)
		return nil
	})
	
	return &pledgeReq
}

type OnSectorTerminateParams struct {
	TerminationType SectorTermination
	Weights         []SectorStorageWeightDesc // TODO: replace with power if it can be computed by miner
	// Pledge          abi.TokenAmount
}

func (a Actor) OnSectorTerminate(rt Runtime, params *OnSectorTerminateParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.Message().Caller()

	var st State
	rt.State().Transaction(&st, func() interface{} {
		rbpower, qapower := powersForWeights(params.Weights)
		err := st.AddToClaim(adt.AsStore(rt), minerAddr, rbpower.Neg(), qapower.Neg())
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to deduct claimed power for sector: %v", err)
		}
		return nil
	})

	if params.TerminationType != SectorTerminationExpired {
		// amountToSlash := pledgePenaltyForSectorTermination(params.Pledge, params.TerminationType)
		// a.slashPledgeCollateral(rt, minerAddr, amountToSlash) // state transactions could be combined.
	}
	return nil
}

type OnSectorTemporaryFaultEffectiveBeginParams struct {
	Weights []SectorStorageWeightDesc // TODO: replace with power if it can be computed by miner
	// Pledge  abi.TokenAmount
}

func (a Actor) OnSectorTemporaryFaultEffectiveBegin(rt Runtime, params *OnSectorTemporaryFaultEffectiveBeginParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	var st State
	rt.State().Transaction(&st, func() interface{} {
		rbpower, qapower := powersForWeights(params.Weights)
		err := st.AddToClaim(adt.AsStore(rt), rt.Message().Caller(), rbpower.Neg(), qapower.Neg())
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to deduct claimed power for sector: %v", err)
		}
		return nil
	})

	return nil
}

type OnSectorTemporaryFaultEffectiveEndParams struct {
	Weights []SectorStorageWeightDesc // TODO: replace with power if it can be computed by miner
	// Pledge  abi.TokenAmount
}

func (a Actor) OnSectorTemporaryFaultEffectiveEnd(rt Runtime, params *OnSectorTemporaryFaultEffectiveEndParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)

	var st State
	rt.State().Transaction(&st, func() interface{} {
		rbpower, qapower := powersForWeights(params.Weights)
		err := st.AddToClaim(adt.AsStore(rt), rt.Message().Caller(), rbpower, qapower)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to add claimed power for sector: %v", err)
		}
		return nil
	})

	return nil
}

type OnSectorModifyWeightDescParams struct {
	PrevWeight SectorStorageWeightDesc // TODO: replace with power if it can be computed by miner
	// PrevPledge abi.TokenAmount
	NewWeight  SectorStorageWeightDesc
}

// Returns new pledge collateral requirement, now committed in place of the old.
func (a Actor) OnSectorModifyWeightDesc(rt Runtime, params *OnSectorModifyWeightDescParams) *abi.TokenAmount {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)

	rwret, code := rt.Send(builtin.RewardActorAddr, builtin.MethodsReward.LastPerEpochReward, nil, big.Zero())
	builtin.RequireSuccess(rt, code, "failed to check epoch reward")
	var epochReward abi.TokenAmount
	if err := rwret.Into(&epochReward); err != nil {
		rt.Abortf(exitcode.SysErrInternal, "failed to unmarshal epoch reward value: %s", err)
	}

	var newPledge abi.TokenAmount
	var st State
	rt.State().Transaction(&st, func() interface{} {
		prevPower := QAPowerForWeight(&params.PrevWeight)
		err := st.AddToClaim(adt.AsStore(rt), rt.Message().Caller(), big.NewIntUnsigned(uint64(params.PrevWeight.SectorSize)).Neg(), prevPower.Neg())
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to deduct claimed power for sector: %v", err)
		}

		// totalPledge := big.Zero() // TODO:

		newPower := QAPowerForWeight(&params.NewWeight)
		// newPledge = InitialPledgeForWeight(newPower, st.TotalQualityAdjPower, rt.TotalFilCircSupply(), totalPledge, epochReward)
		err = st.AddToClaim(adt.AsStore(rt), rt.Message().Caller(), big.NewIntUnsigned(uint64(params.NewWeight.SectorSize)), newPower)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to add power for sector: %v", err)
		}
		return nil
	})

	return &newPledge
}

func (a Actor) OnMinerWindowedPoStSuccess(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.Message().Caller()

	var st State
	rt.State().Transaction(&st, func() interface{} {
		hasFault, err := st.hasDetectedFault(adt.AsStore(rt), minerAddr)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "Failed to check miner for detected fault: %v", err)
		}
		if hasFault {
			if err := st.deleteDetectedFault(adt.AsStore(rt), minerAddr); err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "Failed to delete miner detected fault: %v", err)
			}
		}

		return nil
	})
	return nil
}

type OnMinerWindowedPoStFailureParams struct {
	NumConsecutiveFailures int64
}

func (a Actor) OnMinerWindowedPoStFailure(rt Runtime, params *OnMinerWindowedPoStFailureParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.Message().Caller()

	var claim *Claim
	var st State
	rt.State().Transaction(&st, func() interface{} {
		faulty, err := st.hasDetectedFault(adt.AsStore(rt), minerAddr)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "Failed to check if miner was faulty already: %s", err)
		}

		if faulty {
			return nil
		}

		if err := st.putDetectedFault(adt.AsStore(rt), minerAddr); err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "Failed to put miner fault: %v", err)
		}

		var found bool
		claim, found, err = st.getClaim(adt.AsStore(rt), minerAddr)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "Failed to get miner power from claimed power table for surprise PoSt failure: %v", err)
		}
		if !found {
			rt.Abortf(exitcode.ErrIllegalState, "Failed to find miner power in claimed power table for surprise PoSt failure")
		}

		if claim.QualityAdjPower.GreaterThanEqual(ConsensusMinerMinPower) {
			// Ensure we only deduct this once...
			st.TotalQualityAdjPower = big.Sub(st.TotalQualityAdjPower, claim.QualityAdjPower)
			st.TotalRawBytePower = big.Sub(st.TotalRawBytePower, claim.RawBytePower)
		}
		return nil
	})

	if params.NumConsecutiveFailures > WindowedPostFailureLimit {
		err := a.deleteMinerActor(rt, minerAddr)
		abortIfError(rt, err, "failed to delete failed miner %v", minerAddr)
	} else {
		// Penalise pledge collateral without reducing the claim.
		// The miner will have to deposit more when recovering the fault (unless already in sufficient surplus).
		// _ := pledgePenaltyForWindowedPoStFailure(params.NumConsecutiveFailures)
		// a.slashPledgeCollateral(rt, minerAddr, amountToSlash)
	}
	return nil
}

type EnrollCronEventParams struct {
	EventEpoch abi.ChainEpoch
	Payload    []byte
}

func (a Actor) EnrollCronEvent(rt Runtime, params *EnrollCronEventParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.Message().Caller()
	minerEvent := CronEvent{
		MinerAddr:       minerAddr,
		CallbackPayload: params.Payload,
	}

	var st State
	rt.State().Transaction(&st, func() interface{} {
		err := st.appendCronEvent(adt.AsStore(rt), params.EventEpoch, &minerEvent)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to enroll cron event: %v", err)
		}
		return nil
	})
	return nil
}

type ReportConsensusFaultParams struct {
	BlockHeader1     []byte
	BlockHeader2     []byte
	BlockHeaderExtra []byte
}

func (a Actor) ReportConsensusFault(rt Runtime, params *ReportConsensusFaultParams) *adt.EmptyValue {
	currEpoch := rt.CurrEpoch()
	earliest := currEpoch - ConsensusFaultReportingWindow
	fault, err := rt.Syscalls().VerifyConsensusFault(params.BlockHeader1, params.BlockHeader2, params.BlockHeaderExtra, earliest)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalArgument, "fault not verified: %s", err)
	}

	// Note: only the first reporter of any fault is rewarded.
	// Subsequent invocations fail because the target miner has been removed.
	reporter := rt.Message().Caller()
	var st State
	reward := rt.State().Transaction(&st, func() interface{} {
		store := adt.AsStore(rt)
		claim, powerOk, err := st.getClaim(store, fault.Target)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to read claimed power for fault: %v", err)
		}
		if !powerOk {
			rt.Abortf(exitcode.ErrIllegalArgument, "miner %v not registered (already slashed?)", fault.Target)
		}
		Assert(claim.RawBytePower.GreaterThanEqual(big.Zero()))
		Assert(claim.QualityAdjPower.GreaterThanEqual(big.Zero()))

		// TODO Slashing
		// currBalance, err := st.getMinerBalance(store, fault.Target)
		// abortIfError(rt, err, "failed to get miner pledge balance")
		// Assert(currBalance.GreaterThanEqual(big.Zero()))

		// // Elapsed since the fault (i.e. since the higher of the two blocks)
		// faultAge := currEpoch - fault.Epoch
		// if faultAge <= 0 {
		// 	rt.Abortf(exitcode.ErrIllegalArgument, "invalid fault epoch %v ahead of current %v", fault.Epoch, currEpoch)
		// }

		// // Note: this slashes the miner's whole balance, including any excess over the required claim.Pledge.
		// collateralToSlash := pledgePenaltyForConsensusFault(currBalance, fault.Type)
		// targetReward := rewardForConsensusSlashReport(faultAge, collateralToSlash)

		// availableReward, err := st.subtractMinerBalance(store, fault.Target, targetReward, big.Zero())
		// abortIfError(rt, err, "failed to subtract pledge for reward")
		// return availableReward
		return big.Zero()
	}).(abi.TokenAmount)

	// reward reporter
	_, code := rt.Send(reporter, builtin.MethodSend, nil, reward)
	builtin.RequireSuccess(rt, code, "failed to reward reporter")

	// burn the rest of pledge collateral
	// delete miner from power table
	err = a.deleteMinerActor(rt, fault.Target)
	abortIfError(rt, err, "failed to remove slashed miner %v", fault.Target)
	return nil
}

// Called by Cron.
func (a Actor) OnEpochTickEnd(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.CronActorAddr)

	if err := a.processDeferredCronEvents(rt); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "Failed to process deferred cron events: %v", err)
	}

	var st State
	rt.State().Readonly(&st)

	// update network KPI in RewardActor
	_, code := rt.Send(
		builtin.RewardActorAddr,
		builtin.MethodsReward.UpdateNetworkKPI,
		&st.TotalRawBytePower,
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to update network KPI with Reward Actor")

	return nil
}

func (a Actor) AddPledgeCollateral(rt Runtime, pledgeAmount abi.TokenAmount) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	msgValue := rt.Message().ValueReceived()
	Assert(msgValue.IsZero())

	var st State
	rt.State().Transaction(&st, func() interface{} {
		st.TotalPledgeCollateral = big.Add(st.TotalPledgeCollateral, pledgeAmount)
		Assert(st.TotalPledgeCollateral.GreaterThanEqual(big.Zero()))

		return nil
	})
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// Method utility functions
////////////////////////////////////////////////////////////////////////////////

func (a Actor) processDeferredCronEvents(rt Runtime) error {
	rtEpoch := rt.CurrEpoch()

	var cronEvents []CronEvent
	var st State
	rt.State().Transaction(&st, func() interface{} {
		store := adt.AsStore(rt)

		for epoch := st.LastEpochTick + 1; epoch <= rtEpoch; epoch++ {
			epochEvents, err := st.loadCronEvents(store, epoch)
			if err != nil {
				return errors.Wrapf(err, "failed to load cron events at %v", epoch)
			}

			cronEvents = append(cronEvents, epochEvents...)

			if len(epochEvents) > 0 {
				err = st.clearCronEvents(store, epoch)
				if err != nil {
					return errors.Wrapf(err, "failed to clear cron events at %v", epoch)
				}
			}
		}

		st.LastEpochTick = rtEpoch

		return nil
	})

	for _, event := range cronEvents {
		_, code := rt.Send(
			event.MinerAddr,
			builtin.MethodsMiner.OnDeferredCronEvent,
			vmr.CBORBytes(event.CallbackPayload),
			abi.NewTokenAmount(0),
		)
		builtin.RequireSuccess(rt, code, "failed to defer cron event")
	}
	return nil
}

func (a Actor) deleteMinerActor(rt Runtime, miner addr.Address) error {
	var st State
	var txErr error
	amountSlashed := rt.State().Transaction(&st, func() interface{} {
		var err error

		err = st.deleteClaim(adt.AsStore(rt), miner)
		if err != nil {
			txErr = errors.Wrapf(err, "failed to delete %v from claimed power table", miner)
			return big.Zero()
		}

		st.MinerCount -= 1
		hasFault, err := st.hasDetectedFault(adt.AsStore(rt), miner)
		if err != nil {
			return err
		}
		if hasFault {
			if err := st.deleteDetectedFault(adt.AsStore(rt), miner); err != nil {
				return err
			}
		}

		// table := adt.AsBalanceTable(adt.AsStore(rt), st.EscrowTable)
		// balance, err := table.Remove(miner)
		// if err != nil {
		// 	txErr = errors.Wrapf(err, "failed to delete pledge balance entry for %v", miner)
		// 	return big.Zero()
		// }
		// st.EscrowTable = table.Root()
		// return balance
		return big.Zero()
	}).(abi.TokenAmount)

	if txErr != nil {
		return txErr
	}

	// Delete the actor, burning any balance it has (sector pre-commit deposits).
	_, code := rt.Send(
		miner,
		builtin.MethodsMiner.OnDeleteMiner,
		&builtin.BurntFundsActorAddr,
		abi.NewTokenAmount(0),
	)
	builtin.RequireSuccess(rt, code, "failed to delete miner actor")

	_, code = rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, amountSlashed)
	builtin.RequireSuccess(rt, code, "failed to burn funds")

	return nil
}

func powersForWeights(weights []SectorStorageWeightDesc) (abi.StoragePower, abi.StoragePower) {
	// returns (rbpower, qapower)
	rbpower := big.Zero()
	qapower := big.Zero()
	for i := range weights {
		rbpower = big.Add(rbpower, big.NewIntUnsigned(uint64(weights[i].SectorSize)))
		qapower = big.Add(qapower, QAPowerForWeight(&weights[i]))
	}
	return rbpower, qapower
}

func abortIfError(rt Runtime, err error, msg string, args ...interface{}) {
	if err != nil {
		code := exitcode.ErrIllegalState
		if _, ok := err.(adt.ErrNotFound); ok {
			code = exitcode.ErrNotFound
		}
		fmtmst := fmt.Sprintf(msg, args...)
		rt.Abortf(code, "%s: %v", fmtmst, err)
	}
}

func bigProduct(p big.Int, rest ...big.Int) big.Int {
	for _, r := range rest {
		p = big.Mul(p, r)
	}
	return p
}
