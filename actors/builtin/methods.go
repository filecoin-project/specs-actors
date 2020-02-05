package builtin

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
)

const (
	MethodSend        = abi.MethodNum(0)
	MethodConstructor = abi.MethodNum(1)

	// TODO fin: remove this once canonical method numbers are finalized
	MethodPlaceholder = abi.MethodNum(1 << 30)
)

type accMethods struct {
	Constructor   abi.MethodNum
	PubkeyAddress abi.MethodNum
}

var MethodsAccount = accMethods{1, 2}

type iaMethods struct {
	Constructor abi.MethodNum
	Exec        abi.MethodNum
}

var MethodsInit = iaMethods{MethodConstructor, 2}

type cronMethods struct {
	Constructor abi.MethodNum
	EpochTick   abi.MethodNum
}

var MethodsCron = cronMethods{MethodConstructor, 2}

type rewardMethods struct {
	Constructor      abi.MethodNum
	AwardBlockReward abi.MethodNum
	WithdrawReward   abi.MethodNum
}

var MethodsReward = rewardMethods{MethodConstructor, 2, 3}

type msMethods struct {
	Constructor                 abi.MethodNum
	Propose                     abi.MethodNum
	Approve                     abi.MethodNum
	Cancel                      abi.MethodNum
	ClearCompleted              abi.MethodNum
	AddSigner                   abi.MethodNum
	RemoveSigner                abi.MethodNum
	SwapSigner                  abi.MethodNum
	ChangeNumApprovalsThreshold abi.MethodNum
}

var MethodsMultisig = msMethods{MethodConstructor, 2, 3, 4, 5, 6, 7, 8, 9}

type smaMethods struct {
	Constructor abi.MethodNum

	AddBalance          abi.MethodNum
	WithdrawBalance     abi.MethodNum
	HandleExpiredDeals  abi.MethodNum
	PublishStorageDeals abi.MethodNum

	VerifyDealsOnSectorProveCommit abi.MethodNum
	OnMinerSectorsTerminate        abi.MethodNum

	ComputeDataCommitment abi.MethodNum
	GetWeightForDealSet   abi.MethodNum
}

var StorageMarketMethods = smaMethods{MethodConstructor, 2, 3, 4, 5, 6, 7, 8, 9}

const (
	// Proxy cron tick method (via StoragePowerActor)
	Method_StorageMinerActor_OnDeferredCronEvent = MethodPlaceholder + iota

	// User-callable methods
	Method_StorageMinerActor_PreCommitSector
	Method_StorageMinerActor_ProveCommitSector
	Method_StorageMinerActor_DeclareTemporaryFaults
	Method_StorageMinerActor_RecoverTemporaryFaults
	Method_StorageMinerActor_ExtendSectorExpiration
	Method_StorageMinerActor_TerminateSectors
	Method_StorageMinerActor_SubmitSurprisePoStResponse
	Method_StorageMinerActor_StageWorkerKeyChange

	// Internal mechanism events
	Method_StorageMinerActor_OnVerifiedElectionPoSt
	Method_StorageMinerActor_OnSurprisePoStChallenge
	Method_StorageMinerActor_OnDeleteMiner

	// State queries
	Method_StorageMinerActor_GetPoStState
	Method_StorageMinerActor_GetOwnerAddr
	Method_StorageMinerActor_GetWorkerAddr
	Method_StorageMinerActor_GetWorkerVRFKey
)

const (
	// Cron tick method
	Method_StoragePowerActor_OnEpochTickEnd = MethodPlaceholder + iota

	// User-callable methods
	Method_StoragePowerActor_AddBalance
	Method_StoragePowerActor_WithdrawBalance
	Method_StoragePowerActor_CreateMiner
	Method_StoragePowerActor_DeleteMiner
	Method_StoragePowerActor_ReportConsensusFault

	// Internal mechanism events
	Method_StoragePowerActor_OnSectorProveCommit
	Method_StoragePowerActor_OnSectorTemporaryFaultEffectiveBegin
	Method_StoragePowerActor_OnSectorTemporaryFaultEffectiveEnd
	Method_StoragePowerActor_OnSectorModifyWeightDesc
	Method_StoragePowerActor_OnSectorTerminate
	Method_StoragePowerActor_OnMinerSurprisePoStSuccess
	Method_StoragePowerActor_OnMinerSurprisePoStFailure
	Method_StoragePowerActor_OnMinerEnrollCronEvent

	// State queries
	Method_StoragePowerActor_GetMinerConsensusPower
	Method_StoragePowerActor_GetMinerUnmetPledgeCollateralRequirement
)

const (
	// State updates
	Method_PaymentChannelActor_Create = MethodPlaceholder + iota
	Method_PaymentChannelActor_UpdateChannelState
	Method_PaymentChannelActor_Settle
	Method_PaymentChannelActor_Collect

	// State queries
	Method_PaymentChannelActor_GetOwner
	Method_PaymentChannelActor_GetInfo
)
