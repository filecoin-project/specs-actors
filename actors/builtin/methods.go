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

const (
	Method_InitActor_Exec = MethodPlaceholder + iota
	Method_InitActor_GetActorIDForAddress
)

const (
	Method_CronActor_EpochTick = MethodPlaceholder + iota
)

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

var MethodsMultisig = msMethods{1, 2, 3, 4, 5, 6, 7, 8, 9}

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
	Method_StorageMarketActor_OnEpochTickEnd = MethodPlaceholder + iota

	// User-callable methods
	Method_StorageMarketActor_AddBalance
	Method_StorageMarketActor_WithdrawBalance
	Method_StorageMarketActor_PublishStorageDeals

	// Internal mechanism events
	Method_StorageMarketActor_VerifyDealsOnSectorProveCommit
	Method_StorageMarketActor_OnMinerSectorsTerminate

	// State queries
	Method_StorageMarketActor_GetPieceInfosForDealIDs
	Method_StorageMarketActor_GetWeightForDealSet
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
