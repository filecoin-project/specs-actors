package indices

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	actor_util "github.com/filecoin-project/specs-actors/actors/util"
	"github.com/ipfs/go-cid"
)

var PARAM_FINISH = actor_util.PARAM_FINISH

// Data in Indices are populated at instantiation with data from the state tree
// Indices itself has no state tree or access to the runtime
// it is a passive data structure that allows for convenience access to network indices
// and pure functions in implementing economic policies given states
type Indices interface {
	Epoch() abi.ChainEpoch
	NetworkKPI() big.Int
	TotalNetworkSectorWeight() abi.SectorWeight
	TotalPledgeCollateral() abi.TokenAmount
	TotalNetworkEffectivePower() abi.StoragePower // power above minimum miner size
	TotalNetworkPower() abi.StoragePower          // total network power irrespective of meeting minimum miner size

	TotalMinedFIL() abi.TokenAmount
	TotalUnminedFIL() abi.TokenAmount
	TotalBurnedFIL() abi.TokenAmount
	LastEpochReward() abi.TokenAmount

	StorageDeal_DurationBounds(
		pieceSize abi.PieceSize,
		startEpoch abi.ChainEpoch,
	) (minDuration abi.ChainEpoch, maxDuration abi.ChainEpoch)
	StorageDeal_StoragePricePerEpochBounds(
		pieceSize abi.PieceSize,
		startEpoch abi.ChainEpoch,
		endEpoch abi.ChainEpoch,
	) (minPrice abi.TokenAmount, maxPrice abi.TokenAmount)
	StorageDeal_ProviderCollateralBounds(
		pieceSize abi.PieceSize,
		startEpoch abi.ChainEpoch,
		endEpoch abi.ChainEpoch,
	) (minProviderCollateral abi.TokenAmount, maxProviderCollateral abi.TokenAmount)
	StorageDeal_ClientCollateralBounds(
		pieceSize abi.PieceSize,
		startEpoch abi.ChainEpoch,
		endEpoch abi.ChainEpoch,
	) (minClientCollateral abi.TokenAmount, maxClientCollateral abi.TokenAmount)
	SectorWeight(
		sectorSize abi.SectorSize,
		startEpoch abi.ChainEpoch,
		endEpoch abi.ChainEpoch,
		dealWeight abi.DealWeight,
	) abi.SectorWeight
	PledgeCollateralReq(minerNominalPower abi.StoragePower) abi.TokenAmount
	SectorWeightProportion(minerActiveSectorWeight abi.SectorWeight) big.Int
	PledgeCollateralProportion(minerPledgeCollateral abi.TokenAmount) big.Int
	StoragePower(
		minerActiveSectorWeight abi.SectorWeight,
		minerInactiveSectorWeight abi.SectorWeight,
		minerPledgeCollateral abi.TokenAmount,
	) abi.StoragePower
	StoragePowerProportion(
		minerStoragePower abi.StoragePower,
	) big.Int
	CurrEpochBlockReward() abi.TokenAmount
	GetCurrBlockRewardRewardForMiner(
		minerStoragePower abi.StoragePower,
		minerPledgeCollateral abi.TokenAmount,
		// TODO extend or eliminate
	) abi.TokenAmount
	StoragePower_PledgeSlashForSectorTermination(
		storageWeightDesc actor_util.SectorStorageWeightDesc,
		terminationType actor_util.SectorTermination,
	) abi.TokenAmount
	StoragePower_PledgeSlashForSurprisePoStFailure(
		minerClaimedPower abi.StoragePower,
		numConsecutiveFailures int64,
	) abi.TokenAmount
	StorageMining_PreCommitDeposit(
		sectorSize abi.SectorSize,
		expirationEpoch abi.ChainEpoch,
	) abi.TokenAmount
	StorageMining_TemporaryFaultFee(
		storageWeightDescs []actor_util.SectorStorageWeightDesc,
		duration abi.ChainEpoch,
	) abi.TokenAmount
	NetworkTransactionFee(
		toActorCodeID cid.Cid,
		methodNum abi.MethodNum,
	) abi.TokenAmount
	GetCurrBlockRewardForMiner(
		minerStoragePower abi.StoragePower,
		minerPledgeCollateral abi.TokenAmount,
	) abi.TokenAmount
}

type IndicesImpl struct {
	// these fields are computed from StateTree upon construction
	// they are treated as globally available states
	Epoch                      abi.ChainEpoch
	NetworkKPI                 big.Int
	TotalNetworkSectorWeight   abi.SectorWeight
	TotalPledgeCollateral      abi.TokenAmount
	TotalNetworkEffectivePower abi.StoragePower // power above minimum miner size
	TotalNetworkPower          abi.StoragePower // total network power irrespective of meeting minimum miner size

	TotalMinedFIL   abi.TokenAmount
	TotalUnminedFIL abi.TokenAmount
	TotalBurnedFIL  abi.TokenAmount
	LastEpochReward abi.TokenAmount
}

func (inds *IndicesImpl) StorageDeal_DurationBounds(
	pieceSize abi.PieceSize,
	startEpoch abi.ChainEpoch,
) (minDuration abi.ChainEpoch, maxDuration abi.ChainEpoch) {

	// placeholder
	PARAM_FINISH()
	minDuration = abi.ChainEpoch(0)
	maxDuration = abi.ChainEpoch(1 << 20)
	return
}

func (inds *IndicesImpl) StorageDeal_StoragePricePerEpochBounds(
	pieceSize abi.PieceSize,
	startEpoch abi.ChainEpoch,
	endEpoch abi.ChainEpoch,
) (minPrice abi.TokenAmount, maxPrice abi.TokenAmount) {

	// placeholder
	PARAM_FINISH()
	panic("")
}

func (inds *IndicesImpl) StorageDeal_ProviderCollateralBounds(
	pieceSize abi.PieceSize,
	startEpoch abi.ChainEpoch,
	endEpoch abi.ChainEpoch,
) (minProviderCollateral abi.TokenAmount, maxProviderCollateral abi.TokenAmount) {

	// placeholder
	PARAM_FINISH()
	panic("")
}

func (inds *IndicesImpl) StorageDeal_ClientCollateralBounds(
	pieceSize abi.PieceSize,
	startEpoch abi.ChainEpoch,
	endEpoch abi.ChainEpoch,
) (minClientCollateral abi.TokenAmount, maxClientCollateral abi.TokenAmount) {

	// placeholder
	PARAM_FINISH()
	panic("")
}

func (inds *IndicesImpl) SectorWeight(
	sectorSize abi.SectorSize,
	startEpoch abi.ChainEpoch,
	endEpoch abi.ChainEpoch,
	dealWeight abi.DealWeight,
) abi.SectorWeight {
	// for every sector, given its size, start, end, and deals within the sector
	// assign sector power for the duration of its lifetime
	PARAM_FINISH()
	panic("")
}

func (inds *IndicesImpl) PledgeCollateralReq(minerNominalPower abi.StoragePower) abi.TokenAmount {
	PARAM_FINISH()
	panic("")
}

func (inds *IndicesImpl) SectorWeightProportion(minerActiveSectorWeight abi.SectorWeight) big.Int {
	// return proportion of SectorWeight for miner
	PARAM_FINISH()
	panic("")
}

func (inds *IndicesImpl) PledgeCollateralProportion(minerPledgeCollateral abi.TokenAmount) big.Int {
	// return proportion of Pledge Collateral for miner
	PARAM_FINISH()
	panic("")
}

func (inds *IndicesImpl) StoragePower(
	minerActiveSectorWeight abi.SectorWeight,
	minerInactiveSectorWeight abi.SectorWeight,
	minerPledgeCollateral abi.TokenAmount,
) abi.StoragePower {
	// return StoragePower based on inputs
	// StoragePower for miner = func(ActiveSectorWeight for miner, PledgeCollateral for miner, global indices)
	PARAM_FINISH()
	panic("")
}

func (inds *IndicesImpl) StoragePowerProportion(
	minerStoragePower abi.StoragePower,
) big.Int {
	PARAM_FINISH()
	panic("")
}

func (inds *IndicesImpl) CurrEpochBlockReward() abi.TokenAmount {
	// total block reward allocated for CurrEpoch
	// each expected winner get an equal share of this reward
	// computed as a function of NetworkKPI, LastEpochReward, TotalUnmminedFIL, etc
	PARAM_FINISH()
	panic("")
}

func (inds *IndicesImpl) GetCurrBlockRewardRewardForMiner(
	minerStoragePower abi.StoragePower,
	minerPledgeCollateral abi.TokenAmount,
	// TODO extend or eliminate
) abi.TokenAmount {
	PARAM_FINISH()
	panic("")
}

// TerminationFault
func (inds *IndicesImpl) StoragePower_PledgeSlashForSectorTermination(
	storageWeightDesc actor_util.SectorStorageWeightDesc,
	terminationType actor_util.SectorTermination,
) abi.TokenAmount {
	PARAM_FINISH()
	panic("")
}

// DetectedFault
func (inds *IndicesImpl) StoragePower_PledgeSlashForSurprisePoStFailure(
	minerClaimedPower abi.StoragePower,
	numConsecutiveFailures int64,
) abi.TokenAmount {
	PARAM_FINISH()
	panic("")
}

// FIL deposit per sector precommit in Interactive PoRep
// refunded after ProveCommit but burned if PreCommit expires
func (inds *IndicesImpl) StorageMining_PreCommitDeposit(
	sectorSize abi.SectorSize,
	expirationEpoch abi.ChainEpoch,
) abi.TokenAmount {
	PARAM_FINISH()
	PRECOMMIT_DEPOSIT_PER_BYTE := abi.TokenAmount(big.NewInt(0)) // placeholder
	return abi.TokenAmount(big.Mul(PRECOMMIT_DEPOSIT_PER_BYTE, big.NewInt(int64(sectorSize))))
}

func (inds *IndicesImpl) StorageMining_TemporaryFaultFee(
	storageWeightDescs []actor_util.SectorStorageWeightDesc,
	duration abi.ChainEpoch,
) abi.TokenAmount {
	PARAM_FINISH()
	panic("")
}

func (inds *IndicesImpl) NetworkTransactionFee(
	toActorCodeID cid.Cid,
	methodNum abi.MethodNum,
) abi.TokenAmount {
	PARAM_FINISH()
	panic("")
}

func (inds *IndicesImpl) GetCurrBlockRewardForMiner(
	minerStoragePower abi.StoragePower,
	minerPledgeCollateral abi.TokenAmount,
) abi.TokenAmount {
	PARAM_FINISH()
	panic("")
}

func ConsensusPowerForStorageWeight(
	storageWeightDesc actor_util.SectorStorageWeightDesc,
) abi.StoragePower {
	PARAM_FINISH()
	panic("")
}

func StorageDeal_ProviderInitTimedOutSlashAmount(providerCollateral abi.TokenAmount) abi.TokenAmount {
	// placeholder
	PARAM_FINISH()
	return providerCollateral
}

func StoragePower_MinMinerSizeStor() abi.StoragePower {
	PARAM_FINISH()
	var MIN_MINER_SIZE_STOR = abi.NewStoragePower(100 * (1 << 40)) // placeholder, 100 TB
	return MIN_MINER_SIZE_STOR
}

func StoragePower_MinMinerSizeTarg() int64 {
	PARAM_FINISH()
	const MIN_MINER_SIZE_TARG = 3 // placeholder
	return MIN_MINER_SIZE_TARG
}

// how long miner has to respond to the challenge before it expires
func StorageMining_SurprisePoStChallengeDuration() abi.ChainEpoch {
	PARAM_FINISH()
	const CHALLENGE_DURATION = abi.ChainEpoch(4) // placeholder, 2 hours
	return CHALLENGE_DURATION
}

// sets the average frequency
func StorageMining_SurprisePoStProvingPeriod() abi.ChainEpoch {
	PARAM_FINISH()
	const PROVING_PERIOD = abi.ChainEpoch(2) // placeholder, 2 days
	return PROVING_PERIOD
}

// how long after a POST challenge before a miner can get challenged again
func StorageMining_PoStNoChallengePeriod() abi.ChainEpoch {
	PARAM_FINISH()
	const SURPRISE_NO_CHALLENGE_PERIOD = abi.ChainEpoch(0) // placeholder, 2 hours
	return SURPRISE_NO_CHALLENGE_PERIOD
}

// number of detected faults before a miner's sectors are all terminated
func StoragePower_SurprisePoStMaxConsecutiveFailures() int64 {
	PARAM_FINISH()
	const MAX_CONSECUTIVE_FAULTS = 3 // placeholder
	return MAX_CONSECUTIVE_FAULTS
}

// Time between when a temporary sector fault is declared, and when it becomes
// effective for purposes of reducing the active proving set for PoSts.
func StorageMining_DeclaredFaultEffectiveDelay() abi.ChainEpoch {
	PARAM_FINISH()
	const DECLARED_FAULT_EFFECTIVE_DELAY = abi.ChainEpoch(20) // placeholder
	return DECLARED_FAULT_EFFECTIVE_DELAY
}

// If a sector PreCommit appear at epoch T, then the corresponding ProveCommit
// must appear between epochs
//   (T + MIN_PROVE_COMMIT_SECTOR_EPOCH, T + MAX_PROVE_COMMIT_SECTOR_EPOCH)
// inclusive.
// TODO: placeholder epoch values -- will be set later
func StorageMining_MinProveCommitSectorEpoch() abi.ChainEpoch {
	PARAM_FINISH()
	const MIN_PROVE_COMMIT_SECTOR_EPOCH = abi.ChainEpoch(5)
	return MIN_PROVE_COMMIT_SECTOR_EPOCH
}

func StorageMining_MaxProveCommitSectorEpoch() abi.ChainEpoch {
	PARAM_FINISH()
	const MAX_PROVE_COMMIT_SECTOR_EPOCH = abi.ChainEpoch(10)
	return MAX_PROVE_COMMIT_SECTOR_EPOCH
}

func StorageMining_SpcLookbackPoSt() abi.ChainEpoch {
	PARAM_FINISH()
	const SPC_LOOKBACK_POST = abi.ChainEpoch(1) // cheap to generate, should be set as close to current TS as possible
	return SPC_LOOKBACK_POST
}

func StorageMining_Finality() abi.ChainEpoch {
	PARAM_FINISH()
	const FINALITY = 500
	return FINALITY
}

func StorageMining_SpcLookbackElection() abi.ChainEpoch {
	PARAM_FINISH()
	// same as the post lookback given EPoSt
	spcLookbackElection := StorageMining_SpcLookbackPoSt()
	return spcLookbackElection
}

func StorageMining_WorkerKeyChangeFreeze() abi.ChainEpoch {
	PARAM_FINISH()
	workerKeyChangeFreeze := 2 * StorageMining_SpcLookbackElection()
	return workerKeyChangeFreeze
}

func StorageMining_SpcLookbackSeal() abi.ChainEpoch {
	PARAM_FINISH()
	return StorageMining_Finality() // should be approximately the same as finality
}

func StorageMining_MaxSealTime32GiBWinStackedSDR() abi.ChainEpoch {
	PARAM_FINISH()
	MAX_SEAL_TIME_32GIB_WIN_STACKED_SDR := abi.ChainEpoch(1) // TODO: Change to a dictionary with RegisteredProofs as the key.
	return MAX_SEAL_TIME_32GIB_WIN_STACKED_SDR
}

func ConsensusFault_SlasherInitialShareNum() int {
	PARAM_FINISH()
	const SLASHER_INITIAL_SHARE_NUM = 1 // placeholder
	return SLASHER_INITIAL_SHARE_NUM
}

func ConsensusFault_SlasherInitialShareDenom() int {
	PARAM_FINISH()
	const SLASHER_INITIAL_SHARE_DENOM = 1000 // placeholder
	return SLASHER_INITIAL_SHARE_DENOM
}

func ConsensusFault_SlasherShareGrowthRateNum() int {
	PARAM_FINISH()
	const SLASHER_SHARE_GROWTH_RATE_NUM = 102813 // placeholder
	return SLASHER_SHARE_GROWTH_RATE_NUM
}

func ConsensusFault_SlasherShareGrowthRateDenom() int {
	PARAM_FINISH()
	const SLASHER_SHARE_GROWTH_RATE_DENOM = 100000 // placeholder
	return SLASHER_SHARE_GROWTH_RATE_DENOM
}
