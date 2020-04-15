package miner

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	power "github.com/filecoin-project/specs-actors/actors/builtin/power"
)

// An approximation to chain state finality (should include message propagation time as well).
const ChainFinalityish = abi.ChainEpoch(500) // PARAM_FINISH

// Maximum duration to allow for the sealing process for seal algorithms.
// Dependent on algorithm and sector size
var MaxSealDuration = map[abi.RegisteredProof]abi.ChainEpoch{
	abi.RegisteredProof_StackedDRG32GiBSeal:  abi.ChainEpoch(10000), // PARAM_FINISH
	abi.RegisteredProof_StackedDRG2KiBSeal:   abi.ChainEpoch(10000),
	abi.RegisteredProof_StackedDRG8MiBSeal:   abi.ChainEpoch(10000),
	abi.RegisteredProof_StackedDRG512MiBSeal: abi.ChainEpoch(10000),
}

// Number of epochs between publishing the precommit and when the challenge for interactive PoRep is drawn
// used to ensure it is not predictable by miner.
const PreCommitChallengeDelay = abi.ChainEpoch(10)

// Lookback from the current epoch from which to obtain a PoSt challenge.
// A lookback of 1 means consulting the immediate parent tipset/state.
const PoStLookback = abi.ChainEpoch(1) // PARAM_FINISH

// Lookback from the current epoch for state view for elections; for Election PoSt, same as the PoSt lookback.
const ElectionLookback = PoStLookback // PARAM_FINISH

// Delay between declaration of a temporary sector fault and effectiveness of reducing the active proving set for PoSts.
const DeclaredFaultEffectiveDelay = abi.ChainEpoch(20) // PARAM_FINISH

// Staging period for a miner worker key change.
const WorkerKeyChangeDelay = 2 * ElectionLookback // PARAM_FINISH

// Deposit per sector required at pre-commitment, refunded after the commitment is proven (else burned).
func precommitDeposit(sectorSize abi.SectorSize, duration abi.ChainEpoch) abi.TokenAmount {
	depositPerByte := abi.NewTokenAmount(0) // PARAM_FINISH
	return big.Mul(depositPerByte, big.NewIntUnsigned(uint64(sectorSize)))
}

func temporaryFaultFee(weights []*power.SectorStorageWeightDesc, duration abi.ChainEpoch) abi.TokenAmount {
	return big.Zero() // PARAM_FINISH
}

// MaxFaultsCount is the maximum number of faults that can be declared
const MaxFaultsCount = 32 << 20

// ProvingPeriod defines the frequency of PoSt challenges that a miner will have to respond to
const ProvingPeriod = 300

type BigFrac struct {
	numerator   big.Int
	denominator big.Int
}

// Penalty to pledge collateral for the termination of an individual sector.
func pledgePenaltyForSectorTermination(termType power.SectorTermination, desc *power.SectorStorageWeightDesc) abi.TokenAmount {
	return big.Zero() // PARAM_FINISH
}

// Penalty to pledge collateral for repeated failure to prove storage.
// TODO: this should take in desc power.SectorStorageWeightDesc
func pledgePenaltyForWindowedPoStFailure(failures int64) abi.TokenAmount {
	return big.Zero() // PARAM_FINISH
}

var consensusFaultReporterInitialShare = BigFrac{
	// PARAM_FINISH
	numerator:   big.NewInt(1),
	denominator: big.NewInt(1000),
}
var consensusFaultReporterShareGrowthRate = BigFrac{
	// PARAM_FINISH
	numerator:   big.NewInt(101251),
	denominator: big.NewInt(100000),
}

const EpochDurationSeconds = 30
const SecondsInYear = 31556925
const SecondsInDay = 86400

// Specification for a linear vesting schedule.
type VestSpec struct {
	InitialDelay abi.ChainEpoch // Delay before any amount starts vesting.
	VestPeriod   abi.ChainEpoch // Period over which the total should vest, after the initial delay.
	StepDuration abi.ChainEpoch // Duration between successive incremental vests (independent of vesting period).
	Quantization abi.ChainEpoch // Maximum precision of vesting table (limits cardinality of table).
}

var PledgeVestingSpec = VestSpec{
	InitialDelay: abi.ChainEpoch(SecondsInYear / EpochDurationSeconds),    // 1 year, PARAM_FINISH
	VestPeriod:   abi.ChainEpoch(SecondsInYear / EpochDurationSeconds),    // 1 year, PARAM_FINISH
	StepDuration: abi.ChainEpoch(7 * SecondsInDay / EpochDurationSeconds), // 1 week, PARAM_FINISH
	Quantization: SecondsInDay / EpochDurationSeconds,                     // 1 day, PARAM_FINISH
}

func rewardForConsensusSlashReport(elapsedEpoch abi.ChainEpoch, collateral abi.TokenAmount) abi.TokenAmount {
	// PARAM_FINISH
	// var growthRate = SLASHER_SHARE_GROWTH_RATE_NUM / SLASHER_SHARE_GROWTH_RATE_DENOM
	// var multiplier = growthRate^elapsedEpoch
	// var slasherProportion = min(INITIAL_SLASHER_SHARE * multiplier, 1.0)
	// return collateral * slasherProportion

	// BigInt Operation
	// NUM = SLASHER_SHARE_GROWTH_RATE_NUM^elapsedEpoch * INITIAL_SLASHER_SHARE_NUM * collateral
	// DENOM = SLASHER_SHARE_GROWTH_RATE_DENOM^elapsedEpoch * INITIAL_SLASHER_SHARE_DENOM
	// slasher_amount = min(NUM/DENOM, collateral)
	maxReporterShareNum := big.NewInt(1)
	maxReporterShareDen := big.NewInt(2)

	elapsed := big.NewInt(int64(elapsedEpoch))
	slasherShareNumerator := big.Exp(consensusFaultReporterShareGrowthRate.numerator, elapsed)
	slasherShareDenominator := big.Exp(consensusFaultReporterShareGrowthRate.denominator, elapsed)

	num := big.Mul(big.Mul(slasherShareNumerator, consensusFaultReporterInitialShare.numerator), collateral)
	denom := big.Mul(slasherShareDenominator, consensusFaultReporterInitialShare.denominator)
	return big.Min(big.Div(num, denom), big.Div(big.Mul(collateral, maxReporterShareNum), maxReporterShareDen))
}
