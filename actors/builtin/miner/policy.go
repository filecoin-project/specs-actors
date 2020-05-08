package miner

import (
	"fmt"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
)

// The period over which all a miner's active sectors will be challenged.
const WPoStProvingPeriod = abi.ChainEpoch(builtin.EpochsInDay) // 24 hours

// The duration of a deadline's challenge window, the period before a deadline when the challenge is available.
const WPoStChallengeWindow = abi.ChainEpoch(1800 / builtin.EpochDurationSeconds) // Half an hour (=48 per day)

// The number of non-overlapping PoSt deadlines in each proving period.
const WPoStPeriodDeadlines = uint64(WPoStProvingPeriod / WPoStChallengeWindow)

func init() {
	// Check that the challenge windows divide the proving period evenly.
	if WPoStProvingPeriod%WPoStChallengeWindow != 0 {
		panic(fmt.Sprintf("incompatible proving period %d and challenge window %d", WPoStProvingPeriod, WPoStChallengeWindow))
	}
	if abi.ChainEpoch(WPoStPeriodDeadlines)*WPoStChallengeWindow != WPoStProvingPeriod {
		panic(fmt.Sprintf("incompatible proving period %d and challenge window %d", WPoStProvingPeriod, WPoStChallengeWindow))
	}
}

// The maximum number of sectors that a miner can have simultaneously active.
// This also bounds the number of faults that can be declared, etc.
// TODO raise this number, carefully
const SectorsMax = 32 << 20 // PARAM_FINISH

// The maximum number of proving partitions a miner can have simultaneously active.
func activePartitionsMax(partitionSectorCount uint64) uint64 {
	return (SectorsMax / partitionSectorCount) + WPoStPeriodDeadlines
}

// The maximum number of partitions that may be submitted in a single message.
// This bounds the size of a list/set of sector numbers that might be instantiated to process a submission.
func windowPoStMessagePartitionsMax(partitionSectorCount uint64) uint64 {
	return 100_000 / partitionSectorCount
}

// The maximum number of new sectors that may be staged by a miner during a single proving period.
const NewSectorsPerPeriodMax = 128 << 10

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

// Lookback from the current epoch for state view for leader elections.
const ElectionLookback = abi.ChainEpoch(1) // PARAM_FINISH

// Lookback from the deadline's challenge window opening from which to sample chain randomness for the challenge seed.
const WPoStChallengeLookback = abi.ChainEpoch(20) // PARAM_FINISH

// Minimum period before a deadline's challenge window opens that a fault must be declared for that deadline.
// A fault declaration may appear in the challenge epoch, since it must have been posted before the
// epoch completed, and hence before the challenge was knowable.
const FaultDeclarationCutoff = WPoStChallengeLookback // PARAM_FINISH

// The maximum age of a fault before the sector is terminated.
const FaultMaxAge = WPoStProvingPeriod*14 - 1

// Staging period for a miner worker key change.
const WorkerKeyChangeDelay = 2 * ElectionLookback // PARAM_FINISH

// Deposit per sector required at pre-commitment, refunded after the commitment is proven (else burned).
func precommitDeposit(sectorSize abi.SectorSize, duration abi.ChainEpoch) abi.TokenAmount {
	depositPerByte := abi.NewTokenAmount(0) // PARAM_FINISH
	return big.Mul(depositPerByte, big.NewIntUnsigned(uint64(sectorSize)))
}

type BigFrac struct {
	numerator   big.Int
	denominator big.Int
}

// Penalty to locked pledge collateral for the termination of a sector before scheduled expiry.
func pledgePenaltyForSectorTermination(sector *SectorOnChainInfo) abi.TokenAmount {
	return big.Zero() // PARAM_FINISH
}

// Penalty to locked pledge collateral for a "skipped" sector or missing PoSt fault.
func pledgePenaltyForSectorUndeclaredFault(sector *SectorOnChainInfo) abi.TokenAmount {
	return big.Zero() // PARAM_FINISH
}

// Penalty to locked pledge collateral for a declared or on-going sector fault.
func pledgePenaltyForSectorDeclaredFault(sector *SectorOnChainInfo) abi.TokenAmount {
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

// Specification for a linear vesting schedule.
type VestSpec struct {
	InitialDelay abi.ChainEpoch // Delay before any amount starts vesting.
	VestPeriod   abi.ChainEpoch // Period over which the total should vest, after the initial delay.
	StepDuration abi.ChainEpoch // Duration between successive incremental vests (independent of vesting period).
	Quantization abi.ChainEpoch // Maximum precision of vesting table (limits cardinality of table).
}

var PledgeVestingSpec = VestSpec{
	InitialDelay: abi.ChainEpoch(7 * builtin.EpochsInDay), // 1 week for testnet, PARAM_FINISH
	VestPeriod:   abi.ChainEpoch(7 * builtin.EpochsInDay), // 1 week for testnet, PARAM_FINISH
	StepDuration: abi.ChainEpoch(1 * builtin.EpochsInDay), // 1 day for testnet, PARAM_FINISH
	Quantization: 12 * builtin.EpochsInHour,               // 12 hours for testnet, PARAM_FINISH
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
