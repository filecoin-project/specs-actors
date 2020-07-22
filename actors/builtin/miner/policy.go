package miner

import (
	"fmt"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	. "github.com/filecoin-project/specs-actors/actors/util"
)
// PARAM_SPEC
// The period over which a miner's active sectors are expected to be proven via WindowPoSt.
// Motivation: This guarantees that (1) user data is proven once a day, (2) user data is stored for 24h by a rational miner (due to WindowPoSt cost assumption).
const WPoStProvingPeriod = abi.ChainEpoch(builtin.EpochsInDay) // 24 hours

// PARAM_SPEC
// The period between the opening and the closing of a WindowPoSt deadline in which the miner is expected to provide a WindowPoSt proof.
// Motivation: This guarantees that a miner has enough time to propagate a WindowPoSt for the current deadline.
// Usage: This is used to calculate the opening and close of a deadline window and to calculate the number of deadlines in a proving period.
const WPoStChallengeWindow = abi.ChainEpoch(30 * 60 / builtin.EpochDurationSeconds) // 30 minutes (48 per day)

// PARAM_SPEC
// The number of non-overlapping PoSt deadlines in a proving period.
// Motivation: This guarantees that a miner can spread their WindowPoSt duties across a proving period, instead of submitting a single large WindowPoSt submission.
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

// PARAM_SPEC
// The maximum number of sectors that a miner can have (and implicitly the maximum number of faults, recoveries, etc.)
// Note that when sectors are terminated they count towards SectorsMax until cleanup.
// Motivation: This guarantees that actors operations per miner are bounded.
// TODO raise this number, carefully
// https://github.com/filecoin-project/specs-actors/issues/470
const SectorsMax = 32 << 20 // PARAM_FINISH

// The maximum number of partitions that may be required to be loaded in a single invocation.
// This limits the number of simultaneous fault, recovery, or sector-extension declarations.
// With 48 deadlines (half-hour), 200 partitions per declaration permits loading a full EiB of 32GiB
// sectors with 1 message per epoch within a single half-hour deadline. A miner can of course submit more messages.
const AddressedPartitionsMax = 200

// The maximum number of sector infos that may be required to be loaded in a single invocation.
const AddressedSectorsMax = 10_000

// The maximum number of partitions that may be required to be loaded in a single invocation,
// when all the sector infos for the partitions will be loaded.
func loadPartitionsSectorsMax(partitionSectorCount uint64) uint64 {
	return min64(AddressedSectorsMax/partitionSectorCount, AddressedPartitionsMax)
}

// The maximum number of new sectors that may be staged by a miner during a single proving period.
const NewSectorsPerPeriodMax = 128 << 10

// PARAM_SPEC
// Epochs after which chain state is final with overwhelming probability (hence the likelihood of two fork of this size is negligible)
// Motivation: This is a conservative value that is chosen via simulations of all known attacks.
const ChainFinality = abi.ChainEpoch(1400)

var SealedCIDPrefix = cid.Prefix{
	Version:  1,
	Codec:    cid.FilCommitmentSealed,
	MhType:   mh.POSEIDON_BLS12_381_A1_FC1,
	MhLength: 32,
}

// List of proof types which can be used when creating new miner actors
var SupportedProofTypes = map[abi.RegisteredSealProof]struct{}{
	abi.RegisteredSealProof_StackedDrg32GiBV1: {},
	abi.RegisteredSealProof_StackedDrg64GiBV1: {},
}

// Maximum duration to allow for the sealing process for seal algorithms.
// Dependent on algorithm and sector size
var MaxSealDuration = map[abi.RegisteredSealProof]abi.ChainEpoch{
	abi.RegisteredSealProof_StackedDrg32GiBV1:  abi.ChainEpoch(10000), // PARAM_FINISH
	abi.RegisteredSealProof_StackedDrg2KiBV1:   abi.ChainEpoch(10000),
	abi.RegisteredSealProof_StackedDrg8MiBV1:   abi.ChainEpoch(10000),
	abi.RegisteredSealProof_StackedDrg512MiBV1: abi.ChainEpoch(10000),
	abi.RegisteredSealProof_StackedDrg64GiBV1:  abi.ChainEpoch(10000),
}

// PARAM_SPEC
// Number of epochs between publishing the precommit and when the challenge for interactive PoRep is drawn.
// Motivation: This guarantees that (1) a miner cannot predict a future challenge (2) a miner cannot do a long fork in the past to insert a precommit after seeing a challenge.
const PreCommitChallengeDelay = abi.ChainEpoch(150)

// Lookback from the current epoch for state view for leader elections.
const ElectionLookback = abi.ChainEpoch(1) // PARAM_FINISH

// PARAM_SPEC
// Lookback from the deadline's challenge window opening from which to sample chain randomness for the WindowPoSt challenge seed.
// This lookback exists so that deadline windows can be non-overlapping (which make the programming simpler)
// Motivation: A miner can start the WindowPoSt computation without waiting for chain stability. This value cannot be too large since it could compromise the rationality of honest storage (due to WindowPoSt cost assumptions)
const WPoStChallengeLookback = abi.ChainEpoch(20)

// PARAM_SPEC
// Minimum period between fault declaration and the next deadline opening.
// If the number of epochs between fault declaration and deadline's challenge window opening is lower than FaultDeclarationCutoff,
// the fault declaration is considered invalid for that deadline.
// Motivation: This guarantees that a miner is not likely to successfully fork the chain and declare a fault after seeing the challenges.
const FaultDeclarationCutoff = WPoStChallengeLookback + 50

// PARAM_SPEC
// The maximum age of a fault before the sector is terminated.
// Motivation: This guarantees to clients that a Filecoin miner cannot lose the file for longer than 14 days.
const FaultMaxAge = WPoStProvingPeriod * 14

// PARAM_SPEC
// Staging period for a miner worker key change.
// Future improvement: Finality is a harsh delay for a miner who has lost their worker key, as the miner will miss Window PoSts until
// it can be changed. It's the only safe value, though. We may implement a mitigation mechanism such as a second
// key or allowing the owner account to submit PoSts while a key change is pending.
// Motivation: This guarantees that a miner cannot choose a more favorable worker key that wins leader elections.
const WorkerKeyChangeDelay = ChainFinality

// PARAM_SPEC
// A sector extension can be a maximum number of epochs past the current epoch.
// A sector may be extended multiple times, however, the maximum extension will be the minimum of
// CurrEpoch + MaximumSectorExpirationExtension and sector.ActivationEpoch+sealProof.SectorMaximumLifetime()
const MaxSectorExpirationExtension = builtin.EpochsInYear

// PARAM_SPEC
// Ratio of sector size to maximum deals per sector.
// The maximum number of deals is the sector size divided by this number (2^27)
// which limits 32GiB sectors to 256 deals and 64GiB sectors to 512
const DealLimitDenominator = 134217728

// PARAM_SPEC
// Quality multiplier for CC sectors
var QualityBaseMultiplier = big.NewInt(10)

// PARAM_SPEC
// Quality multiplier for a standard deal in a sector
var DealWeightMultiplier = big.NewInt(10)

// PARAM_SPEC
// Quality multiplier for verified deals
var VerifiedDealWeightMultiplier = big.NewInt(100)

// PARAM_SPEC
// Precision for Quality Adjusted power
const SectorQualityPrecision = 20

// DealWeight and VerifiedDealWeight are spacetime occupied by regular deals and verified deals in a sector.
// Sum of DealWeight and VerifiedDealWeight should be less than or equal to total SpaceTime of a sector.
// Sectors full of VerifiedDeals will have a SectorQuality of VerifiedDealWeightMultiplier/QualityBaseMultiplier.
// Sectors full of Deals will have a SectorQuality of DealWeightMultiplier/QualityBaseMultiplier.
// Sectors with neither will have a SectorQuality of QualityBaseMultiplier/QualityBaseMultiplier.
// SectorQuality of a sector is a weighted average of multipliers based on their propotions.
func QualityForWeight(size abi.SectorSize, duration abi.ChainEpoch, dealWeight, verifiedWeight abi.DealWeight) abi.SectorQuality {
	// sectorSpaceTime = size * duration
	sectorSpaceTime := big.Mul(big.NewIntUnsigned(uint64(size)), big.NewInt(int64(duration)))
	// totalDealSpaceTime = dealWeight + verifiedWeight
	totalDealSpaceTime := big.Add(dealWeight, verifiedWeight)
	Assert(sectorSpaceTime.GreaterThanEqual(totalDealSpaceTime))

	// Base - all size * duration of non-deals
	// weightedBaseSpaceTime = (sectorSpaceTime - totalDealSpaceTime) * QualityBaseMultiplier
	weightedBaseSpaceTime := big.Mul(big.Sub(sectorSpaceTime, totalDealSpaceTime), QualityBaseMultiplier)
	// Deal - all deal size * deal duration * 10
	// weightedDealSpaceTime = dealWeight * DealWeightMultiplier
	weightedDealSpaceTime := big.Mul(dealWeight, DealWeightMultiplier)
	// Verified - all verified deal size * verified deal duration * 100
	// weightedVerifiedSpaceTime = verifiedWeight * VerifiedDealWeightMultiplier
	weightedVerifiedSpaceTime := big.Mul(verifiedWeight, VerifiedDealWeightMultiplier)
	// Sum - sum of all spacetime
	// weightedSumSpaceTime = weightedBaseSpaceTime + weightedDealSpaceTime + weightedVerifiedSpaceTime
	weightedSumSpaceTime := big.Add(weightedBaseSpaceTime, big.Add(weightedDealSpaceTime, weightedVerifiedSpaceTime))
	// scaledUpWeightedSumSpaceTime = weightedSumSpaceTime * 2^20
	scaledUpWeightedSumSpaceTime := big.Lsh(weightedSumSpaceTime, SectorQualityPrecision)

	// Average of weighted space time: (scaledUpWeightedSumSpaceTime / sectorSpaceTime * 10)
	return big.Div(big.Div(scaledUpWeightedSumSpaceTime, sectorSpaceTime), QualityBaseMultiplier)
}

// Returns the power for a sector size and weight.
func QAPowerForWeight(size abi.SectorSize, duration abi.ChainEpoch, dealWeight, verifiedWeight abi.DealWeight) abi.StoragePower {
	quality := QualityForWeight(size, duration, dealWeight, verifiedWeight)
	return big.Rsh(big.Mul(big.NewIntUnsigned(uint64(size)), quality), SectorQualityPrecision)
}

// Returns the quality-adjusted power for a sector.
func QAPowerForSector(size abi.SectorSize, sector *SectorOnChainInfo) abi.StoragePower {
	duration := sector.Expiration - sector.Activation
	return QAPowerForWeight(size, duration, sector.DealWeight, sector.VerifiedDealWeight)
}

// Deposit per sector required at pre-commitment, refunded after the commitment is proven (else burned).
func precommitDeposit(qaSectorPower abi.StoragePower, networkQAPower abi.StoragePower, baselinePower abi.StoragePower, networkTotalPledge, epochTargetReward, circulatingSupply abi.TokenAmount) abi.TokenAmount {
	return InitialPledgeForPower(qaSectorPower, networkQAPower, baselinePower, networkTotalPledge, epochTargetReward, circulatingSupply)
}

// Determine maximum number of deal miner's sector can hold
func dealPerSectorLimit(size abi.SectorSize) uint64 {
	return max64(256, uint64(size/DealLimitDenominator))
}

type BigFrac struct {
	numerator   big.Int
	denominator big.Int
}

// PARAM_SPEC
// Initial share of a slasher's reward
var consensusFaultReporterInitialShare = BigFrac{
	// PARAM_FINISH
	numerator:   big.NewInt(1),
	denominator: big.NewInt(1000),
}

// PARAM_SPEC
// Growth rate of a slasher's reward
var consensusFaultReporterShareGrowthRate = BigFrac{
	// PARAM_FINISH
	numerator:   big.NewInt(101251),
	denominator: big.NewInt(100000),
}

// PARAM_SPEC
// Maximum share of slasher's reward
var consensusFaultMaxReporterShare = BigFrac{
	// PARAM_FINISH
	numerator:   big.NewInt(1),
	denominator: big.NewInt(2),
}

// Specification for a linear vesting schedule.
type VestSpec struct {
	InitialDelay abi.ChainEpoch // Delay before any amount starts vesting.
	VestPeriod   abi.ChainEpoch // Period over which the total should vest, after the initial delay.
	StepDuration abi.ChainEpoch // Duration between successive incremental vests (independent of vesting period).
	Quantization abi.ChainEpoch // Maximum precision of vesting table (limits cardinality of table).
}

// PARAM_SPEC
var PledgeVestingSpec = VestSpec{
	InitialDelay: abi.ChainEpoch(180 * builtin.EpochsInDay), // PARAM_FINISH
	VestPeriod:   abi.ChainEpoch(180 * builtin.EpochsInDay), // PARAM_FINISH
	StepDuration: abi.ChainEpoch(1 * builtin.EpochsInDay),   // PARAM_FINISH
	Quantization: 12 * builtin.EpochsInHour,                 // PARAM_FINISH
}

// PARAM_SPEC
var RewardVestingSpec = VestSpec{
	InitialDelay: abi.ChainEpoch(20 * builtin.EpochsInDay),  // PARAM_FINISH
	VestPeriod:   abi.ChainEpoch(180 * builtin.EpochsInDay), // PARAM_FINISH
	StepDuration: abi.ChainEpoch(1 * builtin.EpochsInDay),   // PARAM_FINISH
	Quantization: 12 * builtin.EpochsInHour,                 // PARAM_FINISH
}

// When a user (called slasher) reports a consensus fault, they earn a share of the miner's current balance
// This amount is:  Min(initialShare * growthRate^elapsed, maxReporterShare) * collateral
// Given current parameter choice, the longer a slasher waits, the higher their reward.
// There it a maximum of reward to be earned.
func RewardForConsensusSlashReport(elapsedEpoch abi.ChainEpoch, collateral abi.TokenAmount) abi.TokenAmount {
	// High level description
	// PARAM_FINISH
	// var growthRate = SLASHER_SHARE_GROWTH_RATE_NUM / SLASHER_SHARE_GROWTH_RATE_DENOM
	// var multiplier = growthRate^elapsedEpoch
	// var slasherProportion = min(INITIAL_SLASHER_SHARE * multiplier, 1.0)
	// return collateral * slasherProportion

	// BigInt Operation
	// NUM = SLASHER_SHARE_GROWTH_RATE_NUM^elapsedEpoch * INITIAL_SLASHER_SHARE_NUM * collateral
	// DENOM = SLASHER_SHARE_GROWTH_RATE_DENOM^elapsedEpoch * INITIAL_SLASHER_SHARE_DENOM
	// slasher_amount = min(NUM/DENOM, collateral)
	
	elapsed := big.NewInt(int64(elapsedEpoch))
	
	// The following is equivalent to: slasherShare = growthRate^elapsed
	// slasherShareNumerator = growthRateNumerator^elapsed
	slasherShareNumerator := big.Exp(consensusFaultReporterShareGrowthRate.numerator, elapsed)
	// slasherShareDenominator = growthRateDenominator^elapsed
	slasherShareDenominator := big.Exp(consensusFaultReporterShareGrowthRate.denominator, elapsed)

	// The following is equivalent to: reward = slasherShare * initialShare * collateral
	// num = slasherShareNumerator * initialShareNumerator * collateral
	num := big.Mul(big.Mul(slasherShareNumerator, consensusFaultReporterInitialShare.numerator), collateral)
	// denom = slasherShareDenominator * initialShareDenominator
	denom := big.Mul(slasherShareDenominator, consensusFaultReporterInitialShare.denominator)

	// The following is equivalent to: Min(reward, collateral * maxReporterShare)
	// Min(rewardNum/rewardDenom, maxReporterShareNum/maxReporterShareDen*collateral)
	return big.Min(big.Div(num, denom), big.Div(big.Mul(collateral, consensusFaultMaxReporterShare.numerator), consensusFaultMaxReporterShare.denominator))
}
