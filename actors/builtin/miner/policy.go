package miner

import (
	"fmt"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	"github.com/filecoin-project/specs-actors/v7/actors/builtin"
)

type Policy struct {
	// The period over which a miner's active sectors are expected to be proven via WindowPoSt.
	// This guarantees that (1) user data is proven daily, (2) user data is stored for 24h by a rational miner
	// (due to Window PoSt cost assumption).
	wPoStProvingPeriod abi.ChainEpoch

	// The period between the opening and the closing of a WindowPoSt deadline in which the miner is expected to
	// provide a Window PoSt proof.
	// This provides a miner enough time to compute and propagate a Window PoSt proof.
	wPoStChallengeWindow abi.ChainEpoch

	// WPoStDisputeWindow is the period after a challenge window ends during which
	// PoSts submitted during that period may be disputed.
	wPoStDisputeWindow abi.ChainEpoch

	// The number of non-overlapping PoSt deadlines in a proving period.
	// This spreads a miner's Window PoSt work across a proving period.
	wPoStPeriodDeadlines uint64

	// The maximum number of sector infos that can be loaded in a single invocation.
	// This limits the amount of state to be read in a single message execution.
	AddressedSectorsMax uint64

	// MaxPeerIDLength is the maximum length allowed for any on-chain peer ID.
	// Most Peer IDs are expected to be less than 50 bytes.
	MaxPeerIDLength uint64

	// MaxMultiaddrData is the maximum amount of data that can be stored in multiaddrs.
	MaxMultiaddrData uint64

	// Epochs after which chain state is final with overwhelming probability (hence the likelihood of two fork of this size is negligible)
	// This is a conservative value that is chosen via simulations of all known attacks.
	chainFinality abi.ChainEpoch

	// Maximum delay between challenge and pre-commitment.
	// This prevents a miner sealing sectors far in advance of committing them to the chain, thus committing to a
	// particular chain.
	MaxPreCommitRandomnessLookback abi.ChainEpoch

	// Number of epochs between publishing a sector pre-commitment and when the challenge for interactive PoRep is drawn.
	// This (1) prevents a miner predicting a challenge before staking their pre-commit deposit, and
	// (2) prevents a miner attempting a long fork in the past to insert a pre-commitment after seeing the challenge.
	PreCommitChallengeDelay abi.ChainEpoch

	// Lookback from the deadline's challenge window opening from which to sample chain randomness for the WindowPoSt challenge seed.
	// This means that deadline windows can be non-overlapping (which make the programming simpler) without requiring a
	// miner to wait for chain stability during the challenge window.
	// This value cannot be too large lest it compromise the rationality of honest storage (from Window PoSt cost assumptions).
	wPoStChallengeLookback abi.ChainEpoch

	// Minimum period between fault declaration and the next deadline opening.
	// If the number of epochs between fault declaration and deadline's challenge window opening is lower than FaultDeclarationCutoff,
	// the fault declaration is considered invalid for that deadline.
	// This guarantees that a miner is not likely to successfully fork the chain and declare a fault after seeing the challenges.
	FaultDeclarationCutoff abi.ChainEpoch

	// The maximum age of a fault before the sector is terminated.
	// This bounds the time a miner can lose client's data before sacrificing pledge and deal collateral.
	FaultMaxAge abi.ChainEpoch

	// Staging period for a miner worker key change.
	// This delay prevents a miner choosing a more favorable worker key that wins leader elections.
	WorkerKeyChangeDelay abi.ChainEpoch

	// Minimum number of epochs past the current epoch a sector may be set to expire.
	MinSectorExpiration abi.ChainEpoch

	// The maximum number of epochs past the current epoch that sector lifetime may be extended.
	// A sector may be extended multiple times, however, the total maximum lifetime is also bounded by
	// the associated seal proof's maximum lifetime.
	MaxSectorExpirationExtension abi.ChainEpoch

	// Ratio of sector size to maximum number of deals per sector.
	// The maximum number of deals is the sector size divided by this number (2^27)
	// which limits 32GiB sectors to 256 deals and 64GiB sectors to 512
	DealLimitDenominator uint64

	// The vesting schedule for total rewards (block reward + gas reward) earned by a block producer.
	RewardVestingSpec VestSpec
}

func checkParams(wPoStProvingPeriod abi.ChainEpoch, wPoStChallengeWindow abi.ChainEpoch, wPoStPeriodDeadlines uint64,
	wPoStDisputeWindow abi.ChainEpoch, chainFinality abi.ChainEpoch, wPoStChallengeLookback abi.ChainEpoch) {
	if wPoStProvingPeriod%wPoStChallengeWindow != 0 {
		panic(fmt.Sprintf("incompatible proving period %d and challenge window %d", wPoStProvingPeriod, wPoStChallengeWindow))
	}
	// Check that WPoStPeriodDeadlines is consistent with the proving period and challenge window.
	if abi.ChainEpoch(wPoStPeriodDeadlines)*wPoStChallengeWindow != wPoStProvingPeriod {
		panic(fmt.Sprintf("incompatible proving period %d and challenge window %d", wPoStProvingPeriod, wPoStChallengeWindow))
	}

	// Check to make sure the dispute window is longer than finality so there's always some time to dispute bad proofs.
	if wPoStDisputeWindow <= chainFinality {
		panic(fmt.Sprintf("the proof dispute period %d must exceed finality %d", wPoStDisputeWindow, chainFinality))
	}

	// A deadline becomes immutable one challenge window before it's challenge window opens.
	// The challenge lookback must fall within this immutability period.
	if wPoStChallengeLookback > wPoStChallengeWindow {
		panic("the challenge lookback cannot exceed one challenge window")
	}

	// Deadlines are immutable when the challenge window is open, and during
	// the previous challenge window.
	immutableWindow := 2 * wPoStChallengeWindow

	// We want to reserve at least one deadline's worth of time to compact a
	// deadline.
	minCompactionWindow := wPoStChallengeWindow

	// Make sure we have enough time in the proving period to do everything we need.
	if (minCompactionWindow + immutableWindow + wPoStDisputeWindow) > wPoStProvingPeriod {
		panic(fmt.Sprintf("together, the minimum compaction window (%d) immutability window (%d) and the dispute window (%d) exceed the proving period (%d)",
			minCompactionWindow, immutableWindow, wPoStDisputeWindow, wPoStProvingPeriod))
	}
}

func (p *Policy) SetWPoStProvingPeriod(wPoStProvingPeriod abi.ChainEpoch) {
	checkParams(wPoStProvingPeriod, p.wPoStChallengeWindow,
		p.wPoStPeriodDeadlines, p.wPoStDisputeWindow,
		p.chainFinality, p.wPoStChallengeLookback)

	p.wPoStProvingPeriod = wPoStProvingPeriod
}
func (p *Policy) SetWPoStChallengeWindow(wPoStChallengeWindow abi.ChainEpoch) {
	checkParams(p.wPoStProvingPeriod, wPoStChallengeWindow,
		p.wPoStPeriodDeadlines, p.wPoStDisputeWindow,
		p.chainFinality, p.wPoStChallengeLookback)

	p.wPoStChallengeWindow = wPoStChallengeWindow
}
func (p *Policy) SetWPoStPeriodDeadlines(wPoStPeriodDeadlines uint64) {
	checkParams(p.wPoStProvingPeriod, p.wPoStChallengeWindow,
		wPoStPeriodDeadlines, p.wPoStDisputeWindow,
		p.chainFinality, p.wPoStChallengeLookback)

	p.wPoStPeriodDeadlines = wPoStPeriodDeadlines
}
func (p *Policy) SetWPoStDisputeWindow(wPoStDisputeWindow abi.ChainEpoch) {
	checkParams(p.wPoStProvingPeriod, p.wPoStChallengeWindow,
		p.wPoStPeriodDeadlines, wPoStDisputeWindow,
		p.chainFinality, p.wPoStChallengeLookback)

	p.wPoStDisputeWindow = wPoStDisputeWindow
}
func (p *Policy) SetChainFinality(chainFinality abi.ChainEpoch) {
	checkParams(p.wPoStProvingPeriod, p.wPoStChallengeWindow,
		p.wPoStPeriodDeadlines, p.wPoStDisputeWindow,
		chainFinality, p.wPoStChallengeLookback)

	p.chainFinality = chainFinality
}
func (p *Policy) SetWPoStChallengeLookback(wPoStChallengeLookback abi.ChainEpoch) {
	checkParams(p.wPoStProvingPeriod, p.wPoStChallengeWindow,
		p.wPoStPeriodDeadlines, p.wPoStDisputeWindow,
		p.chainFinality, wPoStChallengeLookback)

	p.wPoStChallengeLookback = wPoStChallengeLookback
}

func MakePolicy(wPoStProvingPeriod abi.ChainEpoch,
	wPoStChallengeWindow abi.ChainEpoch,
	wPoStDisputeWindow abi.ChainEpoch,
	wPoStPeriodDeadlines uint64,
	addressedSectorsMax uint64,
	maxPeerIDLength uint64,
	maxMultiaddrData uint64,
	chainFinality abi.ChainEpoch,
	maxPreCommitRandomnessLookback abi.ChainEpoch,
	preCommitChallengeDelay abi.ChainEpoch,
	wPoStChallengeLookback abi.ChainEpoch,
	faultDeclarationCutoff abi.ChainEpoch,
	faultMaxAge abi.ChainEpoch,
	workerKeyChangeDelay abi.ChainEpoch,
	minSectorExpiration abi.ChainEpoch,
	maxSectorExpirationExtension abi.ChainEpoch,
	dealLimitDenominator uint64,
	rewardVestingSpec VestSpec) Policy {

	checkParams(wPoStProvingPeriod, wPoStChallengeWindow,
		wPoStPeriodDeadlines, wPoStDisputeWindow,
		chainFinality, wPoStChallengeLookback)

	return Policy{
		wPoStProvingPeriod,
		wPoStChallengeWindow,
		wPoStDisputeWindow,
		wPoStPeriodDeadlines,
		addressedSectorsMax,
		maxPeerIDLength,
		maxMultiaddrData,
		chainFinality,
		maxPreCommitRandomnessLookback,
		preCommitChallengeDelay,
		wPoStChallengeLookback,
		faultDeclarationCutoff,
		faultMaxAge,
		workerKeyChangeDelay,
		minSectorExpiration,
		maxSectorExpirationExtension,
		dealLimitDenominator,
		rewardVestingSpec,
	}
}

const DefaultChainFinality = abi.ChainEpoch(900)

var DefaultWPoStProvingPeriod = builtin.DefaultNetworkPolicy.EpochsInDay()

const DefaultWPoStChallengeLookback = abi.ChainEpoch(20)

var DefaultMinerPolicy = MakePolicy(DefaultWPoStProvingPeriod,
	abi.ChainEpoch(30*60/builtin.DefaultNetworkPolicy.EpochDurationSeconds()),
	2*DefaultChainFinality,
	48,
	25_000,
	128,
	1024,
	DefaultChainFinality,
	builtin.DefaultNetworkPolicy.EpochsInDay()+DefaultChainFinality,
	abi.ChainEpoch(150),
	abi.ChainEpoch(20),
	DefaultWPoStChallengeLookback+50,
	DefaultWPoStProvingPeriod*42,
	DefaultChainFinality,
	180*builtin.DefaultNetworkPolicy.EpochsInDay(),
	540*builtin.DefaultNetworkPolicy.EpochsInDay(),
	134217728,
	VestSpec{
		InitialDelay: abi.ChainEpoch(0),
		VestPeriod:   180 * builtin.DefaultNetworkPolicy.EpochsInDay(),
		StepDuration: builtin.DefaultNetworkPolicy.EpochsInDay(),
		Quantization: 12 * builtin.DefaultNetworkPolicy.EpochsInHour(),
	},
)

var CurrentMinerPolicy = DefaultMinerPolicy

func ChainFinality() abi.ChainEpoch {
	return CurrentMinerPolicy.chainFinality
}
func WPoStPeriodDeadlines() uint64 {
	return CurrentMinerPolicy.wPoStPeriodDeadlines
}
func WPoStProvingPeriod() abi.ChainEpoch {
	return CurrentMinerPolicy.wPoStProvingPeriod
}
func WPoStChallengeWindow() abi.ChainEpoch {
	return CurrentMinerPolicy.wPoStChallengeWindow
}
func WPoStChallengeLookback() abi.ChainEpoch {
	return CurrentMinerPolicy.wPoStChallengeLookback
}
func FaultDeclarationCutoff() abi.ChainEpoch {
	return CurrentMinerPolicy.FaultDeclarationCutoff
}
func WPoStDisputeWindow() abi.ChainEpoch {
	return CurrentMinerPolicy.wPoStDisputeWindow
}
func AddressedSectorsMax() uint64 {
	return CurrentMinerPolicy.AddressedSectorsMax
}
func FaultMaxAge() abi.ChainEpoch {
	return CurrentMinerPolicy.FaultMaxAge
}
func RewardVestingSpec() *VestSpec {
	return &CurrentMinerPolicy.RewardVestingSpec
}

func MaxPeerIDLength() uint64 {
	return CurrentMinerPolicy.MaxPeerIDLength
}
func MaxMultiaddrData() uint64 {
	return CurrentMinerPolicy.MaxMultiaddrData
}

func MinSectorExpiration() abi.ChainEpoch {
	return CurrentMinerPolicy.MinSectorExpiration
}
func MaxSectorExpirationExtension() abi.ChainEpoch {
	return CurrentMinerPolicy.MaxSectorExpirationExtension
}
func WorkerKeyChangeDelay() abi.ChainEpoch {
	return CurrentMinerPolicy.WorkerKeyChangeDelay
}
func MaxPreCommitRandomnessLookback() abi.ChainEpoch {
	return CurrentMinerPolicy.MaxPreCommitRandomnessLookback
}
func PreCommitChallengeDelay() abi.ChainEpoch {
	return CurrentMinerPolicy.PreCommitChallengeDelay
}

const MaxPartitionsPerDeadline = 3000

// The maximum number of partitions that can be loaded in a single invocation.
// This limits the number of simultaneous fault, recovery, or sector-extension declarations.
// We set this to same as MaxPartitionsPerDeadline so we can process that many partitions every deadline.
const AddressedPartitionsMax = MaxPartitionsPerDeadline

// Maximum number of unique "declarations" in batch operations.
const DeclarationsMax = AddressedPartitionsMax

// Maximum number of control addresses a miner may register.
const MaxControlAddresses = 10

// The maximum number of partitions that may be required to be loaded in a single invocation,
// when all the sector infos for the partitions will be loaded.
func loadPartitionsSectorsMax(partitionSectorCount uint64) uint64 {
	return min64(CurrentMinerPolicy.AddressedSectorsMax/partitionSectorCount, AddressedPartitionsMax)
}

// Prefix for sealed sector CIDs (CommR).
var SealedCIDPrefix = cid.Prefix{
	Version:  1,
	Codec:    cid.FilCommitmentSealed,
	MhType:   mh.POSEIDON_BLS12_381_A1_FC1,
	MhLength: 32,
}

// List of proof types which may be used when creating a new miner actor.
// This is mutable to allow configuration of testing and development networks.
var WindowPoStProofTypes = map[abi.RegisteredPoStProof]struct{}{
	abi.RegisteredPoStProof_StackedDrgWindow32GiBV1: {},
	abi.RegisteredPoStProof_StackedDrgWindow64GiBV1: {},
}

// Checks whether a PoSt proof type is supported for new miners.
func CanWindowPoStProof(s abi.RegisteredPoStProof) bool {
	_, ok := WindowPoStProofTypes[s]
	return ok
}

// List of proof types which may be used when pre-committing a new sector.
// This is mutable to allow configuration of testing and development networks.
// From network version 8, sectors sealed with the V1 seal proof types cannot be committed.
var PreCommitSealProofTypesV8 = map[abi.RegisteredSealProof]struct{}{
	abi.RegisteredSealProof_StackedDrg32GiBV1_1: {},
	abi.RegisteredSealProof_StackedDrg64GiBV1_1: {},
}

// Checks whether a seal proof type is supported for new miners and sectors.
func CanPreCommitSealProof(s abi.RegisteredSealProof) bool {
	_, ok := PreCommitSealProofTypesV8[s]
	return ok
}

// Checks whether a seal proof type is supported for new miners and sectors.
// As of network version 11, all permitted seal proof types may be extended.
func CanExtendSealProofType(_ abi.RegisteredSealProof) bool {
	return true
}

// Maximum delay to allow between sector pre-commit and subsequent proof.
// The allowable delay depends on seal proof algorithm.
func MaxProveCommitDuration() map[abi.RegisteredSealProof]abi.ChainEpoch {
	// TODO: HOW MANY PARAMETERS?
	return map[abi.RegisteredSealProof]abi.ChainEpoch{
		abi.RegisteredSealProof_StackedDrg32GiBV1:  builtin.EpochsInDay() + CurrentMinerPolicy.PreCommitChallengeDelay,
		abi.RegisteredSealProof_StackedDrg2KiBV1:   builtin.EpochsInDay() + CurrentMinerPolicy.PreCommitChallengeDelay,
		abi.RegisteredSealProof_StackedDrg8MiBV1:   builtin.EpochsInDay() + CurrentMinerPolicy.PreCommitChallengeDelay,
		abi.RegisteredSealProof_StackedDrg512MiBV1: builtin.EpochsInDay() + CurrentMinerPolicy.PreCommitChallengeDelay,
		abi.RegisteredSealProof_StackedDrg64GiBV1:  builtin.EpochsInDay() + CurrentMinerPolicy.PreCommitChallengeDelay,

		abi.RegisteredSealProof_StackedDrg32GiBV1_1:  30*builtin.EpochsInDay() + CurrentMinerPolicy.PreCommitChallengeDelay,
		abi.RegisteredSealProof_StackedDrg2KiBV1_1:   30*builtin.EpochsInDay() + CurrentMinerPolicy.PreCommitChallengeDelay,
		abi.RegisteredSealProof_StackedDrg8MiBV1_1:   30*builtin.EpochsInDay() + CurrentMinerPolicy.PreCommitChallengeDelay,
		abi.RegisteredSealProof_StackedDrg512MiBV1_1: 30*builtin.EpochsInDay() + CurrentMinerPolicy.PreCommitChallengeDelay,
		abi.RegisteredSealProof_StackedDrg64GiBV1_1:  30*builtin.EpochsInDay() + CurrentMinerPolicy.PreCommitChallengeDelay,
	}
}

// The maximum number of sector pre-commitments in a single batch.
// 32 sectors per epoch would support a single miner onboarding 1EiB of 32GiB sectors in 1 year.
const PreCommitSectorBatchMaxSize = 256

// Number of epochs after a consensus fault for which a miner is ineligible
// for permissioned actor methods and winning block elections.
func ConsensusFaultIneligibilityDuration() abi.ChainEpoch {
	return CurrentMinerPolicy.chainFinality
}

// DealWeight and VerifiedDealWeight are spacetime occupied by regular deals and verified deals in a sector.
// Sum of DealWeight and VerifiedDealWeight should be less than or equal to total SpaceTime of a sector.
// Sectors full of VerifiedDeals will have a SectorQuality of VerifiedDealWeightMultiplier/QualityBaseMultiplier.
// Sectors full of Deals will have a SectorQuality of DealWeightMultiplier/QualityBaseMultiplier.
// Sectors with neither will have a SectorQuality of QualityBaseMultiplier/QualityBaseMultiplier.
// SectorQuality of a sector is a weighted average of multipliers based on their proportions.
func QualityForWeight(size abi.SectorSize, duration abi.ChainEpoch, dealWeight, verifiedWeight abi.DealWeight) abi.SectorQuality {
	// sectorSpaceTime = size * duration
	sectorSpaceTime := big.Mul(big.NewIntUnsigned(uint64(size)), big.NewInt(int64(duration)))
	// totalDealSpaceTime = dealWeight + verifiedWeight
	totalDealSpaceTime := big.Add(dealWeight, verifiedWeight)

	// Base - all size * duration of non-deals
	// weightedBaseSpaceTime = (sectorSpaceTime - totalDealSpaceTime) * QualityBaseMultiplier
	weightedBaseSpaceTime := big.Mul(big.Sub(sectorSpaceTime, totalDealSpaceTime), builtin.QualityBaseMultiplier)
	// Deal - all deal size * deal duration * 10
	// weightedDealSpaceTime = dealWeight * DealWeightMultiplier
	weightedDealSpaceTime := big.Mul(dealWeight, builtin.DealWeightMultiplier)
	// Verified - all verified deal size * verified deal duration * 100
	// weightedVerifiedSpaceTime = verifiedWeight * VerifiedDealWeightMultiplier
	weightedVerifiedSpaceTime := big.Mul(verifiedWeight, builtin.VerifiedDealWeightMultiplier)
	// Sum - sum of all spacetime
	// weightedSumSpaceTime = weightedBaseSpaceTime + weightedDealSpaceTime + weightedVerifiedSpaceTime
	weightedSumSpaceTime := big.Sum(weightedBaseSpaceTime, weightedDealSpaceTime, weightedVerifiedSpaceTime)
	// scaledUpWeightedSumSpaceTime = weightedSumSpaceTime * 2^20
	scaledUpWeightedSumSpaceTime := big.Lsh(weightedSumSpaceTime, builtin.SectorQualityPrecision)

	// Average of weighted space time: (scaledUpWeightedSumSpaceTime / sectorSpaceTime * 10)
	return big.Div(big.Div(scaledUpWeightedSumSpaceTime, sectorSpaceTime), builtin.QualityBaseMultiplier)
}

// The power for a sector size, committed duration, and weight.
func QAPowerForWeight(size abi.SectorSize, duration abi.ChainEpoch, dealWeight, verifiedWeight abi.DealWeight) abi.StoragePower {
	quality := QualityForWeight(size, duration, dealWeight, verifiedWeight)
	return big.Rsh(big.Mul(big.NewIntUnsigned(uint64(size)), quality), builtin.SectorQualityPrecision)
}

// The quality-adjusted power for a sector.
func QAPowerForSector(size abi.SectorSize, sector *SectorOnChainInfo) abi.StoragePower {
	duration := sector.Expiration - sector.Activation
	return QAPowerForWeight(size, duration, sector.DealWeight, sector.VerifiedDealWeight)
}

// Determine maximum number of deal miner's sector can hold
func SectorDealsMax(size abi.SectorSize) uint64 {
	return max64(256, uint64(size)/CurrentMinerPolicy.DealLimitDenominator)
}

// Default share of block reward allocated as reward to the consensus fault reporter.
// Applied as epochReward / (expectedLeadersPerEpoch * consensusFaultReporterDefaultShare)
const consensusFaultReporterDefaultShare int64 = 4

// Specification for a linear vesting schedule.
type VestSpec struct {
	InitialDelay abi.ChainEpoch // Delay before any amount starts vesting.
	VestPeriod   abi.ChainEpoch // Period over which the total should vest, after the initial delay.
	StepDuration abi.ChainEpoch // Duration between successive incremental vests (independent of vesting period).
	Quantization abi.ChainEpoch // Maximum precision of vesting table (limits cardinality of table).
}

// When an actor reports a consensus fault, they earn a share of the penalty paid by the miner.
func RewardForConsensusSlashReport(epochReward abi.TokenAmount) abi.TokenAmount {
	return big.Div(epochReward,
		big.Mul(big.NewInt(builtin.ExpectedLeadersPerEpoch()),
			big.NewInt(consensusFaultReporterDefaultShare)),
	)
}

// The reward given for successfully disputing a window post.
func RewardForDisputedWindowPoSt(proofType abi.RegisteredPoStProof, disputedPower PowerPair) abi.TokenAmount {
	// This is currently just the base. In the future, the fee may scale based on the disputed power.
	return CurrentMoniesPolicy.BaseRewardForDisputedWindowPoSt
}

const MaxAggregatedSectors = 819
const MinAggregatedSectors = 4
const MaxAggregateProofSize = 81960

// The delay between pre commit expiration and clean up from state. This enforces that expired pre-commits
// stay in state for a period of time creating a grace period during which a late-running aggregated prove-commit
// can still prove its non-expired precommits without resubmitting a message
func ExpiredPreCommitCleanUpDelay() abi.ChainEpoch {
	return 8 * builtin.EpochsInHour()
}
