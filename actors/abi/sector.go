package abi

import (
	cid "github.com/ipfs/go-cid"
	"github.com/pkg/errors"
	stabi "github.com/filecoin-project/go-state-types/abi"
)


// Metadata about a seal proof type.
type SealProofPolicy struct {
	WindowPoStPartitionSectors uint64
	SectorMaxLifetime          stabi.ChainEpoch
	ConsensusMinerMinPower     stabi.StoragePower
}

// For all Stacked DRG sectors, the max is 5 years
const epochsPerYear = 1_051_200
const fiveYears = stabi.ChainEpoch(5 * epochsPerYear)

// Partition sizes must match those used by the proofs library.
// See https://github.com/filecoin-project/rust-fil-proofs/blob/master/filecoin-proofs/src/constants.rs#L85
var SealProofPolicies = map[stabi.RegisteredSealProof]*SealProofPolicy{
	stabi.RegisteredSealProof_StackedDrg2KiBV1: {
		WindowPoStPartitionSectors: 2,
		SectorMaxLifetime:          fiveYears,
		ConsensusMinerMinPower:     stabi.NewStoragePower(0),
	},
	stabi.RegisteredSealProof_StackedDrg8MiBV1: {
		WindowPoStPartitionSectors: 2,
		SectorMaxLifetime:          fiveYears,
		ConsensusMinerMinPower:     stabi.NewStoragePower(16 << 20),
	},
	stabi.RegisteredSealProof_StackedDrg512MiBV1: {
		WindowPoStPartitionSectors: 2,
		SectorMaxLifetime:          fiveYears,
		ConsensusMinerMinPower:     stabi.NewStoragePower(1 << 30),
	},
	stabi.RegisteredSealProof_StackedDrg32GiBV1: {

		WindowPoStPartitionSectors: 2349,
		SectorMaxLifetime:          fiveYears,
		ConsensusMinerMinPower:     stabi.NewStoragePower(100 << 40),
	},
	stabi.RegisteredSealProof_StackedDrg64GiBV1: {
		WindowPoStPartitionSectors: 2300,
		SectorMaxLifetime:          fiveYears,
		ConsensusMinerMinPower:     stabi.NewStoragePower(200 << 40),
	},
}

// Returns the partition size, in sectors, associated with a proof type.
// The partition size is the number of sectors proved in a single PoSt proof.
func SealProofWindowPoStPartitionSectors(p stabi.RegisteredSealProof) (uint64, error) {
	info, ok := SealProofPolicies[p]
	if !ok {
		return 0, errors.Errorf("unsupported proof type: %v", p)
	}
	return info.WindowPoStPartitionSectors, nil
}

// SectorMaximumLifetime is the maximum duration a sector sealed with this proof may exist between activation and expiration
func SealProofSectorMaximumLifetime(p stabi.RegisteredSealProof) (stabi.ChainEpoch, error) {
	info, ok := SealProofPolicies[p]
	if !ok {
		return 0, errors.Errorf("unsupported proof type: %v", p)
	}
	return info.SectorMaxLifetime, nil
}

// The minimum power of an individual miner to meet the threshold for leader election (in bytes).
// Motivation:
// - Limits sybil generation
// - Improves consensus fault detection
// - Guarantees a minimum fee for consensus faults
// - Ensures that a specific soundness for the power table
// Note: We may be able to reduce this in the future, addressing consensus faults with more complicated penalties,
// sybil generation with crypto-economic mechanism, and PoSt soundness by increasing the challenges for small miners.
func ConsensusMinerMinPower(p stabi.RegisteredSealProof) (stabi.StoragePower, error) {
	info, ok := SealProofPolicies[p]
	if !ok {
		return stabi.NewStoragePower(0), errors.Errorf("unsupported proof type: %v", p)
	}
	return info.ConsensusMinerMinPower, nil
}


var PoStSealProofTypes = map[stabi.RegisteredPoStProof]stabi.RegisteredSealProof{
	stabi.RegisteredPoStProof_StackedDrgWinning2KiBV1:   stabi.RegisteredSealProof_StackedDrg2KiBV1,
	stabi.RegisteredPoStProof_StackedDrgWindow2KiBV1:    stabi.RegisteredSealProof_StackedDrg2KiBV1,
	stabi.RegisteredPoStProof_StackedDrgWinning8MiBV1:   stabi.RegisteredSealProof_StackedDrg8MiBV1,
	stabi.RegisteredPoStProof_StackedDrgWindow8MiBV1:    stabi.RegisteredSealProof_StackedDrg8MiBV1,
	stabi.RegisteredPoStProof_StackedDrgWinning512MiBV1: stabi.RegisteredSealProof_StackedDrg512MiBV1,
	stabi.RegisteredPoStProof_StackedDrgWindow512MiBV1:  stabi.RegisteredSealProof_StackedDrg512MiBV1,
	stabi.RegisteredPoStProof_StackedDrgWinning32GiBV1:  stabi.RegisteredSealProof_StackedDrg32GiBV1,
	stabi.RegisteredPoStProof_StackedDrgWindow32GiBV1:   stabi.RegisteredSealProof_StackedDrg32GiBV1,
	stabi.RegisteredPoStProof_StackedDrgWinning64GiBV1:  stabi.RegisteredSealProof_StackedDrg64GiBV1,
	stabi.RegisteredPoStProof_StackedDrgWindow64GiBV1:   stabi.RegisteredSealProof_StackedDrg64GiBV1,
}

// Maps PoSt proof types back to seal proof types.
func RegisteredSealProof(p stabi.RegisteredPoStProof) (stabi.RegisteredSealProof, error) {
	sp, ok := PoStSealProofTypes[p]
	if !ok {
		return 0, errors.Errorf("unsupported PoSt proof type: %v", p)
	}
	return sp, nil
}

func PoStProofSectorSize(p stabi.RegisteredPoStProof) (stabi.SectorSize, error) {
	sp, err := RegisteredSealProof(p)
	if err != nil {
		return 0, err
	}
	return sp.SectorSize()
}

// Returns the partition size, in sectors, associated with a proof type.
// The partition size is the number of sectors proved in a single PoSt proof.
func PoStProofWindowPoStPartitionSectors(p stabi.RegisteredPoStProof) (uint64, error) {
	sp, err := RegisteredSealProof(p)
	if err != nil {
		return 0, err
	}
	return SealProofWindowPoStPartitionSectors(sp)
}

///
/// Sealing
///

// Information needed to verify a seal proof.
type SealVerifyInfo struct {
	SealProof stabi.RegisteredSealProof
	stabi.SectorID
	DealIDs               []stabi.DealID
	Randomness            stabi.SealRandomness
	InteractiveRandomness stabi.InteractiveSealRandomness
	Proof                 []byte

	// Safe because we get those from the miner actor
	SealedCID   cid.Cid `checked:"true"` // CommR
	UnsealedCID cid.Cid `checked:"true"` // CommD
}

///
/// PoSting
///

// Information about a sector necessary for PoSt verification.
type SectorInfo struct {
	SealProof    stabi.RegisteredSealProof // RegisteredProof used when sealing - needs to be mapped to PoSt registered proof when used to verify a PoSt
	SectorNumber stabi.SectorNumber
	SealedCID    cid.Cid // CommR
}

type PoStProof struct {
	PoStProof  stabi.RegisteredPoStProof
	ProofBytes []byte
}

// Information needed to verify a Winning PoSt attached to a block header.
// Note: this is not used within the state machine, but by the consensus/election mechanisms.
type WinningPoStVerifyInfo struct {
	Randomness        stabi.PoStRandomness
	Proofs            []PoStProof
	ChallengedSectors []SectorInfo
	Prover            stabi.ActorID // used to derive 32-byte prover ID
}

// Information needed to verify a Window PoSt submitted directly to a miner actor.
type WindowPoStVerifyInfo struct {
	Randomness        stabi.PoStRandomness
	Proofs            []PoStProof
	ChallengedSectors []SectorInfo
	Prover            stabi.ActorID // used to derive 32-byte prover ID
}
