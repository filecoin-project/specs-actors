package abi

import (
	"fmt"
	"math"
	"strconv"

	cid "github.com/ipfs/go-cid"
	"github.com/pkg/errors"

	big "github.com/filecoin-project/specs-actors/actors/abi/big"
)

// SectorNumber is a numeric identifier for a sector. It is usually relative to a miner.
type SectorNumber uint64

func (s SectorNumber) String() string {
	return strconv.FormatUint(uint64(s), 10)
}

// The maximum assignable sector number.
// Raising this would require modifying our AMT implementation.
const MaxSectorNumber = math.MaxInt64

// SectorSize indicates one of a set of possible sizes in the network.
// Ideally, SectorSize would be an enum
// type SectorSize enum {
//   1KiB = 1024
//   1MiB = 1048576
//   1GiB = 1073741824
//   1TiB = 1099511627776
//   1PiB = 1125899906842624
//   1EiB = 1152921504606846976
//   max  = 18446744073709551615
// }
type SectorSize uint64

// Formats the size as a decimal string.
func (s SectorSize) String() string {
	return strconv.FormatUint(uint64(s), 10)
}

// Abbreviates the size as a human-scale number.
// This approximates (truncates) the size unless it is a power of 1024.
func (s SectorSize) ShortString() string {
	var biUnits = []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
	unit := 0
	for s >= 1024 && unit < len(biUnits)-1 {
		s /= 1024
		unit++
	}
	return fmt.Sprintf("%d%s", s, biUnits[unit])
}

type SectorID struct {
	Miner  ActorID
	Number SectorNumber
}

// The unit of storage power (measured in bytes)
type StoragePower = big.Int

type SectorQuality = big.Int

func NewStoragePower(n int64) StoragePower {
	return big.NewInt(n)
}

// These enumerations must match the proofs library and never change.
type RegisteredSealProof int64

const (
	RegisteredSealProof_StackedDrg2KiBV1   = RegisteredSealProof(0)
	RegisteredSealProof_StackedDrg8MiBV1   = RegisteredSealProof(1)
	RegisteredSealProof_StackedDrg512MiBV1 = RegisteredSealProof(2)
	RegisteredSealProof_StackedDrg32GiBV1  = RegisteredSealProof(3)
	RegisteredSealProof_StackedDrg64GiBV1  = RegisteredSealProof(4)
)

type RegisteredPoStProof int64

const (
	RegisteredPoStProof_StackedDrgWinning2KiBV1   = RegisteredPoStProof(0)
	RegisteredPoStProof_StackedDrgWinning8MiBV1   = RegisteredPoStProof(1)
	RegisteredPoStProof_StackedDrgWinning512MiBV1 = RegisteredPoStProof(2)
	RegisteredPoStProof_StackedDrgWinning32GiBV1  = RegisteredPoStProof(3)
	RegisteredPoStProof_StackedDrgWinning64GiBV1  = RegisteredPoStProof(4)
	RegisteredPoStProof_StackedDrgWindow2KiBV1    = RegisteredPoStProof(5)
	RegisteredPoStProof_StackedDrgWindow8MiBV1    = RegisteredPoStProof(6)
	RegisteredPoStProof_StackedDrgWindow512MiBV1  = RegisteredPoStProof(7)
	RegisteredPoStProof_StackedDrgWindow32GiBV1   = RegisteredPoStProof(8)
	RegisteredPoStProof_StackedDrgWindow64GiBV1   = RegisteredPoStProof(9)
)

// Metadata about a seal proof type.
type SealProofInfo struct {
	SectorSize                 SectorSize
	WindowPoStPartitionSectors uint64
	WinningPoStProof           RegisteredPoStProof
	WindowPoStProof            RegisteredPoStProof
	SectorMaxLifetime          ChainEpoch
}

// For all Stacked DRG sectors, the max is 5 years
const epochsPerYear = 1_262_277
const fiveYears = ChainEpoch(5 * epochsPerYear)

// Partition sizes must match those used by the proofs library.
// See https://github.com/filecoin-project/rust-fil-proofs/blob/master/filecoin-proofs/src/constants.rs#L85
var SealProofInfos = map[RegisteredSealProof]*SealProofInfo{
	RegisteredSealProof_StackedDrg2KiBV1: {
		SectorSize:                 2 << 10,
		WindowPoStPartitionSectors: 2,
		WinningPoStProof:           RegisteredPoStProof_StackedDrgWinning2KiBV1,
		WindowPoStProof:            RegisteredPoStProof_StackedDrgWindow2KiBV1,
		SectorMaxLifetime:          fiveYears,
	},
	RegisteredSealProof_StackedDrg8MiBV1: {
		SectorSize:                 8 << 20,
		WindowPoStPartitionSectors: 2,
		WinningPoStProof:           RegisteredPoStProof_StackedDrgWinning8MiBV1,
		WindowPoStProof:            RegisteredPoStProof_StackedDrgWindow8MiBV1,
		SectorMaxLifetime:          fiveYears,
	},
	RegisteredSealProof_StackedDrg512MiBV1: {
		SectorSize:                 512 << 20,
		WindowPoStPartitionSectors: 2,
		WinningPoStProof:           RegisteredPoStProof_StackedDrgWinning512MiBV1,
		WindowPoStProof:            RegisteredPoStProof_StackedDrgWindow512MiBV1,
		SectorMaxLifetime:          fiveYears,
	},
	RegisteredSealProof_StackedDrg32GiBV1: {
		SectorSize:                 32 << 30,
		WindowPoStPartitionSectors: 2349,
		WinningPoStProof:           RegisteredPoStProof_StackedDrgWinning32GiBV1,
		WindowPoStProof:            RegisteredPoStProof_StackedDrgWindow32GiBV1,
		SectorMaxLifetime:          fiveYears,
	},
	RegisteredSealProof_StackedDrg64GiBV1: {
		SectorSize:                 64 << 30,
		WindowPoStPartitionSectors: 2300,
		WinningPoStProof:           RegisteredPoStProof_StackedDrgWinning64GiBV1,
		WindowPoStProof:            RegisteredPoStProof_StackedDrgWindow64GiBV1,
		SectorMaxLifetime:          fiveYears,
	},
}

func (p RegisteredSealProof) SectorSize() (SectorSize, error) {
	info, ok := SealProofInfos[p]
	if !ok {
		return 0, errors.Errorf("unsupported proof type: %v", p)
	}
	return info.SectorSize, nil
}

// Returns the partition size, in sectors, associated with a proof type.
// The partition size is the number of sectors proved in a single PoSt proof.
func (p RegisteredSealProof) WindowPoStPartitionSectors() (uint64, error) {
	info, ok := SealProofInfos[p]
	if !ok {
		return 0, errors.Errorf("unsupported proof type: %v", p)
	}
	return info.WindowPoStPartitionSectors, nil
}

// RegisteredWinningPoStProof produces the PoSt-specific RegisteredProof corresponding
// to the receiving RegisteredProof.
func (p RegisteredSealProof) RegisteredWinningPoStProof() (RegisteredPoStProof, error) {
	info, ok := SealProofInfos[p]
	if !ok {
		return 0, errors.Errorf("unsupported proof type: %v", p)
	}
	return info.WinningPoStProof, nil
}

// RegisteredWindowPoStProof produces the PoSt-specific RegisteredProof corresponding
// to the receiving RegisteredProof.
func (p RegisteredSealProof) RegisteredWindowPoStProof() (RegisteredPoStProof, error) {
	info, ok := SealProofInfos[p]
	if !ok {
		return 0, errors.Errorf("unsupported proof type: %v", p)
	}
	return info.WindowPoStProof, nil
}

// The maximum duration a sector sealed with this proof may exist between activation and expiration.
// This ensures that SDR is secure in the cost model for Window PoSt and in the time model for Winning PoSt
// This is based on estimation of hardware latency improvement and hardware and software cost reduction.
const SectorMaximumLifetimeSDR = ChainEpoch(1_262_277 * 5)

// SectorMaximumLifetime is the maximum duration a sector sealed with this proof may exist between activation and expiration
func (p RegisteredSealProof) SectorMaximumLifetime() (ChainEpoch, error) {
	info, ok := SealProofInfos[p]
	if !ok {
		return 0, errors.Errorf("unsupported proof type: %v", p)
	}
	return info.SectorMaxLifetime, nil
}

var PoStSealProofTypes = map[RegisteredPoStProof]RegisteredSealProof{
	RegisteredPoStProof_StackedDrgWinning2KiBV1:   RegisteredSealProof_StackedDrg2KiBV1,
	RegisteredPoStProof_StackedDrgWindow2KiBV1:    RegisteredSealProof_StackedDrg2KiBV1,
	RegisteredPoStProof_StackedDrgWinning8MiBV1:   RegisteredSealProof_StackedDrg8MiBV1,
	RegisteredPoStProof_StackedDrgWindow8MiBV1:    RegisteredSealProof_StackedDrg8MiBV1,
	RegisteredPoStProof_StackedDrgWinning512MiBV1: RegisteredSealProof_StackedDrg512MiBV1,
	RegisteredPoStProof_StackedDrgWindow512MiBV1:  RegisteredSealProof_StackedDrg512MiBV1,
	RegisteredPoStProof_StackedDrgWinning32GiBV1:  RegisteredSealProof_StackedDrg32GiBV1,
	RegisteredPoStProof_StackedDrgWindow32GiBV1:   RegisteredSealProof_StackedDrg32GiBV1,
	RegisteredPoStProof_StackedDrgWinning64GiBV1:  RegisteredSealProof_StackedDrg64GiBV1,
	RegisteredPoStProof_StackedDrgWindow64GiBV1:   RegisteredSealProof_StackedDrg64GiBV1,
}

// Maps PoSt proof types back to seal proof types.
func (p RegisteredPoStProof) RegisteredSealProof() (RegisteredSealProof, error) {
	sp, ok := PoStSealProofTypes[p]
	if !ok {
		return 0, errors.Errorf("unsupported PoSt proof type: %v", p)
	}
	return sp, nil
}

func (p RegisteredPoStProof) SectorSize() (SectorSize, error) {
	sp, err := p.RegisteredSealProof()
	if err != nil {
		return 0, err
	}
	return sp.SectorSize()
}

// Returns the partition size, in sectors, associated with a proof type.
// The partition size is the number of sectors proved in a single PoSt proof.
func (p RegisteredPoStProof) WindowPoStPartitionSectors() (uint64, error) {
	sp, err := p.RegisteredSealProof()
	if err != nil {
		return 0, err
	}
	return sp.WindowPoStPartitionSectors()
}



///
/// Sealing
///

type SealRandomness Randomness
type InteractiveSealRandomness Randomness

// Information needed to verify a seal proof.
type SealVerifyInfo struct {
	SealProof RegisteredSealProof
	SectorID
	DealIDs               []DealID
	Randomness            SealRandomness
	InteractiveRandomness InteractiveSealRandomness
	Proof                 []byte

	// Safe because we get those from the miner actor
	SealedCID   cid.Cid `checked:"true"` // CommR
	UnsealedCID cid.Cid `checked:"true"` // CommD
}

///
/// PoSting
///

type PoStRandomness Randomness

// Information about a sector necessary for PoSt verification.
type SectorInfo struct {
	SealProof    RegisteredSealProof // RegisteredProof used when sealing - needs to be mapped to PoSt registered proof when used to verify a PoSt
	SectorNumber SectorNumber
	SealedCID    cid.Cid // CommR
}

type PoStProof struct {
	PoStProof  RegisteredPoStProof
	ProofBytes []byte
}

// Information needed to verify a Winning PoSt attached to a block header.
// Note: this is not used within the state machine, but by the consensus/election mechanisms.
type WinningPoStVerifyInfo struct {
	Randomness        PoStRandomness
	Proofs            []PoStProof
	ChallengedSectors []SectorInfo
	Prover            ActorID // used to derive 32-byte prover ID
}

// Information needed to verify a Window PoSt submitted directly to a miner actor.
type WindowPoStVerifyInfo struct {
	Randomness        PoStRandomness
	Proofs            []PoStProof
	ChallengedSectors []SectorInfo
	Prover            ActorID // used to derive 32-byte prover ID
}
