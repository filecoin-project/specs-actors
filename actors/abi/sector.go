package abi

import (
	"fmt"
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

func NewStoragePower(n int64) StoragePower {
	return big.NewInt(n)
}

// This ordering, defines mappings to UInt in a way which MUST never change.
type RegisteredProof int64

const (
	RegisteredProof_StackedDRG32GiBSeal  = RegisteredProof(1)
	RegisteredProof_StackedDRG32GiBPoSt  = RegisteredProof(2)
	RegisteredProof_StackedDRG2KiBSeal   = RegisteredProof(3)
	RegisteredProof_StackedDRG2KiBPoSt   = RegisteredProof(4)
	RegisteredProof_StackedDRG8MiBSeal   = RegisteredProof(5)
	RegisteredProof_StackedDRG8MiBPoSt   = RegisteredProof(6)
	RegisteredProof_StackedDRG512MiBSeal = RegisteredProof(7)
	RegisteredProof_StackedDRG512MiBPoSt = RegisteredProof(8)
)

func (p RegisteredProof) SectorSize() (SectorSize, error) {
	switch p {
	case RegisteredProof_StackedDRG32GiBSeal, RegisteredProof_StackedDRG32GiBPoSt:
		return 32 << 30, nil
	case RegisteredProof_StackedDRG2KiBSeal, RegisteredProof_StackedDRG2KiBPoSt:
		return 2 << 10, nil
	case RegisteredProof_StackedDRG8MiBSeal, RegisteredProof_StackedDRG8MiBPoSt:
		return 8 << 20, nil
	case RegisteredProof_StackedDRG512MiBSeal, RegisteredProof_StackedDRG512MiBPoSt:
		return 512 << 20, nil
	default:
		return 0, errors.Errorf("unsupported proof type: %v", p)
	}

}

// RegisteredPoStProof produces the PoSt-specific RegisteredProof corresponding
// to the receiving RegisteredProof.
func (p RegisteredProof) RegisteredPoStProof() (RegisteredProof, error) {
	switch p {
	case RegisteredProof_StackedDRG32GiBSeal:
		return RegisteredProof_StackedDRG32GiBPoSt, nil
	case RegisteredProof_StackedDRG32GiBPoSt:
		return RegisteredProof_StackedDRG32GiBPoSt, nil
	case RegisteredProof_StackedDRG2KiBSeal:
		return RegisteredProof_StackedDRG2KiBPoSt, nil
	case RegisteredProof_StackedDRG2KiBPoSt:
		return RegisteredProof_StackedDRG2KiBPoSt, nil
	case RegisteredProof_StackedDRG8MiBSeal:
		return RegisteredProof_StackedDRG8MiBPoSt, nil
	case RegisteredProof_StackedDRG8MiBPoSt:
		return RegisteredProof_StackedDRG8MiBPoSt, nil
	case RegisteredProof_StackedDRG512MiBSeal:
		return RegisteredProof_StackedDRG512MiBPoSt, nil
	case RegisteredProof_StackedDRG512MiBPoSt:
		return RegisteredProof_StackedDRG512MiBPoSt, nil
	default:
		return 0, errors.Errorf("unsupported mapping from %+v to PoSt-specific RegisteredProof", p)
	}
}

// RegisteredSealProof produces the seal-specific RegisteredProof corresponding
// to the receiving RegisteredProof.
func (p RegisteredProof) RegisteredSealProof() (RegisteredProof, error) {
	switch p {
	case RegisteredProof_StackedDRG32GiBSeal:
		return RegisteredProof_StackedDRG32GiBSeal, nil
	case RegisteredProof_StackedDRG32GiBPoSt:
		return RegisteredProof_StackedDRG32GiBSeal, nil
	case RegisteredProof_StackedDRG2KiBSeal:
		return RegisteredProof_StackedDRG2KiBSeal, nil
	case RegisteredProof_StackedDRG2KiBPoSt:
		return RegisteredProof_StackedDRG2KiBSeal, nil
	case RegisteredProof_StackedDRG8MiBSeal:
		return RegisteredProof_StackedDRG8MiBSeal, nil
	case RegisteredProof_StackedDRG8MiBPoSt:
		return RegisteredProof_StackedDRG8MiBSeal, nil
	case RegisteredProof_StackedDRG512MiBSeal:
		return RegisteredProof_StackedDRG512MiBSeal, nil
	case RegisteredProof_StackedDRG512MiBPoSt:
		return RegisteredProof_StackedDRG512MiBSeal, nil
	default:
		return 0, errors.Errorf("unsupported mapping from %+v to seal-specific RegisteredProof", p)
	}
}

///
/// Sealing
///

type SealRandomness Randomness
type InteractiveSealRandomness Randomness

// SealVerifyInfo is the structure of all the information a verifier
// needs to verify a Seal.
type SealVerifyInfo struct {
	SectorID
	OnChain               OnChainSealVerifyInfo
	Randomness            SealRandomness
	InteractiveRandomness InteractiveSealRandomness
	UnsealedCID           cid.Cid // CommD
}

// OnChainSealVerifyInfo is the structure of information that must be sent with
// a message to commit a sector. Most of this information is not needed in the
// state tree but will be verified in sm.CommitSector. See SealCommitment for
// data stored on the state tree for each sector.
type OnChainSealVerifyInfo struct {
	SealedCID        cid.Cid    // CommR
	InteractiveEpoch ChainEpoch // Used to derive the interactive PoRep challenge.
	RegisteredProof
	Proof   []byte
	DealIDs []DealID
	SectorNumber
	SealRandEpoch ChainEpoch // Used to tie the seal to a chain.
}

///
/// PoSting
///

type ChallengeTicketsCommitment []byte
type PoStRandomness Randomness
type PartialTicket []byte // 32 bytes

type PoStVerifyInfo struct {
	Randomness      PoStRandomness
	Candidates      []PoStCandidate // From OnChain*PoStVerifyInfo
	Proofs          []PoStProof
	EligibleSectors []SectorInfo
	Prover          ActorID // used to derive 32-byte prover ID
	ChallengeCount  uint64
}

type SectorInfo struct {
	RegisteredProof // RegisteredProof used when sealing - needs to be mapped to PoSt registered proof when used to verify a PoSt
	SectorNumber    SectorNumber
	SealedCID       cid.Cid // CommR
}

type OnChainElectionPoStVerifyInfo struct {
	Candidates []PoStCandidate // each PoStCandidate has its own RegisteredProof
	Proofs     []PoStProof     // each PoStProof has its own RegisteredProof
	Randomness PoStRandomness
}

type OnChainPoStVerifyInfo struct {
	Candidates []PoStCandidate // each PoStCandidate has its own RegisteredProof
	Proofs     []PoStProof     // each PoStProof has its own RegisteredProof
}

type PoStCandidate struct {
	RegisteredProof
	PartialTicket  PartialTicket             // Optional —  will eventually be omitted for SurprisePoSt verification, needed for now.
	PrivateProof   PrivatePoStCandidateProof // Optional — should be ommitted for verification.
	SectorID       SectorID
	ChallengeIndex int64
}

type PoStProof struct { //<curve, system> {
	RegisteredProof
	ProofBytes []byte
}

type PrivatePoStCandidateProof struct {
	RegisteredProof
	Externalized []byte
}
