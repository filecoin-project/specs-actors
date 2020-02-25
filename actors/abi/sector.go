package abi

import (
	cid "github.com/ipfs/go-cid"
	"github.com/pkg/errors"

	big "github.com/filecoin-project/specs-actors/actors/abi/big"
)

// SectorNumber is a numeric identifier for a sector. It is usually relative to a miner.
type SectorNumber uint64

// SectorSize indicates one of a set of possible sizes in the network.
// Ideally, SectorSize would be an enum
// type SectorSize enum {
//   1KiB = 1024
//   1MiB = 1048576
//   1GiB = 1073741824
//   1TiB = 1099511627776
//   1PiB = 1125899906842624
// }
type SectorSize uint64

type SectorID struct {
	Miner  ActorID
	Number SectorNumber
}

// The unit of sector weight (power-epochs)
type SectorWeight = big.Int

// The unit of storage power (measured in bytes)
type StoragePower = big.Int

func NewStoragePower(n int64) StoragePower {
	return StoragePower(big.NewInt(n))
}

// This ordering, defines mappings to UInt in a way which MUST never change.
type RegisteredProof int64

const (
	RegisteredProof_WinStackedDRG32GiBSeal = RegisteredProof(1)
	RegisteredProof_WinStackedDRG32GiBPoSt = RegisteredProof(2)
	RegisteredProof_StackedDRG32GiBSeal    = RegisteredProof(3)
	RegisteredProof_StackedDRG32GiBPoSt    = RegisteredProof(4)
	RegisteredProof_StackedDRG1KiBSeal     = RegisteredProof(5)
	RegisteredProof_StackedDRG1KiBPoSt     = RegisteredProof(6)
	RegisteredProof_StackedDRG16MiBSeal    = RegisteredProof(7)
	RegisteredProof_StackedDRG16MiBPoSt    = RegisteredProof(8)
	RegisteredProof_StackedDRG256MiBSeal   = RegisteredProof(9)
	RegisteredProof_StackedDRG256MiBPoSt   = RegisteredProof(10)
	RegisteredProof_StackedDRG1GiBSeal     = RegisteredProof(11)
	RegisteredProof_StackedDRG1GiBPoSt     = RegisteredProof(12)
)

// RegisteredPoStProof produces the PoSt-specific RegisteredProof corresponding
// to the receiving RegisteredProof.
func (p RegisteredProof) RegisteredPoStProof() (RegisteredProof, error) {
	switch p {
	case RegisteredProof_WinStackedDRG32GiBSeal:
		return RegisteredProof_WinStackedDRG32GiBPoSt, nil
	case RegisteredProof_WinStackedDRG32GiBPoSt:
		return RegisteredProof_WinStackedDRG32GiBPoSt, nil
	case RegisteredProof_StackedDRG32GiBSeal:
		return RegisteredProof_StackedDRG32GiBPoSt, nil
	case RegisteredProof_StackedDRG32GiBPoSt:
		return RegisteredProof_StackedDRG32GiBPoSt, nil
	case RegisteredProof_StackedDRG1KiBSeal:
		return RegisteredProof_StackedDRG1KiBPoSt, nil
	case RegisteredProof_StackedDRG1KiBPoSt:
		return RegisteredProof_StackedDRG1KiBPoSt, nil
	case RegisteredProof_StackedDRG16MiBSeal:
		return RegisteredProof_StackedDRG16MiBPoSt, nil
	case RegisteredProof_StackedDRG16MiBPoSt:
		return RegisteredProof_StackedDRG16MiBPoSt, nil
	case RegisteredProof_StackedDRG256MiBSeal:
		return RegisteredProof_StackedDRG256MiBPoSt, nil
	case RegisteredProof_StackedDRG256MiBPoSt:
		return RegisteredProof_StackedDRG256MiBPoSt, nil
	case RegisteredProof_StackedDRG1GiBSeal:
		return RegisteredProof_StackedDRG1GiBPoSt, nil
	case RegisteredProof_StackedDRG1GiBPoSt:
		return RegisteredProof_StackedDRG1GiBPoSt, nil
	default:
		return 0, errors.Errorf("unsupported mapping from %+v to PoSt-specific RegisteredProof", p)
	}
}

// RegisteredSealProof produces the seal-specific RegisteredProof corresponding
// to the receiving RegisteredProof.
func (p RegisteredProof) RegisteredSealProof() (RegisteredProof, error) {
	switch p {
	case RegisteredProof_WinStackedDRG32GiBSeal:
		return RegisteredProof_WinStackedDRG32GiBSeal, nil
	case RegisteredProof_WinStackedDRG32GiBPoSt:
		return RegisteredProof_WinStackedDRG32GiBSeal, nil
	case RegisteredProof_StackedDRG32GiBSeal:
		return RegisteredProof_StackedDRG32GiBSeal, nil
	case RegisteredProof_StackedDRG32GiBPoSt:
		return RegisteredProof_StackedDRG32GiBSeal, nil
	case RegisteredProof_StackedDRG1KiBSeal:
		return RegisteredProof_StackedDRG1KiBSeal, nil
	case RegisteredProof_StackedDRG1KiBPoSt:
		return RegisteredProof_StackedDRG1KiBSeal, nil
	case RegisteredProof_StackedDRG16MiBSeal:
		return RegisteredProof_StackedDRG16MiBSeal, nil
	case RegisteredProof_StackedDRG16MiBPoSt:
		return RegisteredProof_StackedDRG16MiBSeal, nil
	case RegisteredProof_StackedDRG256MiBSeal:
		return RegisteredProof_StackedDRG256MiBSeal, nil
	case RegisteredProof_StackedDRG256MiBPoSt:
		return RegisteredProof_StackedDRG256MiBSeal, nil
	case RegisteredProof_StackedDRG1GiBSeal:
		return RegisteredProof_StackedDRG1GiBSeal, nil
	case RegisteredProof_StackedDRG1GiBPoSt:
		return RegisteredProof_StackedDRG1GiBSeal, nil
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
	Proof   SealProof
	DealIDs []DealID
	SectorNumber
	SealRandEpoch ChainEpoch // Used to tie the seal to a chain.
}

type SealProof struct { //<curve, system> {
	ProofBytes []byte
}

///
/// PoSting
///

type ChallengeTicketsCommitment []byte
type PoStRandomness Randomness
type PartialTicket []byte // 32 bytes

// TODO Porcu: refactor these types to get rid of the squishy optional fields.
type PoStVerifyInfo struct {
	Randomness      PoStRandomness
	Candidates      []PoStCandidate // From OnChain*PoStVerifyInfo
	Proofs          []PoStProof
	EligibleSectors []SectorInfo
	Prover          ActorID // used to derive 32-byte prover ID
}

type SectorInfo struct {
	SectorNumber SectorNumber
	SealedCID    cid.Cid // CommR
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
