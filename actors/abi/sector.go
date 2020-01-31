package abi

import (
	cid "github.com/ipfs/go-cid"

	"github.com/filecoin-project/specs-actors/actors/abi/big"
)

// SectorNumber is a numeric identifier for a sector. It is usually relative to a miner.
type SectorNumber int64

// SectorSize indicates one of a set of possible sizes in the network.
// Ideally, SectorSize would be an enum
// type SectorSize enum {
//   1KiB = 1024
//   1MiB = 1048576
//   1GiB = 1073741824
//   1TiB = 1099511627776
//   1PiB = 1125899906842624
// }
type SectorSize int64

type SectorID struct {
	Miner  ActorID
	Number SectorNumber
}

// The unit of sector weight (power-epochs)
type SectorWeight big.Int

// The unit of storage power (measured in bytes)
type StoragePower = big.Int

func NewStoragePower(n int64) StoragePower {
	return StoragePower(big.NewInt(n))
}

type UnsealedSectorCID cid.Cid // CommD
type SealedSectorCID cid.Cid   // CommR

// This ordering, defines mappings to UInt in a way which MUST never change.
type RegisteredProof int64

const (
	RegisteredProof_WinStackedDRG32GiBSeal = RegisteredProof(1)
	RegisteredProof_WinStackedDRG32GiBPoSt = RegisteredProof(2)
	RegisteredProof_StackedDRG32GiBSeal    = RegisteredProof(3)
	RegisteredProof_StackedDRG32GiBPoSt    = RegisteredProof(4)
)

///
/// Sealing
///

type SealRandomness []byte
type InteractiveSealRandomness []byte

// SealVerifyInfo is the structure of all the information a verifier
// needs to verify a Seal.
type SealVerifyInfo struct {
	SectorID
	OnChain               OnChainSealVerifyInfo
	Randomness            SealRandomness
	InteractiveRandomness InteractiveSealRandomness
	UnsealedCID           UnsealedSectorCID // CommD
}

// OnChainSealVerifyInfo is the structure of information that must be sent with
// a message to commit a sector. Most of this information is not needed in the
// state tree but will be verified in sm.CommitSector. See SealCommitment for
// data stored on the state tree for each sector.
type OnChainSealVerifyInfo struct {
	SealedCID        SealedSectorCID // CommR
	InteractiveEpoch ChainEpoch      // Used to derive the interactive PoRep challenge.
	RegisteredProof
	Proof   SealProof
	DealIDs []DealID
	SectorNumber
	SealEpoch ChainEpoch // Used to tie the seal to a chain.
}

type SealProof struct { //<curve, system> {
	ProofBytes []byte
}

///
/// PoSting
///

type ChallengeTicketsCommitment []byte
type PoStRandomness []byte
type PartialTicket []byte // 32 bytes

// TODO Porcu: refactor these types to get rid of the squishy optional fields.
type PoStVerifyInfo struct {
	Randomness      PoStRandomness
	CommR           SealedSectorCID
	Candidates      []PoStCandidate              // From OnChain*PoStVerifyInfo
	Proofs          []PoStProof                  // From OnChain*PoStVerifyInfo
	EligibleSectors map[SectorID]SealedSectorCID // TODO: HAMT?
}

type OnChainElectionPoStVerifyInfo struct {
	// There should be one RegisteredProof for each PoSt Candidate
	RegisteredProofs []RegisteredProof
	Candidates       []PoStCandidate
	Proofs           []PoStProof
	Randomness       PoStRandomness
}

type OnChainSurprisePoStVerifyInfo struct {
	RegisteredProof
	Candidates []PoStCandidate
	Proofs     []PoStProof
	// CommT      ChallengeTicketsCommitment // Potential future optimization.
}

type PoStCandidate struct {
	RegisteredProof
	PartialTicket  PartialTicket             // Optional —  will eventually be omitted for SurprisePoSt verification, needed for now.
	PrivateProof   PrivatePoStCandidateProof // Optional — should be ommitted for verification.
	SectorID       SectorID
	ChallengeIndex int64
}

type PoStProof struct { //<curve, system> {
	ProofBytes []byte
}

type PrivatePoStCandidateProof struct {
	RegisteredProof
	Externalized []byte
}
