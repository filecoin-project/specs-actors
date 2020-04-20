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

type SectorQuality = big.Int

// The unit of spacetime committed to the network
type Spacetime = big.Int

func NewStoragePower(n int64) StoragePower {
	return big.NewInt(n)
}

// This ordering, defines mappings to UInt in a way which MUST never change.
type RegisteredProof int64

const (
	RegisteredProof_StackedDRG32GiBSeal  = RegisteredProof(1)
	RegisteredProof_StackedDRG32GiBPoSt  = RegisteredProof(2) // No longer used
	RegisteredProof_StackedDRG2KiBSeal   = RegisteredProof(3)
	RegisteredProof_StackedDRG2KiBPoSt   = RegisteredProof(4) // No longer used
	RegisteredProof_StackedDRG8MiBSeal   = RegisteredProof(5)
	RegisteredProof_StackedDRG8MiBPoSt   = RegisteredProof(6) // No longer used
	RegisteredProof_StackedDRG512MiBSeal = RegisteredProof(7)
	RegisteredProof_StackedDRG512MiBPoSt = RegisteredProof(8) // No longer used

	RegisteredProof_StackedDRG2KiBWinningPoSt = RegisteredProof(9)
	RegisteredProof_StackedDRG2KiBWindowPoSt  = RegisteredProof(10)

	RegisteredProof_StackedDRG8MiBWinningPoSt = RegisteredProof(11)
	RegisteredProof_StackedDRG8MiBWindowPoSt  = RegisteredProof(12)

	RegisteredProof_StackedDRG512MiBWinningPoSt = RegisteredProof(13)
	RegisteredProof_StackedDRG512MiBWindowPoSt  = RegisteredProof(14)

	RegisteredProof_StackedDRG32GiBWinningPoSt = RegisteredProof(15)
	RegisteredProof_StackedDRG32GiBWindowPoSt  = RegisteredProof(16)
)

func (p RegisteredProof) SectorSize() (SectorSize, error) {
	// Resolve to seal proof and then compute size from that.
	sp, err := p.RegisteredSealProof()
	if err != nil {
		return 0, err
	}
	switch sp {
	case RegisteredProof_StackedDRG32GiBSeal:
		return 32 << 30, nil
	case RegisteredProof_StackedDRG2KiBSeal:
		return 2 << 10, nil
	case RegisteredProof_StackedDRG8MiBSeal:
		return 8 << 20, nil
	case RegisteredProof_StackedDRG512MiBSeal:
		return 512 << 20, nil
	default:
		return 0, errors.Errorf("unsupported proof type: %v", p)
	}
}

// RegisteredWinningPoStProof produces the PoSt-specific RegisteredProof corresponding
// to the receiving RegisteredProof.
func (p RegisteredProof) RegisteredWinningPoStProof() (RegisteredProof, error) {
	switch p {
	case RegisteredProof_StackedDRG32GiBSeal, RegisteredProof_StackedDRG32GiBWinningPoSt:
		return RegisteredProof_StackedDRG32GiBWinningPoSt, nil
	case RegisteredProof_StackedDRG2KiBSeal, RegisteredProof_StackedDRG2KiBWinningPoSt:
		return RegisteredProof_StackedDRG2KiBWinningPoSt, nil
	case RegisteredProof_StackedDRG8MiBSeal, RegisteredProof_StackedDRG8MiBWinningPoSt:
		return RegisteredProof_StackedDRG8MiBWinningPoSt, nil
	case RegisteredProof_StackedDRG512MiBSeal, RegisteredProof_StackedDRG512MiBWinningPoSt:
		return RegisteredProof_StackedDRG512MiBWinningPoSt, nil
	default:
		return 0, errors.Errorf("unsupported mapping from %+v to PoSt-specific RegisteredProof", p)
	}
}

// RegisteredWindowPoStProof produces the PoSt-specific RegisteredProof corresponding
// to the receiving RegisteredProof.
func (p RegisteredProof) RegisteredWindowPoStProof() (RegisteredProof, error) {
	switch p {
	case RegisteredProof_StackedDRG32GiBSeal, RegisteredProof_StackedDRG32GiBWindowPoSt:
		return RegisteredProof_StackedDRG32GiBWindowPoSt, nil
	case RegisteredProof_StackedDRG2KiBSeal, RegisteredProof_StackedDRG2KiBWindowPoSt:
		return RegisteredProof_StackedDRG2KiBWindowPoSt, nil
	case RegisteredProof_StackedDRG8MiBSeal, RegisteredProof_StackedDRG8MiBWindowPoSt:
		return RegisteredProof_StackedDRG8MiBWindowPoSt, nil
	case RegisteredProof_StackedDRG512MiBSeal, RegisteredProof_StackedDRG512MiBWindowPoSt:
		return RegisteredProof_StackedDRG512MiBWindowPoSt, nil
	default:
		return 0, errors.Errorf("unsupported mapping from %+v to PoSt-specific RegisteredProof", p)
	}
}

// RegisteredSealProof produces the seal-specific RegisteredProof corresponding
// to the receiving RegisteredProof.
func (p RegisteredProof) RegisteredSealProof() (RegisteredProof, error) {
	switch p {
	case RegisteredProof_StackedDRG32GiBSeal, RegisteredProof_StackedDRG32GiBPoSt, RegisteredProof_StackedDRG32GiBWindowPoSt, RegisteredProof_StackedDRG32GiBWinningPoSt:
		return RegisteredProof_StackedDRG32GiBSeal, nil
	case RegisteredProof_StackedDRG2KiBSeal, RegisteredProof_StackedDRG2KiBPoSt, RegisteredProof_StackedDRG2KiBWindowPoSt, RegisteredProof_StackedDRG2KiBWinningPoSt:
		return RegisteredProof_StackedDRG2KiBSeal, nil
	case RegisteredProof_StackedDRG8MiBSeal, RegisteredProof_StackedDRG8MiBPoSt, RegisteredProof_StackedDRG8MiBWindowPoSt, RegisteredProof_StackedDRG8MiBWinningPoSt:
		return RegisteredProof_StackedDRG8MiBSeal, nil
	case RegisteredProof_StackedDRG512MiBSeal, RegisteredProof_StackedDRG512MiBPoSt, RegisteredProof_StackedDRG512MiBWindowPoSt, RegisteredProof_StackedDRG512MiBWinningPoSt:
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

// Information needed to verify a seal proof.
type SealVerifyInfo struct {
	SectorID
	OnChain               OnChainSealVerifyInfo // TODO: don't embed this https://github.com/filecoin-project/specs-actors/issues/276
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

type PoStRandomness Randomness

// Information about a sector necessary for PoSt verification.
type SectorInfo struct {
	RegisteredProof // RegisteredProof used when sealing - needs to be mapped to PoSt registered proof when used to verify a PoSt
	SectorNumber    SectorNumber
	SealedCID       cid.Cid // CommR
}

type PoStProof struct {
	RegisteredProof
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
