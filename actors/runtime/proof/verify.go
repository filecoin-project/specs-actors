package proof

import (
	prooftypes "github.com/filecoin-project/go-state-types/proof"
)

///
/// Sealing
///

// Information needed to verify a seal proof.
//type SealVerifyInfo struct {
//	SealProof abi.RegisteredSealProof
//	abi.SectorID
//	DealIDs               []abi.DealID
//	Randomness            abi.SealRandomness
//	InteractiveRandomness abi.InteractiveSealRandomness
//	ReplicaProof                 []byte
//
//	// Safe because we get those from the miner actor
//	SealedCID   cid.Cid `checked:"true"` // CommR
//	UnsealedCID cid.Cid `checked:"true"` // CommD
//}
type SealVerifyInfo = prooftypes.SealVerifyInfo

type AggregateSealVerifyInfo = prooftypes.AggregateSealVerifyInfo

type AggregateSealVerifyProofAndInfos = prooftypes.AggregateSealVerifyProofAndInfos

///
/// Replica
///

// Information needed to verify a replica update

type ReplicaUpdateInfo = prooftypes.ReplicaUpdateInfo

///
/// PoSting
///

// Information about a proof necessary for PoSt verification.
// type SectorInfo struct {
// 	SealProof    abi.RegisteredSealProof // RegisteredProof used when sealing - needs to be mapped to PoSt registered proof when used to verify a PoSt
// 	SectorNumber abi.SectorNumber
// 	SealedCID    cid.Cid // CommR
// }
type SectorInfo = prooftypes.SectorInfo

type ExtendedSectorInfo = prooftypes.ExtendedSectorInfo

//type PoStProof struct {
//	PoStProof  abi.RegisteredPoStProof
//	ProofBytes []byte
//}
type PoStProof = prooftypes.PoStProof

// Information needed to verify a Winning PoSt attached to a block header.
// Note: this is not used within the state machine, but by the consensus/election mechanisms.
//type WinningPoStVerifyInfo struct {
//	Randomness        abi.PoStRandomness
//	Proofs            []PoStProof
//	ChallengedSectors []SectorInfo
//	Prover            abi.ActorID // used to derive 32-byte prover ID
//}
type WinningPoStVerifyInfo = prooftypes.WinningPoStVerifyInfo

// Information needed to verify a Window PoSt submitted directly to a miner actor.
//type WindowPoStVerifyInfo struct {
// Randomness        abi.PoStRandomness
// Proofs            []PoStProof
// ChallengedSectors []SectorInfo
// Prover            abi.ActorID // used to derive 32-byte prover ID
//}
type WindowPoStVerifyInfo = prooftypes.WindowPoStVerifyInfo
