package proof

import (
	"github.com/filecoin-project/go-state-types/proof"
)

///
/// Sealing
///

// Information needed to verify a seal proof.
type SealVerifyInfo = proof.SealVerifyInfo

///
/// PoSting
///

// Information about a proof necessary for PoSt verification.
type SectorInfo = proof.SectorInfo
type PoStProof = proof.PoStProof

// Information needed to verify a Winning PoSt attached to a block header.
// Note: this is not used within the state machine, but by the consensus/election mechanisms.
type WinningPoStVerifyInfo = proof.WinningPoStVerifyInfo

// Information needed to verify a Window PoSt submitted directly to a miner actor.
type WindowPoStVerifyInfo = proof.WindowPoStVerifyInfo
