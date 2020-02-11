package mock

import (
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/crypto"
	"github.com/filecoin-project/specs-actors/actors/runtime"
)

type VerifyFunc func(signature crypto.Signature, signer addr.Address, plaintext []byte) bool
type HasherFunc func(data []byte) []byte

type syscaller struct {
	T                 testing.T
	SignatureVerifier VerifyFunc
	Hasher            HasherFunc
}

// Interface methods
func (s *syscaller) VerifySignature(sig crypto.Signature, signer addr.Address, plaintext []byte) bool {
	if s.SignatureVerifier == nil {
		s.FailOnUnsetFunc("SignatureVerifier")
	}
	return s.SignatureVerifier(sig,signer,plaintext)
}

func (s *syscaller) Hash_SHA256(data []byte) []byte {
	if s.Hasher == nil {
		s.FailOnUnsetFunc("Hasher")
	}
	return s.Hasher(data)
}

func (s *syscaller) ComputeUnsealedSectorCID(sectorSize abi.SectorSize, pieces []abi.PieceInfo) (cid.Cid, error) {
	s.FailOnUnsetFunc("UnsealedSectorCIDComputer")
}

func (s *syscaller) VerifySeal(sectorSize abi.SectorSize, vi abi.SealVerifyInfo) bool {
	s.FailOnUnsetFunc("SealVerifier")
}

func (s *syscaller) VerifyPoSt(sectorSize abi.SectorSize, vi abi.PoStVerifyInfo) bool {
	s.FailOnUnsetFunc("PoStVerifier")
}

func (s *syscaller) VerifyConsensusFault(h1, h2 []byte) bool {
	s.FailOnUnsetFunc("ConsensusFaultVerifier")
}

func (s *syscaller) FailOnUnsetFunc(unsetFuncName string) {
	s.T.Fatalf("no %s set", unsetFuncName)
}


var _ runtime.Syscalls = &syscaller{}


