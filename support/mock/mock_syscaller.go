package mock

import (
	"fmt"

	addr "github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/crypto"
	"github.com/filecoin-project/specs-actors/actors/runtime"
)

type VerifyFunc func(signature crypto.Signature, signer addr.Address, plaintext []byte) bool
type HasherFunc func(data []byte) []byte

type syscaller struct {
	SignatureVerifier VerifyFunc
	Hasher            HasherFunc
}

// Interface methods
func (s *syscaller) VerifySignature(sig crypto.Signature, signer addr.Address, plaintext []byte) bool {
	if s.SignatureVerifier == nil {
		s.PanicOnUnsetFunc("SignatureVerifier")
	}
	return s.SignatureVerifier(sig, signer, plaintext)
}

func (s *syscaller) HashBlake2b(data []byte) []byte {
	if s.Hasher == nil {
		s.PanicOnUnsetFunc("Hasher")
	}
	return s.Hasher(data)
}

func (s *syscaller) ComputeUnsealedSectorCID(sectorSize abi.SectorSize, pieces []abi.PieceInfo) (cid.Cid, error) {
	s.PanicOnUnsetFunc("UnsealedSectorCIDComputer")
	return cid.Undef, nil
}

func (s *syscaller) VerifySeal(sectorSize abi.SectorSize, vi abi.SealVerifyInfo) bool {
	s.PanicOnUnsetFunc("SealVerifier")
	return false
}

func (s *syscaller) VerifyPoSt(sectorSize abi.SectorSize, vi abi.PoStVerifyInfo) bool {
	s.PanicOnUnsetFunc("PoStVerifier")
	return false
}

func (s *syscaller) VerifyConsensusFault(h1, h2 []byte) bool {
	s.PanicOnUnsetFunc("ConsensusFaultVerifier")
	return false
}

func (s *syscaller) PanicOnUnsetFunc(unsetFuncName string) {
	panic(fmt.Sprintf("no %s set", unsetFuncName))
}

var _ runtime.Syscalls = &syscaller{}
