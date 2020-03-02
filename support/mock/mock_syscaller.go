package mock

import (
	"fmt"

	addr "github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/crypto"
	"github.com/filecoin-project/specs-actors/actors/runtime"
)

type VerifyFunc func(signature crypto.Signature, signer addr.Address, plaintext []byte) error
type HasherFunc func(data []byte) [32]byte

type ComputeUnsealedCID interface {
	Compute(reg abi.RegisteredProof, pieces []abi.PieceInfo) (cid.Cid, error)
}

type syscaller struct {
	SignatureVerifier VerifyFunc
	Hasher            HasherFunc
	ComputeCID        ComputeUnsealedCID
}

// Interface methods
func (s *syscaller) VerifySignature(sig crypto.Signature, signer addr.Address, plaintext []byte) error {
	if s.SignatureVerifier == nil {
		s.PanicOnUnsetFunc("SignatureVerifier")
	}
	return s.SignatureVerifier(sig, signer, plaintext)
}

func (s *syscaller) HashBlake2b(data []byte) [32]byte {
	if s.Hasher == nil {
		s.PanicOnUnsetFunc("Hasher")
	}
	return s.Hasher(data)
}

func (s *syscaller) ComputeUnsealedSectorCID(reg abi.RegisteredProof, pieces []abi.PieceInfo) (cid.Cid, error) {
	if s.ComputeCID == nil {
		s.PanicOnUnsetFunc("UnsealedSectorCIDComputer")
	}
	return s.ComputeCID.Compute(reg, pieces)
}

func (s *syscaller) VerifySeal(vi abi.SealVerifyInfo) error {
	s.PanicOnUnsetFunc("SealVerifier")
	return nil
}

func (s *syscaller) VerifyPoSt(vi abi.PoStVerifyInfo) error {
	s.PanicOnUnsetFunc("PoStVerifier")
	return nil
}

func (s *syscaller) VerifyConsensusFault(h1, h2, extra []byte, earliest abi.ChainEpoch) (*runtime.ConsensusFault, error) {
	s.PanicOnUnsetFunc("ConsensusFaultVerifier")
	return nil, nil
}

func (s *syscaller) PanicOnUnsetFunc(unsetFuncName string) {
	panic(fmt.Sprintf("no %s set", unsetFuncName))
}

var _ runtime.Syscalls = &syscaller{}
