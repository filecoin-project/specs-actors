package mock

import (
	addr "github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/crypto"
	"github.com/filecoin-project/specs-actors/actors/runtime"
)

type VerifyFunc func(signature crypto.Signature, signer addr.Address, plaintext []byte) bool
type HasherFunc func(data []byte) []byte

type syscaller struct {
	Verifier VerifyFunc
	Hasher   HasherFunc
}

// Interface methods
func (s *syscaller) VerifySignature(sig crypto.Signature, signer addr.Address, plaintext []byte) bool {
	if s.Verifier == nil {
		panic("set me")
	}
	return s.Verifier(sig,signer,plaintext)
}

func (s *syscaller) Hash_SHA256(data []byte) []byte {
	if s.Hasher == nil {
		panic("set me")
	}
	return s.Hasher(data)
}

func (s *syscaller) ComputeUnsealedSectorCID(sectorSize abi.SectorSize, pieces []abi.PieceInfo) (cid.Cid, error) {
	panic("implement me")
}

func (s *syscaller) VerifySeal(sectorSize abi.SectorSize, vi abi.SealVerifyInfo) bool {
	panic("implement me")
}

func (s *syscaller) VerifyPoSt(sectorSize abi.SectorSize, vi abi.PoStVerifyInfo) bool {
	panic("implement me")
}

func (s *syscaller) VerifyConsensusFault(h1, h2 []byte) bool {
	panic("implement me")
}


var _ runtime.Syscalls = &syscaller{}


