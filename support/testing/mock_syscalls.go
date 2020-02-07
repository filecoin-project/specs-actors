package testing

import (
	addr "github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/crypto"
)

type MockSyscalls struct {
	VerifiesSig bool
}

func (pcs *MockSyscalls) ComputeUnsealedSectorCID(sectorSize abi.SectorSize, pieces []abi.PieceInfo) (cid.Cid, error) {
	panic("implement me")
}

func (pcs *MockSyscalls) VerifySeal(sectorSize abi.SectorSize, vi abi.SealVerifyInfo) bool {
	panic("implement me")
}

func (pcs *MockSyscalls) VerifyPoSt(sectorSize abi.SectorSize, vi abi.PoStVerifyInfo) bool {
	panic("implement me")
}

func (pcs *MockSyscalls) VerifyConsensusFault(h1, h2 []byte) bool {
	panic("implement me")
}

func (pcs *MockSyscalls) VerifySignature(signature crypto.Signature, signer addr.Address, plaintext []byte) bool {
	return pcs.VerifiesSig
}
func (pcs *MockSyscalls) Hash_SHA256(data []byte) []byte {
	// return crypto.SHA256(data)
	return append(data, 'X')
}
