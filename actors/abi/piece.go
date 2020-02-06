package abi

import (
	cid "github.com/ipfs/go-cid"
)

// PieceSize is the size of a piece, in bytes
type PieceSize struct {
	PayloadSize  uint64
	OverheadSize uint64
}

func (p PieceSize) Total() uint64 {
	return p.PayloadSize + p.OverheadSize
}

type PieceInfo struct {
	Size     uint64 // Size in nodes. For BLS12-381 (capacity 254 bits), must be >= 16. (16 * 8 = 128)
	PieceCID cid.Cid
}
