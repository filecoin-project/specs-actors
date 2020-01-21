package abi

import "math/big"

type DealID int64
type DealIDs struct {
	Items []DealID
}

type DealWeight *big.Int
