package abi

import big "github.com/filecoin-project/specs-actors/actors/abi/bigint"

type DealID int64
type DealIDs struct {
	Items []DealID
}

type DealWeight = big.Int
