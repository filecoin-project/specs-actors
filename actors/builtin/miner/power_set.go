package miner

import (
	"io"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
)

// PowerSet represents a set of "values" and their total power + pledge.
// "things" can be either partitions or sectors, depending on the context.
type PowerSet struct {
	Values      *abi.BitField
	TotalPower  abi.StoragePower
	TotalPledge abi.TokenAmount
}

func NewPowerSet() *PowerSet {
	return &PowerSet{
		Values:      abi.NewBitField(),
		TotalPower:  big.Zero(),
		TotalPledge: big.Zero(),
	}
}

func (e *PowerSet) MarshalCBOR(w io.Writer) error {
	panic("implement me")
}

func (e *PowerSet) UnmarshalCBOR(r io.Reader) error {
	panic("implement me")
}
