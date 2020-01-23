package payment_channel

import (
	"bytes"
	"io"

	addr "github.com/filecoin-project/go-address"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	acrypto "github.com/filecoin-project/specs-actors/actors/crypto"
)

type PaymentChannelActorState struct {
	From addr.Address
	To   addr.Address

	ToSend big.Int

	ClosingAt      abi.ChainEpoch
	MinCloseHeight abi.ChainEpoch

	LaneStates map[int64]*LaneState
}

func (st *PaymentChannelActorState) MarshalCBOR(w io.Writer) error {
	panic("replace with cbor-gen")
}

type LaneState struct {
	Closed   bool
	Redeemed big.Int
	Nonce    int64
}

type SignedVoucher struct {
	TimeLock       abi.ChainEpoch
	SecretPreimage []byte
	Extra          *ModVerifyParams
	Lane           int64
	Nonce          int64
	Amount         big.Int
	MinCloseHeight abi.ChainEpoch

	Merges []Merge

	Signature *acrypto.Signature
}

func (sv *SignedVoucher) SigningBytes() ([]byte, error) {
	osv := *sv
	osv.Signature = nil

	buf := new(bytes.Buffer)
	if err := osv.MarshalCBOR(buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (sv *SignedVoucher) MarshalCBOR(w io.Writer) error {
	panic("replace with cbor-gen")
}

type Merge struct {
	Lane  int64
	Nonce int64
}

type ModVerifyParams struct {
	Actor  addr.Address
	Method abi.MethodNum
	Data   []byte
}
