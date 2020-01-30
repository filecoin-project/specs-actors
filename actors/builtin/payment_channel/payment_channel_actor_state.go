package payment_channel

import (
	"bytes"
	"io"

	addr "github.com/filecoin-project/go-address"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	acrypto "github.com/filecoin-project/specs-actors/actors/crypto"
)

// A given payment channel actor is established by From
// to enable off-chain microtransactions to To to be reconciled
// and tallied on chain.
type PaymentChannelActorState struct {
	// Channel owner, who has funded the actor
	From addr.Address
	// Recipient of payouts from channel
	To addr.Address

	// Amount successfully redeemed through the payment channel, paid out on `Collect()`
	ToSend abi.TokenAmount

	// Height at which the channel will close
	ClosingAt abi.ChainEpoch
	// Height before which the channel cannot close
	MinCloseHeight abi.ChainEpoch

	// Mapping from lane number to lane state for the channel
	LaneStates map[int64]*LaneState
}

func (st *PaymentChannelActorState) MarshalCBOR(w io.Writer) error {
	panic("replace with cbor-gen")
}

func (st *PaymentChannelActorState) UnmarshalCBOR(r io.Reader) error {
	panic("replace with cbor-gen")
}

// The Lane state tracks the latest (highest) voucher nonce used to merge the lane
// as well as the amount it has already redeemed.
type LaneState struct {
	Closed   bool
	Redeemed big.Int
	Nonce    int64
}

// Specifies which `Lane`s to be merged with what `Nonce` on channelUpdate
type Merge struct {
	Lane  int64
	Nonce int64
}

// Modular Verification method
type ModVerifyParams struct {
	Actor  addr.Address
	Method abi.MethodNum
	Data   []byte
}

// A voucher is seent by `From` to `To` off-chain in order to enable
// `To` to redeem payments on-chain in the future
type SignedVoucher struct {
	// TimeLock sets a min epoch before which the voucher cannot be redeemed
	TimeLock abi.ChainEpoch
	// (optional) The SecretPreImage is used by `To` to validate
	SecretPreimage []byte
	// (optional) Extra can be specified by `From` to add a verification method to the voucher
	Extra *ModVerifyParams
	// Specifies which lane the Voucher merges into (will be created if does not exist)
	Lane int64
	// Nonce is set by `From` to prevent redemption of stale vouchers on a lane
	Nonce int64
	// Amount voucher can be redeemed for
	Amount big.Int
	// (optional) MinCloseHeight can extend channel MinCloseHeight if needed
	MinCloseHeight abi.ChainEpoch

	// (optional) Set of lanes to be merged into `Lane`
	Merges []Merge

	// `From`'s signature over the voucher
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
