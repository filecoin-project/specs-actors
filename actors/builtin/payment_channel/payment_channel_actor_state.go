package payment_channel

import (
	addr "github.com/filecoin-project/go-address"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
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

	// Height at which the channel can be `Collected`
	SettlingAt abi.ChainEpoch
	// Height before which the channel `ToSend` cannot be collected
	MinSettleHeight abi.ChainEpoch

	// Mapping from lane number to lane state for the channel
	LaneStates map[string]*LaneState
}

// The Lane state tracks the latest (highest) voucher nonce used to merge the lane
// as well as the amount it has already redeemed.
type LaneState struct {
	Redeemed big.Int
	Nonce    int64
}

// Specifies which `Lane`s to be merged with what `Nonce` on channelUpdate
type Merge struct {
	Lane  int64
	Nonce int64
}

func ConstructState(from addr.Address, to addr.Address) *PaymentChannelActorState {
	return &PaymentChannelActorState{
		From:            from,
		To:              to,
		ToSend:          big.Zero(),
		SettlingAt:      0,
		MinSettleHeight: 0,
		LaneStates:      make(map[string]*LaneState),
	}
}
