package payment_channel

import (
	"bytes"
	"sort"

	addr "github.com/filecoin-project/go-address"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	acrypto "github.com/filecoin-project/specs-actors/actors/crypto"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

const SETTLE_DELAY = abi.ChainEpoch(1) // placeholder PARAM_FINISH

type PaymentChannelActor struct{}

type ConstructorParams struct {
	To addr.Address
}

func (pca *PaymentChannelActor) Constructor(rt vmr.Runtime, params *ConstructorParams) *adt.EmptyValue {
	// Check that both parties are capable of signing vouchers by requiring them to be account actors.
	// The account actor constructor checks that the embedded address is associated with an appropriate key.
	// An alternative (more expensive) would be to send a message to the actor to fetch its key.
	rt.ValidateImmediateCallerType(builtin.AccountActorCodeID)
	targetCodeID, ok := rt.GetActorCodeCID(params.To)
	if !ok {
		rt.Abort(exitcode.ErrIllegalArgument, "no code for target address %v", params.To)
	}
	if targetCodeID != builtin.AccountActorCodeID {
		rt.Abort(exitcode.ErrIllegalArgument, "target actor %v must be an account (%v), was %v",
			params.To, builtin.AccountActorCodeID, targetCodeID)
	}

	// Check that target is a canonical ID address.
	// This is required for consistent caller validation.
	if params.To.Protocol() != addr.ID {
		rt.Abort(exitcode.ErrIllegalArgument, "target address must be an ID-address, %v is %v", params.To, params.To.Protocol())
	}

	rt.State().Construct(func() vmr.CBORMarshaler {
		return ConstructState(rt.ImmediateCaller(), params.To)
	})
	return &adt.EmptyValue{}
}

////////////////////////////////////////////////////////////////////////////////
// Payment Channel state operations
////////////////////////////////////////////////////////////////////////////////

type UpdateChannelStateParams struct {
	Sv     SignedVoucher
	Secret []byte
	Proof  []byte
}

// A voucher is sent by `From` to `To` off-chain in order to enable
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
	// (optional) MinSettleHeight can extend channel MinSettleHeight if needed
	MinSettleHeight abi.ChainEpoch

	// (optional) Set of lanes to be merged into `Lane`
	Merges []Merge

	// Sender's signature over the voucher
	Signature *acrypto.Signature
}

// Modular Verification method
type ModVerifyParams struct {
	Actor  addr.Address
	Method abi.MethodNum
	Data   []byte
}

type PaymentVerifyParams struct {
	Extra []byte
	Proof []byte
}

func (pca *PaymentChannelActor) UpdateChannelState(rt vmr.Runtime, params *UpdateChannelStateParams) *adt.EmptyValue {
	var st PaymentChannelActorState
	rt.State().Readonly(&st)

	// both parties must sign voucher: one who submits it, the other explicitly signs it
	rt.ValidateImmediateCallerIs(st.From, st.To)
	var signer addr.Address
	if rt.ImmediateCaller() == st.From {
		signer = st.To
	} else {
		signer = st.From
	}
	sv := params.Sv

	vb, nerr := sv.SigningBytes()
	if nerr != nil {
		rt.Abort(exitcode.ErrIllegalArgument, "failed to serialize signedvoucher")
	}

	if !rt.Syscalls().VerifySignature(*sv.Signature, signer, vb) {
		rt.Abort(exitcode.ErrIllegalArgument, "voucher signature invalid")
	}

	if rt.CurrEpoch() < sv.TimeLock {
		rt.Abort(exitcode.ErrIllegalArgument, "cannot use this voucher yet!")
	}

	if len(sv.SecretPreimage) > 0 {
		if !bytes.Equal(rt.Syscalls().Hash_SHA256(params.Secret), sv.SecretPreimage) {
			rt.Abort(exitcode.ErrIllegalArgument, "incorrect secret!")
		}
	}

	if sv.Extra != nil {

		_, code := rt.Send(
			sv.Extra.Actor,
			sv.Extra.Method,
			&PaymentVerifyParams{
				sv.Extra.Data,
				params.Proof,
			},
			abi.NewTokenAmount(0),
		)
		builtin.RequireSuccess(rt, code, "spend voucher verification failed")
	}

	rt.State().Transaction(&st, func() interface{} {
		// Find the voucher lane, create and insert it in sorted order if necessary.
		laneIdx, ls := findLane(st.LaneStates, sv.Lane)
		if ls == nil {
			ls = &LaneState{
				ID:       sv.Lane,
				Redeemed: big.NewInt(0),
				Nonce:    0,
			}
			st.LaneStates = append(st.LaneStates[:laneIdx], append([]*LaneState{ls}, st.LaneStates[laneIdx:]...)...)

		}

		if ls.Nonce > sv.Nonce {
			rt.Abort(exitcode.ErrIllegalArgument, "voucher has an outdated nonce, cannot redeem")
		}

		// The next section actually calculates the payment amounts to update the payment channel state
		// 1. (optional) sum already redeemed value of all merging lanes
		redeemedFromOthers := big.Zero()
		for _, merge := range sv.Merges {
			if merge.Lane == sv.Lane {
				rt.Abort(exitcode.ErrIllegalArgument, "voucher cannot merge lanes into its own lane")
			}

			_, otherls := findLane(st.LaneStates, merge.Lane)
			if otherls != nil {
				if otherls.Nonce >= merge.Nonce {
					rt.Abort(exitcode.ErrIllegalArgument, "merged lane in voucher has outdated nonce, cannot redeem")
				}

				redeemedFromOthers = big.Add(redeemedFromOthers, otherls.Redeemed)
				otherls.Nonce = merge.Nonce
			} else {
				rt.Abort(exitcode.ErrIllegalArgument, "voucher specifies invalid merge lane %v", merge.Lane)
			}
		}

		// 2. To prevent double counting, remove already redeemed amounts (from
		// voucher or other lanes) from the voucher amount
		ls.Nonce = sv.Nonce
		balanceDelta := big.Sub(sv.Amount, big.Add(redeemedFromOthers, ls.Redeemed))
		// 3. set new redeemed value for merged-into lane
		ls.Redeemed = sv.Amount

		newSendBalance := big.Add(st.ToSend, balanceDelta)

		// 4. check operation validity
		if newSendBalance.LessThan(big.Zero()) {
			rt.Abort(exitcode.ErrIllegalState, "voucher would leave channel balance negative")
		}
		if newSendBalance.GreaterThan(rt.CurrentBalance()) {
			rt.Abort(exitcode.ErrIllegalState, "not enough funds in channel to cover voucher")
		}

		// 5. add new redemption ToSend
		st.ToSend = newSendBalance

		// update channel settlingAt and MinSettleHeight if delayed by voucher
		if sv.MinSettleHeight != 0 {
			if st.SettlingAt != 0 && st.SettlingAt < sv.MinSettleHeight {
				st.SettlingAt = sv.MinSettleHeight
			}
			if st.MinSettleHeight < sv.MinSettleHeight {
				st.MinSettleHeight = sv.MinSettleHeight
			}
		}
		return nil
	})
	return &adt.EmptyValue{}
}

func (pca *PaymentChannelActor) Settle(rt vmr.Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	var st PaymentChannelActorState
	rt.State().Transaction(&st, func() interface{} {

		rt.ValidateImmediateCallerIs(st.From, st.To)

		if st.SettlingAt != 0 {
			rt.Abort(exitcode.ErrIllegalState, "channel already seettling")
		}

		st.SettlingAt = rt.CurrEpoch() + SETTLE_DELAY
		if st.SettlingAt < st.MinSettleHeight {
			st.SettlingAt = st.MinSettleHeight
		}

		return nil
	})
	return &adt.EmptyValue{}
}

func (pca *PaymentChannelActor) Collect(rt vmr.Runtime, _ *adt.EmptyValue) *adt.EmptyValue {

	var st PaymentChannelActorState
	rt.State().Readonly(&st)
	rt.ValidateImmediateCallerIs(st.From, st.To)

	if st.SettlingAt == 0 || rt.CurrEpoch() < st.SettlingAt {
		rt.Abort(exitcode.ErrForbidden, "payment channel not settling or settled")
	}

	// send remaining balance to "From"

	_, codeFrom := rt.Send(
		st.From,
		builtin.MethodSend,
		nil,
		abi.NewTokenAmount(big.Sub(rt.CurrentBalance(), st.ToSend).Int64()),
	)
	builtin.RequireSuccess(rt, codeFrom, "Failed to send balance to `From`")

	// send ToSend to "To"

	_, codeTo := rt.Send(
		st.From,
		builtin.MethodSend,
		nil,
		abi.NewTokenAmount(st.ToSend.Int64()),
	)
	builtin.RequireSuccess(rt, codeTo, "Failed to send funds to `To`")

	rt.State().Transaction(&st, func() interface{} {
		st.ToSend = big.Zero()
		return nil
	})
	return &adt.EmptyValue{}
}

func (t *SignedVoucher) SigningBytes() ([]byte, error) {
	osv := *t
	osv.Signature = nil

	buf := new(bytes.Buffer)
	if err := osv.MarshalCBOR(buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Returns the insertion index for a lane ID, with the matching lane state if found, or nil.
func findLane(lanes []*LaneState, ID int64) (int, *LaneState) {
	insertionIdx := sort.Search(len(lanes), func(i int) bool {
		return lanes[i].ID >= ID
	})
	if insertionIdx == len(lanes) || lanes[insertionIdx].ID != int64(insertionIdx) {
		// Not found
		return insertionIdx, nil
	}
	return insertionIdx, lanes[insertionIdx]
}
