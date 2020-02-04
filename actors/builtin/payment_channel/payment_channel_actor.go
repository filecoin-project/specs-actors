package payment_channel

import (
	"bytes"
	"io"

	addr "github.com/filecoin-project/go-address"
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	indices "github.com/filecoin-project/specs-actors/actors/runtime/indices"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type PaymentChannelActor struct{}

/////////////////
// Constructor //
/////////////////

type PCAConstructorParams struct {
	To addr.Address
}

func (pca *PaymentChannelActor) Constructor(rt vmr.Runtime, params *PCAConstructorParams) *adt.EmptyValue {

	// TODO anorth: ensure parties are valid
	// rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	// toID, err := addr.IDFromAddress(params.To)
	// if err != nil {
	// 	rt.Abort(exitcode.ErrIllegalArgument, "receiver must be ID address")
	// }

	// if !toID.PubKeyAddress.Protocol() == addr.SECP256K1 && !toID.PubKeyAddress.Protocol() == addr.BLS {
	// 	rt.Abort(exitcode.ErrIllegalArgument, "address must use BLS or SECP protocol, got %v", toID.PubKeyAddress.Protocol())
	// }

	var st PaymentChannelActorState
	rt.State().Transaction(&st, func() interface{} {
		st.From = rt.ImmediateCaller()
		st.To = params.To
		st.LaneStates = make(map[int64]*LaneState)
		return nil
	})
	return &adt.EmptyValue{}
}

////////////////////////////////////////////////////////////////////////////////
// Payment Channel state operations
////////////////////////////////////////////////////////////////////////////////

type PCAUpdateChannelStateParams struct {
	Sv     SignedVoucher
	Secret []byte
	Proof  []byte
}

type PaymentVerifyParams struct {
	Extra []byte
	Proof []byte
}

func (st *PaymentVerifyParams) MarshalCBOR(w io.Writer) error {
	panic("replace with cbor-gen")
}

func (st *PaymentVerifyParams) UnmarshalCBOR(r io.Reader) error {
	panic("replace with cbor-gen")
}

func (pca *PaymentChannelActor) UpdateChannelState(rt vmr.Runtime, params *PCAUpdateChannelStateParams) *adt.EmptyValue {
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
		ls, ok := st.LaneStates[sv.Lane]
		// create voucher lane if it does not already exist
		if !ok {
			ls = new(LaneState)
			ls.Redeemed = big.NewInt(0)
			st.LaneStates[sv.Lane] = ls
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

			otherls := st.LaneStates[merge.Lane]

			if otherls.Nonce >= merge.Nonce {
				rt.Abort(exitcode.ErrIllegalArgument, "merged lane in voucher has outdated nonce, cannot redeem")
			}

			redeemedFromOthers = big.Add(redeemedFromOthers, otherls.Redeemed)
			otherls.Nonce = merge.Nonce
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

func (pca *PaymentChannelActor) Settle(rt vmr.Runtime) *adt.EmptyValue {
	var st PaymentChannelActorState
	rt.State().Transaction(&st, func() interface{} {

		rt.ValidateImmediateCallerIs(st.From, st.To)

		if st.SettlingAt != 0 {
			rt.Abort(exitcode.ErrIllegalState, "channel already seettling")
		}

		st.SettlingAt = rt.CurrEpoch() + indices.PaymentChannel_PaymentChannelSettleDelay()
		if st.SettlingAt < st.MinSettleHeight {
			st.SettlingAt = st.MinSettleHeight
		}

		return nil
	})
	return &adt.EmptyValue{}
}

func (pca *PaymentChannelActor) Collect(rt vmr.Runtime) *adt.EmptyValue {

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
