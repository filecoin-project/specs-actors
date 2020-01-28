package payment_channel

import (
	"bytes"

	addr "github.com/filecoin-project/go-address"
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	indices "github.com/filecoin-project/specs-actors/actors/runtime/indices"
	"github.com/filecoin-project/specs-actors/actors/serde"
)

type PaymentChannelActor struct{}

/////////////////
// Constructor //
/////////////////

type PCAConstructorParams struct {
	To addr.Address
}

func (pca *PaymentChannelActor) Constructor(rt vmr.Runtime, params *PCAConstructorParams) *vmr.EmptyReturn {

	var st PaymentChannelActorState
	rt.State().Transaction(&st, func() interface{} {
		st.From = rt.ImmediateCaller()
		st.To = params.To
		st.LaneStates = make(map[int64]*LaneState)
		return nil
	})
	return &vmr.EmptyReturn{}
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

func (pca *PaymentChannelActor) UpdateChannelState(rt vmr.Runtime, params *PCAUpdateChannelStateParams) *vmr.EmptyReturn {

	var st PaymentChannelActorState
	rt.State().Readonly(&st)

	sv := params.Sv

	vb, nerr := sv.SigningBytes()
	if nerr != nil {
		rt.Abort(exitcode.ErrIllegalArgument, "failed to serialize signedvoucher")
	}

	if !rt.Syscalls().VerifySignature(*sv.Signature, st.From, vb) {
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
			serde.MustSerializeParams(
				&PaymentVerifyParams{
					sv.Extra.Data,
					params.Proof,
				},
			),
			abi.NewTokenAmount(0),
		)
		vmr.RequireSuccess(rt, code, "spend voucher verification failed")
	}

	rt.State().Transaction(&st, func() interface{} {
		ls, ok := st.LaneStates[sv.Lane]
		// create voucher lane if it does not already exist
		if !ok {
			ls = new(LaneState)
			ls.Redeemed = big.NewInt(0)
			st.LaneStates[sv.Lane] = ls
		}
		if ls.Closed {
			rt.Abort(exitcode.ErrIllegalArgument, "cannot redeem a voucher on a closed lane")
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

		// update channel closingAt and MinCloseAt if delayed by voucher
		if sv.MinCloseHeight != 0 {
			if st.ClosingAt != 0 && st.ClosingAt < sv.MinCloseHeight {
				st.ClosingAt = sv.MinCloseHeight
			}
			if st.MinCloseHeight < sv.MinCloseHeight {
				st.MinCloseHeight = sv.MinCloseHeight
			}
		}
		return nil
	})
	return &vmr.EmptyReturn{}
}

func (pca *PaymentChannelActor) Close(rt vmr.Runtime) *vmr.EmptyReturn {

	var st PaymentChannelActorState
	rt.State().Transaction(&st, func() interface{} {

		rt.ValidateImmediateCallerIs(st.From, st.To)

		if st.ClosingAt != 0 {
			rt.Abort(exitcode.ErrIllegalState, "channel already closing")
		}

		st.ClosingAt = rt.CurrEpoch() + indices.PaymentChannel_PaymentChannelClosingDelay()
		if st.ClosingAt < st.MinCloseHeight {
			st.ClosingAt = st.MinCloseHeight
		}

		return nil
	})
	return &vmr.EmptyReturn{}
}

func (pca *PaymentChannelActor) Collect(rt vmr.Runtime) *vmr.EmptyReturn {

	var st PaymentChannelActorState
	rt.State().Readonly(&st)

	if st.ClosingAt == 0 {
		rt.Abort(exitcode.ErrForbidden, "payment channel not closing or closed")
	}

	if rt.CurrEpoch() < st.ClosingAt {
		rt.Abort(exitcode.ErrForbidden, "payment channel not closed yet")
	}

	// send remaining balance to "From"

	_, code := rt.Send(
		st.From,
		builtin.MethodSend,
		nil,
		abi.NewTokenAmount(big.Sub(rt.CurrentBalance(), st.ToSend).Int64()),
	)
	vmr.RequireSuccess(rt, code, "Failed to send balance to `From`")

	// send ToSend to "To"

	_, code2 := rt.Send(
		st.From,
		builtin.MethodSend,
		nil,
		abi.NewTokenAmount(st.ToSend.Int64()),
	)
	vmr.RequireSuccess(rt, code2, "Failed to send funds to `To`")

	rt.State().Transaction(&st, func() interface{} {
		st.ToSend = big.Zero()
		return nil
	})
	return &vmr.EmptyReturn{}
}
