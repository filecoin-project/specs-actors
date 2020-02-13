package paych_test

import (
	"context"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	. "github.com/filecoin-project/specs-actors/actors/builtin/paych"
	"github.com/filecoin-project/specs-actors/actors/crypto"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

func TestPaymentChannelActor_Constructor(t *testing.T) {
	ctx := context.Background()
	actor := pcActorHarness{Actor{}, t}

	newPaychAddr := tutil.NewIDAddr(t, 100)
	payerAddr := tutil.NewIDAddr(t, 101)
	callerAddr := tutil.NewIDAddr(t, 102)

	t.Run("can create a payment channel actor", func(t *testing.T) {
		builder := mock.NewBuilder(ctx, newPaychAddr).
			WithCaller(callerAddr, builtin.InitActorCodeID).
			WithActorType(newPaychAddr, builtin.AccountActorCodeID).
			WithActorType(payerAddr, builtin.AccountActorCodeID)
		rt := builder.Build(t)
		actor.constructAndVerify(rt, payerAddr, newPaychAddr)
		var st State
		rt.GetState(&st)
		assert.Equal(t, newPaychAddr, st.To)
		assert.Equal(t, payerAddr, st.From)
		assert.Empty(t, st.LaneStates)
	})

	testCases := []struct {
		desc         string
		newActorAddr addr.Address
		callerCode   cid.Cid
		newActorCode cid.Cid
		payerCode    cid.Cid
		expExitCode  exitcode.ExitCode
	}{
		{"fails if target (to) is not account actor",
			newPaychAddr,
			builtin.InitActorCodeID,
			builtin.MultisigActorCodeID,
			builtin.AccountActorCodeID,
			exitcode.ErrIllegalArgument,
		}, {"fails if sender (from) is not account actor",
			newPaychAddr,
			builtin.InitActorCodeID,
			builtin.MultisigActorCodeID,
			builtin.AccountActorCodeID,
			exitcode.ErrIllegalArgument,
		}, {"fails if addr is not ID type",
			tutil.NewActorAddr(t, "beach blanket babylon"),
			builtin.InitActorCodeID,
			builtin.MultisigActorCodeID,
			builtin.AccountActorCodeID,
			exitcode.ErrIllegalArgument,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			builder := mock.NewBuilder(ctx, newPaychAddr).
				WithCaller(callerAddr, tc.callerCode).
				WithActorType(newPaychAddr, tc.newActorCode).
				WithActorType(payerAddr, tc.payerCode)
			rt := builder.Build(t)
			rt.ExpectValidateCallerType(builtin.InitActorCodeID)
			rt.ExpectAbort(tc.expExitCode, func() {
				rt.Call(actor.Constructor, &ConstructorParams{To: newPaychAddr})
			})
		})
	}
}

func TestPaymentChannelActor_CreateLaneSuccess(t *testing.T) {
	ctx := context.Background()
	actor := pcActorHarness{Actor{}, t}

	newPaychAddr := tutil.NewIDAddr(t, 100)
	callerAddr := tutil.NewIDAddr(t, 101)
	payerAddr := tutil.NewIDAddr(t, 102)
	balance := abi.NewTokenAmount(100)
	received := abi.NewTokenAmount(0)
	lane := uint64(999)
	nonce := uint64(1)

	versig := func(sig crypto.Signature, signer addr.Address, plaintext []byte) bool { return true }
	hasher := func(data []byte) [8]byte {
		return [8]byte{}
	}

	builder := mock.NewBuilder(ctx, newPaychAddr).
		WithBalance(balance, received).
		WithEpoch(abi.ChainEpoch(2)).
		WithCaller(callerAddr, builtin.InitActorCodeID).
		WithActorType(newPaychAddr, builtin.AccountActorCodeID).
		WithActorType(payerAddr, builtin.AccountActorCodeID).
		WithVerifiesSig(versig).
		WithHasher(hasher)

	// set up trivial signature verifier and hasher
	t.Run("Can add a lane/update actor state", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt, payerAddr, newPaychAddr)
		amt := big.NewInt(10)
		sig := &crypto.Signature{Type: crypto.SigTypeBLS, Data: []byte("doesn't matter")}
		tl := abi.ChainEpoch(1)
		sv := SignedVoucher{TimeLock: tl, Lane: lane, Nonce: nonce, Amount: amt, Signature: sig}
		ucp := &UpdateChannelStateParams{Sv: sv}

		// payerAddr updates state, creates a lane
		rt.SetCaller(payerAddr, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(payerAddr, newPaychAddr)
		constructRet := rt.Call(actor.UpdateChannelState, ucp).(*adt.EmptyValue)
		assert.Equal(t, adt.EmptyValue{}, *constructRet)
		rt.Verify()

		var st State
		rt.GetState(&st)
		assert.Equal(t, newPaychAddr, st.To)
		assert.Equal(t, payerAddr, st.From)
		assert.Len(t, st.LaneStates, 1)
		ls := st.LaneStates[0]
		assert.Equal(t, amt, ls.Redeemed)
		assert.Equal(t, nonce, ls.Nonce)
		assert.Equal(t, lane, ls.ID)
	})
}

func TestPaymentChannelActor_CreateLaneFailure(t *testing.T) {
	ctx := context.Background()
	actor := pcActorHarness{Actor{}, t}

	initActorAddr := tutil.NewIDAddr(t, 100)
	newPaychAddr := tutil.NewIDAddr(t, 101)
	payerAddr := tutil.NewIDAddr(t, 102)

	sig := &crypto.Signature{Type: crypto.SigTypeBLS, Data: []byte("doesn't matter")}

	testCases := []struct {
		desc       string
		targetCode cid.Cid

		balance  int64
		received int64
		epoch    int64

		tl    int64
		lane  uint64
		nonce uint64
		amt   int64

		secretPreimage []byte
		sig            *crypto.Signature
		verifySig      bool
		expExitCode    exitcode.ExitCode
	}{
		{desc: "fails if balance too low", targetCode: builtin.AccountActorCodeID,
			amt: 10, epoch: 1, tl: 1,
			sig: sig, verifySig: true,
			expExitCode: exitcode.ErrIllegalState},
		{desc: "fails if new send balance is negative", targetCode: builtin.AccountActorCodeID,
			amt: -1, epoch: 1, tl: 1,
			sig: sig, verifySig: true,
			expExitCode: exitcode.ErrIllegalState},
		{desc: "fails if signature not valid", targetCode: builtin.AccountActorCodeID,
			amt: 1, epoch: 1, tl: 1,
			sig: nil, verifySig: true,
			expExitCode: exitcode.ErrIllegalArgument},
		{desc: "fails if too early for voucher", targetCode: builtin.AccountActorCodeID,
			amt: 1, epoch: 1, tl: 10,
			sig: sig, verifySig: true,
			expExitCode: exitcode.ErrIllegalArgument},
		{desc: "fails if signature not verified", targetCode: builtin.AccountActorCodeID,
			amt: 1, epoch: 1, tl: 1, sig: sig, verifySig: false,
			expExitCode: exitcode.ErrIllegalArgument},
		{desc: "fails if SigningBytes fails", targetCode: builtin.AccountActorCodeID,
			amt: 1, epoch: 1, tl: 1, sig: sig, verifySig: true,
			secretPreimage: make([]byte, 2<<21),
			expExitCode:    exitcode.ErrIllegalArgument},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			versig := func(sig crypto.Signature, signer addr.Address, plaintext []byte) bool { return tc.verifySig }
			hasher := func(data []byte) []byte { return data }

			builder := mock.NewBuilder(ctx, newPaychAddr).
				WithBalance(abi.NewTokenAmount(9), abi.NewTokenAmount(tc.received)).
				WithEpoch(abi.ChainEpoch(tc.epoch)).
				WithCaller(initActorAddr, builtin.InitActorCodeID).
				WithActorType(newPaychAddr, builtin.AccountActorCodeID).
				WithActorType(payerAddr, builtin.AccountActorCodeID).
				WithVerifiesSig(versig).
				WithHasher(hasher)

			rt := builder.Build(t)
			actor.constructAndVerify(rt, payerAddr, newPaychAddr)

			sv := SignedVoucher{
				TimeLock:       abi.ChainEpoch(tc.tl),
				Lane:           tc.lane,
				Nonce:          tc.nonce,
				Amount:         big.NewInt(tc.amt),
				Signature:      tc.sig,
				SecretPreimage: tc.secretPreimage,
			}
			ucp := &UpdateChannelStateParams{Sv: sv}

			rt.SetCaller(payerAddr, tc.targetCode)
			rt.ExpectValidateCallerAddr(payerAddr, newPaychAddr)
			rt.ExpectAbort(tc.expExitCode, func() {
				rt.Call(actor.UpdateChannelState, ucp)
			})

			// verify no lane was created
			verifyInitialState(t, rt, payerAddr, newPaychAddr)
		})
	}
}
func TestActor_UpdateChannelStateRedeem(t *testing.T) {
	ctx := context.Background()

	t.Run("redeems voucher", func(t *testing.T) {
		rt := requireCreateChannelWithLane(t, ctx)
		var st1 State
		rt.GetState(&st1)

		// payerAddr updates the lane with "new" state
		rt.ExpectValidateCallerAddr(st1.From, st1.To)

		sv.Amount = big.NewInt(9)
		ucp.Sv = sv

		constructRet = rt.Call(actor.UpdateChannelState, ucp).(*adt.EmptyValue)
		assert.Equal(t, adt.EmptyValue{}, *constructRet)
		rt.Verify()

		var st2 State
		rt.GetState(&st2)
		assert.Equal(t, st1.From, st2.From)
		assert.Equal(t, st1.To, st2.To)
		assert.Equal(t, st1.MinSettleHeight, st2.MinSettleHeight)
		assert.Equal(t, st1.SettlingAt, st2.SettlingAt)

		expBI := big.Add(amt, sv.Amount)
		assert.Equal(t, expBI, st2.ToSend)
		assert.Len(t, st2.LaneStates, 2)
		lastLs := st2.LaneStates[1]

		assert.Equal(t, amt, lastLs.Redeemed)
	})

	t.Run("redeems voucher for correct lane", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt, payerAddr, newPaychAddr)

		amt := big.NewInt(10)
		lane := uint64(999)
		nonce := uint64(1)
		sig := &crypto.Signature{Type: crypto.SigTypeBLS, Data: []byte("doesn't matter")}
		tl := abi.ChainEpoch(1)
		sv := SignedVoucher{TimeLock: tl, Lane: lane, Nonce: nonce, Amount: amt, Signature: sig}

		ucp := &UpdateChannelStateParams{Sv: sv}
		// create another lane
		sv.Lane++
		ucp.Sv = sv
		rt.SetCaller(payerAddr, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(payerAddr, newPaychAddr)
		constructRet := rt.Call(actor.UpdateChannelState, ucp).(*adt.EmptyValue)
		assert.Equal(t, adt.EmptyValue{}, *constructRet)
		rt.Verify()

		var st1 State
		rt.GetState(&st1)
		assert.Equal(t, amt, st1.ToSend)
		assert.Len(t, st1.LaneStates, 1)
		// payerAddr updates the lane with new state
		rt.ExpectValidateCallerAddr(payerAddr, newPaychAddr)

		sv.Amount = big.NewInt(9)
		ucp.Sv = sv

		constructRet = rt.Call(actor.UpdateChannelState, ucp).(*adt.EmptyValue)
		assert.Equal(t, adt.EmptyValue{}, *constructRet)
		rt.Verify()

		var st2 State
		rt.GetState(&st2)

		// toSend updated to add new voucher amount
		toSendAmt := big.NewInt(0).Add(sv.Amount.Int,amt.Int)
		assert.Len(t, st2.LaneStates,2)
		assert.Equal(t, toSendAmt, st2.ToSend.Int)

		// payment channel To redeems the voucher for the new lane
		rt.ExpectValidateCallerAddr(payerAddr, newPaychAddr)
		constructRet = rt.Call(actor.UpdateChannelState, ucp).(*adt.EmptyValue)
		assert.Equal(t, adt.EmptyValue{}, *constructRet)
		rt.Verify()

		var st3 State
		rt.GetState(&st3)
		assert.Len(t, st2.LaneStates,2)

		// All lanes reflect total redeemed amount
		assert.Equal(t,sv.Amount, st3.LaneStates[1].Redeemed)
		assert.Equal(t, sv.Amount, st3.LaneStates[0].Redeemed)

		// toSend is now all 3 amounts added together
		toSendAmt = toSendAmt.Add(toSendAmt,sv.Amount.Int)
		assert.Equal(t, toSendAmt, st3.ToSend.Int)
	})
}

func TestActor_UpdateChannelStateMerge(t *testing.T)          {}
func TestActor_UpdateChannelStateExtra(t *testing.T)          {}
func TestActor_UpdateChannelStateSecretPreimage(t *testing.T) {}
func TestActor_Settle(t *testing.T)                           {

}
func TestActor_Collect(t *testing.T)                          {}

func TestActor_Exports(t *testing.T)                          {

}

type pcActorHarness struct {
	Actor
	t testing.TB
}

func requireCreateChannelWithLane(t *testing.T, ctx context.Context) (*mock.Runtime, SignedVoucher) {
	actor := pcActorHarness{Actor{}, t}

	newPaychAddr := tutil.NewIDAddr(t, 100)
	callerAddr := tutil.NewIDAddr(t, 101)
	payerAddr := tutil.NewIDAddr(t, 102)
	balance := abi.NewTokenAmount(100)
	received := abi.NewTokenAmount(0)
	lane := uint64(999)
	nonce := uint64(1)
	amt := big.NewInt(10)

	versig := func(sig crypto.Signature, signer addr.Address, plaintext []byte) bool { return true }
	hasher := func(data []byte) []byte { return data }

	builder := mock.NewBuilder(ctx, newPaychAddr).
		WithBalance(balance, received).
		WithEpoch(abi.ChainEpoch(2)).
		WithCaller(callerAddr, builtin.InitActorCodeID).
		WithActorType(newPaychAddr, builtin.AccountActorCodeID).
		WithActorType(payerAddr, builtin.AccountActorCodeID).
		WithVerifiesSig(versig).
		WithHasher(hasher)

	rt := builder.Build(t)
	actor.constructAndVerify(rt, payerAddr, newPaychAddr)

	sig := &crypto.Signature{Type: crypto.SigTypeBLS, Data: []byte("doesn't matter")}
	tl := abi.ChainEpoch(1)
	sv := SignedVoucher{TimeLock: tl, Lane: lane, Nonce: nonce, Amount: amt, Signature: sig}

	ucp := &UpdateChannelStateParams{Sv: sv}
	// payerAddr updates state, creates a lane
	rt.SetCaller(payerAddr, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(payerAddr, newPaychAddr)
	constructRet := rt.Call(actor.UpdateChannelState, ucp).(*adt.EmptyValue)
	require.Equal(t, adt.EmptyValue{}, *constructRet)
	rt.Verify()
	return rt, sv
}

func (h *pcActorHarness) constructAndVerify(rt *mock.Runtime, sender, receiver addr.Address) {
	params := &ConstructorParams{To: receiver, From: sender}

	rt.ExpectValidateCallerType(builtin.InitActorCodeID)
	constructRet := rt.Call(h.Actor.Constructor, params).(*adt.EmptyValue)
	assert.Equal(h.t, adt.EmptyValue{}, *constructRet)
	rt.Verify()

	var st State
	rt.GetState(&st)
	assert.Equal(h.t, receiver, st.To)
	assert.Equal(h.t, sender, st.From)
	assert.Equal(h.t, abi.NewTokenAmount(0), st.ToSend)
	assert.Equal(h.t, abi.ChainEpoch(0), st.SettlingAt)
	assert.Equal(h.t, abi.ChainEpoch(0), st.MinSettleHeight)
	assert.Len(h.t, st.LaneStates, 0)
}

func verifyInitialState(t *testing.T, rt *mock.Runtime, sender, receiver addr.Address) {
	var st State
	rt.GetState(&st)
	assert.Equal(t, receiver, st.To)
	assert.Equal(t, sender, st.From)
	assert.Equal(t, abi.NewTokenAmount(0), st.ToSend)
	assert.Equal(t, abi.ChainEpoch(0), st.SettlingAt)
	assert.Equal(t, abi.ChainEpoch(0), st.MinSettleHeight)
	assert.Len(t, st.LaneStates, 0)
}
