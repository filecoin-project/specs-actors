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
	rt, _, sv := requireCreateChannelWithLanes(t, ctx, 1)
	var st State
	rt.GetState(&st)
	assert.Len(t, st.LaneStates, 1)
	ls := st.LaneStates[0]
	assert.Equal(t, sv.Amount, ls.Redeemed)
	assert.Equal(t, sv.Nonce, ls.Nonce)
	assert.Equal(t, sv.Lane, ls.ID)
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

	t.Run("redeeming voucher updates correctly with one lane", func(t *testing.T) {
		rt, actor, sv := requireCreateChannelWithLanes(t, ctx, 1)
		var st1 State
		rt.GetState(&st1)

		newVoucherAmt := big.NewInt(9)
		ucp := &UpdateChannelStateParams{Sv: *sv}
		ucp.Sv.Amount = newVoucherAmt

		// Sending to same lane updates the lane with "new" state
		rt.ExpectValidateCallerAddr(st1.From, st1.To)
		constructRet := rt.Call(actor.UpdateChannelState, ucp).(*adt.EmptyValue)
		require.Equal(t, adt.EmptyValue{}, *constructRet)
		rt.Verify()

		var st2 State
		rt.GetState(&st2)
		newLs := st2.LaneStates[0]

		assert.Equal(t, st1.From, st2.From)
		assert.Equal(t, st1.To, st2.To)
		assert.Equal(t, st1.MinSettleHeight, st2.MinSettleHeight)
		assert.Equal(t, st1.SettlingAt, st2.SettlingAt)
		assert.Equal(t, newLs.Redeemed, st2.ToSend)
		assert.Equal(t, newVoucherAmt, st2.ToSend)
	})

	t.Run("redeems voucher for correct lane", func(t *testing.T) {
		rt, actor, sv := requireCreateChannelWithLanes(t, ctx, 3)
		var st1, st2 State
		rt.GetState(&st1)

		initialAmt := st1.ToSend

		newVoucherAmt := big.NewInt(9)
		ucp := &UpdateChannelStateParams{Sv: *sv}
		ucp.Sv.Amount = newVoucherAmt
		ucp.Sv.Lane = 1
		lsToUpdate := st1.LaneStates[ucp.Sv.Lane]
		ucp.Sv.Nonce = lsToUpdate.Nonce + 1

		// Sending to same lane updates the lane with "new" state
		rt.ExpectValidateCallerAddr(st1.From, st1.To)
		constructRet := rt.Call(actor.UpdateChannelState, ucp).(*adt.EmptyValue)
		require.Equal(t, adt.EmptyValue{}, *constructRet)
		rt.Verify()

		rt.GetState(&st2)
		lUpdated := st2.LaneStates[ucp.Sv.Lane]

		bDelta := big.Sub(ucp.Sv.Amount, lsToUpdate.Redeemed)
		expToSend := big.Add(initialAmt, bDelta)
		assert.Equal(t, expToSend, st2.ToSend)
		assert.Equal(t, ucp.Sv.Amount, lUpdated.Redeemed)
		assert.Equal(t, ucp.Sv.Nonce, lUpdated.Nonce)
	})
}

func TestActor_UpdateChannelStateMergeSuccess(t *testing.T) {
	// Check that a lane merge correctly updates lane states
	numLanes := 3
	rt, actor, sv := requireCreateChannelWithLanes(t, context.Background(), numLanes)
	var st1 State

	rt.GetState(&st1)
	rt.SetCaller(st1.From, builtin.AccountActorCodeID)

	var st2 State
	mergeTo := st1.LaneStates[0]
	mergeFrom := st1.LaneStates[1]

	// Note sv.Amount = 4
	sv.Lane = mergeTo.ID
	mergeNonce := mergeTo.Nonce + 10

	merges := []Merge{{Lane: mergeFrom.ID, Nonce: mergeNonce}}
	sv.Merges = merges

	ucp := &UpdateChannelStateParams{Sv: *sv}
	rt.ExpectValidateCallerAddr(st1.From, st1.To)
	_ = rt.Call(actor.UpdateChannelState, ucp).(*adt.EmptyValue)
	rt.Verify()

	rt.GetState(&st2)
	newMergeTo := st2.LaneStates[0]
	newMergeFrom := st2.LaneStates[1]
	require.NotNil(t, newMergeTo)
	require.NotNil(t, newMergeFrom)

	assert.Equal(t, int(mergeNonce), int(newMergeFrom.Nonce))
	assert.Equal(t, mergeFrom.Redeemed, newMergeFrom.Redeemed)
	assert.Equal(t, int(sv.Nonce), int(newMergeTo.Nonce))
	assert.Equal(t, sv.Amount, newMergeTo.Redeemed)

	redeemed := big.Add(mergeFrom.Redeemed, mergeTo.Redeemed)
	expDelta := big.Sub(sv.Amount, redeemed)
	expSendAmt := big.Add(st1.ToSend, expDelta)
	assert.Equal(t, expSendAmt, st2.ToSend)
	assert.Len(t, st2.LaneStates, numLanes)
}

func TestActor_UpdateChannelStateMergeFailure(t *testing.T) {
	testCases := []struct {
		name                           string
		balance                        int64
		lane, voucherNonce, mergeNonce uint64
		expExitCode                    exitcode.ExitCode
	}{
		{
			name: "fails: merged lane in voucher has outdated nonce, cannot redeem",
			lane: 1, voucherNonce: 10, mergeNonce: 1,
			expExitCode: exitcode.ErrIllegalArgument,
		},
		{
			name: "fails: voucher has an outdated nonce, cannot redeem",
			lane: 1, voucherNonce: 0, mergeNonce: 10,
			expExitCode: exitcode.ErrIllegalArgument,
		},
		{
			name: "fails: not enough funds in channel to cover voucher",
			lane: 1, balance: 1, voucherNonce: 10, mergeNonce: 10,
			expExitCode: exitcode.ErrIllegalState,
		},
		{
			name: "fails: voucher cannot merge lanes into its own lane",
			lane: 0, balance: 1, voucherNonce: 10, mergeNonce: 10,
			expExitCode: exitcode.ErrIllegalArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rt, actor, sv := requireCreateChannelWithLanes(t, context.Background(), 2)
			if tc.balance > 0 {
				rt.SetBalance(abi.NewTokenAmount(tc.balance))
			}

			var st1 State
			rt.GetState(&st1)
			mergeTo := st1.LaneStates[0]
			mergeFrom := st1.LaneStates[tc.lane]

			sv.Lane = mergeTo.ID
			sv.Nonce = tc.voucherNonce
			merges := []Merge{{Lane: mergeFrom.ID, Nonce: tc.mergeNonce}}
			sv.Merges = merges
			ucp := &UpdateChannelStateParams{Sv: *sv}

			rt.SetCaller(st1.From, builtin.AccountActorCodeID)
			rt.ExpectValidateCallerAddr(st1.From, st1.To)
			rt.ExpectAbort(tc.expExitCode, func() {
				rt.Call(actor.UpdateChannelState, ucp)
			})

		})
	}
}

func TestActor_UpdateChannelStateExtra(t *testing.T)          {}
func TestActor_UpdateChannelStateSecretPreimage(t *testing.T) {}
func TestActor_Settle(t *testing.T)                           {}
func TestActor_Collect(t *testing.T)                          {}

func TestActor_Exports(t *testing.T) {}

type pcActorHarness struct {
	Actor
	t testing.TB
}

type laneParams struct {
	epochNum    int64
	from, to    addr.Address
	amt         big.Int
	lane, nonce uint64
}

func requireCreateChannelWithLanes(t *testing.T, ctx context.Context, numLanes int) (*mock.Runtime, *pcActorHarness, *SignedVoucher) {
	actor := pcActorHarness{Actor{}, t}

	newPaychAddr := tutil.NewIDAddr(t, 100)
	callerAddr := tutil.NewIDAddr(t, 101)
	payerAddr := tutil.NewIDAddr(t, 102)
	balance := abi.NewTokenAmount(100)
	received := abi.NewTokenAmount(0)
	curEpoch := 2

	versig := func(sig crypto.Signature, signer addr.Address, plaintext []byte) bool { return true }
	hasher := func(data []byte) []byte { return data }

	builder := mock.NewBuilder(ctx, newPaychAddr).
		WithBalance(balance, received).
		WithEpoch(abi.ChainEpoch(curEpoch)).
		WithCaller(callerAddr, builtin.InitActorCodeID).
		WithActorType(newPaychAddr, builtin.AccountActorCodeID).
		WithActorType(payerAddr, builtin.AccountActorCodeID).
		WithVerifiesSig(versig).
		WithHasher(hasher)

	rt := builder.Build(t)
	actor.constructAndVerify(rt, payerAddr, newPaychAddr)
	verifyInitialState(t, rt, payerAddr, newPaychAddr)

	var lastSv *SignedVoucher
	for i := 0; i < numLanes; i++ {
		amt := big.NewInt(int64(i + 1))
		lastSv = requireAddNewLane(t, rt, &actor, laneParams{
			epochNum: int64(curEpoch),
			from:     payerAddr,
			to:       newPaychAddr,
			amt:      amt,
			lane:     uint64(i),
			nonce:    uint64(i + 1),
		})
	}
	return rt, &actor, lastSv
}

func requireAddNewLane(t *testing.T, rt *mock.Runtime, actor *pcActorHarness, params laneParams) *SignedVoucher {
	sig := &crypto.Signature{Type: crypto.SigTypeBLS, Data: []byte("doesn't matter")}
	tl := abi.ChainEpoch(params.epochNum)
	sv := SignedVoucher{TimeLock: tl, Lane: params.lane, Nonce: params.nonce, Amount: params.amt, Signature: sig}
	ucp := &UpdateChannelStateParams{Sv: sv}

	rt.SetCaller(params.from, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(params.from, params.to)
	constructRet := rt.Call(actor.UpdateChannelState, ucp).(*adt.EmptyValue)
	require.Equal(t, adt.EmptyValue{}, *constructRet)
	rt.Verify()
	return &sv
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
