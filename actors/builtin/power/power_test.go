package power_test

import (
	"bytes"
	"context"
	"testing"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	initact "github.com/filecoin-project/specs-actors/actors/builtin/init"
	power "github.com/filecoin-project/specs-actors/actors/builtin/power"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
	mock "github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

func TestExports(t *testing.T) {
	mock.CheckActorExports(t, power.Actor{})
}

func TestConstruction(t *testing.T) {
	actor := newHarness(t)
	owner := tutil.NewIDAddr(t, 101)
	miner := tutil.NewIDAddr(t, 103)
	actr := tutil.NewActorAddr(t, "actor")

	builder := mock.NewBuilder(context.Background(), builtin.StoragePowerActorAddr).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("simple construction", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
	})

	t.Run("create miner", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		actor.createMiner(rt, owner, owner, miner, actr, abi.PeerID("miner"), []abi.Multiaddrs{{1}}, abi.RegisteredSealProof_StackedDrg2KiBV1, abi.NewTokenAmount(10))

		var st power.State
		rt.GetState(&st)
		assert.Equal(t, int64(1), st.MinerCount)
		assert.Equal(t, abi.NewStoragePower(0), st.TotalQualityAdjPower)
		assert.Equal(t, abi.NewStoragePower(0), st.TotalRawBytePower)
		assert.Equal(t, int64(0), st.NumMinersMeetingMinPower)

		claim, err := adt.AsMap(adt.AsStore(rt), st.Claims)
		assert.NoError(t, err)
		keys, err := claim.CollectKeys()
		require.NoError(t, err)
		assert.Equal(t, 1, len(keys))
		var actualClaim power.Claim
		found, err_ := claim.Get(asKey(keys[0]), &actualClaim)
		require.NoError(t, err_)
		assert.True(t, found)
		assert.Equal(t, power.Claim{big.Zero(), big.Zero()}, actualClaim) // miner has not proven anything

		verifyEmptyMap(t, rt, st.CronEventQueue)
	})
}

func TestCron(t *testing.T) {
	actor := newHarness(t)
	miner1 := tutil.NewIDAddr(t, 101)
	miner2 := tutil.NewIDAddr(t, 102)

	builder := mock.NewBuilder(context.Background(), builtin.StoragePowerActorAddr).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("calls reward actor", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		expectedPower := big.NewInt(0)
		rt.SetEpoch(1)
		rt.ExpectValidateCallerAddr(builtin.CronActorAddr)
		rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.UpdateNetworkKPI, &expectedPower, abi.NewTokenAmount(0), nil, 0)
		rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)
		rt.Call(actor.Actor.OnEpochTickEnd, nil)
		rt.Verify()
	})

	t.Run("event scheduled in null round called next round", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		//  0 - genesis
		//  1 - block - registers events
		//  2 - null  - has event
		//  3 - null
		//  4 - block - has event

		rt.SetEpoch(1)
		actor.enrollCronEvent(rt, miner1, 2, []byte{0x1, 0x3})
		actor.enrollCronEvent(rt, miner2, 4, []byte{0x2, 0x3})

		expectedRawBytePower := big.NewInt(0)
		rt.SetEpoch(4)
		rt.ExpectValidateCallerAddr(builtin.CronActorAddr)
		rt.ExpectSend(miner1, builtin.MethodsMiner.OnDeferredCronEvent, vmr.CBORBytes([]byte{0x1, 0x3}), big.Zero(), nil, exitcode.Ok)
		rt.ExpectSend(miner2, builtin.MethodsMiner.OnDeferredCronEvent, vmr.CBORBytes([]byte{0x2, 0x3}), big.Zero(), nil, exitcode.Ok)
		rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.UpdateNetworkKPI, &expectedRawBytePower, big.Zero(), nil, exitcode.Ok)
		rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)
		rt.Call(actor.Actor.OnEpochTickEnd, nil)
		rt.Verify()
	})

	t.Run("handles failed call", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.SetEpoch(1)
		actor.enrollCronEvent(rt, miner1, 2, []byte{})
		actor.enrollCronEvent(rt, miner2, 2, []byte{})

		expectedPower := big.NewInt(0)
		rt.SetEpoch(2)
		rt.ExpectValidateCallerAddr(builtin.CronActorAddr)
		// First send fails
		rt.ExpectSend(miner1, builtin.MethodsMiner.OnDeferredCronEvent, vmr.CBORBytes([]byte{}), big.Zero(), nil, exitcode.ErrIllegalState)
		// Subsequent one still invoked
		rt.ExpectSend(miner2, builtin.MethodsMiner.OnDeferredCronEvent, vmr.CBORBytes([]byte{}), big.Zero(), nil, exitcode.Ok)
		// Reward actor still invoked
		rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.UpdateNetworkKPI, &expectedPower, big.Zero(), nil, exitcode.Ok)
		rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)
		rt.Call(actor.Actor.OnEpochTickEnd, nil)
		rt.Verify()

		// Next epoch, only the reward actor is invoked
		rt.SetEpoch(3)
		rt.ExpectValidateCallerAddr(builtin.CronActorAddr)
		rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.UpdateNetworkKPI, &expectedPower, big.Zero(), nil, exitcode.Ok)
		rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)
		rt.Call(actor.Actor.OnEpochTickEnd, nil)
		rt.Verify()
	})
}

//
// Misc. Utility Functions
//

type key string

func asKey(in string) adt.Keyer {
	return key(in)
}

func verifyEmptyMap(t testing.TB, rt *mock.Runtime, cid cid.Cid) {
	mapChecked, err := adt.AsMap(adt.AsStore(rt), cid)
	assert.NoError(t, err)
	keys, err := mapChecked.CollectKeys()
	require.NoError(t, err)
	assert.Empty(t, keys)
}

type spActorHarness struct {
	power.Actor
	t testing.TB
}

func newHarness(t testing.TB) *spActorHarness {
	return &spActorHarness{
		Actor: power.Actor{},
		t:     t,
	}
}

func (h *spActorHarness) constructAndVerify(rt *mock.Runtime) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	ret := rt.Call(h.Actor.Constructor, nil)
	assert.Nil(h.t, ret)
	rt.Verify()

	var st power.State
	rt.GetState(&st)
	assert.Equal(h.t, abi.NewStoragePower(0), st.TotalRawBytePower)
	assert.Equal(h.t, abi.NewStoragePower(0), st.TotalQualityAdjPower)
	assert.Equal(h.t, int64(0), st.MinerCount)
	assert.Equal(h.t, int64(0), st.NumMinersMeetingMinPower)

	verifyEmptyMap(h.t, rt, st.Claims)
	verifyEmptyMap(h.t, rt, st.CronEventQueue)
}

func (h *spActorHarness) createMiner(rt *mock.Runtime, owner, worker, miner, robust addr.Address, peer abi.PeerID,
	multiaddrs []abi.Multiaddrs, sealProofType abi.RegisteredSealProof, value abi.TokenAmount) {
	createMinerParams := &power.CreateMinerParams{
		Owner:         owner,
		Worker:        worker,
		SealProofType: sealProofType,
		Peer:          peer,
		Multiaddrs:    multiaddrs,
	}

	// owner send CreateMiner to Actor
	rt.SetCaller(owner, builtin.AccountActorCodeID)
	rt.SetReceived(value)
	rt.SetBalance(value)
	rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)

	createMinerRet := &power.CreateMinerReturn{
		IDAddress:     miner,  // miner actor id address
		RobustAddress: robust, // should be long miner actor address
	}

	msgParams := &initact.ExecParams{
		CodeCID:           builtin.StorageMinerActorCodeID,
		ConstructorParams: initCreateMinerBytes(h.t, owner, worker, peer, multiaddrs, sealProofType),
	}
	rt.ExpectSend(builtin.InitActorAddr, builtin.MethodsInit.Exec, msgParams, value, createMinerRet, 0)
	rt.Call(h.Actor.CreateMiner, createMinerParams)
	rt.Verify()
}

func (h *spActorHarness) enrollCronEvent(rt *mock.Runtime, miner addr.Address, epoch abi.ChainEpoch, payload []byte) {
	rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)
	rt.SetCaller(miner, builtin.StorageMinerActorCodeID)
	rt.Call(h.Actor.EnrollCronEvent, &power.EnrollCronEventParams{
		EventEpoch: epoch,
		Payload:    payload,
	})
	rt.Verify()
}

func initCreateMinerBytes(t testing.TB, owner, worker addr.Address, peer abi.PeerID, multiaddrs []abi.Multiaddrs, sealProofType abi.RegisteredSealProof) []byte {
	params := &power.MinerConstructorParams{
		OwnerAddr:     owner,
		WorkerAddr:    worker,
		SealProofType: sealProofType,
		PeerId:        peer,
		Multiaddrs:    multiaddrs,
	}

	buf := new(bytes.Buffer)
	require.NoError(t, params.MarshalCBOR(buf))
	return buf.Bytes()
}

func (s key) Key() string {
	return string(s)
}
