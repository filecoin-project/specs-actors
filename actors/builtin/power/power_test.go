package power_test

import (
	"bytes"
	"context"
	"testing"

	cid "github.com/ipfs/go-cid"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	initact "github.com/filecoin-project/specs-actors/actors/builtin/init"
	power "github.com/filecoin-project/specs-actors/actors/builtin/power"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
	mock "github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

func TestConstruction(t *testing.T) {
	actor := spActorHarness{power.Actor{}, t}
	powerActor := tutil.NewIDAddr(t, 100)
	owner1 := tutil.NewIDAddr(t, 101)
	worker1 := tutil.NewIDAddr(t, 102)
	miner1 := tutil.NewIDAddr(t, 103)
	unused := tutil.NewIDAddr(t, 104)

	builder := mock.NewBuilder(context.Background(), powerActor).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("simple construction", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
	})

	t.Run("create miner", func(t *testing.T) {
		createMinerParams := &power.CreateMinerParams{
			Worker:     worker1,
			SectorSize: abi.SectorSize(int64(32)),
			Peer:       "miner1",
		}

		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// owner1 send CreateMiner to Actor
		rt.SetCaller(owner1, builtin.AccountActorCodeID)
		rt.SetReceived(abi.NewTokenAmount(1))
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)

		createMinerRet := &power.CreateMinerReturn{
			IDAddress:     miner1, // miner actor id address
			RobustAddress: unused, // should be long miner actor address
		}
		var ctorParamBytes []byte
		err := createMinerParams.MarshalCBOR(bytes.NewBuffer(ctorParamBytes))
		require.NoError(t, err)
		msgParams := &initact.ExecParams{
			CodeCID:           builtin.StorageMinerActorCodeID,
			ConstructorParams: ctorParamBytes,
		}
		rt.ExpectSend(builtin.InitActorAddr, builtin.MethodsInit.Exec, msgParams, abi.NewTokenAmount(0), &mock.ReturnWrapper{createMinerRet}, 0)
		rt.Call(actor.Actor.CreateMiner, createMinerParams)
		rt.Verify()

		var st power.State
		rt.GetState(&st)
		assert.Equal(t, int64(1), st.MinerCount)
		assert.Equal(t, abi.NewStoragePower(0), st.TotalNetworkPower)
		assert.Equal(t, int64(0), st.NumMinersMeetingMinPower)

		claimedPower := adt.AsMap(rt.Store(), st.ClaimedPower)
		keys, err := claimedPower.CollectKeys()
		require.NoError(t, err)
		assert.Equal(t, 1, len(keys))
		var actualClaimedPower abi.StoragePower
		found, err_ := claimedPower.Get(asKey(keys[0]), &actualClaimedPower)
		require.NoError(t, err_)
		assert.True(t, found)
		assert.Equal(t, abi.NewStoragePower(0), actualClaimedPower) // miner has not proven anything

		escrowTable := adt.AsMap(rt.Store(), st.EscrowTable)
		keys, err = escrowTable.CollectKeys()
		require.NoError(t, err)
		assert.Equal(t, 1, len(keys))
		var pledgeCollateral abi.TokenAmount
		found, err_ = escrowTable.Get(asKey(keys[0]), &pledgeCollateral)
		require.NoError(t, err_)
		assert.True(t, found)
		assert.Equal(t, abi.NewTokenAmount(1), pledgeCollateral) // miner has 1 FIL in EscrowTable

		verifyEmptyMap(t, rt, st.PoStDetectedFaultMiners)
		verifyEmptyMap(t, rt, st.CronEventQueue)
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
	mapChecked := adt.AsMap(rt.Store(), cid)
	keys, err := mapChecked.CollectKeys()
	require.NoError(t, err)
	assert.Empty(t, keys)
}

type spActorHarness struct {
	power.Actor
	t testing.TB
}

func (s key) Key() string {
	return string(s)
}

func (h *spActorHarness) constructAndVerify(rt *mock.Runtime) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	constructRet := rt.Call(h.Actor.Constructor, &adt.EmptyValue{}).(*adt.EmptyValue)
	assert.Equal(h.t, adt.EmptyValue{}, *constructRet)
	rt.Verify()

	var st power.State
	rt.GetState(&st)
	assert.Equal(h.t, abi.NewStoragePower(0), st.TotalNetworkPower)
	assert.Equal(h.t, int64(0), st.MinerCount)
	assert.Equal(h.t, int64(0), st.NumMinersMeetingMinPower)

	verifyEmptyMap(h.t, rt, st.EscrowTable)
	verifyEmptyMap(h.t, rt, st.ClaimedPower)
	verifyEmptyMap(h.t, rt, st.PoStDetectedFaultMiners)
	verifyEmptyMap(h.t, rt, st.CronEventQueue)
}
