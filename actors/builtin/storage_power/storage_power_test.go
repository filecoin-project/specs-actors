package storage_power_test

import (
	"context"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/storage_power"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	"github.com/stretchr/testify/assert"
)

func TestConstruction(t *testing.T) {
	actor := spActorHarness{storage_power.StoragePowerActor{}, t}
	powerActor := newIDAddr(t, 100)
	owner1 := newIDAddr(t, 101)
	worker1 := newIDAddr(t, 102)
	miner1 := newIDAddr(t, 103)
	unused := newIDAddr(t, 104)

	builder := mock.NewBuilder(context.Background(), powerActor).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("simple construction", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
	})

	t.Run("create miner", func(t *testing.T) {
		createMinerParams := &storage_power.CreateMinerParams{
			Worker:     worker1,
			SectorSize: abi.SectorSize(int64(32)),
			Peer:       "miner1",
		}

		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// owner1 send CreateMiner to StoragePowerActor
		rt.SetCaller(owner1, builtin.AccountActorCodeID)
		rt.SetReceived(abi.NewTokenAmount(1))
		rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)

		createMinerRet := &storage_power.CreateMinerReturn{
			IDAddress:     miner1,     // miner actor id address
			RobustAddress: unused, // should be long miner actor address
		}
		rt.ExpectSend(owner1, builtin.MethodSend, createMinerParams, abi.NewTokenAmount(10), &mock.ReturnWrapper{createMinerRet}, 0)
		rt.Call(actor.StoragePowerActor.CreateMiner, createMinerParams)
		rt.Verify()

		var st storage_power.StoragePowerActorState
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

func newIDAddr(t *testing.T, id uint64) addr.Address {
	address, err := addr.NewIDAddress(id)
	if err != nil {
		t.Fatal(err)
	}
	return address
}

func verifyEmptyMap(t testing.TB, rt *mock.Runtime, cid cid.Cid) {
	mapChecked := adt.AsMap(rt.Store(), cid)
	keys, err := mapChecked.CollectKeys()
	require.NoError(t, err)
	assert.Empty(t, keys)
}

type spActorHarness struct {
	storage_power.StoragePowerActor
	t testing.TB
}

func (s key) Key() string {
	return string(s)
}

func (h *spActorHarness) constructAndVerify(rt *mock.Runtime) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	constructRet := rt.Call(h.StoragePowerActor.Constructor, &adt.EmptyValue{}).(*adt.EmptyValue)
	assert.Equal(h.t, adt.EmptyValue{}, *constructRet)
	rt.Verify()

	var st storage_power.StoragePowerActorState
	rt.GetState(&st)
	assert.Equal(h.t, abi.NewStoragePower(0), st.TotalNetworkPower)
	assert.Equal(h.t, int64(0), st.MinerCount)
	assert.Equal(h.t, int64(0), st.NumMinersMeetingMinPower)

	verifyEmptyMap(h.t, rt, st.EscrowTable)
	verifyEmptyMap(h.t, rt, st.ClaimedPower)
	verifyEmptyMap(h.t, rt, st.PoStDetectedFaultMiners)
	verifyEmptyMap(h.t, rt, st.CronEventQueue)
}
