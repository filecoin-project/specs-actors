package power_test

import (
	"bytes"
	"context"
	"testing"

	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"

	addr "github.com/filecoin-project/go-address"
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	initact "github.com/filecoin-project/specs-actors/actors/builtin/init"
	power "github.com/filecoin-project/specs-actors/actors/builtin/power"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
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

	owner2 := tutil.NewIDAddr(t, 104)
	worker2 := tutil.NewIDAddr(t, 105)
	miner2 := tutil.NewIDAddr(t, 106)

	unused := tutil.NewIDAddr(t, 107)

	builder := mock.NewBuilder(context.Background(), powerActor).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("simple construction", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
	})

	t.Run("create miner", func(t *testing.T) {
		createMinerParams := &power.CreateMinerParams{
			Owner:      owner1,
			Worker:     worker1,
			SectorSize: abi.SectorSize(int64(32)),
			Peer:       "miner1",
		}
		initCreateMinerParams := &power.MinerConstructorParams{
			OwnerAddr:  owner1,
			WorkerAddr: worker1,
			SectorSize: abi.SectorSize(int64(32)),
			PeerId:     "miner1",
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
		createMinerBuf := new(bytes.Buffer)
		err := createMinerParams.MarshalCBOR(createMinerBuf)
		require.NoError(t, err)

		initCreateMinerBuf := new(bytes.Buffer)
		err = initCreateMinerParams.MarshalCBOR(initCreateMinerBuf)
		require.NoError(t, err)
		msgParams := &initact.ExecParams{
			CodeCID:           builtin.StorageMinerActorCodeID,
			ConstructorParams: initCreateMinerBuf.Bytes(),
		}
		rt.ExpectSend(builtin.InitActorAddr, builtin.MethodsInit.Exec, msgParams, abi.NewTokenAmount(0), &mock.ReturnWrapper{createMinerRet}, 0)

		rt.Call(actor.Actor.CreateMiner, createMinerParams)
		rt.Verify()

		var st power.State
		rt.GetState(&st)
		assert.Equal(t, int64(1), st.MinerCount)
		assert.Equal(t, abi.NewStoragePower(0), st.TotalNetworkPower)
		assert.Equal(t, int64(0), st.NumMinersMeetingMinPower)

		claim := adt.AsMap(adt.AsStore(rt), st.Claims)
		keys, err := claim.CollectKeys()
		require.NoError(t, err)
		assert.Equal(t, 1, len(keys))
		var actualClaim power.Claim
		found, err_ := claim.Get(asKey(keys[0]), &actualClaim)
		require.NoError(t, err_)
		assert.True(t, found)
		assert.Equal(t, power.Claim{big.Zero(), big.Zero()}, actualClaim) // miner has not proven anything

		escrowTable := adt.AsMap(adt.AsStore(rt), st.EscrowTable)
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

	t.Run("ensure cronevents scheduled in null rounds are executed on next block", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		createMiner := func(owner, worker, miner, robust addr.Address, peer peer.ID) {
			createMinerParams := &power.CreateMinerParams{
				Owner:      owner,
				Worker:     worker,
				SectorSize: abi.SectorSize(int64(32)),
				Peer:       peer,
			}
			initCreateMinerParams := &power.MinerConstructorParams{
				OwnerAddr:  owner,
				WorkerAddr: worker,
				SectorSize: abi.SectorSize(int64(32)),
				PeerId:     peer,
			}

			// owner send CreateMiner to Actor
			rt.SetCaller(owner, builtin.AccountActorCodeID)
			rt.SetReceived(abi.NewTokenAmount(1))
			rt.ExpectValidateCallerType(builtin.AccountActorCodeID, builtin.MultisigActorCodeID)

			createMinerRet := &power.CreateMinerReturn{
				IDAddress:     miner,  // miner actor id address
				RobustAddress: robust, // should be long miner actor address
			}
			createMinerBuf := new(bytes.Buffer)
			err := createMinerParams.MarshalCBOR(createMinerBuf)
			require.NoError(t, err)

			initCreateMinerBuf := new(bytes.Buffer)
			err = initCreateMinerParams.MarshalCBOR(initCreateMinerBuf)
			require.NoError(t, err)
			msgParams := &initact.ExecParams{
				CodeCID:           builtin.StorageMinerActorCodeID,
				ConstructorParams: initCreateMinerBuf.Bytes(),
			}
			rt.ExpectSend(builtin.InitActorAddr, builtin.MethodsInit.Exec, msgParams, abi.NewTokenAmount(0), &mock.ReturnWrapper{createMinerRet}, 0)
			rt.Call(actor.Actor.CreateMiner, createMinerParams)
		}

		createMiner(owner1, worker1, miner1, unused, "miner1")
		createMiner(owner2, worker2, miner2, unused, "miner2")

		rt.Verify()

		//  0 - genesis
		//  1 - block - registers events
		//  2 - null  - has event
		//  3 - null
		//  4 - block - has event

		enrollCronEventParams1 := &power.EnrollCronEventParams{
			EventEpoch: 2,
			Payload:    []byte{0x1, 0x3},
		}
		enrollCronEventParams2 := &power.EnrollCronEventParams{
			EventEpoch: 4,
			Payload:    []byte{0x2, 0x3},
		}

		rt.SetEpoch(1)

		rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)

		rt.SetCaller(miner1, builtin.StorageMinerActorCodeID)
		rt.Call(actor.Actor.EnrollCronEvent, enrollCronEventParams1)

		rt.ExpectValidateCallerType(builtin.StorageMinerActorCodeID)
		rt.SetCaller(miner2, builtin.StorageMinerActorCodeID)
		rt.Call(actor.Actor.EnrollCronEvent, enrollCronEventParams2)

		rt.Verify()

		rt.SetEpoch(4)
		rt.ExpectValidateCallerAddr(builtin.CronActorAddr)
		rt.ExpectSend(miner1, builtin.MethodsMiner.OnDeferredCronEvent, vmr.CBORBytes(enrollCronEventParams1.Payload), abi.NewTokenAmount(0), nil, 0)
		rt.ExpectSend(miner2, builtin.MethodsMiner.OnDeferredCronEvent, vmr.CBORBytes(enrollCronEventParams2.Payload), abi.NewTokenAmount(0), nil, 0)
		rt.SetCaller(builtin.CronActorAddr, builtin.CronActorCodeID)
		rt.Call(actor.Actor.OnEpochTickEnd, &adt.EmptyValue{})
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
	mapChecked := adt.AsMap(adt.AsStore(rt), cid)
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
	ret := rt.Call(h.Actor.Constructor, &adt.EmptyValue{})
	assert.Nil(h.t, ret)
	rt.Verify()

	var st power.State
	rt.GetState(&st)
	assert.Equal(h.t, abi.NewStoragePower(0), st.TotalNetworkPower)
	assert.Equal(h.t, int64(0), st.MinerCount)
	assert.Equal(h.t, int64(0), st.NumMinersMeetingMinPower)

	verifyEmptyMap(h.t, rt, st.EscrowTable)
	verifyEmptyMap(h.t, rt, st.Claims)
	verifyEmptyMap(h.t, rt, st.PoStDetectedFaultMiners)
	verifyEmptyMap(h.t, rt, st.CronEventQueue)
}
