package init_test

import (
	"context"
	"testing"

	cid "github.com/ipfs/go-cid"
	assert "github.com/stretchr/testify/assert"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	init_ "github.com/filecoin-project/specs-actors/actors/builtin/init"
	runtime "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
	mock "github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

func TestConstructor(t *testing.T) {
	actor := initHarness{init_.Actor{}, t}

	receiver := tutil.NewIDAddr(t, 1000)
	builder := mock.NewBuilder(context.Background(), receiver).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)
	rt := builder.Build(t)
	actor.constructAndVerify(rt)
}

func TestExec(t *testing.T) {
	actor := initHarness{init_.Actor{}, t}

	receiver := tutil.NewIDAddr(t, 1000)
	anne := tutil.NewIDAddr(t, 1001)
	builder := mock.NewBuilder(context.Background(), receiver).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("abort actors that cannot call exec", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.execAndVerify(rt, builtin.AccountActorCodeID, []byte{})
		})
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.execAndVerify(rt, cid.Undef, []byte{})
		})
	})

	var fakeParams = runtime.CBORBytes([]byte{'D', 'E', 'A', 'D', 'B', 'E', 'E', 'F'})
	var balance = abi.NewTokenAmount(100)

	t.Run("happy path exec create 2 payment channels", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt)
		// anne execs a payment channel actor with 100 FIL.
		rt.SetCaller(anne, builtin.AccountActorCodeID)

		rt.SetBalance(balance)
		rt.SetReceived(balance)

		// re-org-stable address of the payment channel actor
		uniqueAddr1 := tutil.NewActorAddr(t, "paych")
		rt.SetNewActorAddress(uniqueAddr1)

		// next id address
		expectedIdAddr1 := tutil.NewIDAddr(t, 100)
		rt.ExpectCreateActor(builtin.PaymentChannelActorCodeID, expectedIdAddr1)

		// expect anne creating a payment channel to trigger a send to the payment channels constructor
		rt.ExpectSend(expectedIdAddr1, builtin.MethodConstructor, fakeParams, balance, nil, exitcode.Ok)
		execRet1 := actor.execAndVerify(rt, builtin.PaymentChannelActorCodeID, fakeParams)
		assert.Equal(t, uniqueAddr1, execRet1.RobustAddress)
		assert.Equal(t, expectedIdAddr1, execRet1.IDAddress)

		var st init_.State
		rt.GetState(&st)
		actualIdAddr, err := st.ResolveAddress(rt.Store(), uniqueAddr1)
		assert.NoError(t, err)
		assert.Equal(t, expectedIdAddr1, actualIdAddr)

		// creating another actor should get a different address, the below logic is a repeat of the above to insure
		// the next ID address created is incremented. 100 -> 101
		rt.SetBalance(balance)
		rt.SetReceived(balance)
		uniqueAddr2 := tutil.NewActorAddr(t, "paych2")
		rt.SetNewActorAddress(uniqueAddr2)
		// the incremented ID address.
		expectedIdAddr2 := tutil.NewIDAddr(t, 101)
		rt.ExpectCreateActor(builtin.PaymentChannelActorCodeID, expectedIdAddr2)

		// expect anne creating a payment channel to trigger a send to the payment channels constructor
		rt.ExpectSend(expectedIdAddr2, builtin.MethodConstructor, fakeParams, balance, nil, exitcode.Ok)
		execRet2 := actor.execAndVerify(rt, builtin.PaymentChannelActorCodeID, fakeParams)
		assert.Equal(t, uniqueAddr2, execRet2.RobustAddress)
		assert.Equal(t, expectedIdAddr2, execRet2.IDAddress)

		var st2 init_.State
		rt.GetState(&st2)
		actualIdAddr2, err := st2.ResolveAddress(rt.Store(), uniqueAddr2)
		assert.NoError(t, err)
		assert.Equal(t, expectedIdAddr2, actualIdAddr2)
	})

	t.Run("happy path exec create storage miner", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt)

		// only the storage power actor can create a miner
		rt.SetCaller(builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID)

		// re-org-stable address of the storage miner actor
		uniqueAddr := tutil.NewActorAddr(t, "miner")
		rt.SetNewActorAddress(uniqueAddr)

		// next id address
		expectedIdAddr := tutil.NewIDAddr(t, 100)
		rt.ExpectCreateActor(builtin.StorageMinerActorCodeID, expectedIdAddr)

		// expect storage power actor creating a storage miner actor to trigger a send to the storage miner actors constructor
		rt.ExpectSend(expectedIdAddr, builtin.MethodConstructor, fakeParams, big.Zero(), nil, exitcode.Ok)
		execRet := actor.execAndVerify(rt, builtin.StorageMinerActorCodeID, fakeParams)
		assert.Equal(t, uniqueAddr, execRet.RobustAddress)
		assert.Equal(t, expectedIdAddr, execRet.IDAddress)

		var st init_.State
		rt.GetState(&st)
		actualIdAddr, err := st.ResolveAddress(rt.Store(), uniqueAddr)
		assert.NoError(t, err)
		assert.Equal(t, expectedIdAddr, actualIdAddr)

		// should return the same address if not able to resolve
		expUnknowAddr := tutil.NewActorAddr(t, "flurbo")
		actualUnknownAddr, err := st.ResolveAddress(rt.Store(), expUnknowAddr)
		assert.NoError(t, err)
		assert.Equal(t, expUnknowAddr, actualUnknownAddr)
	})

	t.Run("sending to constructor failure", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt)

		// only the storage power actor can create a miner
		rt.SetCaller(builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID)

		// re-org-stable address of the storage miner actor
		uniqueAddr := tutil.NewActorAddr(t, "miner")
		rt.SetNewActorAddress(uniqueAddr)

		// next id address
		expectedIdAddr := tutil.NewIDAddr(t, 100)
		rt.ExpectCreateActor(builtin.StorageMinerActorCodeID, expectedIdAddr)

		// expect storage power actor creating a storage miner actor to trigger a send to the storage miner actors constructor
		rt.ExpectSend(expectedIdAddr, builtin.MethodConstructor, fakeParams, big.Zero(), nil, exitcode.ErrIllegalState)
		var execRet *init_.ExecReturn
		rt.ExpectAbort(exitcode.ErrIllegalState, func() {
			execRet = actor.execAndVerify(rt, builtin.StorageMinerActorCodeID, fakeParams)
			assert.Nil(t, execRet)
		})

		// since the send failed the uniqueAddr should resolve to itself instead of an ID address
		var st init_.State
		rt.GetState(&st)
		noResoAddr, err := st.ResolveAddress(rt.Store(), uniqueAddr)
		assert.NoError(t, err)
		assert.Equal(t, uniqueAddr, noResoAddr)

	})

}

type initHarness struct {
	init_.Actor
	t testing.TB
}

func (h *initHarness) constructAndVerify(rt *mock.Runtime) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	ret := rt.Call(h.Constructor, &adt.EmptyValue{}).(*adt.EmptyValue)
	assert.Equal(h.t, &adt.EmptyValue{}, ret)
	rt.Verify()

	var st init_.State
	rt.GetState(&st)
	emptyMap := adt.AsMap(rt.Store(), st.AddressMap)
	assert.Equal(h.t, emptyMap.Root(), st.AddressMap)
	assert.Equal(h.t, abi.ActorID(builtin.FirstNonSingletonActorId), st.NextID)
	assert.Equal(h.t, "mock", st.NetworkName)
}

func (h *initHarness) execAndVerify(rt *mock.Runtime, codeID cid.Cid, constructorParams []byte) *init_.ExecReturn {
	rt.ExpectValidateCallerAny()
	ret := rt.Call(h.Exec, &init_.ExecParams{
		CodeCID:           codeID,
		ConstructorParams: constructorParams,
	}).(*init_.ExecReturn)
	rt.Verify()
	return ret
}
