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
	actor := initHarness{init_.InitActor{}, t}

	receiver := tutil.NewIDAddr(t, 1000)
	builder := mock.NewBuilder(context.Background(), receiver).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)
	rt := builder.Build(t)
	actor.constructAndVerify(rt)
}

func TestExec(t *testing.T) {
	actor := initHarness{init_.InitActor{}, t}

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

	t.Run("happy path exec create payment channel", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt)

		// anne execs a payment channel actor with 100 FIL.
		rt.SetCaller(anne, builtin.AccountActorCodeID)
		rt.SetBalance(balance)
		rt.SetReceived(balance)

		// re-org-stable address of the payment channel actor
		uniqueAddr := tutil.NewActorAddr(t, "paych")
		rt.SetNewActorAddress(uniqueAddr)

		// next id address
		expectedIdAddr := tutil.NewIDAddr(t, 100)
		rt.ExpectCreateActor(builtin.PaymentChannelActorCodeID, expectedIdAddr)

		// expect anne creating a payment channel to trigger a send to the payment channels constructor
		rt.ExpectSend(expectedIdAddr, builtin.MethodConstructor, fakeParams, balance, nil, exitcode.Ok)
		execRet := actor.execAndVerify(rt, builtin.PaymentChannelActorCodeID, fakeParams)
		assert.Equal(t, uniqueAddr, execRet.RobustAddress)
		assert.Equal(t, expectedIdAddr, execRet.IDAddress)

		var st init_.InitActorState
		rt.GetState(&st)
		actualIdAddr, err := st.ResolveAddress(rt.Store(), uniqueAddr)
		assert.NoError(t, err)
		assert.Equal(t, expectedIdAddr, actualIdAddr)

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

		var st init_.InitActorState
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

}

type initHarness struct {
	init_.InitActor
	t testing.TB
}

func (h *initHarness) constructAndVerify(rt *mock.Runtime) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	ret := rt.Call(h.Constructor, &adt.EmptyValue{}).(*adt.EmptyValue)
	assert.Equal(h.t, &adt.EmptyValue{}, ret)
	rt.Verify()

	var st init_.InitActorState
	rt.GetState(&st)
	emptyMap := adt.AsMap(rt.Store(), st.AddressMap)
	assert.Equal(h.t, emptyMap.Root(), st.AddressMap)
	assert.Equal(h.t, abi.ActorID(builtin.FirstNonSingletonActorId), st.NextID)
	assert.Equal(h.t, "mock", st.NetworkName)
}

func (h *initHarness) execAndVerify(rt *mock.Runtime, codeID cid.Cid, constructorParams []byte) *init_.ExecReturn {
	rt.ExpectValidateCallerAny()
	ret := rt.Call(h.Exec, &init_.ExecParams{
		CodeID:            codeID,
		ConstructorParams: constructorParams,
	}).(*init_.ExecReturn)
	rt.Verify()
	return ret
}
