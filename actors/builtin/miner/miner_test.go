package miner_test

import (
	"bytes"
	"context"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/minio/blake2b-simd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

const SectorSize = abi.SectorSize(32 << 20)

func TestExports(t *testing.T) {
	mock.CheckActorExports(t, miner.Actor{})
}
func TestConstruction(t *testing.T) {
	actor := miner.Actor{}

	owner := tutil.NewIDAddr(t, 100)
	worker := tutil.NewIDAddr(t, 101)
	workerKey := tutil.NewBLSAddr(t, 0)
	receiver := tutil.NewIDAddr(t, 1000)
	builder := mock.NewBuilder(context.Background(), receiver).
		WithActorType(owner, builtin.AccountActorCodeID).
		WithActorType(worker, builtin.AccountActorCodeID).
		WithHasher(blake2b.Sum256).
		WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("simple construction", func(t *testing.T) {
		rt := builder.Build(t)
		params := miner.ConstructorParams{
			OwnerAddr:  owner,
			WorkerAddr: worker,
			SectorSize: SectorSize,
			PeerId:     "peer",
		}

		provingPeriodBoundary := abi.ChainEpoch(2386) // This is just set from running the code.
		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		// Fetch worker pubkey.
		rt.ExpectSend(worker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &workerKey, exitcode.Ok)
		// Register proving period cron.
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent,
			provingPeriodCronPayload(t, provingPeriodBoundary-1), big.Zero(), nil, exitcode.Ok)
		ret := rt.Call(actor.Constructor, &params)
		assert.Nil(t, ret)
		rt.Verify()

		var st miner.State
		rt.GetState(&st)
		assert.Equal(t, params.OwnerAddr, st.Info.Owner)
		assert.Equal(t, params.WorkerAddr, st.Info.Worker)
		assert.Equal(t, params.PeerId, st.Info.PeerId)
		assert.Equal(t, params.SectorSize, st.Info.SectorSize)
		assert.Equal(t, provingPeriodBoundary, st.Info.ProvingPeriodBoundary)

		assert.Equal(t, big.Zero(), st.PreCommitDeposits)
		assert.Equal(t, big.Zero(), st.LockedFunds)
		assert.True(t, st.VestingFunds.Defined())
		assert.True(t, st.PreCommittedSectors.Defined())
		assertEmptyBitfield(t, st.NewSectors)
		assert.True(t, st.SectorExpirations.Defined())
		assert.True(t, st.Deadlines.Defined())
		assertEmptyBitfield(t, st.Faults)
		assert.True(t, st.FaultEpochs.Defined())
		assertEmptyBitfield(t, st.Recoveries)
		assertEmptyBitfield(t, st.PostSubmissions)

		var deadlines miner.Deadlines
		assert.True(t, rt.Store().Get(st.Deadlines, &deadlines))
		for i := uint64(0); i < miner.WPoStPeriodDeadlines; i++ {
			assertEmptyBitfield(t, deadlines.Due[i])
		}
	})
}

func TestControlAddresses(t *testing.T) {
	actor := harness{miner.Actor{}, t}
	owner := tutil.NewIDAddr(t, 100)
	worker := tutil.NewIDAddr(t, 101)
	workerKey := tutil.NewBLSAddr(t, 0)
	receiver := tutil.NewIDAddr(t, 1000)
	builder := mock.NewBuilder(context.Background(), receiver).
		WithActorType(owner, builtin.AccountActorCodeID).
		WithActorType(worker, builtin.AccountActorCodeID).
		WithHasher(blake2b.Sum256).
		WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("get addresses", func(t *testing.T) {
		rt := builder.Build(t)

		actor.constructAndVerify(rt, owner, worker, workerKey)

		o, w := actor.controlAddresses(rt)
		assert.Equal(t, owner, o)
		assert.Equal(t, worker, w)
	})
}

type harness struct {
	a miner.Actor
	t testing.TB
}

func (h *harness) constructAndVerify(rt *mock.Runtime, owner, worker, key addr.Address) {
	params := miner.ConstructorParams{
		OwnerAddr:  owner,
		WorkerAddr: worker,
		SectorSize: SectorSize,
		PeerId:     "peer",
	}

	provingPeriodBoundary := abi.ChainEpoch(2386) // This is just set from running the code.
	rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
	// Fetch worker pubkey.
	rt.ExpectSend(worker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &key, exitcode.Ok)
	// Register proving period cron.
	rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent,
		provingPeriodCronPayload(h.t, provingPeriodBoundary-1), big.Zero(), nil, exitcode.Ok)
	ret := rt.Call(h.a.Constructor, &params)
	assert.Nil(h.t, ret)
	rt.Verify()
}

func (h *harness) controlAddresses(rt *mock.Runtime) (owner, worker addr.Address) {
	rt.ExpectValidateCallerAny()
	ret := rt.Call(h.a.ControlAddresses, nil).(*miner.GetControlAddressesReturn)
	require.NotNil(h.t, ret)
	return ret.Owner, ret.Worker
}

func provingPeriodCronPayload(t testing.TB, epoch abi.ChainEpoch) *power.EnrollCronEventParams {
	eventPayload := miner.CronEventPayload{EventType: miner.CronEventProvingPeriod}
	buf := bytes.Buffer{}
	err := eventPayload.MarshalCBOR(&buf)
	require.NoError(t, err)
	return &power.EnrollCronEventParams{
		EventEpoch: epoch,
		Payload:    buf.Bytes(),
	}
}

func assertEmptyBitfield(t *testing.T, b *abi.BitField) {
	empty, err := b.IsEmpty()
	require.NoError(t, err)
	assert.True(t, empty)

}
