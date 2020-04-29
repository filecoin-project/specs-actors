package miner_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/minio/blake2b-simd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/crypto"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

var testPid peer.ID

func init() {
	pid, err := peer.IDB58Decode("12D3KooWGzxzKZYveHXtpG6AsrUJBcWxHBFS2HsEoGTxrMLvKXtf")
	if err != nil {
		panic(err)
	}
	testPid = pid
}

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
			OwnerAddr:     owner,
			WorkerAddr:    worker,
			SealProofType: abi.RegisteredProof_StackedDRG2KiBSeal,
			PeerId:        testPid,
		}

		provingPeriodStart := abi.ChainEpoch(2386) // This is just set from running the code.
		rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
		// Fetch worker pubkey.
		rt.ExpectSend(worker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &workerKey, exitcode.Ok)
		// Register proving period cron.
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent,
			makeProvingPeriodCronEventParams(t, provingPeriodStart-1), big.Zero(), nil, exitcode.Ok)
		ret := rt.Call(actor.Constructor, &params)
		assert.Nil(t, ret)
		rt.Verify()

		var st miner.State
		rt.GetState(&st)
		assert.Equal(t, params.OwnerAddr, st.Info.Owner)
		assert.Equal(t, params.WorkerAddr, st.Info.Worker)
		assert.Equal(t, params.PeerId, st.Info.PeerId)
		assert.Equal(t, abi.RegisteredProof_StackedDRG2KiBSeal, st.Info.SealProofType)
		assert.Equal(t, abi.SectorSize(2048), st.Info.SectorSize)
		assert.Equal(t, uint64(2), st.Info.WindowPoStPartitionSectors)
		assert.Equal(t, provingPeriodStart, st.ProvingPeriodStart)

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

// Tests for fetching and manipulating miner addresses.
func TestControlAddresses(t *testing.T) {
	owner := tutil.NewIDAddr(t, 100)
	worker := tutil.NewIDAddr(t, 101)
	workerKey := tutil.NewBLSAddr(t, 0)
	receiver := tutil.NewIDAddr(t, 1000)
	actor := newHarness(t, owner, worker, workerKey)
	builder := mock.NewBuilder(context.Background(), receiver).
		WithActorType(owner, builtin.AccountActorCodeID).
		WithActorType(worker, builtin.AccountActorCodeID).
		WithHasher(fixedHasher(0)).
		WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("get addresses", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt, miner.WPoStProvingPeriod)

		o, w := actor.controlAddresses(rt)
		assert.Equal(t, owner, o)
		assert.Equal(t, worker, w)
	})

	// TODO: test changing worker (with delay), changing peer id
}

// Test for sector precommitment and proving.
func TestCommitments(t *testing.T) {
	owner := tutil.NewIDAddr(t, 100)
	worker := tutil.NewIDAddr(t, 101)
	workerKey := tutil.NewBLSAddr(t, 0)
	receiver := tutil.NewIDAddr(t, 1000)
	actor := newHarness(t, owner, worker, workerKey)
	periodBoundary := abi.ChainEpoch(100)
	builder := mock.NewBuilder(context.Background(), receiver).
		WithActorType(owner, builtin.AccountActorCodeID).
		WithActorType(worker, builtin.AccountActorCodeID).
		WithHasher(fixedHasher(uint64(periodBoundary))).
		WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("invalid pre-commit rejected", func(t *testing.T) {
		rt := builder.Build(t)
		precommitEpoch := periodBoundary + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt, periodBoundary+miner.WPoStProvingPeriod)
		st := getState(rt)
		deadline := st.DeadlineInfo(precommitEpoch)

		challengeEpoch := precommitEpoch - miner.PreCommitChallengeDelay

		// Good commitment.
		actor.preCommitSector(rt, makePreCommit(100, challengeEpoch, deadline.PeriodEnd()), big.Zero())

		// Duplicate sector ID
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.preCommitSector(rt, makePreCommit(100, challengeEpoch, deadline.PeriodEnd()), big.Zero())
		})

		// Expires at current epoch
		rt.SetEpoch(deadline.PeriodEnd())
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.preCommitSector(rt, makePreCommit(111, challengeEpoch, deadline.PeriodEnd()), big.Zero())
		})

		// Expires before current epoch
		rt.SetEpoch(deadline.PeriodEnd() + 1)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.preCommitSector(rt, makePreCommit(112, challengeEpoch, deadline.PeriodEnd()), big.Zero())
		})

		// Expires not on period end
		rt.SetEpoch(precommitEpoch)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.preCommitSector(rt, makePreCommit(113, challengeEpoch, deadline.PeriodEnd()-1), big.Zero())
		})

		// TODO: test insufficient funds when the precommit deposit is set above zero
	})

	// TODO
	// already proven
	// commitment expires before proof
	// commitment proven ok
}

func TestProvingPeriodCron(t *testing.T) {
	owner := tutil.NewIDAddr(t, 100)
	worker := tutil.NewIDAddr(t, 101)
	workerKey := tutil.NewBLSAddr(t, 0)
	receiver := tutil.NewIDAddr(t, 1000)
	actor := newHarness(t, owner, worker, workerKey)
	periodOffset := abi.ChainEpoch(100)
	builder := mock.NewBuilder(context.Background(), receiver).
		WithActorType(owner, builtin.AccountActorCodeID).
		WithActorType(worker, builtin.AccountActorCodeID).
		WithHasher(fixedHasher(uint64(periodOffset))).
		WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("empty periods", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt, periodOffset)
		st := getState(rt)
		assert.Equal(t, periodOffset, st.ProvingPeriodStart)

		// First cron invocation just before the first proving period starts.
		rt.SetEpoch(periodOffset - 1)
		secondCronEpoch := periodOffset + miner.WPoStProvingPeriod - 1
		actor.onProvingPeriodCron(rt, secondCronEpoch)
		// The proving period start isn't changed, because the period hadn't started yet.
		st = getState(rt)
		assert.Equal(t, periodOffset, st.ProvingPeriodStart)

		rt.SetEpoch(secondCronEpoch)
		actor.onProvingPeriodCron(rt, periodOffset+2*miner.WPoStProvingPeriod-1)
		// Proving period moves forward
		st = getState(rt)
		assert.Equal(t, periodOffset+miner.WPoStProvingPeriod, st.ProvingPeriodStart)
	})
}

type actorHarness struct {
	a miner.Actor
	t testing.TB

	owner  addr.Address
	worker addr.Address
	key    addr.Address
}

func newHarness(t testing.TB, owner, worker, key addr.Address) *actorHarness {
	return &actorHarness{miner.Actor{}, t, owner, worker, key}
}

func (h *actorHarness) constructAndVerify(rt *mock.Runtime, provingPeriodStart abi.ChainEpoch) {
	params := miner.ConstructorParams{
		OwnerAddr:     h.owner,
		WorkerAddr:    h.worker,
		SealProofType: abi.RegisteredProof_StackedDRG2KiBSeal,
		PeerId:        testPid,
	}

	rt.ExpectValidateCallerAddr(builtin.InitActorAddr)
	// Fetch worker pubkey.
	rt.ExpectSend(h.worker, builtin.MethodsAccount.PubkeyAddress, nil, big.Zero(), &h.key, exitcode.Ok)
	// Register proving period cron.
	rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent,
		makeProvingPeriodCronEventParams(h.t, provingPeriodStart-1), big.Zero(), nil, exitcode.Ok)
	ret := rt.Call(h.a.Constructor, &params)
	assert.Nil(h.t, ret)
	rt.Verify()
}

func (h *actorHarness) controlAddresses(rt *mock.Runtime) (owner, worker addr.Address) {
	rt.ExpectValidateCallerAny()
	ret := rt.Call(h.a.ControlAddresses, nil).(*miner.GetControlAddressesReturn)
	require.NotNil(h.t, ret)
	rt.Verify()
	return ret.Owner, ret.Worker
}

func (h *actorHarness) preCommitSector(rt *mock.Runtime, params *miner.SectorPreCommitInfo, pledgeDelta abi.TokenAmount) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(h.worker)
	if !pledgeDelta.IsZero() {
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdatePledgeTotal, &pledgeDelta, big.Zero(), nil, exitcode.Ok)
	}

	{
		eventPayload := miner.CronEventPayload{
			EventType: miner.CronEventPreCommitExpiry,
			Sectors:   bitfield.NewFromSet([]uint64{uint64(params.SectorNumber)}),
		}
		buf := bytes.Buffer{}
		err := eventPayload.MarshalCBOR(&buf)
		require.NoError(h.t, err)
		cronParams := power.EnrollCronEventParams{
			EventEpoch: rt.Epoch() + miner.MaxSealDuration[params.RegisteredProof] + 1,
			Payload:    buf.Bytes(),
		}
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent, &cronParams, big.Zero(), nil, exitcode.Ok)
	}

	rt.Call(h.a.PreCommitSector, params)
	rt.Verify()
}

func (h *actorHarness) proveCommitSector(rt *mock.Runtime, precommit *miner.SectorPreCommitInfo, params *miner.ProveCommitSectorParams) {
	rt.ExpectValidateCallerAny()
	commd := cbg.CborCid(tutil.MakeCID("commd"))
	{
		cdcParams := market.ComputeDataCommitmentParams{
			DealIDs:    precommit.DealIDs,
			SectorType: precommit.RegisteredProof,
		}
		rt.ExpectSend(builtin.StorageMarketActorAddr, builtin.MethodsMarket.ComputeDataCommitment, &cdcParams, big.Zero(), &commd, exitcode.Ok)
	}
	{
		var buf bytes.Buffer
		err := rt.Receiver().MarshalCBOR(&buf)
		require.NoError(h.t, err)
		rt.ExpectGetRandomness(crypto.DomainSeparationTag_SealRandomness, precommit.SealRandEpoch, buf.Bytes(), abi.Randomness("sealrand"))
	}
	rt.Call(h.a.ProveCommitSector, params)
	rt.Verify()
}

func (h *actorHarness) onProvingPeriodCron(rt *mock.Runtime, expectedEnrollment abi.ChainEpoch) {
	rt.ExpectValidateCallerAddr(builtin.StoragePowerActorAddr)
	// Re-enrollment for next period.
	rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent,
		makeProvingPeriodCronEventParams(h.t, expectedEnrollment), big.Zero(), nil, exitcode.Ok)
	rt.SetCaller(builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID)
	rt.Call(h.a.OnDeferredCronEvent, &miner.CronEventPayload{
		EventType: miner.CronEventProvingPeriod,
	})
	rt.Verify()
}

func getState(rt *mock.Runtime) *miner.State {
	var st miner.State
	rt.GetState(&st)
	return &st
}

func makeProvingPeriodCronEventParams(t testing.TB, epoch abi.ChainEpoch) *power.EnrollCronEventParams {
	eventPayload := miner.CronEventPayload{EventType: miner.CronEventProvingPeriod}
	buf := bytes.Buffer{}
	err := eventPayload.MarshalCBOR(&buf)
	require.NoError(t, err)
	return &power.EnrollCronEventParams{
		EventEpoch: epoch,
		Payload:    buf.Bytes(),
	}
}

func makePreCommit(sectorNo abi.SectorNumber, challenge, expiration abi.ChainEpoch) *miner.SectorPreCommitInfo {
	return &miner.SectorPreCommitInfo{
		RegisteredProof: abi.RegisteredProof_StackedDRG2KiBSeal,
		SectorNumber:    sectorNo,
		SealedCID:       tutil.MakeCID("commr"),
		SealRandEpoch:   challenge,
		DealIDs:         nil,
		Expiration:      expiration,
	}
}

func makeProveCommit(sectorNo abi.SectorNumber) *miner.ProveCommitSectorParams {
	return &miner.ProveCommitSectorParams{
		SectorNumber: sectorNo,
		Proof:        []byte("proof"),
	}
}

func assertEmptyBitfield(t *testing.T, b *abi.BitField) {
	empty, err := b.IsEmpty()
	require.NoError(t, err)
	assert.True(t, empty)

}

// Returns a fake hashing function that always arranges the first 8 bytes of the digest to be the binary
// encoding of a target uint64.
func fixedHasher(target uint64) func([]byte) [32]byte {
	return func(_ []byte) [32]byte {
		var buf bytes.Buffer
		err := binary.Write(&buf, binary.BigEndian, target)
		if err != nil {
			panic(err)
		}
		var digest [32]byte
		copy(digest[:], buf.Bytes())
		return digest
	}
}
