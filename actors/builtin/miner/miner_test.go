package miner_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	addr "github.com/filecoin-project/go-address"
	bitfield "github.com/filecoin-project/go-bitfield"
	cid "github.com/ipfs/go-cid"
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

var testPid abi.PeerID
var testMultiaddrs []abi.Multiaddrs

func init() {
	testPid = abi.PeerID("peerID")

	testMultiaddrs = []abi.Multiaddrs{
		{1},
		{2},
	}

	miner.SupportedProofTypes = map[abi.RegisteredSealProof]struct{}{
		abi.RegisteredSealProof_StackedDrg2KiBV1: {},
	}
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
			SealProofType: abi.RegisteredSealProof_StackedDrg2KiBV1,
			PeerId:        testPid,
			Multiaddrs:    testMultiaddrs,
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
		assert.Equal(t, params.Multiaddrs, st.Info.Multiaddrs)
		assert.Equal(t, abi.RegisteredSealProof_StackedDrg2KiBV1, st.Info.SealProofType)
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
	actor := newHarness(t, tutil.NewIDAddr(t, 1000), owner, worker, workerKey)
	builder := mock.NewBuilder(context.Background(), actor.receiver).
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
	// https://github.com/filecoin-project/specs-actors/issues/479
}

// Test for sector precommitment and proving.
func TestCommitments(t *testing.T) {
	owner := tutil.NewIDAddr(t, 100)
	worker := tutil.NewIDAddr(t, 101)
	workerKey := tutil.NewBLSAddr(t, 0)
	actor := newHarness(t, tutil.NewIDAddr(t, 1000), owner, worker, workerKey)
	periodBoundary := abi.ChainEpoch(100)
	builder := mock.NewBuilder(context.Background(), actor.receiver).
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
		challengeEpoch := precommitEpoch - 1

		// Good commitment.
		actor.preCommitSector(rt, makePreCommit(100, challengeEpoch, deadline.PeriodEnd()), big.Zero())

		// Duplicate sector ID
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.preCommitSector(rt, makePreCommit(100, challengeEpoch, deadline.PeriodEnd()), big.Zero())
		})

		// Bad seal proof type
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			pc := makePreCommit(114, challengeEpoch, deadline.PeriodEnd())
			pc.SealProof = abi.RegisteredSealProof_StackedDrg8MiBV1
			actor.preCommitSector(rt, pc, big.Zero())
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

	t.Run("invalid proof rejected", func(t *testing.T) {
		rt := builder.Build(t)
		precommitEpoch := periodBoundary + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt, periodBoundary+miner.WPoStProvingPeriod)
		st := getState(rt)
		deadline := st.DeadlineInfo(precommitEpoch)

		// Make a good commitment for the proof to target.
		sectorNo := abi.SectorNumber(100)
		precommit := makePreCommit(sectorNo, precommitEpoch-1, deadline.PeriodEnd())
		actor.preCommitSector(rt, precommit, big.Zero())

		// Sector pre-commitment missing.
		rt.SetEpoch(precommitEpoch + miner.PreCommitChallengeDelay + 1)
		rt.ExpectAbort(exitcode.ErrNotFound, func() {
			actor.proveCommitSector(rt, precommit, precommitEpoch, makeProveCommit(sectorNo+1), proveCommitConf{})
		})
		rt.Reset()

		// Too late.
		rt.SetEpoch(precommitEpoch + miner.MaxSealDuration[precommit.SealProof] + 1)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.proveCommitSector(rt, precommit, precommitEpoch, makeProveCommit(sectorNo), proveCommitConf{})
		})
		rt.Reset()

		// TODO: too early to prove sector
		// TODO: seal rand epoch too old
		// TODO: commitment expires before proof
		// https://github.com/filecoin-project/specs-actors/issues/479

		// Set the right epoch for all following tests
		rt.SetEpoch(precommitEpoch + miner.PreCommitChallengeDelay + 1)

		// Invalid deals (market VerifyDealsOnSectorProveCommit aborts)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.proveCommitSector(rt, precommit, precommitEpoch, makeProveCommit(sectorNo), proveCommitConf{
				verifyDealsExit: exitcode.ErrIllegalArgument,
			})
		})
		rt.Reset()

		// Insufficient funds for initial pledge
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			actor.proveCommitSector(rt, precommit, precommitEpoch, makeProveCommit(sectorNo), proveCommitConf{
				initialPledge: big.NewInt(10000),
			})
		})
		rt.Reset()

		// Invalid seal proof
		/* TODO: how should this test work?
		// https://github.com/filecoin-project/specs-actors/issues/479
		rt.ExpectAbort(exitcode.ErrIllegalState, func() {
			actor.proveCommitSector(rt, precommit, precommitEpoch, makeProveCommit(sectorNo), proveCommitConf{
				verifySealErr: fmt.Errorf("for testing"),
			})
		})
		rt.Reset()
		*/

		// Good proof
		rt.SetBalance(big.NewInt(5000))
		actor.proveCommitSector(rt, precommit, precommitEpoch, makeProveCommit(sectorNo), proveCommitConf{
			initialPledge: rt.Balance(),
		})
		st = getState(rt)
		// Verify new sectors
		newSectors, err := st.NewSectors.All(miner.SectorsMax)
		require.NoError(t, err)
		assert.Equal(t, []uint64{uint64(sectorNo)}, newSectors)
		// Verify pledge lock-up
		assert.Equal(t, rt.Balance(), st.LockedFunds)
		rt.Reset()

		// Duplicate proof (sector no-longer pre-committed)
		rt.ExpectAbort(exitcode.ErrNotFound, func() {
			actor.proveCommitSector(rt, precommit, precommitEpoch, makeProveCommit(sectorNo), proveCommitConf{})
		})
		rt.Reset()
	})
}

func TestWindowPost(t *testing.T) {
	owner := tutil.NewIDAddr(t, 100)
	worker := tutil.NewIDAddr(t, 101)
	workerKey := tutil.NewBLSAddr(t, 0)
	actor := newHarness(t, tutil.NewIDAddr(t, 1000), owner, worker, workerKey)
	periodOffset := abi.ChainEpoch(100)
	builder := mock.NewBuilder(context.Background(), actor.receiver).
		WithActorType(owner, builtin.AccountActorCodeID).
		WithActorType(worker, builtin.AccountActorCodeID).
		WithHasher(fixedHasher(uint64(periodOffset))).
		WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	t.Run("test proof", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt, periodOffset)
		precommitEpoch := abi.ChainEpoch(1)
		rt.SetEpoch(precommitEpoch)
		st := getState(rt)
		store := rt.AdtStore()
		partitionSize := st.Info.WindowPoStPartitionSectors
		deadline := st.DeadlineInfo(rt.Epoch())
		expiration := deadline.PeriodEnd() + 10*miner.WPoStProvingPeriod
		_ = actor.commitAndProveSectors(rt, 1, expiration, big.Zero())

		// Skip to end of proving period, cron adds sectors to proving set.
		deadline = st.DeadlineInfo(rt.Epoch())
		rt.SetEpoch(deadline.PeriodEnd())
		nextCron := deadline.NextPeriodStart() + miner.WPoStProvingPeriod - 1
		actor.onProvingPeriodCron(rt, nextCron, true, rt.Epoch()-miner.ElectionLookback)
		st = getState(rt)
		rt.SetEpoch(deadline.NextPeriodStart())

		// Iterate deadlines in the proving period, setting epoch to the first in each deadline.
		// Submit a window post for all partitions due at each deadline when necessary.
		deadline = st.DeadlineInfo(rt.Epoch())
		for !deadline.PeriodElapsed() {
			st = getState(rt)
			deadlines, err := st.LoadDeadlines(store)
			require.NoError(t, err)

			firstPartIdx, sectorCount, err := miner.PartitionsForDeadline(deadlines, partitionSize, deadline.Index)
			if sectorCount != 0 {
				partitionCount, _, err := miner.DeadlineCount(deadlines, partitionSize, deadline.Index)
				require.NoError(t, err)

				partitions := make([]uint64, partitionCount)
				for i := uint64(0); i < partitionCount; i++ {
					partitions[i] = firstPartIdx + i
				}

				partitionsSectors, err := miner.ComputePartitionsSectors(deadlines, partitionSize, deadline.Index, partitions)
				require.NoError(t, err)
				provenSectors, err := abi.BitFieldUnion(partitionsSectors...)
				require.NoError(t, err)
				infos, _, err := st.LoadSectorInfosForProof(store, provenSectors)
				require.NoError(t, err)

				actor.submitWindowPost(rt, deadline, partitions, infos)

			}

			rt.SetEpoch(deadline.Close + 1)
			deadline = st.DeadlineInfo(rt.Epoch())
		}

		// Oops, went one epoch too far, rewind to last epoch of last deadline window for the cron.
		rt.SetEpoch(rt.Epoch() - 1)

		empty, err := st.PostSubmissions.IsEmpty()
		require.NoError(t, err)
		assert.False(t, empty, "no post submission")
	})
}

func TestProvingPeriodCron(t *testing.T) {
	owner := tutil.NewIDAddr(t, 100)
	worker := tutil.NewIDAddr(t, 101)
	workerKey := tutil.NewBLSAddr(t, 0)
	actor := newHarness(t, tutil.NewIDAddr(t, 1000), owner, worker, workerKey)
	periodOffset := abi.ChainEpoch(100)
	builder := mock.NewBuilder(context.Background(), actor.receiver).
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
		actor.onProvingPeriodCron(rt, secondCronEpoch, false, 0)
		// The proving period start isn't changed, because the period hadn't started yet.
		st = getState(rt)
		assert.Equal(t, periodOffset, st.ProvingPeriodStart)

		rt.SetEpoch(secondCronEpoch)
		actor.onProvingPeriodCron(rt, periodOffset+2*miner.WPoStProvingPeriod-1, false, 0)
		// Proving period moves forward
		st = getState(rt)
		assert.Equal(t, periodOffset+miner.WPoStProvingPeriod, st.ProvingPeriodStart)
	})

	t.Run("first period gets randomness from previous epoch", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt, periodOffset)
		st := getState(rt)

		sectorInfo := actor.commitAndProveSectors(rt, 1, periodOffset+100*miner.WPoStProvingPeriod-1, big.Zero())

		// Flag new sectors to trigger request for randomness
		rt.Transaction(st, func() interface{} {
			st.NewSectors.Set(uint64(sectorInfo[0].SectorNumber))
			return nil
		})

		// First cron invocation just before the first proving period starts
		// requires randomness come from current epoch minus lookback
		rt.SetEpoch(periodOffset - 1)
		secondCronEpoch := periodOffset + miner.WPoStProvingPeriod - 1
		actor.onProvingPeriodCron(rt, secondCronEpoch, true, rt.Epoch()-miner.ElectionLookback)

		// cron invocation after the proving period starts, requires randomness come from end of proving period
		rt.SetEpoch(periodOffset)
		actor.advanceProvingPeriodWithoutFaults(rt)

		// triggers a new request for randomness
		rt.Transaction(st, func() interface{} {
			st.NewSectors.Set(uint64(sectorInfo[0].SectorNumber))
			return nil
		})

		thirdCronEpoch := secondCronEpoch + miner.WPoStProvingPeriod
		actor.onProvingPeriodCron(rt, thirdCronEpoch, true, getState(rt).DeadlineInfo(rt.Epoch()).PeriodEnd())
	})
}

func TestExtendSectorExpiration(t *testing.T) {
	owner := tutil.NewIDAddr(t, 100)
	worker := tutil.NewIDAddr(t, 101)
	workerKey := tutil.NewBLSAddr(t, 0)
	actor := newHarness(t, tutil.NewIDAddr(t, 1000), owner, worker, workerKey)
	periodOffset := abi.ChainEpoch(100)
	builder := mock.NewBuilder(context.Background(), actor.receiver).
		WithActorType(owner, builtin.AccountActorCodeID).
		WithActorType(worker, builtin.AccountActorCodeID).
		WithHasher(fixedHasher(uint64(periodOffset))).
		WithCaller(builtin.InitActorAddr, builtin.InitActorCodeID)

	commitSector := func(t *testing.T, rt *mock.Runtime) *miner.SectorOnChainInfo {
		actor.constructAndVerify(rt, periodOffset)
		precommitEpoch := abi.ChainEpoch(1)
		rt.SetEpoch(precommitEpoch)
		st := getState(rt)
		deadline := st.DeadlineInfo(rt.Epoch())
		expiration := deadline.PeriodEnd() + 10*miner.WPoStProvingPeriod
		sectorInfo := actor.commitAndProveSectors(rt, 1, expiration, big.Zero())

		sector, found, err := getState(rt).GetSector(rt.AdtStore(), sectorInfo[0].SectorNumber)
		require.NoError(t, err)
		require.True(t, found)
		return sector
	}

	t.Run("rejects negative extension", func(t *testing.T) {
		rt := builder.Build(t)
		sector := commitSector(t, rt)
		// attempt to shorten epoch
		newExpiration := sector.Info.Expiration - abi.ChainEpoch(miner.WPoStProvingPeriod)
		params := &miner.ExtendSectorExpirationParams{
			SectorNumber:  sector.Info.SectorNumber,
			NewExpiration: newExpiration,
		}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.extendSector(rt, sector, 0, params)
		})
	})

	t.Run("rejects extension to invalid epoch", func(t *testing.T) {
		rt := builder.Build(t)
		sector := commitSector(t, rt)

		// attempt to extend to an epoch that is not a multiple of the proving period + the commit epoch
		extension := 42*miner.WPoStProvingPeriod + 1
		newExpiration := sector.Info.Expiration - abi.ChainEpoch(extension)
		params := &miner.ExtendSectorExpirationParams{
			SectorNumber:  sector.Info.SectorNumber,
			NewExpiration: newExpiration,
		}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.extendSector(rt, sector, uint64(extension), params)
		})
	})

	t.Run("updates expiration with valid params", func(t *testing.T) {
		rt := builder.Build(t)
		sector := commitSector(t, rt)

		extension := uint64(42 * miner.WPoStProvingPeriod)
		newExpiration := sector.Info.Expiration + abi.ChainEpoch(extension)
		params := &miner.ExtendSectorExpirationParams{
			SectorNumber:  sector.Info.SectorNumber,
			NewExpiration: newExpiration,
		}

		actor.extendSector(rt, sector, extension, params)

		// assert sector expiration is set to the new value
		st := getState(rt)
		sector, found, err := st.GetSector(rt.AdtStore(), sector.Info.SectorNumber)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, newExpiration, sector.Info.Expiration)

		// assert the only expiration is for the new target epoch
		err = st.ForEachSectorExpiration(rt.AdtStore(), func(expiry abi.ChainEpoch, sectors *abi.BitField) error {
			assert.Equal(t, newExpiration, expiry)
			expiringSectors, err := sectors.All(10)
			require.NoError(t, err)
			assert.Equal(t, []uint64{uint64(sector.Info.SectorNumber)}, expiringSectors)
			return nil
		})
		assert.NoError(t, err)
	})
}

type actorHarness struct {
	a miner.Actor
	t testing.TB

	receiver addr.Address // The miner actor's own address
	owner    addr.Address
	worker   addr.Address
	key      addr.Address

	nextSectorNo abi.SectorNumber
}

func newHarness(t testing.TB, receiver, owner, worker, key addr.Address) *actorHarness {
	return &actorHarness{miner.Actor{}, t, receiver, owner, worker, key, 100}
}

func (h *actorHarness) constructAndVerify(rt *mock.Runtime, provingPeriodStart abi.ChainEpoch) {
	params := miner.ConstructorParams{
		OwnerAddr:     h.owner,
		WorkerAddr:    h.worker,
		SealProofType: abi.RegisteredSealProof_StackedDrg2KiBV1,
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
			EventEpoch: rt.Epoch() + miner.MaxSealDuration[params.SealProof] + 1,
			Payload:    buf.Bytes(),
		}
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent, &cronParams, big.Zero(), nil, exitcode.Ok)
	}

	rt.Call(h.a.PreCommitSector, params)
	rt.Verify()
}

// Options for proveCommitSector behaviour.
// Default zero values should let everything be ok.
type proveCommitConf struct {
	initialPledge   big.Int
	verifyDealsExit exitcode.ExitCode
	verifySealErr   error
}

func (h *actorHarness) proveCommitSector(rt *mock.Runtime, precommit *miner.SectorPreCommitInfo, precommitEpoch abi.ChainEpoch,
	params *miner.ProveCommitSectorParams, conf proveCommitConf) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAny()
	commd := cbg.CborCid(tutil.MakeCID("commd"))
	sealRand := abi.SealRandomness([]byte{1, 2, 3, 4})
	sealIntRand := abi.InteractiveSealRandomness([]byte{5, 6, 7, 8})
	interactiveEpoch := precommitEpoch + miner.PreCommitChallengeDelay
	dealWeight := big.NewInt(10)
	verifiedDealWeight := big.NewInt(100)
	{
		cdcParams := market.ComputeDataCommitmentParams{
			DealIDs:    precommit.DealIDs,
			SectorType: precommit.SealProof,
		}
		rt.ExpectSend(builtin.StorageMarketActorAddr, builtin.MethodsMarket.ComputeDataCommitment, &cdcParams, big.Zero(), &commd, exitcode.Ok)
	}
	{
		var buf bytes.Buffer
		err := rt.Receiver().MarshalCBOR(&buf)
		require.NoError(h.t, err)
		rt.ExpectGetRandomness(crypto.DomainSeparationTag_SealRandomness, precommit.SealRandEpoch, buf.Bytes(), abi.Randomness(sealRand))
		rt.ExpectGetRandomness(crypto.DomainSeparationTag_InteractiveSealChallengeSeed, interactiveEpoch, buf.Bytes(), abi.Randomness(sealIntRand))
	}
	{
		actorId, err := addr.IDFromAddress(h.receiver)
		require.NoError(h.t, err)
		seal := abi.SealVerifyInfo{
			SectorID: abi.SectorID{
				Miner:  abi.ActorID(actorId),
				Number: precommit.SectorNumber,
			},
			SealedCID:             precommit.SealedCID,
			SealProof:             precommit.SealProof,
			Proof:                 params.Proof,
			DealIDs:               precommit.DealIDs,
			Randomness:            sealRand,
			InteractiveRandomness: sealIntRand,
			UnsealedCID:           cid.Cid(commd),
		}
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.SubmitPoRepForBulkVerify, &seal, abi.NewTokenAmount(0), nil, 0)
	}

	rt.Call(h.a.ProveCommitSector, params)

	rt.SetCaller(builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID)
	rt.ExpectValidateCallerAddr(builtin.StoragePowerActorAddr)
	{
		vdParams := market.VerifyDealsOnSectorProveCommitParams{
			DealIDs:      precommit.DealIDs,
			SectorExpiry: precommit.Expiration,
		}
		vdRet := market.VerifyDealsOnSectorProveCommitReturn{
			DealWeight:         dealWeight,
			VerifiedDealWeight: verifiedDealWeight,
		}
		rt.ExpectSend(builtin.StorageMarketActorAddr, builtin.MethodsMarket.VerifyDealsOnSectorProveCommit, &vdParams, big.Zero(), &vdRet, conf.verifyDealsExit)
	}
	{
		sectorSize, err := precommit.SealProof.SectorSize()
		require.NoError(h.t, err)
		pcParams := power.OnSectorProveCommitParams{Weight: power.SectorStorageWeightDesc{
			SectorSize:         sectorSize,
			Duration:           precommit.Expiration - rt.Epoch(),
			DealWeight:         dealWeight,
			VerifiedDealWeight: verifiedDealWeight,
		}}
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.OnSectorProveCommit, &pcParams, big.Zero(), &conf.initialPledge, exitcode.Ok)
	}
	{
		if !conf.initialPledge.Nil() && conf.initialPledge.GreaterThan(big.Zero()) {
			upParams := conf.initialPledge
			rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdatePledgeTotal, &upParams, big.Zero(), nil, exitcode.Ok)
		}
	}

	if conf.verifySealErr == nil {
		rt.Call(h.a.ConfirmSectorProofsValid, &builtin.ConfirmSectorProofsParams{Sectors: []abi.SectorNumber{params.SectorNumber}})
	}
	rt.Verify()
}

// Pre-commits and then proves a number of sectors.
// The runtime epoch will be moved forward to the epoch of commitment proofs.
func (h *actorHarness) commitAndProveSectors(rt *mock.Runtime, n int, expiration abi.ChainEpoch, pledge abi.TokenAmount) []*miner.SectorPreCommitInfo {
	precommitEpoch := rt.Epoch()
	precommits := make([]*miner.SectorPreCommitInfo, n)

	// Precommit
	for i := 0; i < n; i++ {
		sectorNo := h.nextSectorNo
		precommit := makePreCommit(sectorNo, precommitEpoch-1, expiration)
		h.preCommitSector(rt, precommit, big.Zero())
		precommits[i] = precommit
		h.nextSectorNo++
	}

	rt.SetEpoch(precommitEpoch + miner.PreCommitChallengeDelay + 1)

	// Ensure this this doesn't cross a proving period boundary, else the expected cron call won't be
	// invoked, which might mess things up later.
	deadline := getState(rt).DeadlineInfo(rt.Epoch())
	require.True(h.t, !deadline.PeriodElapsed())

	for _, pc := range precommits {
		h.proveCommitSector(rt, pc, precommitEpoch, makeProveCommit(pc.SectorNumber), proveCommitConf{
			initialPledge: pledge,
		})
	}
	rt.Reset()
	return precommits
}

func (h *actorHarness) submitWindowPost(rt *mock.Runtime, deadline *miner.DeadlineInfo, partitions []uint64, infos []*miner.SectorOnChainInfo) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(h.worker)

	var registeredPoStProof, err = abi.RegisteredSealProof_StackedDrg2KiBV1.RegisteredWindowPoStProof()
	require.NoError(h.t, err)

	proofs := make([]abi.PoStProof, 1) // Number of proofs doesn't depend on partition count
	for i := range proofs {
		proofs[i].PoStProof  = registeredPoStProof
		proofs[i].ProofBytes = []byte(fmt.Sprintf("proof%d", i))
	}
	challengeRand := abi.SealRandomness([]byte{10, 11, 12, 13})

	{
		var buf bytes.Buffer
		err := rt.Receiver().MarshalCBOR(&buf)
		require.NoError(h.t, err)

		rt.ExpectGetRandomness(crypto.DomainSeparationTag_WindowedPoStChallengeSeed, deadline.Challenge, buf.Bytes(), abi.Randomness(challengeRand))
	}
	{
		actorId, err := addr.IDFromAddress(h.receiver)
		require.NoError(h.t, err)

		proofInfos := make([]abi.SectorInfo, len(infos))
		for i, ci := range infos {
			proofInfos[i] = abi.SectorInfo{
				SealProof:       ci.Info.SealProof,
				SectorNumber:    ci.Info.SectorNumber,
				SealedCID:       ci.Info.SealedCID,
			}
		}

		vi := abi.WindowPoStVerifyInfo{
			Randomness:        abi.PoStRandomness(challengeRand),
			Proofs:            proofs,
			ChallengedSectors: proofInfos,
			Prover:            abi.ActorID(actorId),
		}
		rt.ExpectVerifyPoSt(vi, nil)
	}

	params := miner.SubmitWindowedPoStParams{
		Deadline:   deadline.Index,
		Partitions: partitions,
		Proofs:     proofs,
		Skipped:    bitfield.BitField{},
	}

	rt.Call(h.a.SubmitWindowedPoSt, &params)
	rt.Verify()
}

func (h *actorHarness) advanceProvingPeriodWithoutFaults(rt *mock.Runtime) {
	st := getState(rt)
	store := rt.AdtStore()
	partitionSize := st.Info.WindowPoStPartitionSectors

	// Iterate deadlines in the proving period, setting epoch to the first in each deadline.
	// Submit a window post for all partitions due at each deadline when necessary.
	deadline := st.DeadlineInfo(rt.Epoch())
	for !deadline.PeriodElapsed() {
		st = getState(rt)
		deadlines, err := st.LoadDeadlines(store)
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "could not load deadlines")

		firstPartIdx, sectorCount, err := miner.PartitionsForDeadline(deadlines, partitionSize, deadline.Index)
		if sectorCount != 0 {
			partitionCount, _, err := miner.DeadlineCount(deadlines, partitionSize, deadline.Index)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "could not get partition count")

			partitions := make([]uint64, partitionCount)
			for i := uint64(0); i < partitionCount; i++ {
				partitions[i] = firstPartIdx + i
			}

			partitionsSectors, err := miner.ComputePartitionsSectors(deadlines, partitionSize, deadline.Index, partitions)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "could not compute partitions")
			provenSectors, err := abi.BitFieldUnion(partitionsSectors...)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "could not get proven sectors")
			infos, _, err := st.LoadSectorInfosForProof(store, provenSectors)
			builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "could not load sector info for proof")

			h.submitWindowPost(rt, deadline, partitions, infos)
		}

		rt.SetEpoch(deadline.Close + 1)
		deadline = st.DeadlineInfo(rt.Epoch())
	}
}

func (h *actorHarness) extendSector(rt *mock.Runtime, sector *miner.SectorOnChainInfo, extension uint64, params *miner.ExtendSectorExpirationParams) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(h.worker)

	st := getState(rt)

	storageWeightDescPrev := miner.AsStorageWeightDesc(st.Info.SectorSize, sector)
	storageWeightDescNew := power.SectorStorageWeightDesc{
		SectorSize:         storageWeightDescPrev.SectorSize,
		Duration:           storageWeightDescPrev.Duration + abi.ChainEpoch(extension),
		DealWeight:         storageWeightDescPrev.DealWeight,
		VerifiedDealWeight: storageWeightDescPrev.VerifiedDealWeight,
	}

	rt.ExpectSend(builtin.StoragePowerActorAddr,
		builtin.MethodsPower.OnSectorModifyWeightDesc,
		&power.OnSectorModifyWeightDescParams{
			PrevWeight: *storageWeightDescPrev,
			NewWeight:  storageWeightDescNew,
		},
		abi.NewTokenAmount(0),
		nil,
		exitcode.Ok,
	)
	rt.Call(h.a.ExtendSectorExpiration, params)
	rt.Verify()
}

func (h *actorHarness) onProvingPeriodCron(rt *mock.Runtime, expectedEnrollment abi.ChainEpoch, newSectors bool, randEpoch abi.ChainEpoch) {
	rt.ExpectValidateCallerAddr(builtin.StoragePowerActorAddr)
	if newSectors {
		rt.ExpectGetRandomness(crypto.DomainSeparationTag_WindowedPoStDeadlineAssignment, randEpoch, nil, bytes.Repeat([]byte{0}, 32))
	}
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
		SealProof:       abi.RegisteredSealProof_StackedDrg2KiBV1,
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
