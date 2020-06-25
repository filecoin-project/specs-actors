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
	"github.com/filecoin-project/specs-actors/actors/runtime"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
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
	actor := newHarness(t, 0)
	builder := builderForHarness(actor)

	t.Run("get addresses", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		o, w := actor.controlAddresses(rt)
		assert.Equal(t, actor.owner, o)
		assert.Equal(t, actor.worker, w)
	})

	// TODO: test changing worker (with delay), changing peer id
	// https://github.com/filecoin-project/specs-actors/issues/479
}

// Test for sector precommitment and proving.
func TestCommitments(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor)

	networkPower := abi.NewStoragePower(1 << 50)

	t.Run("valid precommit then provecommit", func(t *testing.T) {
		rt := builder.
			WithBalance(abi.NewTokenAmount(1<<50), abi.NewTokenAmount(0)).
			Build(t)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		st := getState(rt)
		deadline := st.DeadlineInfo(precommitEpoch)

		// Make a good commitment for the proof to target.
		sectorNo := abi.SectorNumber(100)
		precommit := makePreCommit(sectorNo, precommitEpoch-1, deadline.PeriodEnd(), nil)
		actor.preCommitSector(rt, precommit, networkPower, big.Zero())

		// assert precommit exists and meets expectations
		st = getState(rt)
		onChainPrecommit, found, err := st.GetPrecommittedSector(rt.AdtStore(), sectorNo)
		require.NoError(t, err)
		require.True(t, found)

		// expect precommit deposit to be initial pledge calculated at precommit time
		sectorSize, err := precommit.SealProof.SectorSize()
		require.NoError(t, err)

		// deal weights mocked by actor harness for market actor must be set in precommit onchain info
		assert.Equal(t, big.NewInt(int64(sectorSize/2)), onChainPrecommit.DealWeight)
		assert.Equal(t, big.NewInt(int64(sectorSize/2)), onChainPrecommit.VerifiedDealWeight)

		qaPower := miner.QAPowerForWeight(sectorSize, precommit.Expiration-precommitEpoch, onChainPrecommit.DealWeight, onChainPrecommit.VerifiedDealWeight)
		expectedDeposit := miner.InitialPledgeForPower(qaPower, networkPower, actor.networkPledge, actor.epochReward, rt.TotalFilCircSupply())
		assert.Equal(t, expectedDeposit, onChainPrecommit.PreCommitDeposit)

		// expect total precommit deposit to equal our new deposit
		assert.Equal(t, expectedDeposit, st.PreCommitDeposits)

		// run prove commit logic
		rt.SetEpoch(precommitEpoch + miner.PreCommitChallengeDelay + 1)
		rt.SetBalance(big.Mul(big.NewInt(1000), big.NewInt(1e18)))
		actor.proveCommitSector(rt, precommit, precommitEpoch, makeProveCommit(sectorNo), proveCommitConf{
			networkPower: 1 << 50,
		})
		st = getState(rt)

		// expect precommit to have been removed
		_, found, err = st.GetPrecommittedSector(rt.AdtStore(), sectorNo)
		require.NoError(t, err)
		require.False(t, found)

		// expect new onchain sector
		onChainSector, found, err := st.GetSector(rt.AdtStore(), sectorNo)
		require.NoError(t, err)
		require.True(t, found)

		// expect deal weights to be transfered to on chain info
		assert.Equal(t, onChainPrecommit.DealWeight, onChainSector.DealWeight)
		assert.Equal(t, onChainPrecommit.VerifiedDealWeight, onChainSector.VerifiedDealWeight)

		// expect activation epoch to be precommit
		assert.Equal(t, precommitEpoch, onChainSector.Activation)

		// expect deposit to have been released
		assert.Equal(t, big.Zero(), st.PreCommitDeposits)

		// expect locked initial pledge of sector to be the same as precommit deposit
		expectedInitialPledge := expectedDeposit
		assert.Equal(t, expectedInitialPledge, st.LockedFunds)
	})

	t.Run("invalid pre-commit rejected", func(t *testing.T) {
		rt := builder.
			WithBalance(abi.NewTokenAmount(1<<50), abi.NewTokenAmount(0)).
			Build(t)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		st := getState(rt)
		deadline := st.DeadlineInfo(precommitEpoch)
		challengeEpoch := precommitEpoch - 1

		// Good commitment.
		actor.preCommitSector(rt, makePreCommit(100, challengeEpoch, deadline.PeriodEnd(), nil), networkPower, big.Zero())

		// Duplicate sector ID
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.preCommitSector(rt, makePreCommit(100, challengeEpoch, deadline.PeriodEnd(), nil), networkPower, big.Zero())
		})
		rt.Reset()

		// Bad seal proof type
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			pc := makePreCommit(114, challengeEpoch, deadline.PeriodEnd(), nil)
			pc.SealProof = abi.RegisteredSealProof_StackedDrg8MiBV1
			actor.preCommitSector(rt, pc, networkPower, big.Zero())
		})
		rt.Reset()

		// Expires at current epoch
		rt.SetEpoch(deadline.PeriodEnd())
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.preCommitSector(rt, makePreCommit(111, challengeEpoch, deadline.PeriodEnd(), nil), networkPower, big.Zero())
		})
		rt.Reset()

		// Expires before current epoch
		rt.SetEpoch(deadline.PeriodEnd() + 1)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.preCommitSector(rt, makePreCommit(112, challengeEpoch, deadline.PeriodEnd(), nil), networkPower, big.Zero())
		})
		rt.Reset()

		// Expires not on period end
		rt.SetEpoch(precommitEpoch)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.preCommitSector(rt, makePreCommit(113, challengeEpoch, deadline.PeriodEnd()-1, nil), networkPower, big.Zero())
		})

		// TODO: test insufficient funds when the precommit deposit is set above zero
	})

	t.Run("invalid proof rejected", func(t *testing.T) {
		rt := builder.
			WithBalance(abi.NewTokenAmount(1<<50), abi.NewTokenAmount(0)).
			Build(t)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		st := getState(rt)
		deadline := st.DeadlineInfo(precommitEpoch)

		// Make a good commitment for the proof to target.
		sectorNo := abi.SectorNumber(100)
		precommit := makePreCommit(sectorNo, precommitEpoch-1, deadline.PeriodEnd(), nil)
		actor.preCommitSector(rt, precommit, networkPower, big.Zero())

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

		// Invalid deals (market ActivateDeals aborts)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.proveCommitSector(rt, precommit, precommitEpoch, makeProveCommit(sectorNo), proveCommitConf{
				verifyDealsExit: exitcode.ErrIllegalArgument,
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
		rt.SetBalance(big.Mul(big.NewInt(1000), big.NewInt(1e18)))
		actor.proveCommitSector(rt, precommit, precommitEpoch, makeProveCommit(sectorNo), proveCommitConf{
			networkPower: 1 << 50,
		})
		st = getState(rt)
		// Verify new sectors
		newSectors, err := st.NewSectors.All(miner.SectorsMax)
		require.NoError(t, err)
		assert.Equal(t, []uint64{uint64(sectorNo)}, newSectors)
		// Verify pledge lock-up
		assert.True(t, st.LockedFunds.GreaterThan(big.Zero()))
		rt.Reset()

		// Duplicate proof (sector no-longer pre-committed)
		rt.ExpectAbort(exitcode.ErrNotFound, func() {
			actor.proveCommitSector(rt, precommit, precommitEpoch, makeProveCommit(sectorNo), proveCommitConf{})
		})
		rt.Reset()
	})
}

func TestWindowPost(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	precommitEpoch := abi.ChainEpoch(1)
	builder := builderForHarness(actor).
		WithEpoch(precommitEpoch).
		WithBalance(big.Mul(big.NewInt(1000), big.NewInt(1e18)), big.Zero())

	t.Run("test proof", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		st := getState(rt)
		store := rt.AdtStore()
		partitionSize := st.Info.WindowPoStPartitionSectors
		_ = actor.commitAndProveSectors(rt, 1, 100, 1<<50, nil)

		// Skip to end of proving period, cron adds sectors to proving set.
		deadline := st.DeadlineInfo(rt.Epoch())
		rt.SetEpoch(deadline.PeriodEnd())
		nextCron := deadline.NextPeriodStart() + miner.WPoStProvingPeriod - 1
		actor.onProvingPeriodCron(rt, nextCron, true)
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
			require.NoError(t, err)
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
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(big.Mul(big.NewInt(1000), big.NewInt(1e18)), big.Zero())

	t.Run("empty periods", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		st := getState(rt)
		assert.Equal(t, periodOffset, st.ProvingPeriodStart)

		// First cron invocation just before the first proving period starts.
		rt.SetEpoch(periodOffset - 1)
		secondCronEpoch := periodOffset + miner.WPoStProvingPeriod - 1
		actor.onProvingPeriodCron(rt, secondCronEpoch, false)
		// The proving period start isn't changed, because the period hadn't started yet.
		st = getState(rt)
		assert.Equal(t, periodOffset, st.ProvingPeriodStart)

		rt.SetEpoch(secondCronEpoch)
		actor.onProvingPeriodCron(rt, periodOffset+2*miner.WPoStProvingPeriod-1, false)
		// Proving period moves forward
		st = getState(rt)
		assert.Equal(t, periodOffset+miner.WPoStProvingPeriod, st.ProvingPeriodStart)
	})

	t.Run("first period gets randomness from previous epoch", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		st := getState(rt)

		sectorInfo := actor.commitAndProveSectors(rt, 1, 100, 1<<50, nil)

		// Flag new sectors to trigger request for randomness
		rt.Transaction(st, func() interface{} {
			st.NewSectors.Set(uint64(sectorInfo[0].SectorNumber))
			return nil
		})

		// First cron invocation just before the first proving period starts
		// requires randomness come from current epoch minus lookback
		rt.SetEpoch(periodOffset - 1)
		secondCronEpoch := periodOffset + miner.WPoStProvingPeriod - 1
		actor.onProvingPeriodCron(rt, secondCronEpoch, true)

		// cron invocation after the proving period starts, requires randomness come from end of proving period
		rt.SetEpoch(periodOffset)
		actor.advanceProvingPeriodWithoutFaults(rt)

		// triggers a new request for randomness
		rt.Transaction(st, func() interface{} {
			st.NewSectors.Set(uint64(sectorInfo[0].SectorNumber))
			return nil
		})

		thirdCronEpoch := secondCronEpoch + miner.WPoStProvingPeriod
		actor.onProvingPeriodCron(rt, thirdCronEpoch, true)
	})

	// TODO: test cron being called one epoch late because the scheduled epoch had no blocks.
}

func TestDeclareFaults(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(big.Mul(big.NewInt(1000), big.NewInt(1e18)), big.Zero())

	t.Run("declare fault pays fee", func(t *testing.T) {
		// Get sector into proving state
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		precommits := actor.commitAndProveSectors(rt, 1, 100, 1<<50, nil)

		// Skip to end of proving period, cron adds sectors to proving set.
		st := getState(rt)
		deadline := st.DeadlineInfo(rt.Epoch())
		rt.SetEpoch(deadline.PeriodEnd())
		nextCron := deadline.NextPeriodStart() + miner.WPoStProvingPeriod - 1
		actor.onProvingPeriodCron(rt, nextCron, true)
		st = getState(rt)
		rt.SetEpoch(deadline.NextPeriodStart())
		info, found, err := st.GetSector(rt.AdtStore(), precommits[0].SectorNumber)
		require.NoError(t, err)
		require.True(t, found)

		// Declare the sector as faulted
		ss, err := info.SealProof.SectorSize()
		require.NoError(t, err)
		sectorQAPower := miner.QAPowerForSector(ss, info)
		expectedReward := big.NewInt(1e18) // 1 FIL
		totalQAPower := big.NewInt(1 << 52)
		fee := miner.PledgePenaltyForDeclaredFault(expectedReward, totalQAPower, sectorQAPower)

		actor.declareFaults(rt, expectedReward, totalQAPower, fee, info)
	})
}

func TestExtendSectorExpiration(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	precommitEpoch := abi.ChainEpoch(1)
	builder := builderForHarness(actor).
		WithEpoch(precommitEpoch).
		WithBalance(big.Mul(big.NewInt(1000), big.NewInt(1e18)), big.Zero())

	commitSector := func(t *testing.T, rt *mock.Runtime) *miner.SectorOnChainInfo {
		actor.constructAndVerify(rt)
		sectorInfo := actor.commitAndProveSectors(rt, 1, 100, 0, nil)

		sector, found, err := getState(rt).GetSector(rt.AdtStore(), sectorInfo[0].SectorNumber)
		require.NoError(t, err)
		require.True(t, found)
		return sector
	}

	t.Run("rejects negative extension", func(t *testing.T) {
		rt := builder.Build(t)
		sector := commitSector(t, rt)
		// attempt to shorten epoch
		newExpiration := sector.Expiration - abi.ChainEpoch(miner.WPoStProvingPeriod)
		params := &miner.ExtendSectorExpirationParams{
			SectorNumber:  sector.SectorNumber,
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
		newExpiration := sector.Expiration - abi.ChainEpoch(extension)
		params := &miner.ExtendSectorExpirationParams{
			SectorNumber:  sector.SectorNumber,
			NewExpiration: newExpiration,
		}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.extendSector(rt, sector, extension, params)
		})
	})

	t.Run("updates expiration with valid params", func(t *testing.T) {
		rt := builder.Build(t)
		oldSector := commitSector(t, rt)

		extension := 42 * miner.WPoStProvingPeriod
		newExpiration := oldSector.Expiration + extension
		params := &miner.ExtendSectorExpirationParams{
			SectorNumber:  oldSector.SectorNumber,
			NewExpiration: newExpiration,
		}

		actor.extendSector(rt, oldSector, extension, params)

		// assert sector expiration is set to the new value
		st := getState(rt)
		newSector, found, err := st.GetSector(rt.AdtStore(), oldSector.SectorNumber)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, newExpiration, newSector.Expiration)

		// assert that an expiration exists at the target epoch
		expirations, err := st.GetSectorExpirations(rt.AdtStore(), newExpiration)
		require.NoError(t, err)
		exists, err := expirations.IsSet(uint64(newSector.SectorNumber))
		require.NoError(t, err)
		assert.True(t, exists)

		// assert that the expiration has been removed from the old epoch
		expirations, err = st.GetSectorExpirations(rt.AdtStore(), oldSector.Expiration)
		require.NoError(t, err)
		exists, err = expirations.IsSet(uint64(newSector.SectorNumber))
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestReportConsensusFault(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(abi.NewTokenAmount(1<<50), abi.NewTokenAmount(0))

	rt := builder.Build(t)
	actor.constructAndVerify(rt)
	precommitEpoch := abi.ChainEpoch(1)
	rt.SetEpoch(precommitEpoch)
	dealIDs := [][]abi.DealID{{1, 2}, {3, 4}}
	sectorInfo := actor.commitAndProveSectors(rt, 2, 10, 1<<50, dealIDs)
	_ = sectorInfo

	params := &miner.ReportConsensusFaultParams{
		BlockHeader1:     nil,
		BlockHeader2:     nil,
		BlockHeaderExtra: nil,
	}

	// miner should send a single call to terminate the deals for all its sectors
	allDeals := []abi.DealID{}
	for _, ids := range dealIDs {
		allDeals = append(allDeals, ids...)
	}
	actor.reportConsensusFault(rt, addr.TestAddress, params, allDeals)
}
func TestAddLockedFund(t *testing.T) {

	periodOffset := abi.ChainEpoch(1808)
	actor := newHarness(t, periodOffset)

	builder := builderForHarness(actor).
		WithBalance(big.Mul(big.NewInt(1000), big.NewInt(1e18)), big.Zero())

	t.Run("funds vest", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		st := getState(rt)
		store := rt.AdtStore()

		// Nothing vesting to start
		vestingFunds, err := adt.AsArray(store, st.VestingFunds)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), vestingFunds.Length())
		assert.Equal(t, big.Zero(), st.LockedFunds)

		// Lock some funds with AddLockedFund
		amt := abi.NewTokenAmount(600_000)
		actor.addLockedFund(rt, &amt)
		st = getState(rt)
		newVestingFunds, err := adt.AsArray(store, st.VestingFunds)
		require.NoError(t, err)
		require.Equal(t, uint64(7), newVestingFunds.Length()) // 1 day steps over 1 week

		// Vested FIL pays out on epochs with expected offset
		lockedEntry := abi.NewTokenAmount(0)
		expectedOffset := periodOffset % miner.PledgeVestingSpec.Quantization
		err = newVestingFunds.ForEach(&lockedEntry, func(k int64) error {
			assert.Equal(t, int64(expectedOffset), k % int64(miner.PledgeVestingSpec.Quantization))
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, amt, st.LockedFunds)

	})

}

type actorHarness struct {
	a miner.Actor
	t testing.TB

	receiver addr.Address // The miner actor's own address
	owner    addr.Address
	worker   addr.Address
	key      addr.Address

	periodOffset abi.ChainEpoch
	nextSectorNo abi.SectorNumber

	epochReward   abi.TokenAmount
	networkPledge abi.TokenAmount
}

func newHarness(t testing.TB, provingPeriodOffset abi.ChainEpoch) *actorHarness {
	owner := tutil.NewIDAddr(t, 100)
	worker := tutil.NewIDAddr(t, 101)
	workerKey := tutil.NewBLSAddr(t, 0)
	receiver := tutil.NewIDAddr(t, 1000)
	reward := big.Mul(big.NewIntUnsigned(100), big.NewIntUnsigned(1e18))
	return &actorHarness{
		t:             t,
		receiver:      receiver,
		owner:         owner,
		worker:        worker,
		key:           workerKey,
		periodOffset:  provingPeriodOffset,
		nextSectorNo:  100,
		epochReward:   reward,
		networkPledge: big.Mul(reward, big.NewIntUnsigned(1000)),
	}
}

func (h *actorHarness) constructAndVerify(rt *mock.Runtime) {
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
	nextProvingPeriodEnd := h.periodOffset - 1
	for nextProvingPeriodEnd < rt.Epoch() {
		nextProvingPeriodEnd += miner.WPoStProvingPeriod
	}
	rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent,
		makeProvingPeriodCronEventParams(h.t, nextProvingPeriodEnd), big.Zero(), nil, exitcode.Ok)
	rt.SetCaller(builtin.InitActorAddr, builtin.InitActorCodeID)
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

func (h *actorHarness) preCommitSector(rt *mock.Runtime, params *miner.SectorPreCommitInfo, networkPower abi.StoragePower, pledgeDelta abi.TokenAmount) {

	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(h.worker)

	{
		rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.LastPerEpochReward, nil, big.Zero(), &h.epochReward, exitcode.Ok)

		pwrTotal := power.CurrentTotalPowerReturn{
			RawBytePower:     networkPower,
			QualityAdjPower:  networkPower,
			PledgeCollateral: h.networkPledge,
		}
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.CurrentTotalPower, nil, big.Zero(), &pwrTotal, exitcode.Ok)
	}
	{
		sectorSize, err := params.SealProof.SectorSize()
		require.NoError(h.t, err)

		vdParams := market.VerifyDealsForActivationParams{
			DealIDs:      params.DealIDs,
			SectorStart:  rt.Epoch(),
			SectorExpiry: params.Expiration,
		}

		vdReturn := market.VerifyDealsForActivationReturn{
			DealWeight:         big.NewInt(int64(sectorSize / 2)),
			VerifiedDealWeight: big.NewInt(int64(sectorSize / 2)),
		}
		rt.ExpectSend(builtin.StorageMarketActorAddr, builtin.MethodsMarket.VerifyDealsForActivation, &vdParams, big.Zero(), &vdReturn, exitcode.Ok)
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
	networkPower    uint64
	verifyDealsExit exitcode.ExitCode
	verifySealErr   error
}

func (h *actorHarness) proveCommitSector(rt *mock.Runtime, precommit *miner.SectorPreCommitInfo, precommitEpoch abi.ChainEpoch,
	params *miner.ProveCommitSectorParams, conf proveCommitConf) {
	commd := cbg.CborCid(tutil.MakeCID("commd"))
	sealRand := abi.SealRandomness([]byte{1, 2, 3, 4})
	sealIntRand := abi.InteractiveSealRandomness([]byte{5, 6, 7, 8})
	interactiveEpoch := precommitEpoch + miner.PreCommitChallengeDelay

	// Prepare for and receive call to ProveCommitSector
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
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.SubmitPoRepForBulkVerify, &seal, abi.NewTokenAmount(0), nil, exitcode.Ok)
	}
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAny()
	rt.Call(h.a.ProveCommitSector, params)

	// Prepare for and receive call to ConfirmSectorProofsValid at the end of the same epoch.
	{
		vdParams := market.ActivateDealsParams{
			DealIDs:      precommit.DealIDs,
			SectorExpiry: precommit.Expiration,
		}
		rt.ExpectSend(builtin.StorageMarketActorAddr, builtin.MethodsMarket.ActivateDeals, &vdParams, big.Zero(), nil, conf.verifyDealsExit)
	}
	{
		// expected pledge is the precommit deposit
		st := getState(rt)
		precommitOnChain, found, err := st.GetPrecommittedSector(rt.AdtStore(), precommit.SectorNumber)
		require.NoError(h.t, err)
		require.True(h.t, found)

		sectorSize, err := precommit.SealProof.SectorSize()
		require.NoError(h.t, err)
		newSector := miner.SectorOnChainInfo{
			SectorNumber:       precommit.SectorNumber,
			SealProof:          precommit.SealProof,
			SealedCID:          precommit.SealedCID,
			DealIDs:            precommit.DealIDs,
			Expiration:         precommit.Expiration,
			Activation:         precommitEpoch,
			DealWeight:         precommitOnChain.DealWeight,
			VerifiedDealWeight: precommitOnChain.VerifiedDealWeight,
		}
		qaPower := miner.QAPowerForSector(sectorSize, &newSector)
		pcParams := power.UpdateClaimedPowerParams{
			RawByteDelta:         big.NewIntUnsigned(uint64(sectorSize)),
			QualityAdjustedDelta: qaPower,
		}
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdateClaimedPower, &pcParams, big.Zero(), nil, exitcode.Ok)

		expectedPledge := precommitOnChain.PreCommitDeposit
		rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.UpdatePledgeTotal, &expectedPledge, big.Zero(), nil, exitcode.Ok)
	}

	if conf.verifySealErr == nil {
		rt.SetCaller(builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID)
		rt.ExpectValidateCallerAddr(builtin.StoragePowerActorAddr)
		rt.Call(h.a.ConfirmSectorProofsValid, &builtin.ConfirmSectorProofsParams{Sectors: []abi.SectorNumber{params.SectorNumber}})
	}
	rt.Verify()
}

// Pre-commits and then proves a number of sectors.
// The sectors will expire at the end of  lifetimePeriods proving periods after now.
// The runtime epoch will be moved forward to the epoch of commitment proofs.
func (h *actorHarness) commitAndProveSectors(rt *mock.Runtime, n int, lifetimePeriods uint64, networkPower uint64, dealIDs [][]abi.DealID) []*miner.SectorPreCommitInfo {
	precommitEpoch := rt.Epoch()

	st := getState(rt)
	deadline := st.DeadlineInfo(rt.Epoch())
	expiration := deadline.PeriodEnd() + abi.ChainEpoch(lifetimePeriods)*miner.WPoStProvingPeriod

	// Precommit
	precommits := make([]*miner.SectorPreCommitInfo, n)
	for i := 0; i < n; i++ {
		sectorNo := h.nextSectorNo
		var sectorDealIDs []abi.DealID
		if dealIDs != nil {
			sectorDealIDs = dealIDs[i]
		}
		precommit := makePreCommit(sectorNo, precommitEpoch-1, expiration, sectorDealIDs)
		h.preCommitSector(rt, precommit, abi.NewStoragePower(int64(networkPower)), big.Zero())
		precommits[i] = precommit
		h.nextSectorNo++
	}

	rt.SetEpoch(precommitEpoch + miner.PreCommitChallengeDelay + 1)

	// Ensure this this doesn't cross a proving period boundary, else the expected cron call won't be
	// invoked, which might mess things up later.
	deadline = getState(rt).DeadlineInfo(rt.Epoch())
	require.True(h.t, !deadline.PeriodElapsed())

	for _, pc := range precommits {
		h.proveCommitSector(rt, pc, precommitEpoch, makeProveCommit(pc.SectorNumber), proveCommitConf{
			networkPower: networkPower,
		})
	}
	rt.Reset()
	return precommits
}

func (h *actorHarness) submitWindowPost(rt *mock.Runtime, deadline *miner.DeadlineInfo, partitions []uint64, infos []*miner.SectorOnChainInfo) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(h.worker)

	reward := big.NewIntUnsigned(1e18)
	rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.LastPerEpochReward, nil, big.Zero(), &reward, exitcode.Ok)

	pwrTotal := power.CurrentTotalPowerReturn{
		QualityAdjPower: big.NewIntUnsigned(1 << 50),
	}
	rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.CurrentTotalPower, nil, big.Zero(), &pwrTotal, exitcode.Ok)

	var registeredPoStProof, err = abi.RegisteredSealProof_StackedDrg2KiBV1.RegisteredWindowPoStProof()
	require.NoError(h.t, err)

	proofs := make([]abi.PoStProof, 1) // Number of proofs doesn't depend on partition count
	for i := range proofs {
		proofs[i].PoStProof = registeredPoStProof
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
				SealProof:    ci.SealProof,
				SectorNumber: ci.SectorNumber,
				SealedCID:    ci.SealedCID,
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

func (h *actorHarness) declareFaults(rt *mock.Runtime, expectedReward abi.TokenAmount, totalQAPower abi.StoragePower, fee abi.TokenAmount, faultSectorInfos ...*miner.SectorOnChainInfo) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(h.worker)

	ss, err := faultSectorInfos[0].SealProof.SectorSize()
	require.NoError(h.t, err)
	expectedRawDelta, expectedQADelta := miner.PowerForSectors(ss, faultSectorInfos)
	expectedRawDelta = expectedRawDelta.Neg()
	expectedQADelta = expectedQADelta.Neg()

	expectedTotalPower := &power.CurrentTotalPowerReturn{
		QualityAdjPower: totalQAPower,
	}

	expectQueryNetworkInfo(rt, expectedTotalPower, expectedReward)

	// expect power update
	claim := &power.UpdateClaimedPowerParams{
		RawByteDelta:         expectedRawDelta,
		QualityAdjustedDelta: expectedQADelta,
	}
	rt.ExpectSend(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.UpdateClaimedPower,
		claim,
		abi.NewTokenAmount(0),
		nil,
		exitcode.Ok,
	)

	// expect fee
	rt.ExpectSend(
		builtin.BurntFundsActorAddr,
		builtin.MethodSend,
		nil,
		fee,
		nil,
		exitcode.Ok,
	)

	// expect pledge update
	pledgeDelta := fee.Neg()
	rt.ExpectSend(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.UpdatePledgeTotal,
		&pledgeDelta,
		abi.NewTokenAmount(0),
		nil,
		exitcode.Ok,
	)

	// Calculate params from faulted sector infos
	st := getState(rt)
	params := makeFaultParamsFromFaultingSectors(h.t, st, rt.AdtStore(), faultSectorInfos)
	rt.Call(h.a.DeclareFaults, params)
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
		builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "could not get partitions for deadline")
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
	// Rewind one epoch to leave the current epoch as the penultimate one in the proving period,
	// ready for proving-period cron.
	rt.SetEpoch(rt.Epoch() - 1)
}

func (h *actorHarness) extendSector(rt *mock.Runtime, sector *miner.SectorOnChainInfo, extension abi.ChainEpoch, params *miner.ExtendSectorExpirationParams) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(h.worker)

	st := getState(rt)
	newSector := *sector
	newSector.Expiration += extension
	qaDelta := big.Sub(miner.QAPowerForSector(st.Info.SectorSize, &newSector), miner.QAPowerForSector(st.Info.SectorSize, sector))

	rt.ExpectSend(builtin.StoragePowerActorAddr,
		builtin.MethodsPower.UpdateClaimedPower,
		&power.UpdateClaimedPowerParams{
			RawByteDelta:         big.Zero(),
			QualityAdjustedDelta: qaDelta,
		},
		abi.NewTokenAmount(0),
		nil,
		exitcode.Ok,
	)
	rt.Call(h.a.ExtendSectorExpiration, params)
}

func (h *actorHarness) reportConsensusFault(rt *mock.Runtime, from addr.Address, params *miner.ReportConsensusFaultParams, dealIDs []abi.DealID) {
	rt.SetCaller(from, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerType(builtin.CallerTypesSignable...)

	rt.ExpectVerifyConsensusFault(params.BlockHeader1, params.BlockHeader2, params.BlockHeaderExtra, &runtime.ConsensusFault{
		Target: h.receiver,
		Epoch:  rt.Epoch() - 1,
		Type:   runtime.ConsensusFaultDoubleForkMining,
	}, nil)

	// slash reward
	reward := miner.RewardForConsensusSlashReport(1, rt.Balance())
	rt.ExpectSend(from, builtin.MethodSend, nil, reward, nil, exitcode.Ok)

	// power termination
	lockedFunds := getState(rt).LockedFunds
	rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.OnConsensusFault, &lockedFunds, abi.NewTokenAmount(0), nil, exitcode.Ok)

	// expect every deal to be closed out
	rt.ExpectSend(builtin.StorageMarketActorAddr, builtin.MethodsMarket.OnMinerSectorsTerminate, &market.OnMinerSectorsTerminateParams{
		DealIDs: dealIDs,
	}, abi.NewTokenAmount(0), nil, exitcode.Ok)

	// expect actor to be deleted
	rt.ExpectDeleteActor(builtin.BurntFundsActorAddr)

	rt.Call(h.a.ReportConsensusFault, params)
	rt.Verify()
}

func (h *actorHarness) addLockedFund(rt *mock.Runtime, amt *abi.TokenAmount) {
	rt.SetCaller(h.worker, builtin.AccountActorCodeID)
	rt.ExpectValidateCallerAddr(h.worker, h.owner, builtin.RewardActorAddr)
	// expect pledge update
	rt.ExpectSend(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.UpdatePledgeTotal,
		amt,
		abi.NewTokenAmount(0),
		nil,
		exitcode.Ok,
	)

	rt.Call(h.a.AddLockedFund, amt)
}

func (h *actorHarness) onProvingPeriodCron(rt *mock.Runtime, expectedEnrollment abi.ChainEpoch, newSectors bool) {
	rt.ExpectValidateCallerAddr(builtin.StoragePowerActorAddr)
	if newSectors {
		randEpoch := rt.Epoch()-miner.ElectionLookback
		rt.ExpectGetRandomness(crypto.DomainSeparationTag_WindowedPoStDeadlineAssignment, randEpoch, nil, bytes.Repeat([]byte{0}, 32))
	}
	// Re-enrollment for next period.
	reward := big.NewIntUnsigned(1e18)
	rt.ExpectSend(builtin.RewardActorAddr, builtin.MethodsReward.LastPerEpochReward, nil, big.Zero(), &reward, exitcode.Ok)

	pwrTotal := power.CurrentTotalPowerReturn{
		QualityAdjPower: big.NewIntUnsigned(1 << 50),
	}
	rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.CurrentTotalPower, nil, big.Zero(), &pwrTotal, exitcode.Ok)
	rt.ExpectSend(builtin.StoragePowerActorAddr, builtin.MethodsPower.EnrollCronEvent,
		makeProvingPeriodCronEventParams(h.t, expectedEnrollment), big.Zero(), nil, exitcode.Ok)
	rt.SetCaller(builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID)
	rt.Call(h.a.OnDeferredCronEvent, &miner.CronEventPayload{
		EventType: miner.CronEventProvingPeriod,
	})
	rt.Verify()
}

func builderForHarness(actor *actorHarness) *mock.RuntimeBuilder {
	return mock.NewBuilder(context.Background(), actor.receiver).
		WithActorType(actor.owner, builtin.AccountActorCodeID).
		WithActorType(actor.worker, builtin.AccountActorCodeID).
		WithHasher(fixedHasher(uint64(actor.periodOffset)))
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

func makePreCommit(sectorNo abi.SectorNumber, challenge, expiration abi.ChainEpoch, dealIDs []abi.DealID) *miner.SectorPreCommitInfo {
	return &miner.SectorPreCommitInfo{
		SealProof:     abi.RegisteredSealProof_StackedDrg2KiBV1,
		SectorNumber:  sectorNo,
		SealedCID:     tutil.MakeCID("commr"),
		SealRandEpoch: challenge,
		DealIDs:       dealIDs,
		Expiration:    expiration,
	}
}

func makeProveCommit(sectorNo abi.SectorNumber) *miner.ProveCommitSectorParams {
	return &miner.ProveCommitSectorParams{
		SectorNumber: sectorNo,
		Proof:        []byte("proof"),
	}
}

func makeFaultParamsFromFaultingSectors(t testing.TB, st *miner.State, store adt.Store, faultSectorInfos []*miner.SectorOnChainInfo) *miner.DeclareFaultsParams {
	deadlines, err := st.LoadDeadlines(store)
	require.NoError(t, err)
	faultAtDeadline := make(map[uint64][]uint64)
	// Find the deadline for each faulty sector which must be provided with the fault declaration
	for _, sectorInfo := range faultSectorInfos {
		dl, err := miner.FindDeadline(deadlines, sectorInfo.SectorNumber)
		require.NoError(t, err)
		faultAtDeadline[dl] = append(faultAtDeadline[dl], uint64(sectorInfo.SectorNumber))
	}
	params := &miner.DeclareFaultsParams{Faults: []miner.FaultDeclaration{}}
	// Group together faults at the same deadline into a bitfield
	for dl, sectorNumbers := range faultAtDeadline {
		fault := miner.FaultDeclaration{
			Deadline: dl,
			Sectors:  bitfield.NewFromSet(sectorNumbers),
		}
		params.Faults = append(params.Faults, fault)
	}
	return params
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

func expectQueryNetworkInfo(rt *mock.Runtime, expectedTotalPower *power.CurrentTotalPowerReturn, expectedReward big.Int) {
	rt.ExpectSend(
		builtin.RewardActorAddr,
		builtin.MethodsReward.LastPerEpochReward,
		nil,
		big.Zero(),
		&expectedReward,
		exitcode.Ok,
	)

	rt.ExpectSend(
		builtin.StoragePowerActorAddr,
		builtin.MethodsPower.CurrentTotalPower,
		nil,
		big.Zero(),
		expectedTotalPower,
		exitcode.Ok,
	)
}
