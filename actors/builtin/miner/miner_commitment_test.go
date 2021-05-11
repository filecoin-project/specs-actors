package miner_test

import (
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/dline"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-state-types/network"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin"
	"github.com/filecoin-project/specs-actors/v5/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v5/actors/runtime"
	"github.com/filecoin-project/specs-actors/v5/actors/util/smoothing"
	"github.com/filecoin-project/specs-actors/v5/support/mock"
	tutil "github.com/filecoin-project/specs-actors/v5/support/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test for sector precommitment and proving.
func TestCommitments(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)

	t.Run("insufficient funds for pre-commit", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		insufficientBalance := abi.NewTokenAmount(10) // 10 AttoFIL
		rt := builderForHarness(actor).
			WithBalance(insufficientBalance, big.Zero()).
			Build(t)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		deadline := actor.deadline(rt)
		challengeEpoch := precommitEpoch - 1
		expiration := deadline.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod

		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			actor.preCommitSector(rt, actor.makePreCommit(101, challengeEpoch, expiration, nil), preCommitConf{}, true)
		})
		actor.checkState(rt)
	})

	t.Run("deal space exceeds sector space", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)

		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		deadline := actor.deadline(rt)
		challengeEpoch := precommitEpoch - 1
		expiration := deadline.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod

		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "deals too large to fit in sector", func() {
			actor.preCommitSector(rt, actor.makePreCommit(101, challengeEpoch, expiration, []abi.DealID{1}), preCommitConf{
				dealSpace: actor.sectorSize + 1,
			}, true)
		})
		actor.checkState(rt)
	})

	t.Run("precommit pays back fee debt", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)

		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		deadline := actor.deadline(rt)
		challengeEpoch := precommitEpoch - 1
		expiration := deadline.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod

		st := getState(rt)
		st.FeeDebt = abi.NewTokenAmount(9999)
		rt.ReplaceState(st)

		actor.preCommitSector(rt, actor.makePreCommit(101, challengeEpoch, expiration, nil), preCommitConf{}, true)
		st = getState(rt)
		assert.Equal(t, big.Zero(), st.FeeDebt)
		actor.checkState(rt)
	})

	t.Run("invalid pre-commit rejected", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		deadline := actor.deadline(rt)
		challengeEpoch := precommitEpoch - 1

		oldSector := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)[0]
		st := getState(rt)
		assert.True(t, st.DeadlineCronActive)
		// Good commitment.
		expiration := deadline.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod
		actor.preCommitSector(rt, actor.makePreCommit(101, challengeEpoch, expiration, nil), preCommitConf{}, false)
		// Duplicate pre-commit sector ID
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "already allocated", func() {
			actor.preCommitSector(rt, actor.makePreCommit(101, challengeEpoch, expiration, nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Sector ID already committed
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "already allocated", func() {
			actor.preCommitSector(rt, actor.makePreCommit(oldSector.SectorNumber, challengeEpoch, expiration, nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Bad sealed CID
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "sealed CID had wrong prefix", func() {
			pc := actor.makePreCommit(102, challengeEpoch, deadline.PeriodEnd(), nil)
			pc.SealedCID = tutil.MakeCID("Random Data", nil)
			actor.preCommitSector(rt, pc, preCommitConf{}, false)
		})
		rt.Reset()

		// Bad seal proof type
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "unsupported seal proof type", func() {
			pc := actor.makePreCommit(102, challengeEpoch, deadline.PeriodEnd(), nil)
			pc.SealProof = abi.RegisteredSealProof_StackedDrg8MiBV1_1
			actor.preCommitSector(rt, pc, preCommitConf{}, false)
		})
		rt.Reset()

		// Expires at current epoch
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "must be after activation", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, rt.Epoch(), nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Expires before current epoch
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "must be after activation", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, rt.Epoch()-1, nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Expires too early
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "must exceed", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration-20*builtin.EpochsInDay, nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Expires before min duration + max seal duration
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "must exceed", func() {
			expiration := rt.Epoch() + miner.MinSectorExpiration + miner.MaxProveCommitDuration[actor.sealProofType] - 1
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration, nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Errors when expiry too far in the future
		rt.SetEpoch(precommitEpoch)
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "invalid expiration", func() {
			expiration := deadline.PeriodEnd() + miner.WPoStProvingPeriod*(miner.MaxSectorExpirationExtension/miner.WPoStProvingPeriod+1)
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration, nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Sector ID out of range
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "out of range", func() {
			actor.preCommitSector(rt, actor.makePreCommit(abi.MaxSectorNumber+1, challengeEpoch, expiration, nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Seal randomness challenge too far in past
		tooOldChallengeEpoch := precommitEpoch - miner.ChainFinality - miner.MaxProveCommitDuration[actor.sealProofType] - 1
		rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "too old", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, tooOldChallengeEpoch, expiration, nil), preCommitConf{}, false)
		})
		rt.Reset()

		// Try to precommit while in fee debt with insufficient balance
		st = getState(rt)
		st.FeeDebt = big.Add(rt.Balance(), abi.NewTokenAmount(1e18))
		rt.ReplaceState(st)
		rt.ExpectAbortContainsMessage(exitcode.ErrInsufficientFunds, "unlocked balance can not repay fee debt", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration, nil), preCommitConf{}, false)
		})
		// reset state back to normal
		st.FeeDebt = big.Zero()
		rt.ReplaceState(st)
		rt.Reset()

		// Try to precommit with an active consensus fault
		st = getState(rt)

		actor.reportConsensusFault(rt, addr.TestAddress, &runtime.ConsensusFault{
			Target: actor.receiver,
			Epoch:  rt.Epoch() - 1,
			Type:   runtime.ConsensusFaultDoubleForkMining,
		})
		rt.ExpectAbortContainsMessage(exitcode.ErrForbidden, "precommit not allowed during active consensus fault", func() {
			actor.preCommitSector(rt, actor.makePreCommit(102, challengeEpoch, expiration, nil), preCommitConf{}, false)
		})
		// reset state back to normal
		rt.ReplaceState(st)
		rt.Reset()
		actor.checkState(rt)
	})

	t.Run("fails with too many deals", func(t *testing.T) {
		setup := func(proof abi.RegisteredSealProof) (*mock.Runtime, *actorHarness, *dline.Info) {
			actor := newHarness(t, periodOffset)
			actor.setProofType(proof)
			rt := builderForHarness(actor).
				WithBalance(bigBalance, big.Zero()).
				Build(t)
			rt.SetEpoch(periodOffset + 1)
			actor.constructAndVerify(rt)
			deadline := actor.deadline(rt)
			return rt, actor, deadline
		}

		makeDealIDs := func(n int) []abi.DealID {
			ids := make([]abi.DealID, n)
			for i := range ids {
				ids[i] = abi.DealID(i)
			}
			return ids
		}

		// Make a good commitment for the proof to target.
		sectorNo := abi.SectorNumber(100)

		dealLimits := map[abi.RegisteredSealProof]int{
			abi.RegisteredSealProof_StackedDrg2KiBV1_1:  256,
			abi.RegisteredSealProof_StackedDrg32GiBV1_1: 256,
			abi.RegisteredSealProof_StackedDrg64GiBV1_1: 512,
		}

		for proof, limit := range dealLimits {
			// attempt to pre-commmit a sector with too many sectors
			rt, actor, deadline := setup(proof)
			expiration := deadline.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod
			precommit := actor.makePreCommit(sectorNo, rt.Epoch()-1, expiration, makeDealIDs(limit+1))
			rt.ExpectAbortContainsMessage(exitcode.ErrIllegalArgument, "too many deals for sector", func() {
				actor.preCommitSector(rt, precommit, preCommitConf{}, true)
			})

			// sector at or below limit succeeds
			rt, actor, _ = setup(proof)
			precommit = actor.makePreCommit(sectorNo, rt.Epoch()-1, expiration, makeDealIDs(limit))
			actor.preCommitSector(rt, precommit, preCommitConf{}, true)
			actor.checkState(rt)
		}
	})

	t.Run("precommit checks seal proof version", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		actor.setProofType(abi.RegisteredSealProof_StackedDrg32GiBV1)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)

		// Create miner before version 7
		rt.SetNetworkVersion(network.Version6)
		actor.constructAndVerify(rt)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		deadline := actor.deadline(rt)
		challengeEpoch := precommitEpoch - 1
		expiration := deadline.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod
		{
			// After version 7, only V1_1 accepted
			rt.SetNetworkVersion(network.Version8)
			pc := actor.makePreCommit(104, challengeEpoch, expiration, nil)
			pc.SealProof = abi.RegisteredSealProof_StackedDrg32GiBV1
			rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
				actor.preCommitSector(rt, pc, preCommitConf{}, true)
			})
			rt.Reset()
			pc.SealProof = abi.RegisteredSealProof_StackedDrg32GiBV1_1
			actor.preCommitSector(rt, pc, preCommitConf{}, true)
		}

		actor.checkState(rt)
	})

	for _, test := range []struct {
		name                string
		version             network.Version
		expectedPledgeDelta abi.TokenAmount
		sealProofType       abi.RegisteredSealProof
	}{{
		name:                "precommit does not vest funds in version 8",
		version:             network.Version8,
		expectedPledgeDelta: abi.NewTokenAmount(0),
		sealProofType:       abi.RegisteredSealProof_StackedDrg32GiBV1_1,
	}} {
		t.Run(test.name, func(t *testing.T) {
			actor := newHarness(t, periodOffset)
			actor.setProofType(test.sealProofType)
			rt := builderForHarness(actor).
				WithBalance(bigBalance, big.Zero()).
				WithNetworkVersion(test.version).
				Build(t)
			precommitEpoch := periodOffset + 1
			rt.SetEpoch(precommitEpoch)
			actor.constructAndVerify(rt)
			dlInfo := actor.deadline(rt)

			// Make a good commitment for the proof to target.
			sectorNo := abi.SectorNumber(100)
			expiration := dlInfo.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod // something on deadline boundary but > 180 days

			// add 1000 tokens that vest immediately
			st := getState(rt)
			_, err := st.AddLockedFunds(rt.AdtStore(), rt.Epoch(), abi.NewTokenAmount(1000), &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   1,
				StepDuration: 1,
				Quantization: 1,
			})
			require.NoError(t, err)
			rt.ReplaceState(st)

			rt.SetEpoch(rt.Epoch() + 2)

			// Pre-commit with a deal in order to exercise non-zero deal weights.
			precommitParams := actor.makePreCommit(sectorNo, precommitEpoch-1, expiration, []abi.DealID{1})
			actor.preCommitSector(rt, precommitParams, preCommitConf{
				pledgeDelta: &test.expectedPledgeDelta,
			}, true)
		})
	}
}

func TestProveCommit(t *testing.T) {
	periodOffset := abi.ChainEpoch(100)
	actor := newHarness(t, periodOffset)
	builder := builderForHarness(actor).
		WithBalance(bigBalance, big.Zero())

	t.Run("valid precommit then provecommit", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		dlInfo := actor.deadline(rt)

		// Make a good commitment for the proof to target.
		// Use the max sector number to make sure everything works.
		sectorNo := abi.SectorNumber(abi.MaxSectorNumber)
		proveCommitEpoch := precommitEpoch + miner.PreCommitChallengeDelay + 1
		expiration := dlInfo.PeriodEnd() + defaultSectorExpiration*miner.WPoStProvingPeriod // something on deadline boundary but > 180 days
		// Fill the sector with verified deals
		sectorWeight := big.Mul(big.NewInt(int64(actor.sectorSize)), big.NewInt(int64(expiration-proveCommitEpoch)))
		dealWeight := big.Zero()
		verifiedDealWeight := sectorWeight

		// Pre-commit with a deal in order to exercise non-zero deal weights.
		precommitParams := actor.makePreCommit(sectorNo, precommitEpoch-1, expiration, []abi.DealID{1})
		precommit := actor.preCommitSector(rt, precommitParams, preCommitConf{
			dealWeight:         dealWeight,
			verifiedDealWeight: verifiedDealWeight,
		}, true)

		// assert precommit exists and meets expectations
		onChainPrecommit := actor.getPreCommit(rt, sectorNo)

		// deal weights must be set in precommit onchain info
		assert.Equal(t, dealWeight, onChainPrecommit.DealWeight)
		assert.Equal(t, verifiedDealWeight, onChainPrecommit.VerifiedDealWeight)

		pwrEstimate := miner.QAPowerForWeight(actor.sectorSize, precommit.Info.Expiration-precommitEpoch, onChainPrecommit.DealWeight, onChainPrecommit.VerifiedDealWeight)
		expectedDeposit := miner.PreCommitDepositForPower(actor.epochRewardSmooth, actor.epochQAPowerSmooth, pwrEstimate)
		assert.Equal(t, expectedDeposit, onChainPrecommit.PreCommitDeposit)

		// expect total precommit deposit to equal our new deposit
		st := getState(rt)
		assert.Equal(t, expectedDeposit, st.PreCommitDeposits)

		// run prove commit logic
		rt.SetEpoch(proveCommitEpoch)
		rt.SetBalance(big.Mul(big.NewInt(1000), big.NewInt(1e18)))
		actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(sectorNo), proveCommitConf{})

		// expect precommit to have been removed
		st = getState(rt)
		_, found, err := st.GetPrecommittedSector(rt.AdtStore(), sectorNo)
		require.NoError(t, err)
		require.False(t, found)

		// expect deposit to have been transferred to initial pledges
		assert.Equal(t, big.Zero(), st.PreCommitDeposits)

		// The sector is exactly full with verified deals, so expect fully verified power.
		expectedPower := big.Mul(big.NewInt(int64(actor.sectorSize)), big.Div(builtin.VerifiedDealWeightMultiplier, builtin.QualityBaseMultiplier))
		qaPower := miner.QAPowerForWeight(actor.sectorSize, precommit.Info.Expiration-rt.Epoch(), onChainPrecommit.DealWeight, onChainPrecommit.VerifiedDealWeight)
		assert.Equal(t, expectedPower, qaPower)
		expectedInitialPledge := miner.InitialPledgeForPower(qaPower, actor.baselinePower, actor.epochRewardSmooth,
			actor.epochQAPowerSmooth, rt.TotalFilCircSupply())
		assert.Equal(t, expectedInitialPledge, st.InitialPledge)

		// expect new onchain sector
		sector := actor.getSector(rt, sectorNo)
		sectorPower := miner.NewPowerPair(big.NewIntUnsigned(uint64(actor.sectorSize)), qaPower)

		// expect deal weights to be transferred to on chain info
		assert.Equal(t, onChainPrecommit.DealWeight, sector.DealWeight)
		assert.Equal(t, onChainPrecommit.VerifiedDealWeight, sector.VerifiedDealWeight)

		// expect activation epoch to be current epoch
		assert.Equal(t, rt.Epoch(), sector.Activation)

		// expect initial plege of sector to be set
		assert.Equal(t, expectedInitialPledge, sector.InitialPledge)

		// expect locked initial pledge of sector to be the same as pledge requirement
		assert.Equal(t, expectedInitialPledge, st.InitialPledge)

		// expect sector to be assigned a deadline/partition
		dlIdx, pIdx, err := st.FindSector(rt.AdtStore(), sectorNo)
		require.NoError(t, err)
		deadline, partition := actor.getDeadlineAndPartition(rt, dlIdx, pIdx)
		assert.Equal(t, uint64(1), deadline.LiveSectors)
		assertEmptyBitfield(t, deadline.PartitionsPoSted)
		assertEmptyBitfield(t, deadline.EarlyTerminations)

		quant := st.QuantSpecForDeadline(dlIdx)
		quantizedExpiration := quant.QuantizeUp(precommit.Info.Expiration)

		dQueue := actor.collectDeadlineExpirations(rt, deadline)
		assert.Equal(t, map[abi.ChainEpoch][]uint64{
			quantizedExpiration: {pIdx},
		}, dQueue)

		assertBitfieldEquals(t, partition.Sectors, uint64(sectorNo))
		assertEmptyBitfield(t, partition.Faults)
		assertEmptyBitfield(t, partition.Recoveries)
		assertEmptyBitfield(t, partition.Terminated)
		assert.Equal(t, sectorPower, partition.LivePower)
		assert.Equal(t, miner.NewPowerPairZero(), partition.FaultyPower)
		assert.Equal(t, miner.NewPowerPairZero(), partition.RecoveringPower)

		pQueue := actor.collectPartitionExpirations(rt, partition)
		entry, ok := pQueue[quantizedExpiration]
		require.True(t, ok)
		assertBitfieldEquals(t, entry.OnTimeSectors, uint64(sectorNo))
		assertEmptyBitfield(t, entry.EarlySectors)
		assert.Equal(t, expectedInitialPledge, entry.OnTimePledge)
		assert.Equal(t, sectorPower, entry.ActivePower)
		assert.Equal(t, miner.NewPowerPairZero(), entry.FaultyPower)
	})

	t.Run("invalid proof rejected", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		deadline := actor.deadline(rt)

		// Make a good commitment for the proof to target.
		sectorNo := abi.SectorNumber(100)
		params := actor.makePreCommit(sectorNo, precommitEpoch-1, deadline.PeriodEnd()+defaultSectorExpiration*miner.WPoStProvingPeriod, []abi.DealID{1})
		precommit := actor.preCommitSector(rt, params, preCommitConf{}, true)

		// Sector pre-commitment missing.
		rt.SetEpoch(precommitEpoch + miner.PreCommitChallengeDelay + 1)
		rt.ExpectAbort(exitcode.ErrNotFound, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(sectorNo+1), proveCommitConf{})
		})
		rt.Reset()

		// Too late.
		rt.SetEpoch(precommitEpoch + miner.MaxProveCommitDuration[precommit.Info.SealProof] + 1)
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(sectorNo), proveCommitConf{})
		})
		rt.Reset()

		// Too early.
		rt.SetEpoch(precommitEpoch + miner.PreCommitChallengeDelay - 1)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(sectorNo), proveCommitConf{})
		})
		rt.Reset()

		// Set the right epoch for all following tests
		rt.SetEpoch(precommitEpoch + miner.PreCommitChallengeDelay + 1)

		// Invalid deals (market ActivateDeals aborts)
		verifyDealsExit := map[abi.SectorNumber]exitcode.ExitCode{
			precommit.Info.SectorNumber: exitcode.ErrIllegalArgument,
		}
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(sectorNo), proveCommitConf{
				verifyDealsExit: verifyDealsExit,
			})
		})
		rt.Reset()

		rt.SetBalance(big.Mul(big.NewInt(1000), big.NewInt(1e18)))

		proveCommit := makeProveCommit(sectorNo)
		actor.proveCommitSectorAndConfirm(rt, precommit, proveCommit, proveCommitConf{})
		st := getState(rt)

		// Verify new sectors
		// TODO minerstate
		//newSectors, err := st.NewSectors.All(miner.SectorsMax)
		//require.NoError(t, err)
		//assert.Equal(t, []uint64{uint64(sectorNo)}, newSectors)
		// Verify pledge lock-up
		assert.True(t, st.InitialPledge.GreaterThan(big.Zero()))
		rt.Reset()

		// Duplicate proof (sector no-longer pre-committed)
		rt.ExpectAbort(exitcode.ErrNotFound, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(sectorNo), proveCommitConf{})
		})
		rt.Reset()
		actor.checkState(rt)
	})

	t.Run("prove commit aborts if pledge requirement not met", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		// Set the circulating supply high and expected reward low in order to coerce
		// pledge requirements (BR + share of money supply, but capped at 1FIL)
		// to exceed pre-commit deposit (BR only).
		rt.SetCirculatingSupply(big.Mul(big.NewInt(100_000_000), big.NewInt(1e18)))
		actor.epochRewardSmooth = smoothing.TestingConstantEstimate(big.NewInt(1e15))

		// prove one sector to establish collateral and locked funds
		sectors := actor.commitAndProveSectors(rt, 1, defaultSectorExpiration, nil, true)

		// preecommit another sector so we may prove it
		expiration := defaultSectorExpiration*miner.WPoStProvingPeriod + periodOffset - 1
		precommitEpoch := rt.Epoch() + 1
		rt.SetEpoch(precommitEpoch)
		params := actor.makePreCommit(actor.nextSectorNo, rt.Epoch()-1, expiration, nil)
		precommit := actor.preCommitSector(rt, params, preCommitConf{}, false)

		// Confirm the unlocked PCD will not cover the new IP
		assert.True(t, sectors[0].InitialPledge.GreaterThan(precommit.PreCommitDeposit))

		// Set balance to exactly cover locked funds.
		st := getState(rt)
		rt.SetBalance(big.Sum(st.PreCommitDeposits, st.InitialPledge, st.LockedFunds))

		rt.SetEpoch(precommitEpoch + miner.MaxProveCommitDuration[actor.sealProofType] - 1)
		rt.ExpectAbort(exitcode.ErrInsufficientFunds, func() {
			actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(actor.nextSectorNo), proveCommitConf{})
		})
		rt.Reset()

		// succeeds with enough free balance (enough to cover 2x IP)
		rt.SetBalance(big.Sum(st.PreCommitDeposits, st.InitialPledge, st.InitialPledge, st.LockedFunds))
		actor.proveCommitSectorAndConfirm(rt, precommit, makeProveCommit(actor.nextSectorNo), proveCommitConf{})
		actor.checkState(rt)
	})

	t.Run("drop invalid prove commit while processing valid one", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		// make two precommits
		expiration := defaultSectorExpiration*miner.WPoStProvingPeriod + periodOffset - 1
		precommitEpoch := rt.Epoch() + 1
		rt.SetEpoch(precommitEpoch)
		paramsA := actor.makePreCommit(actor.nextSectorNo, rt.Epoch()-1, expiration, []abi.DealID{1})
		preCommitA := actor.preCommitSector(rt, paramsA, preCommitConf{}, true)
		sectorNoA := actor.nextSectorNo
		actor.nextSectorNo++
		paramsB := actor.makePreCommit(actor.nextSectorNo, rt.Epoch()-1, expiration, []abi.DealID{2})
		preCommitB := actor.preCommitSector(rt, paramsB, preCommitConf{}, false)
		sectorNoB := actor.nextSectorNo

		// handle both prove commits in the same epoch
		rt.SetEpoch(precommitEpoch + miner.MaxProveCommitDuration[actor.sealProofType] - 1)

		actor.proveCommitSector(rt, preCommitA, makeProveCommit(sectorNoA))
		actor.proveCommitSector(rt, preCommitB, makeProveCommit(sectorNoB))

		conf := proveCommitConf{
			verifyDealsExit: map[abi.SectorNumber]exitcode.ExitCode{
				sectorNoA: exitcode.ErrIllegalArgument,
			},
		}
		actor.confirmSectorProofsValid(rt, conf, preCommitA, preCommitB)
		actor.checkState(rt)
	})

	t.Run("prove commit just after period start permits PoSt", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)

		// Epoch 101 should be at the beginning of the miner's proving period so there will be time to commit
		// and PoSt a sector.
		rt.SetEpoch(101)
		actor.constructAndVerify(rt)

		// Commit a sector the very next epoch
		rt.SetEpoch(102)
		sector := actor.commitAndProveSector(rt, abi.MaxSectorNumber, defaultSectorExpiration, nil)

		// advance cron to activate power.
		advanceAndSubmitPoSts(rt, actor, sector)
		actor.checkState(rt)
	})

	t.Run("sector with non-positive lifetime is skipped in confirmation", func(t *testing.T) {
		actor := newHarness(t, periodOffset)
		rt := builderForHarness(actor).
			WithBalance(bigBalance, big.Zero()).
			Build(t)
		precommitEpoch := periodOffset + 1
		rt.SetEpoch(precommitEpoch)
		actor.constructAndVerify(rt)
		deadline := actor.deadline(rt)

		sectorNo := abi.SectorNumber(100)
		params := actor.makePreCommit(sectorNo, precommitEpoch-1, deadline.PeriodEnd()+defaultSectorExpiration*miner.WPoStProvingPeriod, nil)
		precommit := actor.preCommitSector(rt, params, preCommitConf{}, true)

		// precommit at correct epoch
		rt.SetEpoch(rt.Epoch() + miner.PreCommitChallengeDelay + 1)
		actor.proveCommitSector(rt, precommit, makeProveCommit(sectorNo))

		// confirm at sector expiration (this probably can't happen)
		rt.SetEpoch(precommit.Info.Expiration)
		// sector skipped but no failure occurs
		actor.confirmSectorProofsValid(rt, proveCommitConf{}, precommit)
		rt.ExpectLogsContain("less than minimum. ignoring")

		// it still skips if sector lifetime is negative
		rt.ClearLogs()
		rt.SetEpoch(precommit.Info.Expiration + 1)
		actor.confirmSectorProofsValid(rt, proveCommitConf{}, precommit)
		rt.ExpectLogsContain("less than minimum. ignoring")

		// it fails up to the miniumum expiration
		rt.ClearLogs()
		rt.SetEpoch(precommit.Info.Expiration - miner.MinSectorExpiration + 1)
		actor.confirmSectorProofsValid(rt, proveCommitConf{}, precommit)
		rt.ExpectLogsContain("less than minimum. ignoring")
		actor.checkState(rt)
	})

	for _, test := range []struct {
		name               string
		version            network.Version
		vestingPledgeDelta abi.TokenAmount
		sealProofType      abi.RegisteredSealProof
	}{{
		name:               "verify proof does not vest at version 8",
		version:            network.Version8,
		vestingPledgeDelta: abi.NewTokenAmount(0),
		sealProofType:      abi.RegisteredSealProof_StackedDrg32GiBV1_1,
	}} {
		t.Run(test.name, func(t *testing.T) {
			actor := newHarness(t, periodOffset)
			actor.setProofType(test.sealProofType)
			rt := builderForHarness(actor).
				WithNetworkVersion(test.version).
				WithBalance(bigBalance, big.Zero()).
				Build(t)
			precommitEpoch := periodOffset + 1
			rt.SetEpoch(precommitEpoch)
			actor.constructAndVerify(rt)
			deadline := actor.deadline(rt)

			// Make a good commitment for the proof to target.
			sectorNo := abi.SectorNumber(100)
			params := actor.makePreCommit(sectorNo, precommitEpoch-1, deadline.PeriodEnd()+defaultSectorExpiration*miner.WPoStProvingPeriod, []abi.DealID{1})
			precommit := actor.preCommitSector(rt, params, preCommitConf{}, true)

			// add 1000 tokens that vest immediately
			st := getState(rt)
			_, err := st.AddLockedFunds(rt.AdtStore(), rt.Epoch(), abi.NewTokenAmount(1000), &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   1,
				StepDuration: 1,
				Quantization: 1,
			})
			require.NoError(t, err)
			rt.ReplaceState(st)

			// Set the right epoch for all following tests
			rt.SetEpoch(precommitEpoch + miner.PreCommitChallengeDelay + 1)
			rt.SetBalance(big.Mul(big.NewInt(1000), big.NewInt(1e18)))

			// Too big at version 4
			proveCommit := makeProveCommit(sectorNo)
			proveCommit.Proof = make([]byte, 192)
			actor.proveCommitSectorAndConfirm(rt, precommit, proveCommit, proveCommitConf{
				vestingPledgeDelta: &test.vestingPledgeDelta,
			})
		})
	}
}
