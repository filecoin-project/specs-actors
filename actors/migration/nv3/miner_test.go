package nv3

import (
	"context"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	cid "github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/states"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/ipld"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

func TestMinerMigrator(t *testing.T) {
	ctx := context.Background()
	store := ipld.NewADTStore(ctx)

	maddr := tutil.NewIDAddr(t, 1000)
	periodBoundary := abi.ChainEpoch(0)

	// Set up state following simplified steps that a miner actor takes.
	// Balances etc are ignored.
	tree, err := states.NewTree(store)
	require.NoError(t, err)

	pst := constructPowerState(t, store)
	initClaim(t, store, pst, maddr)

	phead, err := store.Put(ctx, pst)
	require.NoError(t, err)
	setActorState(t, tree, builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID, phead)

	st, info := constructMinerState(t, store, periodBoundary)
	mhead, err := store.Put(ctx, st)
	require.NoError(t, err)
	setActorState(t, tree, maddr, builtin.StorageMinerActorCodeID, mhead)

	// Pre-commit sectors
	// We need more than one, else attempting to fault one of them later on will fail due to inconsistent state
	// (rather than persisting the inconsistent state!).
	sectorNo := abi.SectorNumber(1)
	otherSectorNo := abi.SectorNumber(2)
	sectorNos := []abi.SectorNumber{sectorNo, otherSectorNo}
	preCommitEpoch := abi.ChainEpoch(1)
	initialExpiration := abi.ChainEpoch(500_000)

	preCommits := make([]*miner.SectorPreCommitOnChainInfo, len(sectorNos))
	for i, n := range sectorNos {
		require.NoError(t, st.AllocateSectorNumber(store, n))
		preCommits[i] = makePreCommit(n, preCommitEpoch, initialExpiration, info)
		require.NoError(t, st.PutPrecommittedSector(store, preCommits[i]))
	}

	// Prove-commit sectors
	activationEpoch := abi.ChainEpoch(10)
	sectors := make([]*miner.SectorOnChainInfo, len(sectorNos))
	for i, preCommit := range preCommits {
		sectors[i] = makeSector(preCommit, activationEpoch)
	}
	require.NoError(t, st.PutSectors(store, sectors...))
	require.NoError(t, st.DeletePrecommittedSectors(store, sectorNos...))

	bothSectorsPower, err := st.AssignSectorsToDeadlines(store, activationEpoch, sectors, info.WindowPoStPartitionSectors, info.SectorSize)
	require.NoError(t, err)
	require.NoError(t, pst.AddToClaim(store, maddr, bothSectorsPower.Raw, bothSectorsPower.QA))
	oneSectorPower := miner.PowerForSector(info.SectorSize, sectors[0])

	// Check state expectations
	dlIdx, pIdx, err := st.FindSector(store, sectorNo)
	assert.Equal(t, uint64(2), dlIdx)
	assert.Equal(t, uint64(0), pIdx)
	_, deadline, partition := loadDeadlineAndPartition(t, st, store, dlIdx, pIdx)
	quant := st.QuantSpecForDeadline(dlIdx)
	expectedExpiration := quant.QuantizeUp(initialExpiration)

	assertDeadlineExpirationQueue(t, store, deadline, pIdx, map[abi.ChainEpoch][]uint64{
		expectedExpiration: {pIdx},
	})
	assertPartitionExpirationQueue(t, store, partition, map[abi.ChainEpoch]*miner.ExpirationSet{
		expectedExpiration: {
			OnTimeSectors: bitfield.NewFromSet([]uint64{uint64(sectorNo), uint64(otherSectorNo)}),
			EarlySectors:  bitfield.New(),
			OnTimePledge:  big.Zero(),
			ActivePower:   bothSectorsPower,
			FaultyPower:   miner.NewPowerPairZero(),
		},
	})

	// Roll forward 10 (arbitrary) proving periods and to the beginning of the deadline *after* this sector's deadline.
	// Assume all proofs have been ok.
	st.ProvingPeriodStart = periodBoundary + (10 * miner.WPoStProvingPeriod)
	st.CurrentDeadline = dlIdx + 1
	rescheduleAndFaultEpoch := st.ProvingPeriodStart + (abi.ChainEpoch(st.CurrentDeadline) * miner.WPoStChallengeWindow)

	// Imagine that another sector was committed that replaces this one, causing it to be rescheduled to expire
	// at the next instance of its deadline.
	// That sector would have to have been in a different deadline in order for the fault declaration below to be added.
	// The hypothetical sector is not added to state, and ignored hereafter.

	rescheduleSectors := make(miner.DeadlineSectorMap)
	require.NoError(t, rescheduleSectors.AddValues(dlIdx, pIdx, uint64(sectorNo)))
	require.NoError(t, st.RescheduleSectorExpirations(store, rescheduleAndFaultEpoch, info.SectorSize, rescheduleSectors))

	// Now the rescheduled sector is declared faulty.
	deadlines, deadline, partition := loadDeadlineAndPartition(t, st, store, dlIdx, pIdx)
	msectors, err := miner.LoadSectors(store, st.Sectors)
	require.NoError(t, err)
	targetDeadline := miner.NewDeadlineInfo(st.ProvingPeriodStart, dlIdx, rescheduleAndFaultEpoch).NextNotElapsed()
	faultExpiration := targetDeadline.Last() + miner.FaultMaxAge
	require.True(t, faultExpiration < expectedExpiration)
	partitionSectors := make(miner.PartitionSectorMap)
	require.NoError(t, partitionSectors.AddValues(pIdx, uint64(sectorNo)))
	newFaultyPower, err := deadline.DeclareFaults(store, msectors, info.SectorSize, quant, faultExpiration, partitionSectors)
	require.NoError(t, err)

	require.NoError(t, deadlines.UpdateDeadline(store, dlIdx, deadline))
	require.NoError(t, st.SaveDeadlines(store, deadlines))

	require.NoError(t, pst.AddToClaim(store, maddr, newFaultyPower.Raw.Neg(), newFaultyPower.QA.Neg()))

	// Show the state is b0rked
	_, deadline, partition = loadDeadlineAndPartition(t, st, store, dlIdx, pIdx)
	advancedExpiration := quant.QuantizeUp(rescheduleAndFaultEpoch)
	require.True(t, advancedExpiration < faultExpiration)
	assertDeadlineExpirationQueue(t, store, deadline, pIdx, map[abi.ChainEpoch][]uint64{
		advancedExpiration: {pIdx}, // Appearing repeatedly here is ok, this queue is best-effort
		faultExpiration:    {pIdx},
		expectedExpiration: {pIdx},
	})
	assertPartitionExpirationQueue(t, store, partition, map[abi.ChainEpoch]*miner.ExpirationSet{
		// The sector should be scheduled here, but with faulty power.
		// It's wrongly here with active power, because fault declaration didn't find it.
		advancedExpiration: {
			OnTimeSectors: bitfield.NewFromSet([]uint64{uint64(sectorNo)}),
			EarlySectors:  bitfield.New(),
			OnTimePledge:  big.Zero(),
			ActivePower:   oneSectorPower, // Power should be faulty
			FaultyPower:   miner.NewPowerPairZero(),
		},
		// The sector should not be scheduled here at all.
		// It's wrongly here because fault declaration didn't find it at advancedExpiration.
		faultExpiration: {
			OnTimeSectors: bitfield.New(),
			EarlySectors:  bitfield.NewFromSet([]uint64{uint64(sectorNo)}),
			OnTimePledge:  big.Zero(),
			ActivePower:   miner.NewPowerPairZero(),
			FaultyPower:   oneSectorPower,
		},
		// The non-rescheduled sector is still here.
		// This wrongly has lost power twice.
		expectedExpiration: {
			OnTimeSectors: bitfield.NewFromSet([]uint64{uint64(otherSectorNo)}),
			EarlySectors:  bitfield.New(),
			OnTimePledge:  big.Zero(),
			ActivePower:   miner.NewPowerPairZero(), // Should have oneSectorPower for the other sector
			FaultyPower:   miner.NewPowerPairZero(),
		},
	})

	phead, err = store.Put(ctx, pst)
	require.NoError(t, err)
	setActorState(t, tree, builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID, phead)

	mhead, err = store.Put(ctx, st)
	require.NoError(t, err)
	setActorState(t, tree, maddr, builtin.StorageMinerActorCodeID, mhead)

	// TODO
	// - simulate popping the first item off the expiration queue and terminating the sector, which will be
	//   the more common (only) case we expect to see in the actual state.

	//
	// Run migration to repair the state
	//
	{
		migrationEpoch := rescheduleAndFaultEpoch + (10 * builtin.EpochsInDay)

		mm := minerMigrator{}
		newHead, err := mm.MigrateState(ctx, store, mhead, migrationEpoch-1, maddr, tree)
		require.NoError(t, err)
		require.False(t, newHead.Equals(mhead))

		var st miner.State
		require.NoError(t, store.Get(ctx, newHead, &st))
		_, deadline, partition = loadDeadlineAndPartition(t, &st, store, dlIdx, pIdx)
		assertDeadlineExpirationQueue(t, store, deadline, pIdx, map[abi.ChainEpoch][]uint64{
			advancedExpiration: {pIdx},
			faultExpiration:    {pIdx},
			expectedExpiration: {pIdx},
		})

		// XXX: this is the old assertion of wrongness. it shoudl fail!
		assertPartitionExpirationQueue(t, store, partition, map[abi.ChainEpoch]*miner.ExpirationSet{
			// The sector should be scheduled here, but with faulty power.
			// It's wrongly here with active power, because fault declaration didn't find it.
			advancedExpiration: {
				OnTimeSectors: bitfield.NewFromSet([]uint64{uint64(sectorNo)}),
				EarlySectors:  bitfield.New(),
				OnTimePledge:  big.Zero(),
				ActivePower:   oneSectorPower, // Power should be faulty
				FaultyPower:   miner.NewPowerPairZero(),
			},
			// The sector should not be scheduled here at all.
			// It's wrongly here because fault declaration didn't find it at advancedExpiration.
			faultExpiration: {
				OnTimeSectors: bitfield.New(),
				EarlySectors:  bitfield.NewFromSet([]uint64{uint64(sectorNo)}),
				OnTimePledge:  big.Zero(),
				ActivePower:   miner.NewPowerPairZero(),
				FaultyPower:   oneSectorPower,
			},
			// The non-rescheduled sector is still here.
			// This wrongly has lost power twice.
			expectedExpiration: {
				OnTimeSectors: bitfield.NewFromSet([]uint64{uint64(otherSectorNo)}),
				EarlySectors:  bitfield.New(),
				OnTimePledge:  big.Zero(),
				ActivePower:   miner.NewPowerPairZero(), // Should have oneSectorPower for the other sector
				FaultyPower:   miner.NewPowerPairZero(),
			},
		})

		assertPartitionExpirationQueue(t, store, partition, map[abi.ChainEpoch]*miner.ExpirationSet{
			// The sector is  scheduled here, with faulty power.
			advancedExpiration: {
				OnTimeSectors: bitfield.NewFromSet([]uint64{uint64(sectorNo)}),
				EarlySectors:  bitfield.New(),
				OnTimePledge:  big.Zero(),
				ActivePower:   miner.NewPowerPairZero(),
				FaultyPower:   oneSectorPower,
			},
			// The migration leaves ths expiration set here, but empty.
			faultExpiration: {
				OnTimeSectors: bitfield.New(),
				EarlySectors:  bitfield.New(),
				OnTimePledge:  big.Zero(),
				ActivePower:   miner.NewPowerPairZero(),
				FaultyPower:   miner.NewPowerPairZero(),
			},
			// The non-rescheduled sector is still here, with active power.
			expectedExpiration: {
				OnTimeSectors: bitfield.NewFromSet([]uint64{uint64(otherSectorNo)}),
				EarlySectors:  bitfield.New(),
				OnTimePledge:  big.Zero(),
				ActivePower:   oneSectorPower,
				FaultyPower:   miner.NewPowerPairZero(),
			},
		})
	}
}

func setActorState(t *testing.T, tree *states.Tree, addr addr.Address, code cid.Cid, head cid.Cid) {
	require.NoError(t, tree.SetActor(addr, &states.Actor{
		Code:       code,
		Head:       head,
		CallSeqNum: 0,
		Balance:    big.Zero(),
	}))
}

func constructPowerState(t *testing.T, store adt.Store) *power.State {
	emptyMap, err := adt.MakeEmptyMap(store).Root()
	require.NoError(t, err)

	emptyMMap, err := adt.MakeEmptyMultimap(store).Root()
	require.NoError(t, err)

	return power.ConstructState(emptyMap, emptyMMap)
}

func initClaim(t *testing.T, store adt.Store, pst *power.State, maddr addr.Address) {
	claims, err := adt.AsMap(store, pst.Claims)
	require.NoError(t, err)
	require.NoError(t, claims.Put(abi.AddrKey(maddr), &power.Claim{abi.NewStoragePower(0), abi.NewStoragePower(0)}))
	pst.Claims, err = claims.Root()
	require.NoError(t, err)
}

func constructMinerState(t *testing.T, store adt.Store, periodBoundary abi.ChainEpoch) (*miner.State, *miner.MinerInfo) {
	emptyMap, err := adt.MakeEmptyMap(store).Root()
	require.NoError(t, err)

	emptyBitfield := bitfield.NewFromSet(nil)
	emptyBitfieldCid, err := store.Put(store.Context(), emptyBitfield)
	require.NoError(t, err)

	emptyArray, err := adt.MakeEmptyArray(store).Root()
	require.NoError(t, err)
	emptyDeadline := miner.ConstructDeadline(emptyArray)
	emptyDeadlineCid, err := store.Put(store.Context(), emptyDeadline)
	require.NoError(t, err)

	emptyDeadlines := miner.ConstructDeadlines(emptyDeadlineCid)
	emptyDeadlinesCid, err := store.Put(context.Background(), emptyDeadlines)
	require.NoError(t, err)

	// state field init
	owner := tutil.NewBLSAddr(t, 1)
	worker := tutil.NewBLSAddr(t, 2)

	testSealProofType := abi.RegisteredSealProof_StackedDrg32GiBV1
	sectorSize, err := testSealProofType.SectorSize()
	require.NoError(t, err)

	partitionSectors, err := builtin.SealProofWindowPoStPartitionSectors(testSealProofType)
	require.NoError(t, err)

	info := &miner.MinerInfo{
		Owner:                      owner,
		Worker:                     worker,
		PendingWorkerKey:           nil,
		PeerId:                     abi.PeerID("peer"),
		Multiaddrs:                 []abi.Multiaddrs{},
		SealProofType:              testSealProofType,
		SectorSize:                 sectorSize,
		WindowPoStPartitionSectors: partitionSectors,
	}
	infoCid, err := store.Put(context.Background(), info)
	require.NoError(t, err)

	emptyVestingFunds := miner.ConstructVestingFunds()
	emptyVestingFundsCid, err := store.Put(context.Background(), emptyVestingFunds)
	require.NoError(t, err)

	state, err := miner.ConstructState(infoCid, periodBoundary, emptyBitfieldCid, emptyArray, emptyMap, emptyDeadlinesCid,
		emptyVestingFundsCid)
	require.NoError(t, err)
	return state, info
}

func makePreCommit(sectorNo abi.SectorNumber, epoch abi.ChainEpoch, expiration abi.ChainEpoch, info *miner.MinerInfo) *miner.SectorPreCommitOnChainInfo {
	return &miner.SectorPreCommitOnChainInfo{
		Info: miner.SectorPreCommitInfo{
			SealProof:     info.SealProofType,
			SectorNumber:  sectorNo,
			SealedCID:     tutil.MakeCID("commr", &miner.SealedCIDPrefix),
			SealRandEpoch: 0,
			DealIDs:       nil,
			Expiration:    expiration,
		},
		PreCommitDeposit:   big.Zero(),
		PreCommitEpoch:     epoch,
		DealWeight:         big.Zero(),
		VerifiedDealWeight: big.Zero(),
	}
}

func makeSector(preCommit *miner.SectorPreCommitOnChainInfo, activationEpoch abi.ChainEpoch) *miner.SectorOnChainInfo {
	return &miner.SectorOnChainInfo{
		SectorNumber:          preCommit.Info.SectorNumber,
		SealProof:             preCommit.Info.SealProof,
		SealedCID:             preCommit.Info.SealedCID,
		DealIDs:               preCommit.Info.DealIDs,
		Activation:            activationEpoch,
		Expiration:            preCommit.Info.Expiration,
		DealWeight:            preCommit.DealWeight,
		VerifiedDealWeight:    preCommit.VerifiedDealWeight,
		InitialPledge:         big.Zero(),
		ExpectedDayReward:     big.Zero(),
		ExpectedStoragePledge: big.Zero(),
	}
}

func loadDeadlineAndPartition(t *testing.T, st *miner.State, store adt.Store, dlIdx uint64, pIdx uint64) (*miner.Deadlines, *miner.Deadline, *miner.Partition) {
	deadlines, err := st.LoadDeadlines(store)
	require.NoError(t, err)
	deadline, err := deadlines.LoadDeadline(store, dlIdx)
	require.NoError(t, err)
	partition, err := deadline.LoadPartition(store, pIdx)
	require.NoError(t, err)
	return deadlines, deadline, partition
}

func collectDeadlineExpirations(t *testing.T, store adt.Store, deadline *miner.Deadline) map[abi.ChainEpoch][]uint64 {
	queue, err := miner.LoadBitfieldQueue(store, deadline.ExpirationsEpochs, miner.NoQuantization)
	require.NoError(t, err)
	expirations := map[abi.ChainEpoch][]uint64{}
	_ = queue.ForEach(func(epoch abi.ChainEpoch, bf bitfield.BitField) error {
		expanded, err := bf.All(miner.SectorsMax)
		require.NoError(t, err)
		expirations[epoch] = expanded
		return nil
	})
	return expirations
}

func collectPartitionExpirations(t *testing.T, store adt.Store, partition *miner.Partition) map[abi.ChainEpoch]*miner.ExpirationSet {
	queue, err := miner.LoadExpirationQueue(store, partition.ExpirationsEpochs, miner.NoQuantization)
	require.NoError(t, err)
	expirations := map[abi.ChainEpoch]*miner.ExpirationSet{}
	var es miner.ExpirationSet
	_ = queue.ForEach(&es, func(i int64) error {
		cpy := es
		expirations[abi.ChainEpoch(i)] = &cpy
		return nil
	})
	return expirations
}

func assertDeadlineExpirationQueue(t *testing.T, store adt.Store, deadline *miner.Deadline, pIdx uint64, expected map[abi.ChainEpoch][]uint64) {
	dlExpirations := collectDeadlineExpirations(t, store, deadline)
	assert.Equal(t, expected, dlExpirations)
}

func assertPartitionExpirationQueue(t *testing.T, store adt.Store, partition *miner.Partition, expected map[abi.ChainEpoch]*miner.ExpirationSet) {
	ptExpirations := collectPartitionExpirations(t, store, partition)
	assert.Equal(t, len(expected), len(ptExpirations))

	for k, expectedSet := range expected {
		pset, ok := ptExpirations[k]
		require.True(t, ok, "no expiration set at %d", k)
		assertBitfieldsEqual(t, expectedSet.OnTimeSectors, pset.OnTimeSectors)
		assertBitfieldsEqual(t, expectedSet.EarlySectors, pset.EarlySectors)
		assert.True(t, expectedSet.ActivePower.Equals(pset.ActivePower),
			"at expiration %d active power expected %v found %v", k, expectedSet.ActivePower, pset.ActivePower)
		assert.True(t, expectedSet.FaultyPower.Equals(pset.FaultyPower),
			"at expiration %d faulty power expected %v found %v", k, expectedSet.FaultyPower, pset.FaultyPower)
		assert.True(t, expectedSet.OnTimePledge.Equals(pset.OnTimePledge))
	}
}

func assertBitfieldsEqual(t *testing.T, expected bitfield.BitField, actual bitfield.BitField) {
	const maxDiff = 100

	missing, err := bitfield.SubtractBitField(expected, actual)
	require.NoError(t, err)
	unexpected, err := bitfield.SubtractBitField(actual, expected)
	require.NoError(t, err)

	missingSet, err := missing.All(maxDiff)
	require.NoError(t, err, "more than %d missing bits expected", maxDiff)
	assert.Empty(t, missingSet, "expected missing bits")

	unexpectedSet, err := unexpected.All(maxDiff)
	require.NoError(t, err, "more than %d unexpected bits", maxDiff)
	assert.Empty(t, unexpectedSet, "unexpected bits set")
}
