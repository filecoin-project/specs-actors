package migration

import (
	"context"
	"strings"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	builtin0 "github.com/filecoin-project/specs-actors/actors/builtin"
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	power0 "github.com/filecoin-project/specs-actors/actors/builtin/power"
	states0 "github.com/filecoin-project/specs-actors/actors/states"
	adt0 "github.com/filecoin-project/specs-actors/actors/util/adt"
	cid "github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	miner2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/support/ipld"
	tutil "github.com/filecoin-project/specs-actors/v2/support/testing"
)

func TestMinerMigrator(t *testing.T) {
	ctx := context.Background()
	store := ipld.NewADTStore(ctx)

	maddr := tutil.NewIDAddr(t, 1000)
	periodBoundary := abi.ChainEpoch(0)

	// Set up state following simplified steps that a miner actor takes.
	// Balances etc are ignored.
	tree, err := states0.NewTree(store)
	require.NoError(t, err)

	pst := constructPowerState(t, store)
	initClaim(t, store, pst, maddr)

	phead, err := store.Put(ctx, pst)
	require.NoError(t, err)
	setActorState(t, tree, builtin0.StoragePowerActorAddr, builtin0.StoragePowerActorCodeID, phead)

	st, info := constructMinerState(t, store, periodBoundary)
	mhead, err := store.Put(ctx, st)
	require.NoError(t, err)
	setActorState(t, tree, maddr, builtin0.StorageMinerActorCodeID, mhead)

	// Pre-commit sectors
	// We need more than one, else attempting to fault one of them later on will fail due to inconsistent state
	// (rather than persisting the inconsistent state!).
	sectorNo := abi.SectorNumber(1)
	otherSectorNo := abi.SectorNumber(2)
	sectorNos := []abi.SectorNumber{sectorNo, otherSectorNo}
	preCommitEpoch := abi.ChainEpoch(1)
	initialExpiration := abi.ChainEpoch(500_000)

	preCommits := make([]*miner0.SectorPreCommitOnChainInfo, len(sectorNos))
	for i, n := range sectorNos {
		require.NoError(t, st.AllocateSectorNumber(store, n))
		preCommits[i] = makePreCommit0(n, preCommitEpoch, initialExpiration, info)
		require.NoError(t, st.PutPrecommittedSector(store, preCommits[i]))
	}

	// Prove-commit sectors
	activationEpoch := abi.ChainEpoch(10)
	sectors := make([]*miner0.SectorOnChainInfo, len(sectorNos))
	for i, preCommit := range preCommits {
		sectors[i] = makeSector0(preCommit, activationEpoch)
	}
	require.NoError(t, st.PutSectors(store, sectors...))
	require.NoError(t, st.DeletePrecommittedSectors(store, sectorNos...))

	bothSectorsPower, err := st.AssignSectorsToDeadlines(store, activationEpoch, sectors, info.WindowPoStPartitionSectors, info.SectorSize)
	require.NoError(t, err)
	require.NoError(t, pst.AddToClaim(store, maddr, bothSectorsPower.Raw, bothSectorsPower.QA))
	oneSectorPower := miner0.PowerForSector(info.SectorSize, sectors[0])

	// Check state expectations
	dlIdx, pIdx, err := st.FindSector(store, sectorNo)
	assert.Equal(t, uint64(2), dlIdx)
	assert.Equal(t, uint64(0), pIdx)
	_, deadline, partition := loadDeadlineAndPartition0(t, st, store, dlIdx, pIdx)
	quant := st.QuantSpecForDeadline(dlIdx)
	expectedExpiration := quant.QuantizeUp(initialExpiration)

	assertDeadlineExpirationQueue0(t, store, deadline, pIdx, map[abi.ChainEpoch][]uint64{
		expectedExpiration: {pIdx},
	})
	assertPartitionExpirationQueue0(t, store, partition, map[abi.ChainEpoch]*miner0.ExpirationSet{
		expectedExpiration: {
			OnTimeSectors: bitfield.NewFromSet([]uint64{uint64(sectorNo), uint64(otherSectorNo)}),
			EarlySectors:  bitfield.New(),
			OnTimePledge:  big.Zero(),
			ActivePower:   bothSectorsPower,
			FaultyPower:   miner0.NewPowerPairZero(),
		},
	})

	// Roll forward 10 (arbitrary) proving periods and to the beginning of the deadline *after* this sector's deadline.
	// Assume all proofs have been ok.
	st.ProvingPeriodStart = periodBoundary + (10 * miner0.WPoStProvingPeriod)
	st.CurrentDeadline = dlIdx + 1
	rescheduleAndFaultEpoch := st.ProvingPeriodStart + (abi.ChainEpoch(st.CurrentDeadline) * miner0.WPoStChallengeWindow)

	// Imagine that another sector was committed that replaces this one, causing it to be rescheduled to expire
	// at the next instance of its deadline.
	// That sector would have to have been in a different deadline in order for the fault declaration below to be added.
	// The hypothetical sector is not added to state, and ignored hereafter.

	rescheduleSectors := make(miner0.DeadlineSectorMap)
	require.NoError(t, rescheduleSectors.AddValues(dlIdx, pIdx, uint64(sectorNo)))
	require.NoError(t, st.RescheduleSectorExpirations(store, rescheduleAndFaultEpoch, info.SectorSize, rescheduleSectors))

	// Now the rescheduled sector is declared faulty.
	deadlines, deadline, partition := loadDeadlineAndPartition0(t, st, store, dlIdx, pIdx)
	msectors, err := miner0.LoadSectors(store, st.Sectors)
	require.NoError(t, err)
	targetDeadline := miner0.NewDeadlineInfo(st.ProvingPeriodStart, dlIdx, rescheduleAndFaultEpoch).NextNotElapsed()
	faultExpiration := targetDeadline.Last() + miner0.FaultMaxAge
	require.True(t, faultExpiration < expectedExpiration)
	partitionSectors := make(miner0.PartitionSectorMap)
	require.NoError(t, partitionSectors.AddValues(pIdx, uint64(sectorNo)))
	newFaultyPower, err := deadline.DeclareFaults(store, msectors, info.SectorSize, quant, faultExpiration, partitionSectors)
	require.NoError(t, err)

	require.NoError(t, deadlines.UpdateDeadline(store, dlIdx, deadline))
	require.NoError(t, st.SaveDeadlines(store, deadlines))

	require.NoError(t, pst.AddToClaim(store, maddr, newFaultyPower.Raw.Neg(), newFaultyPower.QA.Neg()))

	// Show the state is b0rked
	_, deadline, partition = loadDeadlineAndPartition0(t, st, store, dlIdx, pIdx)
	advancedExpiration := quant.QuantizeUp(rescheduleAndFaultEpoch)
	require.True(t, advancedExpiration < faultExpiration)
	assertDeadlineExpirationQueue0(t, store, deadline, pIdx, map[abi.ChainEpoch][]uint64{
		advancedExpiration: {pIdx}, // Appearing repeatedly here is ok, this queue is best-effort
		faultExpiration:    {pIdx},
		expectedExpiration: {pIdx},
	})
	assertPartitionExpirationQueue0(t, store, partition, map[abi.ChainEpoch]*miner0.ExpirationSet{
		// The sector should be scheduled here, but with faulty power.
		// It's wrongly here with active power, because fault declaration didn't find it.
		advancedExpiration: {
			OnTimeSectors: bitfield.NewFromSet([]uint64{uint64(sectorNo)}),
			EarlySectors:  bitfield.New(),
			OnTimePledge:  big.Zero(),
			ActivePower:   oneSectorPower, // Power should be faulty
			FaultyPower:   miner0.NewPowerPairZero(),
		},
		// The sector should not be scheduled here at all.
		// It's wrongly here because fault declaration didn't find it at advancedExpiration.
		faultExpiration: {
			OnTimeSectors: bitfield.New(),
			EarlySectors:  bitfield.NewFromSet([]uint64{uint64(sectorNo)}),
			OnTimePledge:  big.Zero(),
			ActivePower:   miner0.NewPowerPairZero(),
			FaultyPower:   oneSectorPower,
		},
		// The non-rescheduled sector is still here.
		// This wrongly has lost power twice.
		expectedExpiration: {
			OnTimeSectors: bitfield.NewFromSet([]uint64{uint64(otherSectorNo)}),
			EarlySectors:  bitfield.New(),
			OnTimePledge:  big.Zero(),
			ActivePower:   miner0.NewPowerPairZero(), // Should have oneSectorPower for the other sector
			FaultyPower:   miner0.NewPowerPairZero(),
		},
	})

	phead, err = store.Put(ctx, pst)
	require.NoError(t, err)
	setActorState(t, tree, builtin0.StoragePowerActorAddr, builtin0.StoragePowerActorCodeID, phead)

	mhead, err = store.Put(ctx, st)
	require.NoError(t, err)
	setActorState(t, tree, maddr, builtin0.StorageMinerActorCodeID, mhead)

	// TODO
	// - simulate popping the first item off the expiration queue and terminating the sector, which will be
	//   the more common case we expect to see in the actual state.

	//
	// Run migration to repair the state
	//
	{
		mm := minerMigrator{}

		migrationEpoch := rescheduleAndFaultEpoch + (10 * builtin.EpochsInDay)
		newHead, _, err := mm.MigrateState(ctx, store, mhead, MigrationInfo{
			address:    maddr,
			balance:    big.Zero(),
			priorEpoch: migrationEpoch-1,
			powerUpdates: &PowerUpdates{
				claims: make(map[addr.Address]power0.Claim),
				crons:  make(map[abi.ChainEpoch][]power0.CronEvent),
			},
		})
		require.NoError(t, err)
		require.False(t, newHead.Equals(mhead))

		var st miner2.State
		require.NoError(t, store.Get(ctx, newHead, &st))

		// Check general state invariants.
		_, msgs, err := miner2.CheckStateInvariants(&st, store)
		require.NoError(t, err)
		assert.True(t, msgs.IsEmpty(), strings.Join(msgs.Messages(), "\n"))

		// Check specifics.
		_, deadline, partition := loadDeadlineAndPartition2(t, &st, store, dlIdx, pIdx)
		assertDeadlineExpirationQueue2(t, store, deadline, pIdx, map[abi.ChainEpoch][]uint64{
			advancedExpiration: {pIdx},
			faultExpiration:    {pIdx},
			expectedExpiration: {pIdx},
		})

		assertPartitionExpirationQueue2(t, store, partition, map[abi.ChainEpoch]*miner2.ExpirationSet{
			// The sector is  scheduled here, with faulty power.
			advancedExpiration: {
				OnTimeSectors: bitfield.NewFromSet([]uint64{uint64(sectorNo)}),
				EarlySectors:  bitfield.New(),
				OnTimePledge:  big.Zero(),
				ActivePower:   miner2.NewPowerPairZero(),
				FaultyPower:   miner2.PowerPair(oneSectorPower),
			},
			// The migration leaves ths expiration set here, but empty.
			faultExpiration: {
				OnTimeSectors: bitfield.New(),
				EarlySectors:  bitfield.New(),
				OnTimePledge:  big.Zero(),
				ActivePower:   miner2.NewPowerPairZero(),
				FaultyPower:   miner2.NewPowerPairZero(),
			},
			// The non-rescheduled sector is still here, with active power.
			expectedExpiration: {
				OnTimeSectors: bitfield.NewFromSet([]uint64{uint64(otherSectorNo)}),
				EarlySectors:  bitfield.New(),
				OnTimePledge:  big.Zero(),
				ActivePower:   miner2.PowerPair(oneSectorPower),
				FaultyPower:   miner2.NewPowerPairZero(),
			},
		})
	}
}

func setActorState(t *testing.T, tree *states0.Tree, addr addr.Address, code cid.Cid, head cid.Cid) {
	require.NoError(t, tree.SetActor(addr, &states0.Actor{
		Code:       code,
		Head:       head,
		CallSeqNum: 0,
		Balance:    big.Zero(),
	}))
}

func constructPowerState(t *testing.T, store adt0.Store) *power0.State {
	emptyMap, err := adt0.MakeEmptyMap(store).Root()
	require.NoError(t, err)

	emptyMMap, err := adt0.MakeEmptyMultimap(store).Root()
	require.NoError(t, err)

	return power0.ConstructState(emptyMap, emptyMMap)
}

func initClaim(t *testing.T, store adt0.Store, pst *power0.State, maddr addr.Address) {
	claims, err := adt0.AsMap(store, pst.Claims)
	require.NoError(t, err)
	require.NoError(t, claims.Put(abi.AddrKey(maddr), &power0.Claim{abi.NewStoragePower(0), abi.NewStoragePower(0)}))
	pst.Claims, err = claims.Root()
	require.NoError(t, err)
}

func constructMinerState(t *testing.T, store adt0.Store, periodBoundary abi.ChainEpoch) (*miner0.State, *miner0.MinerInfo) {
	emptyMap, err := adt0.MakeEmptyMap(store).Root()
	require.NoError(t, err)

	emptyBitfield := bitfield.NewFromSet(nil)
	emptyBitfieldCid, err := store.Put(store.Context(), emptyBitfield)
	require.NoError(t, err)

	emptyArray, err := adt0.MakeEmptyArray(store).Root()
	require.NoError(t, err)
	emptyDeadline := miner0.ConstructDeadline(emptyArray)
	emptyDeadlineCid, err := store.Put(store.Context(), emptyDeadline)
	require.NoError(t, err)

	emptyDeadlines := miner0.ConstructDeadlines(emptyDeadlineCid)
	emptyDeadlinesCid, err := store.Put(context.Background(), emptyDeadlines)
	require.NoError(t, err)

	// state field init
	owner := tutil.NewBLSAddr(t, 1)
	worker := tutil.NewBLSAddr(t, 2)

	testSealProofType := abi.RegisteredSealProof_StackedDrg32GiBV1
	sectorSize, err := testSealProofType.SectorSize()
	require.NoError(t, err)

	partitionSectors, err := builtin0.SealProofWindowPoStPartitionSectors(testSealProofType)
	require.NoError(t, err)

	info := &miner0.MinerInfo{
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

	emptyVestingFunds := miner0.ConstructVestingFunds()
	emptyVestingFundsCid, err := store.Put(context.Background(), emptyVestingFunds)
	require.NoError(t, err)

	state, err := miner0.ConstructState(infoCid, periodBoundary, emptyBitfieldCid, emptyArray, emptyMap, emptyDeadlinesCid,
		emptyVestingFundsCid)
	require.NoError(t, err)
	return state, info
}

func makePreCommit0(sectorNo abi.SectorNumber, epoch abi.ChainEpoch, expiration abi.ChainEpoch, info *miner0.MinerInfo) *miner0.SectorPreCommitOnChainInfo {
	return &miner0.SectorPreCommitOnChainInfo{
		Info: miner0.SectorPreCommitInfo{
			SealProof:     info.SealProofType,
			SectorNumber:  sectorNo,
			SealedCID:     tutil.MakeCID("commr", &miner0.SealedCIDPrefix),
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

func makeSector0(preCommit *miner0.SectorPreCommitOnChainInfo, activationEpoch abi.ChainEpoch) *miner0.SectorOnChainInfo {
	return &miner0.SectorOnChainInfo{
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

func loadDeadlineAndPartition0(t *testing.T, st *miner0.State, store adt0.Store, dlIdx uint64, pIdx uint64) (*miner0.Deadlines, *miner0.Deadline, *miner0.Partition) {
	deadlines, err := st.LoadDeadlines(store)
	require.NoError(t, err)
	deadline, err := deadlines.LoadDeadline(store, dlIdx)
	require.NoError(t, err)
	partition, err := deadline.LoadPartition(store, pIdx)
	require.NoError(t, err)
	return deadlines, deadline, partition
}

func loadDeadlineAndPartition2(t *testing.T, st *miner2.State, store adt0.Store, dlIdx uint64, pIdx uint64) (*miner2.Deadlines, *miner2.Deadline, *miner2.Partition) {
	deadlines, err := st.LoadDeadlines(store)
	require.NoError(t, err)
	deadline, err := deadlines.LoadDeadline(store, dlIdx)
	require.NoError(t, err)
	partition, err := deadline.LoadPartition(store, pIdx)
	require.NoError(t, err)
	return deadlines, deadline, partition
}

func collectDeadlineExpirations0(t *testing.T, store adt0.Store, deadline *miner0.Deadline) map[abi.ChainEpoch][]uint64 {
	queue, err := miner0.LoadBitfieldQueue(store, deadline.ExpirationsEpochs, miner0.NoQuantization)
	require.NoError(t, err)
	expirations := map[abi.ChainEpoch][]uint64{}
	_ = queue.ForEach(func(epoch abi.ChainEpoch, bf bitfield.BitField) error {
		expanded, err := bf.All(miner0.SectorsMax)
		require.NoError(t, err)
		expirations[epoch] = expanded
		return nil
	})
	return expirations
}

func collectDeadlineExpirations2(t *testing.T, store adt0.Store, deadline *miner2.Deadline) map[abi.ChainEpoch][]uint64 {
	queue, err := miner2.LoadBitfieldQueue(store, deadline.ExpirationsEpochs, miner2.NoQuantization)
	require.NoError(t, err)
	expirations := map[abi.ChainEpoch][]uint64{}
	_ = queue.ForEach(func(epoch abi.ChainEpoch, bf bitfield.BitField) error {
		expanded, err := bf.All(1 << 30)
		require.NoError(t, err)
		expirations[epoch] = expanded
		return nil
	})
	return expirations
}

func collectPartitionExpirations0(t *testing.T, store adt0.Store, partition *miner0.Partition) map[abi.ChainEpoch]*miner0.ExpirationSet {
	queue, err := miner0.LoadExpirationQueue(store, partition.ExpirationsEpochs, miner0.NoQuantization)
	require.NoError(t, err)
	expirations := map[abi.ChainEpoch]*miner0.ExpirationSet{}
	var es miner0.ExpirationSet
	_ = queue.ForEach(&es, func(i int64) error {
		cpy := es
		expirations[abi.ChainEpoch(i)] = &cpy
		return nil
	})
	return expirations
}

func collectPartitionExpirations2(t *testing.T, store adt0.Store, partition *miner2.Partition) map[abi.ChainEpoch]*miner2.ExpirationSet {
	queue, err := miner2.LoadExpirationQueue(store, partition.ExpirationsEpochs, miner2.NoQuantization)
	require.NoError(t, err)
	expirations := map[abi.ChainEpoch]*miner2.ExpirationSet{}
	var es miner2.ExpirationSet
	_ = queue.ForEach(&es, func(i int64) error {
		cpy := es
		expirations[abi.ChainEpoch(i)] = &cpy
		return nil
	})
	return expirations
}

func assertDeadlineExpirationQueue0(t *testing.T, store adt0.Store, deadline *miner0.Deadline, pIdx uint64, expected map[abi.ChainEpoch][]uint64) {
	dlExpirations := collectDeadlineExpirations0(t, store, deadline)
	assert.Equal(t, expected, dlExpirations)
}

func assertDeadlineExpirationQueue2(t *testing.T, store adt0.Store, deadline *miner2.Deadline, pIdx uint64, expected map[abi.ChainEpoch][]uint64) {
	dlExpirations := collectDeadlineExpirations2(t, store, deadline)
	assert.Equal(t, expected, dlExpirations)
}

func assertPartitionExpirationQueue0(t *testing.T, store adt0.Store, partition *miner0.Partition, expected map[abi.ChainEpoch]*miner0.ExpirationSet) {
	ptExpirations := collectPartitionExpirations0(t, store, partition)
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

func assertPartitionExpirationQueue2(t *testing.T, store adt0.Store, partition *miner2.Partition, expected map[abi.ChainEpoch]*miner2.ExpirationSet) {
	ptExpirations := collectPartitionExpirations2(t, store, partition)
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
