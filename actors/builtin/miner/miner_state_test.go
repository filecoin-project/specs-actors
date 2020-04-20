package miner_test

import (
	"context"
	"testing"

	cid "github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	tutils "github.com/filecoin-project/specs-actors/support/testing"
)

const SectorSize = abi.SectorSize(32 << 20)

func TestPrecommittedSectorsStore(t *testing.T) {
	t.Run("Round Trip put get", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)

		expect := harness.newSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(expect)
		actual := harness.assertGetPreCommit(sectorNo)

		assert.Equal(t, expect, actual)
	})

	t.Run("Subsequent puts with same sector number overrides previous value", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)

		firstPut := harness.newSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(firstPut)

		secondPut := harness.newSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(secondPut)

		actual := harness.assertGetPreCommit(sectorNo)
		assert.NotEqual(t, firstPut, actual)
		assert.Equal(t, secondPut, actual)
	})

	t.Run("Round Trip put delete", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)

		expect := harness.newSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(expect)

		harness.assertDeletePreCommit(sectorNo)
		harness.assertPreCommitNotFound(sectorNo)
	})

	t.Run("Subsequent puts with same sector number and a delete result in not found", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)

		firstPut := harness.newSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(firstPut)

		secondPut := harness.newSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(secondPut)

		harness.assertDeletePreCommit(sectorNo)
		harness.assertPreCommitNotFound(sectorNo)
	})

	t.Run("Delete nonexistent value returns an error", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)
		err := harness.s.DeletePrecommittedSector(store, sectorNo)
		assert.Error(t, err)
	})

	t.Run("Get nonexistent value returns false", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)
		harness.assertPreCommitNotFound(sectorNo)
	})

}

func TestSectorsStore(t *testing.T) {
	t.Run("Round Trip put get", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		sectorInfo := harness.newSectorOnChainInfo(sectorNo)

		harness.assertPutSector(sectorInfo)
		harness.assertHasSectorNo(sectorNo)

		out := harness.assertGetSector(sectorNo)
		assert.Equal(t, sectorInfo, out)
	})

	t.Run("Round Trip put delete", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		sectorInfo := harness.newSectorOnChainInfo(sectorNo)

		harness.assertPutSector(sectorInfo)
		harness.assertHasSectorNo(sectorNo)

		harness.assertDeleteSectors(sectorNo)
		harness.assertSectorNotFound(sectorNo)
	})

	t.Run("Subsequent puts override previous values", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		sectorInfo1 := harness.newSectorOnChainInfo(sectorNo)
		sectorInfo2 := harness.newSectorOnChainInfo(sectorNo)

		harness.assertPutSector(sectorInfo1)
		harness.assertPutSector(sectorInfo2)

		actual := harness.assertGetSector(sectorNo)
		assert.Equal(t, sectorInfo2, actual)
		assert.NotEqual(t, sectorInfo1, actual)

		harness.assertDeleteSectors(sectorNo)
		harness.assertSectorNotFound(sectorNo)
	})

	t.Run("Delete nonexistent value returns an error", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		bf := abi.NewBitField()
		bf.Set(uint64(sectorNo))

		err := harness.s.DeleteSectors(store, bf)
		assert.Error(t, err)
	})

	t.Run("Get nonexistent value returns false", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		harness.assertSectorNotFound(sectorNo)
	})

	t.Run("Iterate and Delete multiple sector", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		// set of sectors, the larger numbers here are not significant
		sectorNos := []abi.SectorNumber{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}

		// put all the sectors in the store
		for _, s := range sectorNos {
			harness.assertPutSector(harness.newSectorOnChainInfo(s))
		}

		sectorNoIdx := 0
		err := harness.s.ForEachSector(store, func(si *miner.SectorOnChainInfo) {
			require.Equal(t, sectorNos[sectorNoIdx], si.Info.SectorNumber)
			sectorNoIdx++
		})
		assert.NoError(t, err)

		// ensure we iterated over the expected number of sectors
		assert.Equal(t, len(sectorNos), sectorNoIdx)

		harness.assertDeleteSectors(sectorNos...)

		for _, s := range sectorNos {
			harness.assertSectorNotFound(s)
		}
	})

}

func TestNewSectorsBitField(t *testing.T) {
	t.Run("Add new sectors happy path", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		// set of sectors, the larger numbers here are not significant
		sectorNos := []abi.SectorNumber{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
		harness.assertAddNewSectors(sectorNos...)
		harness.assertNewSectorsCount(uint64(len(sectorNos)))
	})

	t.Run("Add new sectors excludes duplicates", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		sectorNos := []abi.SectorNumber{1, 1, 2, 2, 3, 4, 5}
		harness.assertAddNewSectors(sectorNos...)
		harness.assertNewSectorsCount(5)
	})

	t.Run("Remove sectors happy path", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		sectorNos := []abi.SectorNumber{1, 2, 3, 4, 5}
		harness.assertAddNewSectors(sectorNos...)
		harness.assertNewSectorsCount(uint64(len(sectorNos)))

		harness.assertRemoveNewSectors(1, 3, 5)
		harness.assertNewSectorsCount(2)

		sm, err := harness.s.NewSectors.AllMap(uint64(len(sectorNos)))
		assert.NoError(t, err)
		assert.True(t, sm[2])
		assert.True(t, sm[4])
		assert.False(t, sm[1])
		assert.False(t, sm[3])
		assert.False(t, sm[5])
	})

	t.Run("Add New sectors errors when adding too many new sectors", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		tooManySectors := make([]abi.SectorNumber, miner.NewSectorsPerPeriodMax+1)
		for i := uint64(0); i < miner.NewSectorsPerPeriodMax+1; i++ {
			tooManySectors[i] = abi.SectorNumber(i)
		}

		err := harness.s.AddNewSectors(tooManySectors...)
		assert.Error(t, err)

		// sanity check nothing was added
		// FIXME this is either a bug in code or an incorrect test, if AddNewSectors errors I would expect NewSectors
		// bitfield to remain unmodified, unless of course the runtime is just going to abort on this error, still kinda ugly
		harness.assertNewSectorsCount(0)
	})
}

func TestSectorExpirationStore(t *testing.T) {
	exp1 := abi.ChainEpoch(10)
	exp2 := abi.ChainEpoch(20)

	sectorExpirations := make(map[abi.ChainEpoch][]uint64)
	sectorExpirations[exp1] = []uint64{1, 2, 3, 4, 5}
	sectorExpirations[exp2] = []uint64{6, 7, 8, 9, 10}

	t.Run("Round trip add get sector expirations", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		harness.assertAddSectorExpiration(exp1, sectorExpirations[exp1]...)
		harness.assertAddSectorExpiration(exp2, sectorExpirations[exp2]...)

		out1 := harness.assertGetSectorExpirations(exp1)
		assert.Equal(t, sectorExpirations[exp1], out1)

		out2 := harness.assertGetSectorExpirations(exp2)
		assert.Equal(t, sectorExpirations[exp2], out2)

		// return nothing if there are no sectors at the epoch
		out3 := harness.assertGetSectorExpirations(abi.ChainEpoch(0))
		assert.Empty(t, out3)
	})

	t.Run("Round trip add remove sector expirations", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		harness.assertAddSectorExpiration(exp1, sectorExpirations[exp1]...)
		harness.assertAddSectorExpiration(exp2, sectorExpirations[exp2]...)

		// remove the first sector from expiration set 1
		harness.assertRemoveSectorExpiration(exp1, sectorExpirations[exp1][0])
		out1 := harness.assertGetSectorExpirations(exp1)
		assert.Equal(t, sectorExpirations[exp1][1:], out1)

		// remove all sectors from expiration set 2
		harness.assertRemoveSectorExpiration(exp2, sectorExpirations[exp2]...)
		out2 := harness.assertGetSectorExpirations(exp2)
		assert.Empty(t, out2)
	})

	t.Run("Iteration by expiration", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		harness.assertAddSectorExpiration(exp1, sectorExpirations[exp1]...)
		harness.assertAddSectorExpiration(exp2, sectorExpirations[exp2]...)

		exp1Hit, exp2Hit := false, false
		err := harness.s.ForEachSectorExpiration(store, func(expiry abi.ChainEpoch, sectors *abi.BitField) error {
			if expiry == exp1 {
				sectors, err := sectors.All(miner.SectorsMax)
				assert.NoError(t, err)
				assert.Equal(t, sectorExpirations[expiry], sectors)
				exp1Hit = true
			}
			if expiry == exp2 {
				sectors, err := sectors.All(miner.SectorsMax)
				assert.NoError(t, err)
				assert.Equal(t, sectorExpirations[expiry], sectors)
				exp2Hit = true
			}
			return nil
		})
		assert.NoError(t, err)
		assert.True(t, exp1Hit)
		assert.True(t, exp2Hit)
	})

	t.Run("Adding sectors at expiry merges to existing and clear expirations", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		mergedSectors := []uint64{21, 22, 23, 24, 25}
		harness.assertAddSectorExpiration(exp1, sectorExpirations[exp1]...)
		harness.assertAddSectorExpiration(exp1, mergedSectors...)

		merged := harness.assertGetSectorExpirations(exp1)
		assert.Equal(t, append(sectorExpirations[exp1], mergedSectors...), merged)
	})

	t.Run("clear sectors by expirations", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		harness.assertAddSectorExpiration(exp1, sectorExpirations[exp1]...)
		harness.assertAddSectorExpiration(exp2, sectorExpirations[exp2]...)

		// ensure clearing works
		harness.assertClearSectorExpiration(exp1, exp2)
		empty1 := harness.assertGetSectorExpirations(exp1)
		assert.Empty(t, empty1)

		empty2 := harness.assertGetSectorExpirations(exp2)
		assert.Empty(t, empty2)
	})

	t.Run("fail to add more than SectorsMax number of sectors", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		tooManySectors := make([]uint64, miner.SectorsMax+1)

		for i := uint64(0); i < miner.SectorsMax+1; i++ {
			tooManySectors[i] = i
		}

		err := harness.s.AddSectorExpirations(store, 1, tooManySectors...)
		assert.Error(t, err)

	})

}

type minerStateHarness struct {
	t testing.TB

	s     *miner.State
	store adt.Store

	cidGetter func() cid.Cid
	seed      uint64
}

//
// Sector Expiration Store
//

// internall converst the bit field to slice of uint64
func (h *minerStateHarness) assertGetSectorExpirations(expiry abi.ChainEpoch) []uint64 {
	bf, err := h.s.GetSectorExpirations(h.store, expiry)
	assert.NoError(h.t, err)
	sectors, err := bf.All(miner.SectorsMax)
	assert.NoError(h.t, err)
	return sectors
}

func (h *minerStateHarness) assertAddSectorExpiration(expiry abi.ChainEpoch, sectors ...uint64) {
	err := h.s.AddSectorExpirations(h.store, expiry, sectors...)
	assert.NoError(h.t, err)
}

func (h *minerStateHarness) assertRemoveSectorExpiration(expiry abi.ChainEpoch, sectors ...uint64) {
	err := h.s.RemoveSectorExpirations(h.store, expiry, sectors...)
	assert.NoError(h.t, err)
}

func (h *minerStateHarness) assertClearSectorExpiration(excitations ...abi.ChainEpoch) {
	err := h.s.ClearSectorExpirations(h.store, excitations...)
	assert.NoError(h.t, err)
}

//
// NewSectors BitField Assertions
//

func (h *minerStateHarness) assertAddNewSectors(sectorNos ...abi.SectorNumber) {
	err := h.s.AddNewSectors(sectorNos...)
	assert.NoError(h.t, err)
}

// makes a bit field from the passed sector numbers
func (h *minerStateHarness) assertRemoveNewSectors(sectorNos ...abi.SectorNumber) {
	bf := newBitFieldFromSectorNos(sectorNos...)
	err := h.s.RemoveNewSectors(bf)
	assert.NoError(h.t, err)
}

func (h *minerStateHarness) assertClearNewSectors() {
	h.assertNewSectorsCount(0)
}

func (h *minerStateHarness) assertNewSectorsCount(count uint64) {
	sectorCount, err := h.s.NewSectors.Count()
	assert.NoError(h.t, err)
	assert.Equal(h.t, count, sectorCount)
}

//
// Sector Store Assertion Operations
//

func (h *minerStateHarness) assertGetSectorCount() uint64 {
	out, err := h.s.GetSectorCount(h.store)
	assert.NoError(h.t, err)
	return out
}

func (h *minerStateHarness) assertHasSectorNo(sectorNo abi.SectorNumber) {
	found, err := h.s.HasSectorNo(h.store, sectorNo)
	assert.NoError(h.t, err)
	assert.True(h.t, found)
}

func (h *minerStateHarness) assertSectorNotFound(sectorNo abi.SectorNumber) {
	found, err := h.s.HasSectorNo(h.store, sectorNo)
	assert.NoError(h.t, err)
	assert.False(h.t, found)
}

func (h *minerStateHarness) assertPutSector(sector *miner.SectorOnChainInfo) {
	err := h.s.PutSector(h.store, sector)
	assert.NoError(h.t, err)
}

func (h *minerStateHarness) assertGetSector(sectorNo abi.SectorNumber) *miner.SectorOnChainInfo {
	sectors, found, err := h.s.GetSector(h.store, sectorNo)
	assert.NoError(h.t, err)
	assert.True(h.t, found)
	assert.NotNil(h.t, sectors)
	return sectors
}

// makes a bit field from the passed sector numbers
func (h *minerStateHarness) assertDeleteSectors(sectorNos ...abi.SectorNumber) {
	bf := newBitFieldFromSectorNos(sectorNos...)
	err := h.s.DeleteSectors(h.store, bf)
	assert.NoError(h.t, err)
}

//
// Precommit Store Operations
//

func (h *minerStateHarness) assertPutPreCommit(info *miner.SectorPreCommitOnChainInfo) {
	err := h.s.PutPrecommittedSector(h.store, info)
	assert.NoError(h.t, err)
}

func (h *minerStateHarness) assertGetPreCommit(sectorNo abi.SectorNumber) *miner.SectorPreCommitOnChainInfo {
	out, found, err := h.s.GetPrecommittedSector(h.store, sectorNo)
	assert.NoError(h.t, err)
	assert.True(h.t, found)
	return out
}

func (h *minerStateHarness) assertPreCommitNotFound(sectorNo abi.SectorNumber) {
	out, found, err := h.s.GetPrecommittedSector(h.store, sectorNo)
	assert.NoError(h.t, err)
	assert.False(h.t, found)
	assert.Equal(h.t, &miner.SectorPreCommitOnChainInfo{}, out)
}

func (h *minerStateHarness) assertDeletePreCommit(sectorNo abi.SectorNumber) {
	err := h.s.DeletePrecommittedSector(h.store, sectorNo)
	assert.NoError(h.t, err)
}

//
// Type Construction Methods
//

// returns a unique SectorPreCommitOnChainInfo with each invocation with SectorNumber set to `sectorNo`.
func (h *minerStateHarness) newSectorPreCommitOnChainInfo(sectorNo abi.SectorNumber) *miner.SectorPreCommitOnChainInfo {
	seed := h.getSeed()
	info := h.newSectorPreCommitInfo(sectorNo)
	return &miner.SectorPreCommitOnChainInfo{
		Info:             *info,
		PreCommitDeposit: abi.NewTokenAmount(int64(seed)),
		PreCommitEpoch:   abi.ChainEpoch(seed),
	}
}

// returns a unique SectorOnChainInfo with each invocation with SectorNumber set to `sectorNo`.
func (h *minerStateHarness) newSectorOnChainInfo(sectorNo abi.SectorNumber) *miner.SectorOnChainInfo {
	seed := h.getSeed()
	info := h.newSectorPreCommitInfo(sectorNo)
	return &miner.SectorOnChainInfo{
		Info:            *info,
		ActivationEpoch: abi.ChainEpoch(seed),
		DealWeight:      big.NewInt(int64(seed)),
	}
}

// returns a unique SectorPreCommitInfo with each invocation with SectorNumber set to `sectorNo`.
func (h *minerStateHarness) newSectorPreCommitInfo(sectorNo abi.SectorNumber) *miner.SectorPreCommitInfo {
	seed := h.getSeed()
	return &miner.SectorPreCommitInfo{
		RegisteredProof: abi.RegisteredProof_StackedDRG32GiBPoSt,
		SectorNumber:    sectorNo,
		SealedCID:       h.cidGetter(),
		SealRandEpoch:   abi.ChainEpoch(seed),
		DealIDs:         nil,
		Expiration:      abi.ChainEpoch(seed),
	}
}

func (h *minerStateHarness) getSeed() uint64 {
	defer func() { h.seed += 1 }()
	return h.seed
}

// TODO consider allowing just the runtime store to be constructed, would need to change `AsStore` to operate on
// runtime.Store instead  of the entire runtime.
func constructStateHarness(t *testing.T, store adt.Store, periodBoundary abi.ChainEpoch) *minerStateHarness {
	// store init
	emptyMap, err := adt.MakeEmptyMap(store).Root()
	require.NoError(t, err)

	emptyArray, err := adt.MakeEmptyArray(store).Root()
	require.NoError(t, err)

	emptyDeadlines := miner.ConstructDeadlines()
	emptyDeadlinesCid, err := store.Put(context.Background(), emptyDeadlines)
	require.NoError(t, err)

	// state field init
	owner := tutils.NewBLSAddr(t, 1)
	worker := tutils.NewBLSAddr(t, 2)
	state := miner.ConstructState(emptyArray, emptyMap, emptyDeadlinesCid, owner, worker, "peer", SectorSize, periodBoundary)

	// assert NewSectors bitfield was constructed correctly (empty)
	newSectorsCount, err := state.NewSectors.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), newSectorsCount)

	return &minerStateHarness{
		s:         state,
		t:         t,
		store:     store,
		cidGetter: tutils.NewCidForTestGetter(),
		seed:      0,
	}
}

func newBitFieldFromSectorNos(sectorNos ...abi.SectorNumber) *abi.BitField {
	bf := abi.NewBitField()
	for _, sn := range sectorNos {
		bf.Set(uint64(sn))
	}
	return bf
}
