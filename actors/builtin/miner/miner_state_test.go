package miner_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	cid "github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	tutils "github.com/filecoin-project/specs-actors/support/testing"
)

func TestPrecommittedSectorsStore(t *testing.T) {
	t.Run("Round Trip put get", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)

		expect := harness.newSectorPreCommitOnChainInfo(sectorNo, tutils.CidFromString(t, "1"), abi.NewTokenAmount(1), abi.ChainEpoch(1))
		harness.putPreCommit(expect)
		actual := harness.getPreCommit(sectorNo)

		assert.Equal(t, expect, actual)
	})

	t.Run("Subsequent puts with same sector number overrides previous value", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)

		firstPut := harness.newSectorPreCommitOnChainInfo(sectorNo, tutils.CidFromString(t, "1"), abi.NewTokenAmount(1), abi.ChainEpoch(1))
		harness.putPreCommit(firstPut)

		secondPut := harness.newSectorPreCommitOnChainInfo(sectorNo, tutils.CidFromString(t, "2"), abi.NewTokenAmount(1), abi.ChainEpoch(1))
		harness.putPreCommit(secondPut)

		actual := harness.getPreCommit(sectorNo)
		assert.NotEqual(t, firstPut, actual)
		assert.Equal(t, secondPut, actual)
	})

	t.Run("Round Trip put delete", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)

		expect := harness.newSectorPreCommitOnChainInfo(sectorNo, tutils.CidFromString(t, "1"), abi.NewTokenAmount(1), abi.ChainEpoch(1))
		harness.putPreCommit(expect)

		harness.deletePreCommit(sectorNo)
		assert.False(t, harness.hasPreCommit(sectorNo))
	})

	t.Run("Subsequent puts with same sector number and a delete result in not found", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)

		firstPut := harness.newSectorPreCommitOnChainInfo(sectorNo, tutils.CidFromString(t, "1"), abi.NewTokenAmount(1), abi.ChainEpoch(1))
		harness.putPreCommit(firstPut)

		secondPut := harness.newSectorPreCommitOnChainInfo(sectorNo, tutils.CidFromString(t, "2"), abi.NewTokenAmount(2), abi.ChainEpoch(2))
		harness.putPreCommit(secondPut)

		harness.deletePreCommit(sectorNo)
		assert.False(t, harness.hasPreCommit(sectorNo))
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
		assert.False(t, harness.hasPreCommit(sectorNo))
	})

}

func TestSectorsStore(t *testing.T) {
	t.Run("Round Trip put get", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		sectorInfo := harness.newSectorOnChainInfo(sectorNo, tutils.CidFromString(t, "1"), big.NewInt(1), abi.ChainEpoch(1))

		harness.putSector(sectorInfo)
		harness.hasSectorNo(sectorNo)

		out := harness.getSector(sectorNo)
		assert.Equal(t, sectorInfo, out)
	})

	t.Run("Round Trip put delete", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		sectorInfo := harness.newSectorOnChainInfo(sectorNo, tutils.CidFromString(t, "1"), big.NewInt(1), abi.ChainEpoch(1))

		harness.putSector(sectorInfo)
		harness.hasSectorNo(sectorNo)

		harness.deleteSectors(uint64(sectorNo))
		assert.False(t, harness.hasSectorNo(sectorNo))
	})

	t.Run("Subsequent puts override previous values", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		sectorInfo1 := harness.newSectorOnChainInfo(sectorNo, tutils.CidFromString(t, "1"), big.NewInt(1), abi.ChainEpoch(1))
		sectorInfo2 := harness.newSectorOnChainInfo(sectorNo, tutils.CidFromString(t, "2"), big.NewInt(2), abi.ChainEpoch(2))

		harness.putSector(sectorInfo1)
		harness.putSector(sectorInfo2)

		actual := harness.getSector(sectorNo)
		assert.Equal(t, sectorInfo2, actual)
		assert.NotEqual(t, sectorInfo1, actual)

		harness.deleteSectors(uint64(sectorNo))
		assert.False(t, harness.hasSectorNo(sectorNo))
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
		assert.False(t, harness.hasSectorNo(sectorNo))
	})

	t.Run("Iterate and Delete multiple sector", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		// set of sectors, the larger numbers here are not significant
		sectorNos := []uint64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}

		// put all the sectors in the store
		for _, s := range sectorNos {
			i := int64(0)
			harness.putSector(harness.newSectorOnChainInfo(abi.SectorNumber(s), tutils.CidFromString(t, fmt.Sprintf("%d", i)), big.NewInt(i), abi.ChainEpoch(i)))
			i++
		}

		sectorNoIdx := 0
		err := harness.s.ForEachSector(store, func(si *miner.SectorOnChainInfo) {
			require.Equal(t, abi.SectorNumber(sectorNos[sectorNoIdx]), si.Info.SectorNumber)
			sectorNoIdx++
		})
		assert.NoError(t, err)

		// ensure we iterated over the expected number of sectors
		assert.Equal(t, len(sectorNos), sectorNoIdx)

		harness.deleteSectors(sectorNos...)

		for _, s := range sectorNos {
			assert.False(t, harness.hasSectorNo(abi.SectorNumber(s)))
		}
	})

}

func TestNewSectorsBitField(t *testing.T) {
	t.Run("Add new sectors happy path", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		// set of sectors, the larger numbers here are not significant
		sectorNos := []abi.SectorNumber{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
		harness.addNewSectors(sectorNos...)
		assert.Equal(t, uint64(len(sectorNos)), harness.getNewSectorCount())
	})

	t.Run("Add new sectors excludes duplicates", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		sectorNos := []abi.SectorNumber{1, 1, 2, 2, 3, 4, 5}
		harness.addNewSectors(sectorNos...)
		assert.Equal(t, uint64(5), harness.getNewSectorCount())
	})

	t.Run("Remove sectors happy path", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		sectorNos := []abi.SectorNumber{1, 2, 3, 4, 5}
		harness.addNewSectors(sectorNos...)
		assert.Equal(t, uint64(len(sectorNos)), harness.getNewSectorCount())

		harness.removeNewSectors(1, 3, 5)
		assert.Equal(t, uint64(2), harness.getNewSectorCount())

		sm, err := harness.s.NewSectors.All(uint64(len(sectorNos)))
		assert.NoError(t, err)
		assert.Equal(t, []uint64{2, 4}, sm)
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
		// For omission reason see: https://github.com/filecoin-project/specs-actors/issues/300
		//assert.Equal(t, uint64(0), harness.getNewSectorCount())
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

		harness.addSectorExpiration(exp1, sectorExpirations[exp1]...)
		harness.addSectorExpiration(exp2, sectorExpirations[exp2]...)

		out1 := harness.getSectorExpirations(exp1)
		assert.Equal(t, sectorExpirations[exp1], out1)

		out2 := harness.getSectorExpirations(exp2)
		assert.Equal(t, sectorExpirations[exp2], out2)

		// return nothing if there are no sectors at the epoch
		out3 := harness.getSectorExpirations(abi.ChainEpoch(0))
		assert.Empty(t, out3)
	})

	t.Run("Round trip add remove sector expirations", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		harness.addSectorExpiration(exp1, sectorExpirations[exp1]...)
		harness.addSectorExpiration(exp2, sectorExpirations[exp2]...)

		// remove the first sector from expiration set 1
		harness.removeSectorExpiration(exp1, sectorExpirations[exp1][0])
		out1 := harness.getSectorExpirations(exp1)
		assert.Equal(t, sectorExpirations[exp1][1:], out1)

		// remove all sectors from expiration set 2
		harness.removeSectorExpiration(exp2, sectorExpirations[exp2]...)
		out2 := harness.getSectorExpirations(exp2)
		assert.Empty(t, out2)
	})

	t.Run("Iteration by expiration", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		harness.addSectorExpiration(exp1, sectorExpirations[exp1]...)
		harness.addSectorExpiration(exp2, sectorExpirations[exp2]...)

		exp1Hit, exp2Hit := false, false
		err := harness.s.ForEachSectorExpiration(store, func(expiry abi.ChainEpoch, sectors *abi.BitField) error {
			if expiry == exp1 {
				sectors, err := sectors.All(miner.SectorsMax)
				assert.NoError(t, err)
				assert.Equal(t, sectorExpirations[expiry], sectors)
				exp1Hit = true
			} else if expiry == exp2 {
				sectors, err := sectors.All(miner.SectorsMax)
				assert.NoError(t, err)
				assert.Equal(t, sectorExpirations[expiry], sectors)
				exp2Hit = true
			} else {
				t.Fatalf("unexpected expiry value: %v in sector expirations", expiry)
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
		harness.addSectorExpiration(exp1, sectorExpirations[exp1]...)
		harness.addSectorExpiration(exp1, mergedSectors...)

		merged := harness.getSectorExpirations(exp1)
		assert.Equal(t, append(sectorExpirations[exp1], mergedSectors...), merged)
	})

	t.Run("clear sectors by expirations", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		harness.addSectorExpiration(exp1, sectorExpirations[exp1]...)
		harness.addSectorExpiration(exp2, sectorExpirations[exp2]...)

		// ensure clearing works
		harness.clearSectorExpiration(exp1, exp2)
		empty1 := harness.getSectorExpirations(exp1)
		assert.Empty(t, empty1)

		empty2 := harness.getSectorExpirations(exp2)
		assert.Empty(t, empty2)
	})

}

func TestFaultStore(t *testing.T) {
	fault1 := abi.ChainEpoch(10)
	fault2 := abi.ChainEpoch(20)

	sectorFaults := make(map[abi.ChainEpoch][]uint64)
	faultSet1 := []uint64{1, 2, 3, 4, 5}
	faultSet2 := []uint64{6, 7, 8, 9, 10, 11}
	sectorFaults[fault1] = faultSet1
	sectorFaults[fault2] = faultSet2

	store := adt.NewStore(context.Background())
	harness := constructStateHarness(t, store, abi.ChainEpoch(0))

	harness.addFaults(fault1, sectorFaults[fault1]...)
	harness.addFaults(fault2, sectorFaults[fault2]...)

	fault1Hit, fault2Hit := false, false
	err := harness.s.ForEachFaultEpoch(store, func(epoch abi.ChainEpoch, faults *abi.BitField) error {
		if epoch == fault1 {
			sectors, err := faults.All(uint64(len(faultSet1)))
			require.NoError(t, err)
			assert.Equal(t, faultSet1, sectors)
			fault1Hit = true
		} else if epoch == fault2 {
			sectors, err := faults.All(uint64(len(faultSet2)))
			require.NoError(t, err)
			assert.Equal(t, faultSet2, sectors)
			fault2Hit = true
		} else {
			t.Fatalf("unexpected fault epoch: %v", epoch)
		}
		return nil
	})
	require.NoError(t, err)
	assert.True(t, fault1Hit)
	assert.True(t, fault2Hit)

	// remove the faults
	harness.removeFaults(faultSet1[1:]...)
	harness.removeFaults(faultSet2[2:]...)

	fault1Hit, fault2Hit = false, false
	err = harness.s.ForEachFaultEpoch(store, func(epoch abi.ChainEpoch, faults *abi.BitField) error {
		if epoch == fault1 {
			sectors, err := faults.All(uint64(len(faultSet1)))
			require.NoError(t, err)
			assert.Equal(t, faultSet1[1:], sectors, "expected: %v, actual: %v", faultSet1[1:], sectors)
			fault1Hit = true
		} else if epoch == fault2 {
			sectors, err := faults.All(uint64(len(faultSet2)))
			require.NoError(t, err)
			assert.Equal(t, faultSet2[2:], sectors, "expected: %v, actual: %v", faultSet2[2:], sectors)
			fault2Hit = true
		} else {
			t.Fatalf("unexpected fault epoch: %v", epoch)
		}
		return nil
	})
	require.NoError(t, err)
	assert.True(t, fault1Hit)
	assert.True(t, fault2Hit)

	// now clear all the faults
	err = harness.s.ClearFaultEpochs(store, fault1, fault2)
	require.NoError(t, err)

	err = harness.s.ForEachFaultEpoch(store, func(epoch abi.ChainEpoch, faults *abi.BitField) error {
		t.Fatalf("unexpected fault epoch: %v", epoch)
		return nil
	})
	require.NoError(t, err)
}

func TestRecoveriesBitfield(t *testing.T) {
	t.Run("Add new recoveries happy path", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		// set of sectors, the larger numbers here are not significant
		sectorNos := []uint64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
		harness.addRecoveries(sectorNos...)
		assert.Equal(t, uint64(len(sectorNos)), harness.getRecoveriesCount())
	})

	t.Run("Add new recoveries excludes duplicates", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		sectorNos := []uint64{1, 1, 2, 2, 3, 4, 5}
		harness.addRecoveries(sectorNos...)
		assert.Equal(t, uint64(5), harness.getRecoveriesCount())
	})

	t.Run("Remove recoveries happy path", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		sectorNos := []uint64{1, 2, 3, 4, 5}
		harness.addRecoveries(sectorNos...)
		assert.Equal(t, uint64(len(sectorNos)), harness.getRecoveriesCount())

		harness.removeRecoveries(1, 3, 5)
		assert.Equal(t, uint64(2), harness.getRecoveriesCount())

		recoveries, err := harness.s.Recoveries.All(uint64(len(sectorNos)))
		assert.NoError(t, err)
		assert.Equal(t, []uint64{2, 4}, recoveries)
	})
}

func TestPostSubmissionsBitfield(t *testing.T) {
	t.Run("Add new submission happy path", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		// set of sectors, the larger numbers here are not significant
		sectorNos := []uint64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
		harness.addPoStSubmissions(sectorNos...)
		assert.Equal(t, uint64(len(sectorNos)), harness.getPoStSubmissionsCount())
	})

	t.Run("Add new submission excludes duplicates", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		sectorNos := []uint64{1, 1, 2, 2, 3, 4, 5}
		harness.addPoStSubmissions(sectorNos...)
		assert.Equal(t, uint64(5), harness.getPoStSubmissionsCount())
	})

	t.Run("Clear submission happy path", func(t *testing.T) {
		store := adt.NewStore(context.Background())
		harness := constructStateHarness(t, store, abi.ChainEpoch(0))

		sectorNos := []uint64{1, 2, 3, 4, 5}
		harness.addPoStSubmissions(sectorNos...)
		assert.Equal(t, uint64(len(sectorNos)), harness.getPoStSubmissionsCount())

		harness.clearPoStSubmissions()
		assert.Equal(t, uint64(0), harness.getPoStSubmissionsCount())
	})
}

type minerStateHarness struct {
	t testing.TB

	s     *miner.State
	store adt.Store
}

//
// PostSubmissions Bitfield
//

func (h *minerStateHarness) addPoStSubmissions(partitionNos ...uint64) {
	err := h.s.AddPoStSubmissions(bitfield.NewFromSet(partitionNos))
	require.NoError(h.t, err)
}

func (h *minerStateHarness) clearPoStSubmissions() {
	err := h.s.ClearPoStSubmissions()
	require.NoError(h.t, err)
}

func (h *minerStateHarness) getPoStSubmissionsCount() uint64 {
	count, err := h.s.PostSubmissions.Count()
	require.NoError(h.t, err)
	return count
}

//
// Recoveries Bitfield
//

func (h *minerStateHarness) addRecoveries(sectorNos ...uint64) {
	bf := bitfield.NewFromSet(sectorNos)
	err := h.s.AddRecoveries(bf)
	require.NoError(h.t, err)
}

func (h *minerStateHarness) removeRecoveries(sectorNos ...uint64) {
	bf := bitfield.NewFromSet(sectorNos)
	err := h.s.RemoveRecoveries(bf)
	require.NoError(h.t, err)
}

func (h *minerStateHarness) getRecoveriesCount() uint64 {
	count, err := h.s.Recoveries.Count()
	require.NoError(h.t, err)
	return count
}

//
// Faults Store
//

func (h *minerStateHarness) addFaults(epoch abi.ChainEpoch, sectorNos ...uint64) {
	bf := bitfield.NewFromSet(sectorNos)
	err := h.s.AddFaults(h.store, bf, epoch)
	require.NoError(h.t, err)
}

func (h *minerStateHarness) removeFaults(sectorNos ...uint64) {
	bf := bitfield.NewFromSet(sectorNos)
	err := h.s.RemoveFaults(h.store, bf)
	require.NoError(h.t, err)
}

//
// Sector Expiration Store
//

func (h *minerStateHarness) getSectorExpirations(expiry abi.ChainEpoch) []uint64 {
	bf, err := h.s.GetSectorExpirations(h.store, expiry)
	require.NoError(h.t, err)
	sectors, err := bf.All(miner.SectorsMax)
	require.NoError(h.t, err)
	return sectors
}

func (h *minerStateHarness) addSectorExpiration(expiry abi.ChainEpoch, sectors ...uint64) {
	err := h.s.AddSectorExpirations(h.store, expiry, sectors...)
	require.NoError(h.t, err)
}

func (h *minerStateHarness) removeSectorExpiration(expiry abi.ChainEpoch, sectors ...uint64) {
	err := h.s.RemoveSectorExpirations(h.store, expiry, sectors...)
	require.NoError(h.t, err)
}

func (h *minerStateHarness) clearSectorExpiration(excitations ...abi.ChainEpoch) {
	err := h.s.ClearSectorExpirations(h.store, excitations...)
	require.NoError(h.t, err)
}

//
// NewSectors BitField Assertions
//

func (h *minerStateHarness) addNewSectors(sectorNos ...abi.SectorNumber) {
	err := h.s.AddNewSectors(sectorNos...)
	require.NoError(h.t, err)
}

// makes a bit field from the passed sector numbers
func (h *minerStateHarness) removeNewSectors(sectorNos ...uint64) {
	bf := bitfield.NewFromSet(sectorNos)
	err := h.s.RemoveNewSectors(bf)
	require.NoError(h.t, err)
}

func (h *minerStateHarness) getNewSectorCount() uint64 {
	out, err := h.s.NewSectors.Count()
	require.NoError(h.t, err)
	return out
}

//
// Sector Store Assertion Operations
//

func (h *minerStateHarness) getSectorCount() uint64 {
	out, err := h.s.GetSectorCount(h.store)
	require.NoError(h.t, err)
	return out
}

func (h *minerStateHarness) hasSectorNo(sectorNo abi.SectorNumber) bool {
	found, err := h.s.HasSectorNo(h.store, sectorNo)
	require.NoError(h.t, err)
	return found
}

func (h *minerStateHarness) putSector(sector *miner.SectorOnChainInfo) {
	err := h.s.PutSector(h.store, sector)
	require.NoError(h.t, err)
}

func (h *minerStateHarness) getSector(sectorNo abi.SectorNumber) *miner.SectorOnChainInfo {
	sectors, found, err := h.s.GetSector(h.store, sectorNo)
	require.NoError(h.t, err)
	assert.True(h.t, found)
	assert.NotNil(h.t, sectors)
	return sectors
}

// makes a bit field from the passed sector numbers
func (h *minerStateHarness) deleteSectors(sectorNos ...uint64) {
	bf := bitfield.NewFromSet(sectorNos)
	err := h.s.DeleteSectors(h.store, bf)
	require.NoError(h.t, err)
}

//
// Precommit Store Operations
//

func (h *minerStateHarness) putPreCommit(info *miner.SectorPreCommitOnChainInfo) {
	err := h.s.PutPrecommittedSector(h.store, info)
	require.NoError(h.t, err)
}

func (h *minerStateHarness) getPreCommit(sectorNo abi.SectorNumber) *miner.SectorPreCommitOnChainInfo {
	out, found, err := h.s.GetPrecommittedSector(h.store, sectorNo)
	require.NoError(h.t, err)
	assert.True(h.t, found)
	return out
}

func (h *minerStateHarness) hasPreCommit(sectorNo abi.SectorNumber) bool {
	_, found, err := h.s.GetPrecommittedSector(h.store, sectorNo)
	require.NoError(h.t, err)
	return found
}

func (h *minerStateHarness) deletePreCommit(sectorNo abi.SectorNumber) {
	err := h.s.DeletePrecommittedSector(h.store, sectorNo)
	require.NoError(h.t, err)
}

//
// Type Construction Methods
//

// returns a unique SectorPreCommitOnChainInfo with each invocation with SectorNumber set to `sectorNo`.
func (h *minerStateHarness) newSectorPreCommitOnChainInfo(sectorNo abi.SectorNumber, sealed cid.Cid, deposit abi.TokenAmount, epoch abi.ChainEpoch) *miner.SectorPreCommitOnChainInfo {
	info := h.newSectorPreCommitInfo(sectorNo, sealed)
	return &miner.SectorPreCommitOnChainInfo{
		Info:             *info,
		PreCommitDeposit: deposit,
		PreCommitEpoch:   epoch,
	}
}

// returns a unique SectorOnChainInfo with each invocation with SectorNumber set to `sectorNo`.
func (h *minerStateHarness) newSectorOnChainInfo(sectorNo abi.SectorNumber, sealed cid.Cid, weight big.Int, activation abi.ChainEpoch) *miner.SectorOnChainInfo {
	info := h.newSectorPreCommitInfo(sectorNo, sealed)
	return &miner.SectorOnChainInfo{
		Info:               *info,
		ActivationEpoch:    activation,
		DealWeight:         weight,
		VerifiedDealWeight: weight,
	}
}

const (
	sectorSealRandEpochValue = abi.ChainEpoch(1)
	sectorExpiration         = abi.ChainEpoch(1)
)

// returns a unique SectorPreCommitInfo with each invocation with SectorNumber set to `sectorNo`.
func (h *minerStateHarness) newSectorPreCommitInfo(sectorNo abi.SectorNumber, sealed cid.Cid) *miner.SectorPreCommitInfo {
	return &miner.SectorPreCommitInfo{
		RegisteredProof: abi.RegisteredProof_StackedDRG32GiBPoSt,
		SectorNumber:    sectorNo,
		SealedCID:       sealed,
		SealRandEpoch:   sectorSealRandEpochValue,
		DealIDs:         nil,
		Expiration:      sectorExpiration,
	}
}

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
		t: t,

		s:     state,
		store: store,
	}
}
