package miner_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/runtime"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutils "github.com/filecoin-project/specs-actors/support/testing"
)

const SectorSize = abi.SectorSize(32 << 20)

func TestPrecommittedSectorsStore(t *testing.T) {
	builder := mock.NewBuilder(context.Background(), address.Undef)

	t.Run("Round Trip put get", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)

		expect := harness.newSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(rt, expect)
		actual := harness.assertGetPreCommit(rt, sectorNo)

		assert.Equal(t, expect, actual)
	})

	t.Run("Subsequent puts with same sector number overrides previous value", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)

		firstPut := harness.newSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(rt, firstPut)

		secondPut := harness.newSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(rt, secondPut)

		actual := harness.assertGetPreCommit(rt, sectorNo)
		assert.NotEqual(t, firstPut, actual)
		assert.Equal(t, secondPut, actual)
	})

	t.Run("Round Trip put delete", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)

		expect := harness.newSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(rt, expect)

		harness.assertDeletePreCommit(rt, sectorNo)
		harness.assertPreCommitNotFound(rt, sectorNo)
	})

	t.Run("Subsequent puts with same sector number and a delete result in not found", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)

		firstPut := harness.newSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(rt, firstPut)

		secondPut := harness.newSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(rt, secondPut)

		harness.assertDeletePreCommit(rt, sectorNo)
		harness.assertPreCommitNotFound(rt, sectorNo)
	})

	t.Run("Delete nonexistent value returns an error", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)
		err := harness.s.DeletePrecommittedSector(adt.AsStore(rt), sectorNo)
		assert.Error(t, err)
	})

	t.Run("Get nonexistent value returns false", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)
		harness.assertPreCommitNotFound(rt, sectorNo)
	})

}

func TestSectorsStore(t *testing.T) {
	builder := mock.NewBuilder(context.Background(), address.Undef)

	t.Run("Round Trip put get", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		sectorInfo := harness.newSectorOnChainInfo(sectorNo)

		harness.assertPutSector(rt, sectorInfo)
		harness.assertHasSectorNo(rt, sectorNo)

		out := harness.assertGetSector(rt, sectorNo)
		assert.Equal(t, sectorInfo, out)
	})

	t.Run("Round Trip put delete", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		sectorInfo := harness.newSectorOnChainInfo(sectorNo)

		harness.assertPutSector(rt, sectorInfo)
		harness.assertHasSectorNo(rt, sectorNo)

		harness.assertDeleteSectors(rt, sectorNo)
		harness.assertSectorNotFound(rt, sectorNo)
	})

	t.Run("Subsequent puts override previous values", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		sectorInfo1 := harness.newSectorOnChainInfo(sectorNo)
		sectorInfo2 := harness.newSectorOnChainInfo(sectorNo)

		harness.assertPutSector(rt, sectorInfo1)
		harness.assertPutSector(rt, sectorInfo2)

		actual := harness.assertGetSector(rt, sectorNo)
		assert.Equal(t, sectorInfo2, actual)
		assert.NotEqual(t, sectorInfo1, actual)

		harness.assertDeleteSectors(rt, sectorNo)
		harness.assertSectorNotFound(rt, sectorNo)
	})

	t.Run("Delete nonexistent value returns an error", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		bf := abi.NewBitField()
		bf.Set(uint64(sectorNo))

		err := harness.s.DeleteSectors(adt.AsStore(rt), bf)
		assert.Error(t, err)
	})

	t.Run("Get nonexistent value returns false", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		harness.assertSectorNotFound(rt, sectorNo)
	})

	t.Run("Iterate and Delete multiple sector", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))

		// set of sectors, the larger numbers here are not significant
		sectorNos := []abi.SectorNumber{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}

		// put all the sectors in the store
		for _, s := range sectorNos {
			harness.assertPutSector(rt, harness.newSectorOnChainInfo(s))
		}

		sectorNoIdx := 0
		err := harness.s.ForEachSector(adt.AsStore(rt), func(si *miner.SectorOnChainInfo) {
			require.Equal(t, sectorNos[sectorNoIdx], si.Info.SectorNumber)
			sectorNoIdx++
		})
		assert.NoError(t, err)

		// ensure we iterated over the expected number of sectors
		assert.Equal(t, len(sectorNos), sectorNoIdx)

		harness.assertDeleteSectors(rt, sectorNos...)

		for _, s := range sectorNos {
			harness.assertSectorNotFound(rt, s)
		}
	})

}

func TestNewSectorsBitField(t *testing.T) {
	builder := mock.NewBuilder(context.Background(), address.Undef)

	t.Run("Add new sectors happy path", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))

		// set of sectors, the larger numbers here are not significant
		sectorNos := []abi.SectorNumber{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
		harness.assertAddNewSectors(sectorNos...)
		harness.assertNewSectorsCount(uint64(len(sectorNos)))
	})

	t.Run("Add new sectors excludes duplicates", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))

		sectorNos := []abi.SectorNumber{1, 1, 2, 2, 3, 4, 5}
		harness.assertAddNewSectors(sectorNos...)
		harness.assertNewSectorsCount(5)
	})

	t.Run("Remove sectors happy path", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))

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
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))

		tooManySectors := make([]abi.SectorNumber, miner.NewSectorsPerPeriodMax+1)
		for i := uint64(0); i < miner.NewSectorsPerPeriodMax+1; i++ {
			tooManySectors[i] = abi.SectorNumber(i)
		}

		err := harness.s.AddNewSectors(tooManySectors...)
		assert.Error(t, err)

		// sanity check nothing was added
		// FIXME this is either a bug in code or an incorrect test, if AddNewSectors errors I would expect NewSectors
		// bitfield to remain unmodified.
		harness.assertNewSectorsCount(0)
	})
}

type minerStateHarness struct {
	s *miner.State
	t testing.TB

	cidGetter func() cid.Cid
	seed      uint64
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

func (h *minerStateHarness) assertGetSectorCount(rt runtime.Runtime) uint64 {
	out, err := h.s.GetSectorCount(adt.AsStore(rt))
	assert.NoError(h.t, err)
	return out
}

func (h *minerStateHarness) assertHasSectorNo(rt runtime.Runtime, sectorNo abi.SectorNumber) {
	found, err := h.s.HasSectorNo(adt.AsStore(rt), sectorNo)
	assert.NoError(h.t, err)
	assert.True(h.t, found)
}

func (h *minerStateHarness) assertSectorNotFound(rt runtime.Runtime, sectorNo abi.SectorNumber) {
	found, err := h.s.HasSectorNo(adt.AsStore(rt), sectorNo)
	assert.NoError(h.t, err)
	assert.False(h.t, found)
}

func (h *minerStateHarness) assertPutSector(rt runtime.Runtime, sector *miner.SectorOnChainInfo) {
	err := h.s.PutSector(adt.AsStore(rt), sector)
	assert.NoError(h.t, err)
}

func (h *minerStateHarness) assertGetSector(rt runtime.Runtime, sectorNo abi.SectorNumber) *miner.SectorOnChainInfo {
	sectors, found, err := h.s.GetSector(adt.AsStore(rt), sectorNo)
	assert.NoError(h.t, err)
	assert.True(h.t, found)
	assert.NotNil(h.t, sectors)
	return sectors
}

// makes a bit field from the passed sector numbers
func (h *minerStateHarness) assertDeleteSectors(rt runtime.Runtime, sectorNos ...abi.SectorNumber) {
	bf := newBitFieldFromSectorNos(sectorNos...)
	err := h.s.DeleteSectors(adt.AsStore(rt), bf)
	assert.NoError(h.t, err)
}

//
// Precommit Store Operations
//

func (h *minerStateHarness) assertPutPreCommit(rt runtime.Runtime, info *miner.SectorPreCommitOnChainInfo) {
	err := h.s.PutPrecommittedSector(adt.AsStore(rt), info)
	assert.NoError(h.t, err)
}

func (h *minerStateHarness) assertGetPreCommit(rt runtime.Runtime, sectorNo abi.SectorNumber) *miner.SectorPreCommitOnChainInfo {
	out, found, err := h.s.GetPrecommittedSector(adt.AsStore(rt), sectorNo)
	assert.NoError(h.t, err)
	assert.True(h.t, found)
	return out
}

func (h *minerStateHarness) assertPreCommitNotFound(rt runtime.Runtime, sectorNo abi.SectorNumber) {
	out, found, err := h.s.GetPrecommittedSector(adt.AsStore(rt), sectorNo)
	assert.NoError(h.t, err)
	assert.False(h.t, found)
	assert.Equal(h.t, &miner.SectorPreCommitOnChainInfo{}, out)
}

func (h *minerStateHarness) assertDeletePreCommit(rt runtime.Runtime, sectorNo abi.SectorNumber) {
	err := h.s.DeletePrecommittedSector(adt.AsStore(rt), sectorNo)
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
func constructStateHarness(t *testing.T, rt runtime.Runtime, periodBoundary abi.ChainEpoch) *minerStateHarness {
	// store init
	emptyMap, err := adt.MakeEmptyMap(adt.AsStore(rt)).Root()
	require.NoError(t, err)

	emptyArray, err := adt.MakeEmptyArray(adt.AsStore(rt)).Root()
	require.NoError(t, err)

	emptyDeadlines := miner.ConstructDeadlines()
	emptyDeadlinesCid := rt.Store().Put(emptyDeadlines)

	// state field init
	owner := tutils.NewBLSAddr(t, 1)
	worker := tutils.NewBLSAddr(t, 2)
	state := miner.ConstructState(emptyArray, emptyMap, emptyDeadlinesCid, owner, worker, "peer", SectorSize, periodBoundary)

	// assert NewSectors bitfield was constructed correctly (empty)
	newSectorsCount, err := state.NewSectors.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), newSectorsCount)

	return &minerStateHarness{s: state, t: t, cidGetter: tutils.NewCidForTestGetter(), seed: 0}
}

func newBitFieldFromSectorNos(sectorNos ...abi.SectorNumber) *abi.BitField {
	bf := abi.NewBitField()
	for _, sn := range sectorNos {
		bf.Set(uint64(sn))
	}
	return bf
}
