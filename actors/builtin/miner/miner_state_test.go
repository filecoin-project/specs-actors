package miner_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/abi"
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
		sectorNo := abi.SectorNumber(0)

		expect := harness.makeSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(rt, expect)
		actual := harness.assertGetPreCommit(rt, sectorNo)

		assert.Equal(t, expect, actual)
	})

	t.Run("Subsequent puts with same sector number overrides previous value", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(0)

		firstPut := harness.makeSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(rt, firstPut)

		secondPut := harness.makeSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(rt, secondPut)

		actual := harness.assertGetPreCommit(rt, sectorNo)
		assert.NotEqual(t, firstPut, actual)
		assert.Equal(t, secondPut, actual)
	})

	t.Run("Round Trip put delete", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(0)

		expect := harness.makeSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(rt, expect)

		harness.assertDeletePreCommit(rt, sectorNo)
		harness.assertPreCommitNotFound(rt, sectorNo)
	})

	t.Run("Subsequent puts with same sector number and a delete result in not found", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(0)

		firstPut := harness.makeSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(rt, firstPut)

		secondPut := harness.makeSectorPreCommitOnChainInfo(sectorNo)
		harness.assertPutPreCommit(rt, secondPut)

		harness.assertDeletePreCommit(rt, sectorNo)
		harness.assertPreCommitNotFound(rt, sectorNo)
	})

	t.Run("Delete nonexistent value returns an error", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(0)
		err := harness.s.DeletePrecommittedSector(adt.AsStore(rt), sectorNo)
		assert.Error(t, err)
	})

	t.Run("Get nonexistent value returns false", func(t *testing.T) {
		rt := builder.Build(t)
		harness := constructStateHarness(t, rt, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(0)
		harness.assertPreCommitNotFound(rt, sectorNo)
	})

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

	owner := tutils.NewBLSAddr(t, 1)
	worker := tutils.NewBLSAddr(t, 2)
	state := miner.ConstructState(emptyArray, emptyMap, emptyDeadlinesCid, owner, worker, "peer", SectorSize, periodBoundary)
	return &minerStateHarness{s: state, t: t, cidGetter: NewCidForTestGetter(), seed: 0}
}

type minerStateHarness struct {
	s *miner.State
	t testing.TB

	cidGetter func() cid.Cid
	seed      uint64
}

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

func (h *minerStateHarness) makeSectorPreCommitOnChainInfo(sectorNo abi.SectorNumber) *miner.SectorPreCommitOnChainInfo {
	defer func() { h.seed += 1 }()
	return &miner.SectorPreCommitOnChainInfo{
		Info: miner.SectorPreCommitInfo{
			RegisteredProof: abi.RegisteredProof_StackedDRG32GiBPoSt,
			SectorNumber:    sectorNo,
			SealedCID:       h.cidGetter(), // we need some valid CID here for CBOR serialization when storing.
			SealRandEpoch:   abi.ChainEpoch(h.seed),
			DealIDs:         nil,
			Expiration:      abi.ChainEpoch(h.seed),
		},
		PreCommitDeposit: abi.NewTokenAmount(1),
		PreCommitEpoch:   abi.ChainEpoch(h.seed),
	}
}

// NewCidForTestGetter returns a closure that returns a Cid unique to that invocation.
// The Cid is unique wrt the closure returned, not globally. You can use this function
// in tests.
func NewCidForTestGetter() func() cid.Cid {
	i := 31337
	return func() cid.Cid {
		obj, err := cbor.WrapObject([]int{i}, uint64(mh.BLAKE2B_MIN+31), -1)
		if err != nil {
			panic(err)
		}
		i++
		return obj.Cid()
	}
}
