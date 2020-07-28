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
	"github.com/filecoin-project/specs-actors/support/ipld"
	tutils "github.com/filecoin-project/specs-actors/support/testing"
)

func TestPrecommittedSectorsStore(t *testing.T) {
	t.Run("Put, get and delete", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)

		pc1 := newSectorPreCommitOnChainInfo(sectorNo, tutils.MakeCID("1", &miner.SealedCIDPrefix), abi.NewTokenAmount(1), abi.ChainEpoch(1))
		harness.putPreCommit(pc1)
		assert.Equal(t, pc1, harness.getPreCommit(sectorNo))

		pc2 := newSectorPreCommitOnChainInfo(sectorNo, tutils.MakeCID("2", &miner.SealedCIDPrefix), abi.NewTokenAmount(1), abi.ChainEpoch(1))
		harness.putPreCommit(pc2)
		assert.Equal(t, pc2, harness.getPreCommit(sectorNo))

		harness.deletePreCommit(sectorNo)
		assert.False(t, harness.hasPreCommit(sectorNo))
	})

	t.Run("Delete nonexistent value returns an error", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)
		err := harness.s.DeletePrecommittedSectors(harness.store, sectorNo)
		assert.Error(t, err)
	})

	t.Run("Get nonexistent value returns false", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)
		assert.False(t, harness.hasPreCommit(sectorNo))
	})
}

func TestSectorsStore(t *testing.T) {
	t.Run("Put get and delete", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		sectorInfo1 := newSectorOnChainInfo(sectorNo, tutils.MakeCID("1", &miner.SealedCIDPrefix), big.NewInt(1), abi.ChainEpoch(1))
		sectorInfo2 := newSectorOnChainInfo(sectorNo, tutils.MakeCID("2", &miner.SealedCIDPrefix), big.NewInt(2), abi.ChainEpoch(2))

		harness.putSector(sectorInfo1)
		assert.True(t, harness.hasSectorNo(sectorNo))
		out := harness.getSector(sectorNo)
		assert.Equal(t, sectorInfo1, out)

		harness.putSector(sectorInfo2)
		out = harness.getSector(sectorNo)
		assert.Equal(t, sectorInfo2, out)

		harness.deleteSectors(uint64(sectorNo))
		assert.False(t, harness.hasSectorNo(sectorNo))
	})

	t.Run("Delete nonexistent value returns an error", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		bf := abi.NewBitField()
		bf.Set(uint64(sectorNo))

		assert.Error(t, harness.s.DeleteSectors(harness.store, bf))
	})

	t.Run("Get nonexistent value returns false", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		sectorNo := abi.SectorNumber(1)
		assert.False(t, harness.hasSectorNo(sectorNo))
	})

	t.Run("Iterate and Delete multiple sector", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		// set of sectors, the larger numbers here are not significant
		sectorNos := []uint64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}

		// put all the sectors in the store
		for _, s := range sectorNos {
			i := int64(0)
			harness.putSector(newSectorOnChainInfo(abi.SectorNumber(s), tutils.MakeCID(fmt.Sprintf("%d", i), &miner.SealedCIDPrefix), big.NewInt(i), abi.ChainEpoch(i)))
			i++
		}

		sectorNoIdx := 0
		err := harness.s.ForEachSector(harness.store, func(si *miner.SectorOnChainInfo) {
			require.Equal(t, abi.SectorNumber(sectorNos[sectorNoIdx]), si.SectorNumber)
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

// TODO minerstate: move to partition
//func TestRecoveriesBitfield(t *testing.T) {
//	t.Run("Add new recoveries happy path", func(t *testing.T) {
//		harness := constructStateHarness(t, abi.ChainEpoch(0))
//
//		// set of sectors, the larger numbers here are not significant
//		sectorNos := []uint64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
//		harness.addRecoveries(sectorNos...)
//		assert.Equal(t, uint64(len(sectorNos)), harness.getRecoveriesCount())
//	})
//
//	t.Run("Add new recoveries excludes duplicates", func(t *testing.T) {
//		harness := constructStateHarness(t, abi.ChainEpoch(0))
//
//		sectorNos := []uint64{1, 1, 2, 2, 3, 4, 5}
//		harness.addRecoveries(sectorNos...)
//		assert.Equal(t, uint64(5), harness.getRecoveriesCount())
//	})
//
//	t.Run("Remove recoveries happy path", func(t *testing.T) {
//		harness := constructStateHarness(t, abi.ChainEpoch(0))
//
//		sectorNos := []uint64{1, 2, 3, 4, 5}
//		harness.addRecoveries(sectorNos...)
//		assert.Equal(t, uint64(len(sectorNos)), harness.getRecoveriesCount())
//
//		harness.removeRecoveries(1, 3, 5)
//		assert.Equal(t, uint64(2), harness.getRecoveriesCount())
//
//		recoveries, err := harness.s.Recoveries.All(uint64(len(sectorNos)))
//		assert.NoError(t, err)
//		assert.Equal(t, []uint64{2, 4}, recoveries)
//	})
//}

//func TestPostSubmissionsBitfield(t *testing.T) {
//	t.Run("Add new submission happy path", func(t *testing.T) {
//		harness := constructStateHarness(t, abi.ChainEpoch(0))
//
//		// set of sectors, the larger numbers here are not significant
//		partitionNos := []uint64{10, 20, 30, 40}
//		harness.addPoStSubmissions(partitionNos...)
//		assert.Equal(t, uint64(len(partitionNos)), harness.getPoStSubmissionsCount())
//	})
//
//	t.Run("Add new submission excludes duplicates", func(t *testing.T) {
//		harness := constructStateHarness(t, abi.ChainEpoch(0))
//
//		sectorNos := []uint64{1, 1, 2, 2, 3, 4, 5}
//		harness.addPoStSubmissions(sectorNos...)
//		assert.Equal(t, uint64(5), harness.getPoStSubmissionsCount())
//	})
//
//	t.Run("Clear submission happy path", func(t *testing.T) {
//		harness := constructStateHarness(t, abi.ChainEpoch(0))
//
//		sectorNos := []uint64{1, 2, 3, 4, 5}
//		harness.addPoStSubmissions(sectorNos...)
//		assert.Equal(t, uint64(len(sectorNos)), harness.getPoStSubmissionsCount())
//
//		harness.clearPoStSubmissions()
//		assert.Equal(t, uint64(0), harness.getPoStSubmissionsCount())
//	})
//}

func TestVesting_AddLockedFunds_Table(t *testing.T) {
	vestStartDelay := abi.ChainEpoch(10)
	vestSum := int64(100)

	testcase := []struct {
		desc        string
		vspec       *miner.VestSpec
		periodStart abi.ChainEpoch
		vepocs      []int64
	}{
		{
			desc: "vest funds in a single epoch",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   1,
				StepDuration: 1,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 100, 0},
		},
		{
			desc: "vest funds with period=2",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   2,
				StepDuration: 1,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 50, 50, 0},
		},
		{
			desc: "vest funds with period=2 quantization=2",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   2,
				StepDuration: 1,
				Quantization: 2,
			},
			vepocs: []int64{0, 0, 0, 100, 0},
		},
		{desc: "vest funds with period=3",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   3,
				StepDuration: 1,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 33, 33, 34, 0},
		},
		{
			desc: "vest funds with period=3 quantization=2",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   3,
				StepDuration: 1,
				Quantization: 2,
			},
			vepocs: []int64{0, 0, 0, 66, 0, 34, 0},
		},
		{desc: "vest funds with period=2 step=2",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   2,
				StepDuration: 2,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 0, 100, 0},
		},
		{
			desc: "vest funds with period=5 step=2",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   5,
				StepDuration: 2,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 0, 40, 0, 40, 0, 20, 0},
		},
		{
			desc: "vest funds with delay=1 period=5 step=2",
			vspec: &miner.VestSpec{
				InitialDelay: 1,
				VestPeriod:   5,
				StepDuration: 2,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 0, 0, 40, 0, 40, 0, 20, 0},
		},
		{
			desc: "vest funds with period=5 step=2 quantization=2",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   5,
				StepDuration: 2,
				Quantization: 2,
			},
			vepocs: []int64{0, 0, 0, 40, 0, 40, 0, 20, 0},
		},
		{
			desc: "vest funds with period=5 step=3 quantization=1",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   5,
				StepDuration: 3,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 0, 0, 60, 0, 0, 40, 0},
		},
		{
			desc: "vest funds with period=5 step=3 quantization=2",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   5,
				StepDuration: 3,
				Quantization: 2,
			},
			vepocs: []int64{0, 0, 0, 0, 0, 80, 0, 20, 0},
		},
		{
			desc: "(step greater than period) vest funds with period=5 step=6 quantization=1",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   5,
				StepDuration: 6,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 0, 0, 0, 0, 0, 100, 0},
		},
		{
			desc: "vest funds with delay=5 period=5 step=1 quantization=1",
			vspec: &miner.VestSpec{
				InitialDelay: 5,
				VestPeriod:   5,
				StepDuration: 1,
				Quantization: 1,
			},
			vepocs: []int64{0, 0, 0, 0, 0, 0, 0, 20, 20, 20, 20, 20, 0},
		},
		{
			desc: "vest funds with offset 0",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   10,
				StepDuration: 2,
				Quantization: 2,
			},
			vepocs: []int64{0, 0, 0, 20, 0, 20, 0, 20, 0, 20, 0, 20},
		},
		{
			desc: "vest funds with offset 1",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   10,
				StepDuration: 2,
				Quantization: 2,
			},
			periodStart: abi.ChainEpoch(1),
			// start epoch is at 11 instead of 10 so vepocs are shifted by one from above case
			vepocs: []int64{0, 0, 0, 20, 0, 20, 0, 20, 0, 20, 0, 20},
		},
		{
			desc: "vest funds with proving period start > quantization unit",
			vspec: &miner.VestSpec{
				InitialDelay: 0,
				VestPeriod:   10,
				StepDuration: 2,
				Quantization: 2,
			},
			// 55 % 2 = 1 so expect same vepocs with offset 1 as in previous case
			periodStart: abi.ChainEpoch(55),
			vepocs:      []int64{0, 0, 0, 20, 0, 20, 0, 20, 0, 20, 0, 20},
		},
	}
	for _, tc := range testcase {
		t.Run(tc.desc, func(t *testing.T) {
			harness := constructStateHarness(t, tc.periodStart)
			vestStart := tc.periodStart + vestStartDelay

			harness.addLockedFunds(vestStart, abi.NewTokenAmount(vestSum), tc.vspec)
			assert.Equal(t, abi.NewTokenAmount(vestSum), harness.s.LockedFunds)

			var totalVested int64
			for e, v := range tc.vepocs {
				assert.Equal(t, abi.NewTokenAmount(v), harness.unlockVestedFunds(vestStart+abi.ChainEpoch(e)))
				totalVested += v
				assert.Equal(t, vestSum-totalVested, harness.s.LockedFunds.Int64())
			}

			assert.Equal(t, abi.NewTokenAmount(vestSum), abi.NewTokenAmount(totalVested))
			assert.True(t, harness.vestingFundsStoreEmpty())
			assert.Zero(t, harness.s.LockedFunds.Int64())
		})
	}
}

func TestVestingFunds_AddLockedFunds(t *testing.T) {
	t.Run("LockedFunds increases with sequential calls", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 0,
			VestPeriod:   1,
			StepDuration: 1,
			Quantization: 1,
		}

		vestStart := abi.ChainEpoch(10)
		vestSum := abi.NewTokenAmount(100)

		harness.addLockedFunds(vestStart, vestSum, vspec)
		assert.Equal(t, vestSum, harness.s.LockedFunds)

		harness.addLockedFunds(vestStart, vestSum, vspec)
		assert.Equal(t, big.Mul(vestSum, big.NewInt(2)), harness.s.LockedFunds)
	})

	t.Run("Vests when quantize, step duration, and vesting period are coprime", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 0,
			VestPeriod:   27,
			StepDuration: 5,
			Quantization: 7,
		}
		vestStart := abi.ChainEpoch(10)
		vestSum := abi.NewTokenAmount(100)
		harness.addLockedFunds(vestStart, vestSum, vspec)
		assert.Equal(t, vestSum, harness.s.LockedFunds)

		totalVested := abi.NewTokenAmount(0)
		for e := vestStart; e <= 43; e++ {
			amountVested := harness.unlockVestedFunds(e)
			switch e {
			case 22:
				assert.Equal(t, abi.NewTokenAmount(40), amountVested)
				totalVested = big.Add(totalVested, amountVested)
			case 29:
				assert.Equal(t, abi.NewTokenAmount(26), amountVested)
				totalVested = big.Add(totalVested, amountVested)
			case 36:
				assert.Equal(t, abi.NewTokenAmount(26), amountVested)
				totalVested = big.Add(totalVested, amountVested)
			case 43:
				assert.Equal(t, abi.NewTokenAmount(8), amountVested)
				totalVested = big.Add(totalVested, amountVested)
			default:
				assert.Equal(t, abi.NewTokenAmount(0), amountVested)
			}
		}
		assert.Equal(t, vestSum, totalVested)
		assert.Zero(t, harness.s.LockedFunds.Int64())
		assert.True(t, harness.vestingFundsStoreEmpty())
	})
}

func TestVestingFunds_UnvestedFunds(t *testing.T) {
	t.Run("Unlock unvested funds leaving bucket with non-zero tokens", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 0,
			VestPeriod:   5,
			StepDuration: 1,
			Quantization: 1,
		}
		vestStart := abi.ChainEpoch(100)
		vestSum := abi.NewTokenAmount(100)

		harness.addLockedFunds(vestStart, vestSum, vspec)

		amountUnlocked := harness.unlockUnvestedFunds(vestStart, big.NewInt(39))
		assert.Equal(t, big.NewInt(39), amountUnlocked)

		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart))
		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart+1))

		// expected to be zero due to unlocking of UNvested funds
		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart+2))
		// expected to be non-zero due to unlocking of UNvested funds
		assert.Equal(t, abi.NewTokenAmount(1), harness.unlockVestedFunds(vestStart+3))

		assert.Equal(t, abi.NewTokenAmount(20), harness.unlockVestedFunds(vestStart+4))
		assert.Equal(t, abi.NewTokenAmount(20), harness.unlockVestedFunds(vestStart+5))
		assert.Equal(t, abi.NewTokenAmount(20), harness.unlockVestedFunds(vestStart+6))

		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart+7))

		assert.Zero(t, harness.s.LockedFunds.Int64())
		assert.True(t, harness.vestingFundsStoreEmpty())
	})

	t.Run("Unlock unvested funds leaving bucket with zero tokens", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 0,
			VestPeriod:   5,
			StepDuration: 1,
			Quantization: 1,
		}
		vestStart := abi.ChainEpoch(100)
		vestSum := abi.NewTokenAmount(100)

		harness.addLockedFunds(vestStart, vestSum, vspec)

		amountUnlocked := harness.unlockUnvestedFunds(vestStart, big.NewInt(40))
		assert.Equal(t, big.NewInt(40), amountUnlocked)

		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart))
		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart+1))

		// expected to be zero due to unlocking of UNvested funds
		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart+2))
		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart+3))

		assert.Equal(t, abi.NewTokenAmount(20), harness.unlockVestedFunds(vestStart+4))
		assert.Equal(t, abi.NewTokenAmount(20), harness.unlockVestedFunds(vestStart+5))
		assert.Equal(t, abi.NewTokenAmount(20), harness.unlockVestedFunds(vestStart+6))

		assert.Equal(t, abi.NewTokenAmount(0), harness.unlockVestedFunds(vestStart+7))

		assert.Zero(t, harness.s.LockedFunds.Int64())
		assert.True(t, harness.vestingFundsStoreEmpty())
	})

	t.Run("Unlock all unvested funds", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 0,
			VestPeriod:   5,
			StepDuration: 1,
			Quantization: 1,
		}
		vestStart := abi.ChainEpoch(10)
		vestSum := abi.NewTokenAmount(100)
		harness.addLockedFunds(vestStart, vestSum, vspec)
		unvestedFunds := harness.unlockUnvestedFunds(vestStart, vestSum)
		assert.Equal(t, vestSum, unvestedFunds)

		assert.Zero(t, harness.s.LockedFunds.Int64())
		assert.True(t, harness.vestingFundsStoreEmpty())
	})

	t.Run("Unlock unvested funds value greater than LockedFunds", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 0,
			VestPeriod:   1,
			StepDuration: 1,
			Quantization: 1,
		}
		vestStart := abi.ChainEpoch(10)
		vestSum := abi.NewTokenAmount(100)
		harness.addLockedFunds(vestStart, vestSum, vspec)
		unvestedFunds := harness.unlockUnvestedFunds(vestStart, abi.NewTokenAmount(200))
		assert.Equal(t, vestSum, unvestedFunds)

		assert.Zero(t, harness.s.LockedFunds.Int64())
		assert.True(t, harness.vestingFundsStoreEmpty())

	})

}

type stateHarness struct {
	t testing.TB

	s     *miner.State
	store adt.Store
}

//
// Vesting Store
//

func (h *stateHarness) addLockedFunds(epoch abi.ChainEpoch, sum abi.TokenAmount, spec *miner.VestSpec) {
	err := h.s.AddLockedFunds(h.store, epoch, sum, spec)
	require.NoError(h.t, err)
}

func (h *stateHarness) unlockUnvestedFunds(epoch abi.ChainEpoch, target abi.TokenAmount) abi.TokenAmount {
	amount, err := h.s.UnlockUnvestedFunds(h.store, epoch, target)
	require.NoError(h.t, err)
	return amount
}

func (h *stateHarness) unlockVestedFunds(epoch abi.ChainEpoch) abi.TokenAmount {
	amount, err := h.s.UnlockVestedFunds(h.store, epoch)
	require.NoError(h.t, err)
	return amount
}

func (h *stateHarness) vestingFundsStoreEmpty() bool {
	vestingFunds, err := adt.AsArray(h.store, h.s.VestingFunds)
	require.NoError(h.t, err)
	empty := true
	lockedEntry := abi.NewTokenAmount(0)
	err = vestingFunds.ForEach(&lockedEntry, func(k int64) error {
		empty = false
		return nil
	})
	require.NoError(h.t, err)
	return empty
}

//
// Sector Store Assertion Operations
//

func (h *stateHarness) hasSectorNo(sectorNo abi.SectorNumber) bool {
	found, err := h.s.HasSectorNo(h.store, sectorNo)
	require.NoError(h.t, err)
	return found
}

func (h *stateHarness) putSector(sector *miner.SectorOnChainInfo) {
	err := h.s.PutSectors(h.store, sector)
	require.NoError(h.t, err)
}

func (h *stateHarness) getSector(sectorNo abi.SectorNumber) *miner.SectorOnChainInfo {
	sectors, found, err := h.s.GetSector(h.store, sectorNo)
	require.NoError(h.t, err)
	assert.True(h.t, found)
	assert.NotNil(h.t, sectors)
	return sectors
}

// makes a bit field from the passed sector numbers
func (h *stateHarness) deleteSectors(sectorNos ...uint64) {
	bf := bitfield.NewFromSet(sectorNos)
	err := h.s.DeleteSectors(h.store, bf)
	require.NoError(h.t, err)
}

//
// Precommit Store Operations
//

func (h *stateHarness) putPreCommit(info *miner.SectorPreCommitOnChainInfo) {
	err := h.s.PutPrecommittedSector(h.store, info)
	require.NoError(h.t, err)
}

func (h *stateHarness) getPreCommit(sectorNo abi.SectorNumber) *miner.SectorPreCommitOnChainInfo {
	out, found, err := h.s.GetPrecommittedSector(h.store, sectorNo)
	require.NoError(h.t, err)
	assert.True(h.t, found)
	return out
}

func (h *stateHarness) hasPreCommit(sectorNo abi.SectorNumber) bool {
	_, found, err := h.s.GetPrecommittedSector(h.store, sectorNo)
	require.NoError(h.t, err)
	return found
}

func (h *stateHarness) deletePreCommit(sectorNo abi.SectorNumber) {
	err := h.s.DeletePrecommittedSectors(h.store, sectorNo)
	require.NoError(h.t, err)
}

func constructStateHarness(t *testing.T, periodBoundary abi.ChainEpoch) *stateHarness {
	// store init
	store := ipld.NewADTStore(context.Background())
	emptyMap, err := adt.MakeEmptyMap(store).Root()
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
	owner := tutils.NewBLSAddr(t, 1)
	worker := tutils.NewBLSAddr(t, 2)

	testSealProofType := abi.RegisteredSealProof_StackedDrg2KiBV1

	sectorSize, err := testSealProofType.SectorSize()
	require.NoError(t, err)

	partitionSectors, err := testSealProofType.WindowPoStPartitionSectors()
	require.NoError(t, err)

	info := miner.MinerInfo{
		Owner:                      owner,
		Worker:                     worker,
		PendingWorkerKey:           nil,
		PeerId:                     abi.PeerID("peer"),
		Multiaddrs:                 testMultiaddrs,
		SealProofType:              testSealProofType,
		SectorSize:                 sectorSize,
		WindowPoStPartitionSectors: partitionSectors,
	}
	infoCid, err := store.Put(context.Background(), &info)
	require.NoError(t, err)

	state, err := miner.ConstructState(infoCid, periodBoundary, emptyArray, emptyMap, emptyDeadlinesCid)
	require.NoError(t, err)

	return &stateHarness{
		t: t,

		s:     state,
		store: store,
	}
}

//
// Type Construction Methods
//

// returns a unique SectorPreCommitOnChainInfo with each invocation with SectorNumber set to `sectorNo`.
func newSectorPreCommitOnChainInfo(sectorNo abi.SectorNumber, sealed cid.Cid, deposit abi.TokenAmount, epoch abi.ChainEpoch) *miner.SectorPreCommitOnChainInfo {
	info := newSectorPreCommitInfo(sectorNo, sealed)
	return &miner.SectorPreCommitOnChainInfo{
		Info:               *info,
		PreCommitDeposit:   deposit,
		PreCommitEpoch:     epoch,
		DealWeight:         big.Zero(),
		VerifiedDealWeight: big.Zero(),
	}
}

const (
	sectorSealRandEpochValue = abi.ChainEpoch(1)
	sectorExpiration         = abi.ChainEpoch(1)
)

// returns a unique SectorOnChainInfo with each invocation with SectorNumber set to `sectorNo`.
func newSectorOnChainInfo(sectorNo abi.SectorNumber, sealed cid.Cid, weight big.Int, activation abi.ChainEpoch) *miner.SectorOnChainInfo {
	return &miner.SectorOnChainInfo{
		SectorNumber:       sectorNo,
		SealProof:          abi.RegisteredSealProof_StackedDrg32GiBV1,
		SealedCID:          sealed,
		DealIDs:            nil,
		Activation:         activation,
		Expiration:         sectorExpiration,
		DealWeight:         weight,
		VerifiedDealWeight: weight,
		InitialPledge:      abi.NewTokenAmount(0),
		ExpectedDayReward:  abi.NewTokenAmount(0),
	}
}

// returns a unique SectorPreCommitInfo with each invocation with SectorNumber set to `sectorNo`.
func newSectorPreCommitInfo(sectorNo abi.SectorNumber, sealed cid.Cid) *miner.SectorPreCommitInfo {
	return &miner.SectorPreCommitInfo{
		SealProof:     abi.RegisteredSealProof_StackedDrg32GiBV1,
		SectorNumber:  sectorNo,
		SealedCID:     sealed,
		SealRandEpoch: sectorSealRandEpochValue,
		DealIDs:       nil,
		Expiration:    sectorExpiration,
	}
}
