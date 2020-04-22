package miner_test

import (
	"context"
	"fmt"
	"strings"
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

		pc1 := newSectorPreCommitOnChainInfo(sectorNo, tutils.MakeCID("1"), abi.NewTokenAmount(1), abi.ChainEpoch(1))
		harness.putPreCommit(pc1)
		assert.Equal(t, pc1, harness.getPreCommit(sectorNo))

		pc2 := newSectorPreCommitOnChainInfo(sectorNo, tutils.MakeCID("2"), abi.NewTokenAmount(1), abi.ChainEpoch(1))
		harness.putPreCommit(pc2)
		assert.Equal(t, pc2, harness.getPreCommit(sectorNo))

		harness.deletePreCommit(sectorNo)
		assert.False(t, harness.hasPreCommit(sectorNo))
	})

	t.Run("Delete nonexistent value returns an error", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		sectorNo := abi.SectorNumber(1)
		err := harness.s.DeletePrecommittedSector(harness.store, sectorNo)
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
		sectorInfo1 := newSectorOnChainInfo(sectorNo, tutils.MakeCID("1"), big.NewInt(1), abi.ChainEpoch(1))
		sectorInfo2 := newSectorOnChainInfo(sectorNo, tutils.MakeCID("2"), big.NewInt(2), abi.ChainEpoch(2))

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
			harness.putSector(newSectorOnChainInfo(abi.SectorNumber(s), tutils.MakeCID(fmt.Sprintf("%d", i)), big.NewInt(i), abi.ChainEpoch(i)))
			i++
		}

		sectorNoIdx := 0
		err := harness.s.ForEachSector(harness.store, func(si *miner.SectorOnChainInfo) {
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
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		// set of sectors, the larger numbers here are not significant
		sectorNos := []abi.SectorNumber{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
		harness.addNewSectors(sectorNos...)
		assert.Equal(t, uint64(len(sectorNos)), harness.getNewSectorCount())
	})

	t.Run("Add new sectors excludes duplicates", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		sectorNos := []abi.SectorNumber{1, 1, 2, 2, 3, 4, 5}
		harness.addNewSectors(sectorNos...)
		assert.Equal(t, uint64(5), harness.getNewSectorCount())
	})

	t.Run("Remove sectors happy path", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

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
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		tooManySectors := make([]abi.SectorNumber, miner.NewSectorsPerPeriodMax+1)
		for i := abi.SectorNumber(0); i < miner.NewSectorsPerPeriodMax+1; i++ {
			tooManySectors[i] = i
		}

		err := harness.s.AddNewSectors(tooManySectors...)
		assert.Error(t, err)

		// sanity check nothing was added
		// For omission reason see: https://github.com/filecoin-project/specs-actors/issues/300
		//assert.Equal(t, uint64(0), actorHarness.getNewSectorCount())
	})
}

func TestSectorExpirationStore(t *testing.T) {
	exp1 := abi.ChainEpoch(10)
	exp2 := abi.ChainEpoch(20)

	sectorExpirations := make(map[abi.ChainEpoch][]uint64)
	sectorExpirations[exp1] = []uint64{1, 2, 3, 4, 5}
	sectorExpirations[exp2] = []uint64{6, 7, 8, 9, 10}

	t.Run("Round trip add get sector expirations", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		harness.addSectorExpiration(exp1, sectorExpirations[exp1]...)
		harness.addSectorExpiration(exp2, sectorExpirations[exp2]...)

		assert.Equal(t, sectorExpirations[exp1], harness.getSectorExpirations(exp1))
		assert.Equal(t, sectorExpirations[exp2], harness.getSectorExpirations(exp2))

		// return nothing if there are no sectors at the epoch
		assert.Empty(t, harness.getSectorExpirations(abi.ChainEpoch(0)))

		// remove the first sector from expiration set 1
		harness.removeSectorExpiration(exp1, sectorExpirations[exp1][0])
		assert.Equal(t, sectorExpirations[exp1][1:], harness.getSectorExpirations(exp1))
		assert.Equal(t, sectorExpirations[exp2], harness.getSectorExpirations(exp2)) // No change

		// remove all sectors from expiration set 2
		harness.removeSectorExpiration(exp2, sectorExpirations[exp2]...)
		assert.Empty(t, harness.getSectorExpirations(exp2))
	})

	t.Run("Iteration by expiration", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		harness.addSectorExpiration(exp1, sectorExpirations[exp1]...)
		harness.addSectorExpiration(exp2, sectorExpirations[exp2]...)

		exp1Hit, exp2Hit := false, false
		err := harness.s.ForEachSectorExpiration(harness.store, func(expiry abi.ChainEpoch, sectors *abi.BitField) error {
			if expiry == exp1 {
				sectorSlice, err := sectors.All(miner.SectorsMax)
				assert.NoError(t, err)
				assert.Equal(t, sectorExpirations[expiry], sectorSlice)
				exp1Hit = true
			} else if expiry == exp2 {
				sectorSlice, err := sectors.All(miner.SectorsMax)
				assert.NoError(t, err)
				assert.Equal(t, sectorExpirations[expiry], sectorSlice)
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

	t.Run("Adding sectors at expiry merges with existing", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		mergedSectors := []uint64{21, 22, 23, 24, 25}
		harness.addSectorExpiration(exp1, sectorExpirations[exp1]...)
		harness.addSectorExpiration(exp1, mergedSectors...)

		merged := harness.getSectorExpirations(exp1)
		assert.Equal(t, append(sectorExpirations[exp1], mergedSectors...), merged)
	})

	t.Run("clear sectors by expirations", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

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

	t.Run("Round trip add remove", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		harness.addFaults(fault1, faultSet1...)
		harness.addFaults(fault2, faultSet2...)

		fault1Hit, fault2Hit := false, false
		err := harness.s.ForEachFaultEpoch(harness.store, func(epoch abi.ChainEpoch, faults *abi.BitField) error {
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
		err = harness.s.ForEachFaultEpoch(harness.store, func(epoch abi.ChainEpoch, faults *abi.BitField) error {
			if epoch == fault1 {
				sectors, err := faults.All(uint64(len(faultSet1)))
				require.NoError(t, err)
				assert.Equal(t, faultSet1[:1], sectors, "expected: %v, actual: %v", faultSet1[:1], sectors)
				fault1Hit = true
			} else if epoch == fault2 {
				sectors, err := faults.All(uint64(len(faultSet2)))
				require.NoError(t, err)
				assert.Equal(t, faultSet2[:2], sectors, "expected: %v, actual: %v", faultSet2[:2], sectors)
				fault2Hit = true

			} else {
				t.Fatalf("unexpected fault epoch: %v", epoch)
			}
			return nil
		})
		require.NoError(t, err)
		assert.True(t, fault1Hit)
		assert.True(t, fault2Hit)
	})

	t.Run("Clear all", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		harness.addFaults(fault1, faultSet1...)
		harness.addFaults(fault2, faultSet2...)

		// now clear all the faults
		err := harness.s.ClearFaultEpochs(harness.store, fault1, fault2)
		require.NoError(t, err)

		err = harness.s.ForEachFaultEpoch(harness.store, func(epoch abi.ChainEpoch, faults *abi.BitField) error {
			t.Fatalf("unexpected fault epoch: %v", epoch)
			return nil
		})
		require.NoError(t, err)
	})
}

func TestRecoveriesBitfield(t *testing.T) {
	t.Run("Add new recoveries happy path", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		// set of sectors, the larger numbers here are not significant
		sectorNos := []uint64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
		harness.addRecoveries(sectorNos...)
		assert.Equal(t, uint64(len(sectorNos)), harness.getRecoveriesCount())
	})

	t.Run("Add new recoveries excludes duplicates", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		sectorNos := []uint64{1, 1, 2, 2, 3, 4, 5}
		harness.addRecoveries(sectorNos...)
		assert.Equal(t, uint64(5), harness.getRecoveriesCount())
	})

	t.Run("Remove recoveries happy path", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

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
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		// set of sectors, the larger numbers here are not significant
		partitionNos := []uint64{10, 20, 30, 40}
		harness.addPoStSubmissions(partitionNos...)
		assert.Equal(t, uint64(len(partitionNos)), harness.getPoStSubmissionsCount())
	})

	t.Run("Add new submission excludes duplicates", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		sectorNos := []uint64{1, 1, 2, 2, 3, 4, 5}
		harness.addPoStSubmissions(sectorNos...)
		assert.Equal(t, uint64(5), harness.getPoStSubmissionsCount())
	})

	t.Run("Clear submission happy path", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))

		sectorNos := []uint64{1, 2, 3, 4, 5}
		harness.addPoStSubmissions(sectorNos...)
		assert.Equal(t, uint64(len(sectorNos)), harness.getPoStSubmissionsCount())

		harness.clearPoStSubmissions()
		assert.Equal(t, uint64(0), harness.getPoStSubmissionsCount())
	})
}

func TestVestingFunds_AddLockedFunds(t *testing.T) {
	// TODO(frrist): merge these top-level methods into fewer
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

}

func TestVestingFundsStore(t *testing.T) {
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
	unlocked := harness.unlockUnvestedFunds(vestStart, abi.NewTokenAmount(49))
	assert.Equal(t, abi.NewTokenAmount(49), unlocked)

	vested := harness.unlockVestedFunds(vestStart + 2)
	assert.Equal(t, abi.NewTokenAmount(51), vested)
}

// TODO make top level tests for vested, unvested and then together.

func TestVestingFunds(t *testing.T) {
	t.Run("Vest all funds in single epoch", func(t *testing.T) {
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
		amountVested := harness.unlockVestedFunds(vestStart + 2)
		assert.Equal(t, vestSum, amountVested)
		assert.Zero(t, harness.s.LockedFunds.Int64())
		assert.True(t, harness.vestingFundsStoreEmpty())
		// subsequent calls to unlock return 0
		amountVested = harness.unlockVestedFunds(vestStart + 2)
		assert.Zero(t, amountVested)

	})
	t.Run("Sum of all entries in vesting store is equal to vested sum", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 0,
			VestPeriod:   10,
			StepDuration: 1,
			Quantization: 2,
		}
		vestStart := abi.ChainEpoch(10)
		vestSum := abi.NewTokenAmount(100)
		harness.addLockedFunds(vestStart, vestSum, vspec)
		// Entry: 12 Funds: 20
		// Entry: 14 Funds: 20
		// Entry: 16 Funds: 20
		// Entry: 18 Funds: 20
		// Entry: 20 Funds: 20

		totalVested := abi.NewTokenAmount(0)
		for e := vestStart; e <= vestStart+vspec.VestPeriod+1; e++ {
			amountVested := harness.unlockVestedFunds(e)
			if e == 13 || e == 15 || e == 17 || e == 19 || e == 21 {
				assert.Equal(t, abi.NewTokenAmount(20), amountVested)
				totalVested = big.Add(totalVested, amountVested)
			} else {
				assert.Equal(t, abi.NewTokenAmount(0), amountVested)
			}
		}
		assert.Equal(t, vestSum, totalVested)
		assert.Zero(t, harness.s.LockedFunds.Int64())
		assert.True(t, harness.vestingFundsStoreEmpty())
	})

	t.Run("Vesting with non-zero initial delay", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 10,
			VestPeriod:   10,
			StepDuration: 1,
			Quantization: 2,
		}
		vestStart := abi.ChainEpoch(10)
		vestSum := abi.NewTokenAmount(100)
		harness.addLockedFunds(vestStart, vestSum, vspec)
		// Entry: 22 Funds: 20
		// Entry: 24 Funds: 20
		// Entry: 26 Funds: 20
		// Entry: 28 Funds: 20
		// Entry: 30 Funds: 20

		totalVested := abi.NewTokenAmount(0)
		for e := vestStart; e <= vestStart+vspec.VestPeriod+vspec.InitialDelay+1; e++ {
			amountVested := harness.unlockVestedFunds(e)
			if e == 23 || e == 25 || e == 27 || e == 29 || e == 31 {
				assert.Equal(t, abi.NewTokenAmount(20), amountVested)
				totalVested = big.Add(totalVested, amountVested)
			} else {
				assert.Equal(t, abi.NewTokenAmount(0), amountVested)
			}
		}
		assert.Equal(t, vestSum, totalVested)
		assert.Zero(t, harness.s.LockedFunds.Int64())
		assert.True(t, harness.vestingFundsStoreEmpty())
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
		// Entry: 21 Funds: 40
		// Entry: 28 Funds: 26
		// Entry: 35 Funds: 26
		// Entry: 42 Funds: 8
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

	t.Run("Vests alternating amounts", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 0,
			VestPeriod:   100,
			StepDuration: 9,
			Quantization: 2,
		}
		vestStart := abi.ChainEpoch(100)
		vestSum := abi.NewTokenAmount(1000)
		harness.addLockedFunds(vestStart, vestSum, vspec)
		/*
			Entry: 110 Funds: 100
			Entry: 118 Funds: 80
			Entry: 128 Funds: 100
			Entry: 136 Funds: 80
			Entry: 146 Funds: 100
			Entry: 154 Funds: 80
			Entry: 164 Funds: 100
			Entry: 172 Funds: 80
			Entry: 182 Funds: 100
			Entry: 190 Funds: 80
			Entry: 200 Funds: 100
		*/
		// mapping of epochs to value vested at it, epochs are all incremented by 1 since vesting occurs after them
		vestEpochs := map[abi.ChainEpoch]int64{
			111: 100, 119: 80, 129: 100, 137: 80, 147: 100,
			155: 80, 165: 100, 173: 80, 183: 100, 191: 80, 201: 100,
		}
		totalVested := abi.NewTokenAmount(0)
		for e := vestStart; e < vestStart+vspec.VestPeriod+2; e++ {
			amountVested := harness.unlockVestedFunds(e)
			if a, vests := vestEpochs[e]; vests {
				assert.Equal(t, abi.NewTokenAmount(a), amountVested)
				totalVested = big.Add(totalVested, amountVested)
			} else {
				assert.Equal(t, abi.NewTokenAmount(0), amountVested)
			}
		}
		assert.Equal(t, vestSum, totalVested)
		assert.Zero(t, harness.s.LockedFunds.Int64())
		assert.True(t, harness.vestingFundsStoreEmpty())
	})

	t.Run("Unlock unvested funds leaving bucket with modified amount", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 0,
			VestPeriod:   100,
			StepDuration: 9,
			Quantization: 2,
		}
		vestStart := abi.ChainEpoch(100)
		vestSum := abi.NewTokenAmount(1000)

		harness.addLockedFunds(vestStart, vestSum, vspec)
		//epoch 110: locked Funds = 100
		//epoch 118: locked Funds = 80
		//epoch 128: locked Funds = 100
		//epoch 136: locked Funds = 80
		amountUnlocked := harness.unlockUnvestedFunds(vestStart, big.NewInt(179))
		assert.Equal(t, big.NewInt(179), amountUnlocked)

		//epoch 110: locked Funds = 0 (deleted)
		//epoch 118: locked Funds = 1 (modified amount)
		//epoch 128: locked Funds = 100
		//epoch 136: locked Funds = 80
		amountVested := harness.unlockVestedFunds(118 + 1)
		assert.Equal(t, big.NewInt(1), amountVested)
	})

	t.Run("Unlock unvested funds leaving bucket with zero tokens", func(t *testing.T) {
		harness := constructStateHarness(t, abi.ChainEpoch(0))
		vspec := &miner.VestSpec{
			InitialDelay: 0,
			VestPeriod:   100,
			StepDuration: 9,
			Quantization: 2,
		}
		vestStart := abi.ChainEpoch(100)
		vestSum := abi.NewTokenAmount(1000)

		harness.addLockedFunds(vestStart, vestSum, vspec)
		//epoch 110: locked Funds = 100
		//epoch 118: locked Funds = 80
		//epoch 128: locked Funds = 100
		//epoch 136: locked Funds = 80
		amountUnlocked := harness.unlockUnvestedFunds(vestStart, big.NewInt(180))
		assert.Equal(t, big.NewInt(180), amountUnlocked)

		//epoch 110: locked Funds = 0 (deleted)
		//epoch 118: locked Funds = 0 (deleted)
		//epoch 128: locked Funds = 100
		//epoch 136: locked Funds = 80
		amountVested := harness.unlockVestedFunds(118 + 1)
		assert.Equal(t, big.NewInt(0), amountVested)

	})

	t.Run("Unlock all unvested funds", func(t *testing.T) {
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
		unvestedFunds := harness.unlockUnvestedFunds(vestStart, vestSum)
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
	var lockedEntry abi.TokenAmount
	err = vestingFunds.ForEach(&lockedEntry, func(k int64) error {
		empty = false
		return nil
	})
	require.NoError(h.t, err)
	return empty
}

// utility to print the contents of the vesting store.
func (h *stateHarness) debugVestingStore() string {
	vestingFunds, err := adt.AsArray(h.store, h.s.VestingFunds)
	if err != nil {
		panic(err)
	}
	var sb strings.Builder
	var lockedEntry abi.TokenAmount
	err = vestingFunds.ForEach(&lockedEntry, func(k int64) error {
		sb.WriteString(fmt.Sprintf("Entry: %d Funds: %s\n", k, lockedEntry.String()))
		return nil
	})
	return sb.String()
}

//
// PostSubmissions Bitfield
//

func (h *stateHarness) addPoStSubmissions(partitionNos ...uint64) {
	err := h.s.AddPoStSubmissions(bitfield.NewFromSet(partitionNos))
	require.NoError(h.t, err)
}

func (h *stateHarness) clearPoStSubmissions() {
	err := h.s.ClearPoStSubmissions()
	require.NoError(h.t, err)
}

func (h *stateHarness) getPoStSubmissionsCount() uint64 {
	count, err := h.s.PostSubmissions.Count()
	require.NoError(h.t, err)
	return count
}

//
// Recoveries Bitfield
//

func (h *stateHarness) addRecoveries(sectorNos ...uint64) {
	bf := bitfield.NewFromSet(sectorNos)
	err := h.s.AddRecoveries(bf)
	require.NoError(h.t, err)
}

func (h *stateHarness) removeRecoveries(sectorNos ...uint64) {
	bf := bitfield.NewFromSet(sectorNos)
	err := h.s.RemoveRecoveries(bf)
	require.NoError(h.t, err)
}

func (h *stateHarness) getRecoveriesCount() uint64 {
	count, err := h.s.Recoveries.Count()
	require.NoError(h.t, err)
	return count
}

//
// Faults Store
//

func (h *stateHarness) addFaults(epoch abi.ChainEpoch, sectorNos ...uint64) {
	bf := bitfield.NewFromSet(sectorNos)
	err := h.s.AddFaults(h.store, bf, epoch)
	require.NoError(h.t, err)
}

func (h *stateHarness) removeFaults(sectorNos ...uint64) {
	bf := bitfield.NewFromSet(sectorNos)
	err := h.s.RemoveFaults(h.store, bf)
	require.NoError(h.t, err)
}

//
// Sector Expiration Store
//

func (h *stateHarness) getSectorExpirations(expiry abi.ChainEpoch) []uint64 {
	bf, err := h.s.GetSectorExpirations(h.store, expiry)
	require.NoError(h.t, err)
	sectors, err := bf.All(miner.SectorsMax)
	require.NoError(h.t, err)
	return sectors
}

func (h *stateHarness) addSectorExpiration(expiry abi.ChainEpoch, sectors ...uint64) {
	err := h.s.AddSectorExpirations(h.store, expiry, sectors...)
	require.NoError(h.t, err)
}

func (h *stateHarness) removeSectorExpiration(expiry abi.ChainEpoch, sectors ...uint64) {
	err := h.s.RemoveSectorExpirations(h.store, expiry, sectors...)
	require.NoError(h.t, err)
}

func (h *stateHarness) clearSectorExpiration(excitations ...abi.ChainEpoch) {
	err := h.s.ClearSectorExpirations(h.store, excitations...)
	require.NoError(h.t, err)
}

//
// NewSectors BitField Assertions
//

func (h *stateHarness) addNewSectors(sectorNos ...abi.SectorNumber) {
	err := h.s.AddNewSectors(sectorNos...)
	require.NoError(h.t, err)
}

// makes a bit field from the passed sector numbers
func (h *stateHarness) removeNewSectors(sectorNos ...uint64) {
	bf := bitfield.NewFromSet(sectorNos)
	err := h.s.RemoveNewSectors(bf)
	require.NoError(h.t, err)
}

func (h *stateHarness) getNewSectorCount() uint64 {
	out, err := h.s.NewSectors.Count()
	require.NoError(h.t, err)
	return out
}

//
// Sector Store Assertion Operations
//

func (h *stateHarness) getSectorCount() uint64 {
	out, err := h.s.GetSectorCount(h.store)
	require.NoError(h.t, err)
	return out
}

func (h *stateHarness) hasSectorNo(sectorNo abi.SectorNumber) bool {
	found, err := h.s.HasSectorNo(h.store, sectorNo)
	require.NoError(h.t, err)
	return found
}

func (h *stateHarness) putSector(sector *miner.SectorOnChainInfo) {
	err := h.s.PutSector(h.store, sector)
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
	err := h.s.DeletePrecommittedSector(h.store, sectorNo)
	require.NoError(h.t, err)
}

func constructStateHarness(t *testing.T, periodBoundary abi.ChainEpoch) *stateHarness {
	// store init
	store := ipld.NewADTStore(context.Background())
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
		Info:             *info,
		PreCommitDeposit: deposit,
		PreCommitEpoch:   epoch,
	}
}

// returns a unique SectorOnChainInfo with each invocation with SectorNumber set to `sectorNo`.
func newSectorOnChainInfo(sectorNo abi.SectorNumber, sealed cid.Cid, weight big.Int, activation abi.ChainEpoch) *miner.SectorOnChainInfo {
	info := newSectorPreCommitInfo(sectorNo, sealed)
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
func newSectorPreCommitInfo(sectorNo abi.SectorNumber, sealed cid.Cid) *miner.SectorPreCommitInfo {
	return &miner.SectorPreCommitInfo{
		RegisteredProof: abi.RegisteredProof_StackedDRG32GiBPoSt,
		SectorNumber:    sectorNo,
		SealedCID:       sealed,
		SealRandEpoch:   sectorSealRandEpochValue,
		DealIDs:         nil,
		Expiration:      sectorExpiration,
	}
}
