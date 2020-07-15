package miner

import (
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/abi/big"
)

func TestExpirationSet(t *testing.T) {
	onTimeSectors := bitfield.NewFromSet([]uint64{5, 8, 9})
	earlySectors := bitfield.NewFromSet([]uint64{2, 3})
	onTimePledge := abi.NewTokenAmount(1000)
	activePower := NewPowerPair(abi.NewStoragePower(1<<13), abi.NewStoragePower(1<<14))
	faultyPower := NewPowerPair(abi.NewStoragePower(1<<11), abi.NewStoragePower(1<<12))

	t.Run("adds sectors and power to empty set", func(t *testing.T) {
		set := NewExpirationSetEmpty()

		err := set.Add(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 5, 8, 9)
		assertBitfieldEquals(t, set.EarlySectors, 2, 3)
		assert.Equal(t, onTimePledge, set.OnTimePledge)
		assert.True(t, activePower.Equals(set.ActivePower))
		assert.True(t, faultyPower.Equals(set.FaultyPower))
	})

	t.Run("adds sectors and power to non-empty set", func(t *testing.T) {
		set := NewExpirationSet(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)

		err := set.Add(
			bitfield.NewFromSet([]uint64{6, 7, 11}),
			bitfield.NewFromSet([]uint64{1, 4}),
			abi.NewTokenAmount(300),
			NewPowerPair(abi.NewStoragePower(3*(1<<13)), abi.NewStoragePower(3*(1<<14))),
			NewPowerPair(abi.NewStoragePower(3*(1<<11)), abi.NewStoragePower(3*(1<<12))),
		)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 5, 6, 7, 8, 9, 11)
		assertBitfieldEquals(t, set.EarlySectors, 1, 2, 3, 4)
		assert.Equal(t, abi.NewTokenAmount(1300), set.OnTimePledge)
		active := NewPowerPair(abi.NewStoragePower(1<<15), abi.NewStoragePower(1<<16))
		assert.True(t, active.Equals(set.ActivePower))
		faulty := NewPowerPair(abi.NewStoragePower(1<<13), abi.NewStoragePower(1<<14))
		assert.True(t, faulty.Equals(set.FaultyPower))
	})

	t.Run("removes sectors and power set", func(t *testing.T) {
		set := NewExpirationSet(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)

		err := set.Remove(
			bitfield.NewFromSet([]uint64{9}),
			bitfield.NewFromSet([]uint64{2}),
			abi.NewTokenAmount(800),
			NewPowerPair(abi.NewStoragePower(3*(1<<11)), abi.NewStoragePower(3*(1<<12))),
			NewPowerPair(abi.NewStoragePower(3*(1<<9)), abi.NewStoragePower(3*(1<<10))),
		)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 5, 8)
		assertBitfieldEquals(t, set.EarlySectors, 3)
		assert.Equal(t, abi.NewTokenAmount(200), set.OnTimePledge)
		active := NewPowerPair(abi.NewStoragePower(1<<11), abi.NewStoragePower(1<<12))
		assert.True(t, active.Equals(set.ActivePower))
		faulty := NewPowerPair(abi.NewStoragePower(1<<9), abi.NewStoragePower(1<<10))
		assert.True(t, faulty.Equals(set.FaultyPower))
	})

	t.Run("remove fails when pledge underflows", func(t *testing.T) {
		set := NewExpirationSet(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)

		err := set.Remove(
			bitfield.NewFromSet([]uint64{9}),
			bitfield.NewFromSet([]uint64{2}),
			abi.NewTokenAmount(1200),
			NewPowerPair(abi.NewStoragePower(3*(1<<11)), abi.NewStoragePower(3*(1<<12))),
			NewPowerPair(abi.NewStoragePower(3*(1<<9)), abi.NewStoragePower(3*(1<<10))),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pledge underflow")
	})

	t.Run("remove fails to remove sectors it does not contain", func(t *testing.T) {
		set := NewExpirationSet(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)

		// remove unknown active sector 12
		err := set.Remove(
			bitfield.NewFromSet([]uint64{12}),
			bitfield.NewFromSet([]uint64{}),
			abi.NewTokenAmount(0),
			NewPowerPair(abi.NewStoragePower(3*(1<<11)), abi.NewStoragePower(3*(1<<12))),
			NewPowerPair(abi.NewStoragePower(3*(1<<9)), abi.NewStoragePower(3*(1<<10))),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not contained")

		// remove faulty sector 8, that is active in the set
		err = set.Remove(
			bitfield.NewFromSet([]uint64{0}),
			bitfield.NewFromSet([]uint64{8}),
			abi.NewTokenAmount(0),
			NewPowerPair(abi.NewStoragePower(3*(1<<11)), abi.NewStoragePower(3*(1<<12))),
			NewPowerPair(abi.NewStoragePower(3*(1<<9)), abi.NewStoragePower(3*(1<<10))),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not contained")
	})

	t.Run("remove fails when active or fault qa power underflows", func(t *testing.T) {
		set := NewExpirationSet(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)

		// active removed power > active power
		err := set.Remove(
			bitfield.NewFromSet([]uint64{9}),
			bitfield.NewFromSet([]uint64{2}),
			abi.NewTokenAmount(200),
			NewPowerPair(abi.NewStoragePower(3*(1<<12)), abi.NewStoragePower(3*(1<<13))),
			NewPowerPair(abi.NewStoragePower(3*(1<<9)), abi.NewStoragePower(3*(1<<10))),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "power underflow")

		set = NewExpirationSet(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)

		// faulty removed power > faulty power
		err = set.Remove(
			bitfield.NewFromSet([]uint64{9}),
			bitfield.NewFromSet([]uint64{2}),
			abi.NewTokenAmount(200),
			NewPowerPair(abi.NewStoragePower(3*(1<<11)), abi.NewStoragePower(3*(1<<12))),
			NewPowerPair(abi.NewStoragePower(3*(1<<10)), abi.NewStoragePower(3*(1<<11))),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "power underflow")
	})

	t.Run("set is empty when all sectors removed", func(t *testing.T) {
		set := NewExpirationSetEmpty()

		empty, err := set.IsEmpty()
		require.NoError(t, err)
		assert.True(t, empty)

		err = set.Add(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)
		require.NoError(t, err)

		empty, err = set.IsEmpty()
		require.NoError(t, err)
		assert.False(t, empty)

		err = set.Remove(onTimeSectors, earlySectors, onTimePledge, activePower, faultyPower)
		require.NoError(t, err)

		empty, err = set.IsEmpty()
		require.NoError(t, err)
		assert.True(t, empty)
	})
}

func TestExpirations(t *testing.T) {
	quant := QuantSpec{unit: 10, offset: 3}
	sectors := []*SectorOnChainInfo{{
		Expiration:         7, // 7 -> 13
		SectorNumber:       1,
		DealWeight:         big.Zero(),
		VerifiedDealWeight: big.Zero(),
		InitialPledge:      big.Zero(),
	}, {
		Expiration:         8, // 8 -> 13
		SectorNumber:       2,
		DealWeight:         big.Zero(),
		VerifiedDealWeight: big.Zero(),
		InitialPledge:      big.Zero(),
	}, {
		Expiration:         14, // 14 -> 23
		SectorNumber:       3,
		DealWeight:         big.Zero(),
		VerifiedDealWeight: big.Zero(),
		InitialPledge:      big.Zero(),
	}, {
		Expiration:         13, // 13 -> 13
		SectorNumber:       4,
		DealWeight:         big.Zero(),
		VerifiedDealWeight: big.Zero(),
		InitialPledge:      big.Zero(),
	}}
	result := groupSectorsByExpiration(2048, sectors, quant)
	expected := []*sectorEpochSet{{
		epoch:   13,
		sectors: []uint64{1, 2, 4},
		power:   NewPowerPair(big.NewIntUnsigned(2048*3), big.NewIntUnsigned(2048*3)),
		pledge:  big.Zero(),
	}, {
		epoch:   23,
		sectors: []uint64{3},
		power:   NewPowerPair(big.NewIntUnsigned(2048), big.NewIntUnsigned(2048)),
		pledge:  big.Zero(),
	}}
	require.Equal(t, len(expected), len(result))
	for i, ex := range expected {
		assertSectorSet(t, ex, &result[i])
	}
}

func TestExpirationsEmpty(t *testing.T) {
	sectors := []*SectorOnChainInfo{}
	result := groupSectorsByExpiration(2048, sectors, NoQuantization)
	expected := []sectorEpochSet{}
	require.Equal(t, expected, result)
}

func assertSectorSet(t *testing.T, expected, actual *sectorEpochSet) {
	assert.Equal(t, expected.epoch, actual.epoch)
	assert.Equal(t, expected.sectors, actual.sectors)
	assert.True(t, expected.power.Equals(actual.power), "expected %v, actual %v", expected.power, actual.power)
	assert.True(t, expected.pledge.Equals(actual.pledge), "expected %v, actual %v", expected.pledge, actual.pledge)
}
