package miner

import (
	"context"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
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

func TestExpirationQueue(t *testing.T) {
	sectors := []*SectorOnChainInfo{
		testSector(2, 1, 50, 60, 1000),
		testSector(3, 2, 51, 61, 1001),
		testSector(7, 3, 52, 62, 1002),
		testSector(8, 4, 53, 63, 1003),
		testSector(11, 5, 54, 64, 1004),
		testSector(13, 6, 55, 65, 1005),
	}
	sectorSize := abi.SectorSize(32 * 1 << 30)

	t.Run("added sectors can be popped off queue", func(t *testing.T) {
		queue := emptyExprirationQueue(t)
		queue.AddActiveSectors(sectors, sectorSize)

		// default test quantizing of 1 means every sector is in its own expriation group
		assert.Equal(t, len(sectors), int(queue.Length()))

		_, err := queue.Root()
		require.NoError(t, err)

		// pop off sectors up to and including epoch 8
		set, err := queue.PopUntil(7)
		require.NoError(t, err)

		// only 3 sectors remain
		assert.Equal(t, 3, int(queue.Length()))

		assertBitfieldEquals(t, set.OnTimeSectors, 1, 2, 3)
		assertBitfieldEmpty(t, set.EarlySectors)

		activePower := PowerForSectors(sectorSize, sectors[:3])
		faultyPower := NewPowerPairZero()

		assert.Equal(t, big.NewInt(3003), set.OnTimePledge) // sum of first 3 sector pledges
		assert.True(t, activePower.Equals(set.ActivePower))
		assert.True(t, faultyPower.Equals(set.FaultyPower))

		// pop off rest up to and including epoch 8
		set, err = queue.PopUntil(20)
		require.NoError(t, err)

		assertBitfieldEquals(t, set.OnTimeSectors, 4, 5, 6)
		assertBitfieldEmpty(t, set.EarlySectors)

		activePower = PowerForSectors(sectorSize, sectors[3:])
		faultyPower = NewPowerPairZero()

		assert.Equal(t, big.NewInt(3012), set.OnTimePledge) // sum of last 3 sector pledges
		assert.True(t, activePower.Equals(set.ActivePower))
		assert.True(t, faultyPower.Equals(set.FaultyPower))

		// queue is now empty
		assert.Equal(t, 0, int(queue.Length()))
	})

	t.Run("quantizes added sectors by expiration", func(t *testing.T) {
		queue := emptyExpirationQueueWithQuantizing(t, QuantSpec{unit: 5, offset: 3})
		queue.AddActiveSectors(sectors, sectorSize)

		// work around caching issues in amt
		_, err := queue.Root()
		require.NoError(t, err)

		// quantizing spec means sectors should be grouped into 3 sets expiring at 3, 8 and 13
		assert.Equal(t, 3, int(queue.Length()))

		// set popped before first quantized sector should be empty
		set, err := queue.PopUntil(2)
		require.NoError(t, err)
		assertBitfieldEmpty(t, set.OnTimeSectors)
		assert.Equal(t, 3, int(queue.Length()))

		// first 2 sectors will be in first set popped off at quantization offset (3)
		set, err = queue.PopUntil(3)
		require.NoError(t, err)
		assertBitfieldEquals(t, set.OnTimeSectors, 1, 2)
		assert.Equal(t, 2, int(queue.Length()))

		_, err = queue.Root()
		require.NoError(t, err)

		// no sectors will be popped off in quantization interval
		set, err = queue.PopUntil(7)
		require.NoError(t, err)
		assertBitfieldEmpty(t, set.OnTimeSectors)
		assert.Equal(t, 2, int(queue.Length()))

		// next 2 sectors will be in first set popped off after quantization interval (8)
		set, err = queue.PopUntil(8)
		require.NoError(t, err)
		assertBitfieldEquals(t, set.OnTimeSectors, 3, 4)
		assert.Equal(t, 1, int(queue.Length()))

		_, err = queue.Root()
		require.NoError(t, err)

		// no sectors will be popped off in quantization interval
		set, err = queue.PopUntil(12)
		require.NoError(t, err)
		assertBitfieldEmpty(t, set.OnTimeSectors)
		assert.Equal(t, 1, int(queue.Length()))

		// rest of sectors will be in first set popped off after quantization interval (13)
		set, err = queue.PopUntil(13)
		require.NoError(t, err)
		assertBitfieldEquals(t, set.OnTimeSectors, 5, 6)
		assert.Equal(t, 0, int(queue.Length()))
	})

	t.Run("reschedules sectors to expire later", func(t *testing.T) {
		queue := emptyExprirationQueue(t)
		queue.AddActiveSectors(sectors, sectorSize)

		_, err := queue.Root()
		require.NoError(t, err)

		queue.RescheduleExpirations(abi.ChainEpoch(20), sectors[:3], sectorSize)

		_, err = queue.Root()
		require.NoError(t, err)

		// expect 3 rescheduled sectors to be bundled into 1 group
		assert.Equal(t, 4, int(queue.Length()))

		// rescheduled sectors are no longer scheduled before epoch 8
		set, err := queue.PopUntil(7)
		require.NoError(t, err)
		assertBitfieldEmpty(t, set.OnTimeSectors)
		assert.Equal(t, 4, int(queue.Length()))

		// pop off sectors before new expiration and expect only the rescheduled group to remain
		_, err = queue.PopUntil(19)
		require.NoError(t, err)
		assert.Equal(t, 1, int(queue.Length()))

		// pop off rescheduled sectors
		set, err = queue.PopUntil(20)
		require.NoError(t, err)
		assert.Equal(t, 0, int(queue.Length()))

		// expect all sector stats from first 3 sectors to belong to new expiration group
		assertBitfieldEquals(t, set.OnTimeSectors, 1, 2, 3)
		assertBitfieldEmpty(t, set.EarlySectors)

		activePower := PowerForSectors(sectorSize, sectors[:3])
		faultyPower := NewPowerPairZero()

		assert.Equal(t, big.NewInt(3003), set.OnTimePledge)
		assert.True(t, activePower.Equals(set.ActivePower))
		assert.True(t, faultyPower.Equals(set.FaultyPower))
	})
}

func TestExpirations(t *testing.T) {
	quant := QuantSpec{unit: 10, offset: 3}
	sectors := []*SectorOnChainInfo{
		testSector(7, 1, 0, 0, 0),
		testSector(8, 2, 0, 0, 0),
		testSector(14, 3, 0, 0, 0),
		testSector(13, 4, 0, 0, 0),
	}
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

func testSector(expiration, number, weight, vweight, pledge int64) *SectorOnChainInfo {
	return &SectorOnChainInfo{
		Expiration:         abi.ChainEpoch(expiration),
		SectorNumber:       abi.SectorNumber(number),
		DealWeight:         big.NewInt(weight),
		VerifiedDealWeight: big.NewInt(vweight),
		InitialPledge:      abi.NewTokenAmount(pledge),
	}
}

func assertSectorSet(t *testing.T, expected, actual *sectorEpochSet) {
	assert.Equal(t, expected.epoch, actual.epoch)
	assert.Equal(t, expected.sectors, actual.sectors)
	assert.True(t, expected.power.Equals(actual.power), "expected %v, actual %v", expected.power, actual.power)
	assert.True(t, expected.pledge.Equals(actual.pledge), "expected %v, actual %v", expected.pledge, actual.pledge)
}

func emptyExpirationQueueWithQuantizing(t *testing.T, quant QuantSpec) ExpirationQueue {
	rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
	store := adt.AsStore(rt)
	root, err := adt.MakeEmptyArray(store).Root()
	require.NoError(t, err)

	queue, err := LoadExpirationQueue(store, root, quant)
	require.NoError(t, err)
	return queue
}

func emptyExprirationQueue(t *testing.T) ExpirationQueue {
	return emptyExpirationQueueWithQuantizing(t, NoQuantization)
}
