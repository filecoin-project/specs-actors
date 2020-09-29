package miner

import (
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/util"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type StateSummary struct {
	LivePower   PowerPair
	ActivePower PowerPair
	FaultyPower PowerPair
}

// Checks internal invariants of init state.
func CheckStateInvariants(st *State, store adt.Store) (*StateSummary, *builtin.MessageAccumulator, error) {
	acc := &builtin.MessageAccumulator{}

	// Load data from linked structures.
	info, err := st.GetInfo(store)
	if err != nil {
		return nil, nil, err
	}

	allSectors := map[abi.SectorNumber]*SectorOnChainInfo{}
	if sectorsArr, err := adt.AsArray(store, st.Sectors); err != nil {
		return nil, nil, err
	} else {
		var sector SectorOnChainInfo
		if err = sectorsArr.ForEach(&sector, func(sno int64) error {
			cpy := sector
			allSectors[abi.SectorNumber(sno)] = &cpy
			return nil
		}); err != nil {
			return nil, nil, err
		}
	}

	deadlines, err := st.LoadDeadlines(store)
	if err != nil {
		return nil, nil, err
	}

	livePower := NewPowerPairZero()
	activePower := NewPowerPairZero()
	faultyPower := NewPowerPairZero()

	// Check deadlines
	if err := deadlines.ForEach(store, func(dlIdx uint64, dl *Deadline) error {
		quant := st.QuantSpecForDeadline(dlIdx)
		summary, msgs, err := CheckDeadlineStateInvariants(dl, store, quant, info.SectorSize, allSectors)
		if err != nil {
			return err
		}
		acc.AddAll(msgs)

		livePower = livePower.Add(summary.LivePower)
		activePower = activePower.Add(summary.ActivePower)
		faultyPower = faultyPower.Add(summary.FaultyPower)
		return nil
	}); err != nil {
		return nil, nil, err
	}

	// TODO: check state invariants beyond deadlines.

	return &StateSummary{
		LivePower:   livePower,
		ActivePower: activePower,
		FaultyPower: faultyPower,
	}, acc, nil
}

type DeadlineStateSummary struct {
	LivePower   PowerPair
	ActivePower PowerPair
	FaultyPower PowerPair
}

func CheckDeadlineStateInvariants(deadline *Deadline, store adt.Store, quant QuantSpec, ssize abi.SectorSize, sectors map[abi.SectorNumber]*SectorOnChainInfo) (*DeadlineStateSummary, *builtin.MessageAccumulator, error) {
	acc := &builtin.MessageAccumulator{}

	// Load linked structures.
	partitions, err := deadline.PartitionsArray(store)
	if err != nil {
		return nil, nil, err
	}

	livePower := NewPowerPairZero()
	activePower := NewPowerPairZero()
	faultyPower := NewPowerPairZero()
	var partition Partition
	if err = partitions.ForEach(&partition, func(i int64) error {
		summary, msgs, err := CheckPartitionStateInvariants(&partition, store, quant, ssize, sectors)
		if err != nil {
			return err
		}
		acc.AddAll(msgs)

		livePower = livePower.Add(summary.LivePower)
		activePower = activePower.Add(summary.ActivePower)
		faultyPower = faultyPower.Add(summary.FaultyPower)
		return nil
	}); err != nil {
		return nil, nil, err
	}

	// TODO: check deadline state invariants beyond partitions.

	return &DeadlineStateSummary{
		LivePower:   livePower,
		ActivePower: activePower,
		FaultyPower: faultyPower,
	}, acc, nil
}

type PartitionStateSummary struct {
	LivePower             PowerPair
	ActivePower           PowerPair
	FaultyPower           PowerPair
	ExpirationEpochs      []abi.ChainEpoch // Epochs at which some sector is scheduled to expire.
	EarlyTerminationCount int
}

func CheckPartitionStateInvariants(
	partition *Partition,
	store adt.Store,
	quant QuantSpec,
	sectorSize abi.SectorSize,
	sectors map[abi.SectorNumber]*SectorOnChainInfo,
) (*PartitionStateSummary, *builtin.MessageAccumulator, error) {
	acc := &builtin.MessageAccumulator{}

	live, err := partition.LiveSectors()
	if err != nil {
		return nil, nil, err
	}
	active, err := partition.ActiveSectors()
	if err != nil {
		return nil, nil, err
	}

	liveSectors, missing, err := selectSectorsMap(sectors, live)
	if err != nil {
		return nil, nil, err
	} else if len(missing) > 0 {
		acc.Addf("live sectors missing from all sectors: %v", missing)
	}
	unprovenSectors, missing, err := selectSectorsMap(sectors, partition.Unproven)
	if err != nil {
		return nil, nil, err
	} else if len(missing) > 0 {
		acc.Addf("unproven sectors missing from all sectors: %v", missing)
	}

	// Validate power
	faultySectors, missing, err := selectSectorsMap(sectors, partition.Faults)
	if err != nil {
		return nil, nil, err
	} else if len(missing) > 0 {
		acc.Addf("faulty sectors missing from all sectors: %v", missing)
	}
	faultyPower := powerForSectors(faultySectors, sectorSize)
	acc.Require(partition.FaultyPower.Equals(faultyPower), "faulty power was %v, expected %v", partition.FaultyPower, faultyPower)

	recoveringSectors, missing, err := selectSectorsMap(sectors, partition.Recoveries)
	if err != nil {
		return nil, nil, err
	} else if len(missing) > 0 {
		acc.Addf("recovering sectors missing from all sectors: %v", missing)
	}
	recoveringPower := powerForSectors(recoveringSectors, sectorSize)
	acc.Require(partition.RecoveringPower.Equals(recoveringPower), "recovering power was %v, expected %v", partition.RecoveringPower, recoveringPower)

	livePower := powerForSectors(liveSectors, sectorSize)
	acc.Require(partition.LivePower.Equals(livePower), "live power was %v, expected %v", partition.LivePower, livePower)

	unprovenPower := powerForSectors(unprovenSectors, sectorSize)
	acc.Require(partition.UnprovenPower.Equals(unprovenPower), "unproven power was %v, expected %v", partition.UnprovenPower, unprovenPower)

	activePower := livePower.Sub(faultyPower).Sub(unprovenPower)
	partitionActivePower := partition.ActivePower()
	acc.Require(partitionActivePower.Equals(activePower), "active power was %v, expected %v", partitionActivePower, activePower)

	// All recoveries are faults.
	if err = requireContainsAll(partition.Faults, partition.Recoveries, acc, "faults do not contain recoveries"); err != nil {
		return nil, nil, err
	}

	// All faults are live.
	if err = requireContainsAll(live, partition.Faults, acc, "live does not contain faults"); err != nil {
		return nil, nil, err
	}

	// All unproven are live
	if err = requireContainsAll(live, partition.Unproven, acc, "live does not contain unproven"); err != nil {
		return nil, nil, err
	}

	// All terminated sectors are part of the partition.
	if err = requireContainsAll(partition.Sectors, partition.Terminated, acc, "sectors do not contain terminations"); err != nil {
		return nil, nil, err
	}

	// Live has no terminated sectors
	if err = requireContainsNone(live, partition.Terminated, acc, "live includes terminations"); err != nil {
		return nil, nil, err
	}

	// Live contains active sectors
	if err = requireContainsAll(live, active, acc, "live does not contain active"); err != nil {
		return nil, nil, err
	}

	// Active contains no faults
	if err = requireContainsNone(active, partition.Faults, acc, "active includes faults"); err != nil {
		return nil, nil, err
	}

	// Active contains no unproven
	if err = requireContainsNone(active, partition.Unproven, acc, "active includes unproven"); err != nil {
		return nil, nil, err
	}

	// Validate the expiration queue.
	expQ, err := LoadExpirationQueue(store, partition.ExpirationsEpochs, quant)
	if err != nil {
		return nil, nil, err
	}
	if err = CheckExpirationQueue(expQ, liveSectors, partition.Faults, quant, sectorSize, acc); err != nil {
		return nil, nil, err
	}

	// Validate the early termination queue.
	earlyQ, err := LoadBitfieldQueue(store, partition.EarlyTerminated, NoQuantization)
	if err != nil {
		return nil, nil, err
	}
	earlyTerminationCount, err := CheckEarlyTerminationQueue(earlyQ, partition.Terminated, acc)
	if err != nil {
		return nil, nil, err
	}

	return &PartitionStateSummary{
		LivePower:             livePower,
		ActivePower:           activePower,
		FaultyPower:           partition.FaultyPower,
		EarlyTerminationCount: earlyTerminationCount,
	}, acc, nil
}

// Checks the expiration queue for consistency.
func CheckExpirationQueue(expQ ExpirationQueue, liveSectors map[abi.SectorNumber]*SectorOnChainInfo,
	partitionFaults bitfield.BitField, quant QuantSpec, sectorSize abi.SectorSize, acc *builtin.MessageAccumulator) error {
	seenSectors := make(map[abi.SectorNumber]bool)
	firstQueueEpoch := abi.ChainEpoch(-1)
	var exp ExpirationSet
	err := expQ.ForEach(&exp, func(e int64) error {
		epoch := abi.ChainEpoch(e)
		acc := acc.WithPrefix("expiration epoch %d: ", epoch)
		acc.Require(quant.QuantizeUp(epoch) == epoch,
			"expiration queue key %d is not quantized, expected %d", epoch, quant.QuantizeUp(epoch))
		if firstQueueEpoch == abi.ChainEpoch(-1) {
			firstQueueEpoch = epoch
		}

		onTimeSectorsPledge := big.Zero()
		if err := exp.OnTimeSectors.ForEach(func(n uint64) error {
			sno := abi.SectorNumber(n)
			// Check sectors are present only once.
			acc.Require(!seenSectors[sno], "sector %d in expiration queue twice", sno)
			seenSectors[sno] = true

			// Check expiring sectors are still alive.
			if sector, ok := liveSectors[sno]; ok {
				// The sector can be "on time" either at its target expiration epoch, or in the first queue entry
				// (a CC-replaced sector moved forward).
				target := quant.QuantizeUp(sector.Expiration)
				acc.Require(epoch == target || epoch == firstQueueEpoch, "invalid expiration %d for sector %d, expected %d or %d",
					epoch, sector.SectorNumber, firstQueueEpoch, target)

				onTimeSectorsPledge = big.Add(onTimeSectorsPledge, sector.InitialPledge)
			} else {
				acc.Addf("on-time expiration sector %d isn't live", n)
			}

			return nil
		}); err != nil {
			return err
		}

		if err := exp.EarlySectors.ForEach(func(n uint64) error {
			sno := abi.SectorNumber(n)
			// Check sectors are present only once.
			acc.Require(!seenSectors[sno], "sector %d in expiration queue twice", sno)
			seenSectors[sno] = true

			// Check early sectors are faulty
			if isFaulty, err := partitionFaults.IsSet(n); err != nil {
				return err
			} else if !isFaulty {
				acc.Addf("sector %d expiring early but not faulty", sno)
			}

			// Check expiring sectors are still alive.
			if sector, ok := liveSectors[sno]; ok {
				target := quant.QuantizeUp(sector.Expiration)
				acc.Require(epoch < target, "invalid early expiration %d for sector %d, expected < %d",
					epoch, sector.SectorNumber, target)
			} else {
				acc.Addf("on-time expiration sector %d isn't live", n)
			}

			return nil
		}); err != nil {
			return err
		}

		// Validate power and pledge.
		all, err := bitfield.MergeBitFields(exp.OnTimeSectors, exp.EarlySectors)
		if err != nil {
			return err
		}
		allActive, err := bitfield.SubtractBitField(all, partitionFaults)
		if err != nil {
			return err
		}
		allFaulty, err := bitfield.IntersectBitField(all, partitionFaults)
		if err != nil {
			return err
		}
		activeSectors, missing, err := selectSectorsMap(liveSectors, allActive)
		if err != nil {
			return err
		} else if len(missing) > 0 {
			acc.Addf("active sectors missing from live: %v", missing)
		}
		faultySectors, missing, err := selectSectorsMap(liveSectors, allFaulty)
		if err != nil {
			return err
		} else if len(missing) > 0 {
			acc.Addf("faulty sectors missing from live: %v", missing)
		}
		activeSectorsPower := powerForSectors(activeSectors, sectorSize)
		acc.Require(exp.ActivePower.Equals(activeSectorsPower), "active power recorded %v doesn't match computed %v", exp.ActivePower, activeSectorsPower)

		faultySectorsPower := powerForSectors(faultySectors, sectorSize)
		acc.Require(exp.FaultyPower.Equals(faultySectorsPower), "faulty power recorded %v doesn't match computed %v", exp.FaultyPower, faultySectorsPower)

		acc.Require(exp.OnTimePledge.Equals(onTimeSectorsPledge), "on time pledge recorded %v doesn't match computed %v", exp.OnTimePledge, onTimeSectorsPledge)
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// Checks the early termination queue for consistency.
// Returns the number of sectors in the queue.
func CheckEarlyTerminationQueue(earlyQ BitfieldQueue, terminated bitfield.BitField, acc *builtin.MessageAccumulator) (int, error) {
	seenMap := make(map[uint64]bool)
	seenBf := bitfield.New()
	if err := earlyQ.ForEach(func(epoch abi.ChainEpoch, bf bitfield.BitField) error {
		acc := acc.WithPrefix("early termination epoch %d: ", epoch)
		return bf.ForEach(func(i uint64) error {
			acc.Require(!seenMap[i], "sector %v in early termination queue twice", i)
			seenMap[i] = true
			seenBf.Set(i)
			return nil
		})
	}); err != nil {
		return 0, err
	}

	if err := requireContainsAll(terminated, seenBf, acc, "terminated sectors missing early termination entry"); err != nil {
		return 0, err
	}
	return len(seenMap), nil
}

// Selects a subset of sectors from a map by sector number.
// Returns the selected sectors, and a slice of any sector numbers not found.
func selectSectorsMap(sectors map[abi.SectorNumber]*SectorOnChainInfo, include bitfield.BitField) (map[abi.SectorNumber]*SectorOnChainInfo, []abi.SectorNumber, error) {
	included := map[abi.SectorNumber]*SectorOnChainInfo{}
	missing := []abi.SectorNumber{}
	if err := include.ForEach(func(n uint64) error {
		if s, ok := sectors[abi.SectorNumber(n)]; ok {
			included[abi.SectorNumber(n)] = s
		} else {
			missing = append(missing, abi.SectorNumber(n))
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return included, missing, nil
}

func powerForSectors(sectors map[abi.SectorNumber]*SectorOnChainInfo, ssize abi.SectorSize) PowerPair {
	qa := big.Zero()
	for _, s := range sectors { // nolint:nomaprange
		qa = big.Add(qa, QAPowerForSector(ssize, s))
	}

	return PowerPair{
		Raw: big.Mul(big.NewIntUnsigned(uint64(ssize)), big.NewIntUnsigned(uint64(len(sectors)))),
		QA:  qa,
	}
}

func requireContainsAll(superset, subset bitfield.BitField, acc *builtin.MessageAccumulator, msg string) error {
	contains, err := util.BitFieldContainsAll(superset, subset)
	if err != nil {
		return err
	}
	acc.Require(contains, msg+":%v, %v", superset, subset)
	return nil
}

func requireContainsNone(superset, subset bitfield.BitField, acc *builtin.MessageAccumulator, msg string) error {
	contains, err := util.BitFieldContainsAny(superset, subset)
	if err != nil {
		return err
	}
	acc.Require(!contains, msg+":%v, %v", superset, subset)
	return nil
}
