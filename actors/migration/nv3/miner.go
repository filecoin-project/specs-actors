package nv3

import (
	"bytes"
	"context"
	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/builtin"
	miner "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/states"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

type minerMigrator struct {
}

func (m *minerMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, priorEpoch abi.ChainEpoch, a addr.Address, tree *states.Tree) (cid.Cid, error) {
	epoch := priorEpoch + 1
	var st miner.State
	if err := store.Get(ctx, head, &st); err != nil {
		return cid.Undef, err
	}

	adtStore := adt.WrapStore(ctx, store)

	sectors, err := miner.LoadSectors(adtStore, st.Sectors)
	if err != nil {
		return cid.Undef, err
	}

	info, err := st.GetInfo(adtStore)
	if err != nil {
		return cid.Undef, err
	}

	powerClaimSuspect, err := m.correctForCCUpgradeThenFaultIssue(ctx, store, &st, sectors, epoch, info.SectorSize)
	if err != nil {
		return cid.Undef, err
	}

	if powerClaimSuspect {
		err := m.updatePowerState(ctx, adtStore, &st, tree, a, epoch)
		if err != nil {
			return cid.Undef, err
		}
	}

	newHead, err := store.Put(ctx, &st)
	return newHead, err
}

func (m *minerMigrator) correctForCCUpgradeThenFaultIssue(
	ctx context.Context, store cbor.IpldStore, st *miner.State, sectors miner.Sectors, epoch abi.ChainEpoch,
	sectorSize abi.SectorSize,
) (bool, error) {

	quantSpec := st.QuantSpecEveryDeadline()

	deadlines, err := st.LoadDeadlines(adt.WrapStore(ctx, store))
	if err != nil {
		return false, err
	}

	deadlinesModified := false
	err = deadlines.ForEach(adt.WrapStore(ctx, store), func(dlIdx uint64, deadline *miner.Deadline) error {
		partitions, err := adt.AsArray(adt.WrapStore(ctx, store), deadline.Partitions)
		if err != nil {
			return err
		}

		alteredPartitions := make(map[uint64]miner.Partition)
		allFaultyPower := miner.NewPowerPairZero()
		var part miner.Partition
		err = partitions.ForEach(&part, func(partIdx int64) error {
			exq, err := miner.LoadExpirationQueue(adt.WrapStore(ctx, store), part.ExpirationsEpochs, quantSpec)
			if err != nil {
				return err
			}

			exqRoot, stats, err := m.correctExpirationQueue(exq, sectors, part.Terminated, part.Faults, sectorSize)
			if err != nil {
				return err
			}

			// if unmodified, we're done
			if exqRoot.Equals(cid.Undef) {
				return nil
			}

			if !part.ExpirationsEpochs.Equals(exqRoot) {
				part.ExpirationsEpochs = exqRoot
				alteredPartitions[uint64(partIdx)] = part
			}

			if !part.LivePower.Equals(stats.totalActivePower.Add(stats.totalFaultyPower)) {
				part.LivePower = stats.totalActivePower.Add(stats.totalFaultyPower)
				alteredPartitions[uint64(partIdx)] = part
			}
			if !part.FaultyPower.Equals(stats.totalFaultyPower) {
				part.FaultyPower = stats.totalFaultyPower
				alteredPartitions[uint64(partIdx)] = part
			}
			allFaultyPower = allFaultyPower.Add(part.FaultyPower)

			return nil
		})
		if err != nil {
			return err
		}

		if len(alteredPartitions) > 0 {
			for partIdx, part := range alteredPartitions { // nolint:nomaprange
				if err := partitions.Set(partIdx, &part); err != nil {
					return err
				}
			}

			deadline.Partitions, err = partitions.Root()
			if err != nil {
				return err
			}

			deadline.FaultyPower = allFaultyPower

			if err := deadlines.UpdateDeadline(adt.WrapStore(ctx, store), dlIdx, deadline); err != nil {
				return err
			}
			deadlinesModified = true
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	if !deadlinesModified {
		return false, nil
	}

	if err = st.SaveDeadlines(adt.WrapStore(ctx, store), deadlines); err != nil {
		return false, err
	}

	for st.ProvingPeriodStart+miner.WPoStProvingPeriod <= epoch {
		st.ProvingPeriodStart += miner.WPoStProvingPeriod
	}
	st.CurrentDeadline = uint64((epoch - st.ProvingPeriodStart) / miner.WPoStChallengeWindow)

	// assert ranges for new proving period and deadline
	if st.ProvingPeriodStart <= epoch-miner.WPoStProvingPeriod || st.ProvingPeriodStart > epoch {
		return false, xerrors.Errorf("miner proving period start, %d, is out of range (%d, %d]",
			st.ProvingPeriodStart, epoch-miner.WPoStProvingPeriod, epoch)
	}
	dlInfo := st.DeadlineInfo(epoch)
	if dlInfo.Open > epoch || dlInfo.Close <= epoch {
		return false, xerrors.Errorf("epoch is out of expected range of miner deadline [%d, %d) ∌ %d",
			dlInfo.Open, dlInfo.Close, epoch)
	}

	return true, err
}

func (m *minerMigrator) updatePowerState(ctx context.Context, store adt.Store, st *miner.State,
	tree *states.Tree, a addr.Address, epoch abi.ChainEpoch,
) error {
	powerAct, found, err := tree.GetActor(builtin.StoragePowerActorAddr)
	if err != nil {
		return err
	}
	if !found {
		return xerrors.Errorf("power actor not found at address %s", builtin.StoragePowerActorAddr)
	}

	var powSt power.State
	if err := store.Get(ctx, powerAct.Head, &powSt); err != nil {
		return err
	}

	err = m.updateClaim(ctx, store, st, &powSt, a)
	if err != nil {
		return err
	}

	err = m.updateProvingPeriodCron(a, st, epoch, store, &powSt)
	if err != nil {
		return err
	}

	powerAct.Head, err = store.Put(ctx, &powSt)
	if err != nil {
		return nil
	}

	return tree.SetActor(builtin.StoragePowerActorAddr, powerAct)
}

func (m *minerMigrator) updateProvingPeriodCron(a addr.Address, st *miner.State, epoch abi.ChainEpoch, store adt.Store, powSt *power.State) error {
	var buf bytes.Buffer
	payload := &miner.CronEventPayload{
		EventType: miner.CronEventProvingDeadline,
	}
	err := payload.MarshalCBOR(&buf)
	if err != nil {
		return err
	}

	event := power.CronEvent{
		MinerAddr:       a,
		CallbackPayload: buf.Bytes(),
	}

	dlInfo := st.DeadlineInfo(epoch)
	crontMM, err := adt.AsMultimap(store, powSt.CronEventQueue)
	if err != nil {
		return err
	}
	err = crontMM.Add(abi.IntKey(int64(dlInfo.Last())), &event)
	if err != nil {
		return err
	}

	powSt.CronEventQueue, err = crontMM.Root()
	if err != nil {
		return err
	}
	return nil
}

func (m *minerMigrator) updateClaim(ctx context.Context, store adt.Store, st *miner.State, powSt *power.State, a addr.Address) error {
	deadlines, err := st.LoadDeadlines(store)
	if err != nil {
		return err
	}

	activePower := miner.NewPowerPairZero()
	err = deadlines.ForEach(store, func(dlIdx uint64, dl *miner.Deadline) error {
		partitions, err := dl.PartitionsArray(store)
		if err != nil {
			return err
		}

		var part miner.Partition
		return partitions.ForEach(&part, func(pIdx int64) error {
			activePower = activePower.Add(part.LivePower.Sub(part.FaultyPower))
			return nil
		})
	})
	if err != nil {
		return err
	}

	claims, err := adt.AsMap(store, powSt.Claims)
	if err != nil {
		return err
	}

	var existing power.Claim
	powerDelta := activePower
	found, err := claims.Get(abi.AddrKey(a), &existing)
	if err != nil {
		return err
	}
	if found {
		powerDelta = powerDelta.Sub(miner.NewPowerPair(existing.RawBytePower, existing.QualityAdjPower))
	}
	if err := powSt.AddToClaim(store, a, powerDelta.Raw, powerDelta.QA); err != nil {
		return err
	}

	return nil
}

type expirationQueueStats struct {
	// total of all active power in the expiration queue
	totalActivePower miner.PowerPair
	// total of all faulty power in the expiration queue
	totalFaultyPower miner.PowerPair
}

// Updates the expiration queue by correcting any duplicate entries and their fallout.
// If no changes need to be made cid.Undef will be returned.
// Returns the new root of the expiration queue
func (m *minerMigrator) correctExpirationQueue(exq miner.ExpirationQueue, sectors miner.Sectors,
	allTerminated bitfield.BitField, allFaults bitfield.BitField, sectorSize abi.SectorSize,
) (cid.Cid, expirationQueueStats, error) {
	// processed expired sectors includes all terminated and all sectors seen in earlier expiration sets
	processedExpiredSectors := allTerminated
	expirationSetPowerSuspect := false

	var exs miner.ExpirationSet

	// Check for faults that need to be erased.
	// Erased faults will be removed from bitfields and the power will be recomputed
	// in the subsequent loop.
	err := exq.ForEach(&exs, func(epoch int64) error { //nolint:nomaprange
		// Detect sectors that are present in this expiration set as "early", but that
		// have already terminated or duplicate a prior entry in the queue, and thus will
		// be terminated before this entry is processed. The sector was rescheduled here
		// upon fault, but the entry is stale and should not exist.
		modified := false
		earlyDuplicates, err := bitfield.IntersectBitField(exs.EarlySectors, processedExpiredSectors)
		if err != nil {
			return err
		} else if empty, err := earlyDuplicates.IsEmpty(); err != nil {
			return err
		} else if !empty {
			modified = true
			exs.EarlySectors, err = bitfield.SubtractBitField(exs.EarlySectors, earlyDuplicates)
			if err != nil {
				return err
			}
		}

		// Detect sectors that are terminating on time, but have either already terminated or duplicate
		// an entry in the queue. The sector might be faulty, but were expiring here anyway so not
		// rescheduled as "early".
		onTimeDuplicates, err := bitfield.IntersectBitField(exs.OnTimeSectors, processedExpiredSectors)
		if err != nil {
			return err
		} else if empty, err := onTimeDuplicates.IsEmpty(); err != nil {
			return err
		} else if !empty {
			modified = true
			exs.OnTimeSectors, err = bitfield.SubtractBitField(exs.OnTimeSectors, onTimeDuplicates)
			if err != nil {
				return err
			}
		}

		if modified {
			expirationSetPowerSuspect = true
			exs2, err := copyES(exs)
			if err != nil {
				return err
			}
			if err := exq.Set(uint64(epoch), &exs2); err != nil {
				return err
			}
		}

		// Record all sectors that would be terminated after this queue entry is processed.
		if processedExpiredSectors, err = bitfield.MultiMerge(processedExpiredSectors, exs.EarlySectors, exs.OnTimeSectors); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return cid.Undef, expirationQueueStats{}, err
	}

	// If we didn't find any duplicate sectors, we're done.
	if !expirationSetPowerSuspect {
		return cid.Undef, expirationQueueStats{}, nil
	}

	partitionActivePower := miner.NewPowerPairZero()
	partitionFaultyPower := miner.NewPowerPairZero()
	err = exq.ForEach(&exs, func(epoch int64) error {
		modified, activePower, faultyPower, err := correctExpirationSetPower(&exs, sectors, allFaults, sectorSize)
		if err != nil {
			return err
		}
		partitionActivePower = partitionActivePower.Add(activePower)
		partitionFaultyPower = partitionFaultyPower.Add(faultyPower)

		if modified {
			exs2, err := copyES(exs)
			if err != nil {
				return err
			}
			if err := exq.Set(uint64(epoch), &exs2); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return cid.Undef, expirationQueueStats{}, err
	}

	expirationQueueRoot, err := exq.Root()
	if err != nil {
		return cid.Undef, expirationQueueStats{}, err
	}

	return expirationQueueRoot, expirationQueueStats{
		partitionActivePower,
		partitionFaultyPower,
	}, nil
}

// Recompute active and faulty power for an expiration set.
// The active power for an expiration set should be the sum of the power of all its active sectors,
// where active means all sectors not labeled as a fault in the partition. Similarly, faulty power
// is the sum of faulty sectors.
// If a sector has been rescheduled from ES3 to both ES1 as active and ES2
// as a fault, we expect it to be labeled as a fault in the partition. We have already
// removed the sector from ES2, so this correction should move its active power to faulty power in ES1
// because it is labeled as a fault, remove its power altogether from ES2 because its been removed from
// ES2's bitfields, and correct the double subtraction of power from ES3.
func correctExpirationSetPower(exs *miner.ExpirationSet, sectors miner.Sectors,
	allFaults bitfield.BitField, sectorSize abi.SectorSize,
) (bool, miner.PowerPair, miner.PowerPair, error) {

	modified := false

	allSectors, err := bitfield.MergeBitFields(exs.OnTimeSectors, exs.EarlySectors)
	if err != nil {
		return false, miner.PowerPair{}, miner.PowerPair{}, err
	}

	// correct errors in active power
	activeSectors, err := bitfield.SubtractBitField(allSectors, allFaults)
	if err != nil {
		return false, miner.PowerPair{}, miner.PowerPair{}, err
	}
	s, err := sectors.Load(activeSectors)
	if err != nil {
		return false, miner.PowerPair{}, miner.PowerPair{}, err
	}
	activePower := miner.PowerForSectors(sectorSize, s)

	if !activePower.Equals(exs.ActivePower) {
		exs.ActivePower = activePower
		modified = true
	}

	// correct errors in faulty power
	faultySectors, err := bitfield.IntersectBitField(allSectors, allFaults)
	if err != nil {
		return false, miner.PowerPair{}, miner.PowerPair{}, err
	}
	fs, err := sectors.Load(faultySectors)
	if err != nil {
		return false, miner.PowerPair{}, miner.PowerPair{}, err
	}
	faultyPower := miner.PowerForSectors(sectorSize, fs)

	if !faultyPower.Equals(exs.FaultyPower) {
		exs.FaultyPower = faultyPower
		modified = true
	}

	return modified, activePower, faultyPower, nil
}

func copyES(in miner.ExpirationSet) (miner.ExpirationSet, error) {
	ots, err := in.OnTimeSectors.Copy()
	if err != nil {
		return miner.ExpirationSet{}, err
	}

	es, err := in.EarlySectors.Copy()
	if err != nil {
		return miner.ExpirationSet{}, err
	}

	return miner.ExpirationSet{
		OnTimeSectors: ots,
		EarlySectors:  es,
		OnTimePledge:  in.OnTimePledge,
		ActivePower:   in.ActivePower,
		FaultyPower:   in.FaultyPower,
	}, nil
}
