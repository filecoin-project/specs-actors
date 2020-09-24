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
		err := m.recomputeClaim(ctx, adtStore, &st, tree, a, epoch)
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

			exqRoot, partitionActivePower, partitionFaultyPower, _, err := m.correctExpirationQueue(exq, sectors, part.Terminated, part.Faults, sectorSize)
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

			if !part.LivePower.Equals(partitionActivePower.Add(partitionFaultyPower)) {
				part.LivePower = partitionActivePower.Add(partitionFaultyPower)
				alteredPartitions[uint64(partIdx)] = part
			}
			if !part.FaultyPower.Equals(partitionFaultyPower) {
				part.FaultyPower = partitionFaultyPower
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
			deadlinesModified = true
		}
		return nil
	})

	if !deadlinesModified {
		return false, nil
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
	if dlInfo.Open <= epoch || dlInfo.Close < epoch+miner.PreCommitChallengeDelay {
		return false, xerrors.Errorf("epoch is out of expected range of miner deadline (%d, %d] ∌ %d",
			dlInfo.Open, dlInfo.Close, epoch)
	}

	return true, err
}

func (m *minerMigrator) recomputeClaim(ctx context.Context, store adt.Store, st *miner.State,
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

	powSt.Claims, err = claims.Root()
	if err != nil {
		return err
	}
	return nil
}

// Updates the expiration queue by correcting any duplicate entries and their fallout.
// If no changes need to be made cid.Undef will be returned.
func (m *minerMigrator) correctExpirationQueue(exq miner.ExpirationQueue, sectors miner.Sectors,
	allTerminated bitfield.BitField, allFaults bitfield.BitField, sectorSize abi.SectorSize,
) (cid.Cid, miner.PowerPair, miner.PowerPair, bitfield.BitField, error) {
	// processed expired sectors includes all terminated and all sectors seen in earlier expiration sets
	processedExpiredSectors := allTerminated
	terminatedAsNonFaults := bitfield.New()

	alteredExpirationSets := make(map[abi.ChainEpoch]miner.ExpirationSet)
	erasedFaults := bitfield.New()
	var exs miner.ExpirationSet

	// check for faults that need to be erased
	err := exq.ForEach(&exs, func(epoch int64) error { //nolint:nomaprange
		// handle faulty sector expirations that have already been processed
		earlyDuplicates, err := bitfield.IntersectBitField(exs.EarlySectors, processedExpiredSectors)
		if err != nil {
			return err
		} else if empty, err := earlyDuplicates.IsEmpty(); err != nil {
			return err
		} else if !empty {
			alteredExpirationSets[abi.ChainEpoch(epoch)] = exs
			exs.EarlySectors, err = bitfield.SubtractBitField(exs.EarlySectors, earlyDuplicates)
			if err != nil {
				return err
			}

			terminatedActive, err := bitfield.IntersectBitField(earlyDuplicates, allTerminated)
			if err != nil {
				return err
			}
			terminatedAsNonFaults, err = bitfield.MergeBitFields(terminatedAsNonFaults, terminatedActive)
			if err != nil {
				return err
			}
		}

		onTimeDuplicates, err := bitfield.IntersectBitField(exs.OnTimeSectors, processedExpiredSectors)
		if err != nil {
			return err
		} else if empty, err := onTimeDuplicates.IsEmpty(); err != nil {
			return err
		} else if !empty {
			alteredExpirationSets[abi.ChainEpoch(epoch)] = exs
			exs.OnTimeSectors, err = bitfield.SubtractBitField(exs.OnTimeSectors, onTimeDuplicates)
			if err != nil {
				return err
			}

			terminatedActive, err := bitfield.IntersectBitField(onTimeDuplicates, allTerminated)
			if err != nil {
				return err
			}
			terminatedAsNonFaults, err = bitfield.MergeBitFields(terminatedAsNonFaults, terminatedActive)
			if err != nil {
				return err
			}
		}

		if erasedFaults, err = bitfield.MultiMerge(erasedFaults, earlyDuplicates, onTimeDuplicates); err != nil {
			return err
		}

		// record on time sectors
		if processedExpiredSectors, err = bitfield.MultiMerge(processedExpiredSectors, exs.EarlySectors, exs.OnTimeSectors); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return cid.Undef, miner.PowerPair{}, miner.PowerPair{}, bitfield.BitField{}, err
	}

	// if we didn't find any faults that needed to be erased, we're done
	if len(alteredExpirationSets) == 0 {
		return cid.Undef, miner.PowerPair{}, miner.PowerPair{}, bitfield.BitField{}, nil
	}

	// save expiration sets that have been altered
	for epoch, set := range alteredExpirationSets { //nolint:nomaprange
		if err := exq.Set(uint64(epoch), &set); err != nil {
			return cid.Undef, miner.PowerPair{}, miner.PowerPair{}, bitfield.BitField{}, err
		}
	}

	alteredExpirationSets = make(map[abi.ChainEpoch]miner.ExpirationSet)
	partitionActivePower := miner.NewPowerPairZero()
	partitionFaultyPower := miner.NewPowerPairZero()
	err = exq.ForEach(&exs, func(epoch int64) error {
		modified, activePower, faultyPower, err := correctExpirationSet(&exs, sectors, allFaults, sectorSize)
		if err != nil {
			return err
		}
		partitionActivePower = partitionActivePower.Add(activePower)
		partitionFaultyPower = partitionFaultyPower.Add(faultyPower)

		if modified {
			alteredExpirationSets[abi.ChainEpoch(epoch)] = exs
		}

		return nil
	})
	if err != nil {
		return cid.Undef, miner.PowerPair{}, miner.PowerPair{}, bitfield.BitField{}, err
	}

	/*
		partition.Faults - foundFault = set of faults that have been deducted erroneously
		= sectors cc upgraded and faulted for which cc upgrade expiration set has been processed
	*/

	// save expiration sets that have been altered
	for epoch, set := range alteredExpirationSets { //nolint:nomaprange
		if err := exq.Set(uint64(epoch), &set); err != nil {
			return cid.Undef, miner.PowerPair{}, miner.PowerPair{}, bitfield.BitField{}, err
		}
	}

	expirationQueueRoot, err := exq.Root()
	if err != nil {
		return cid.Undef, miner.PowerPair{}, miner.PowerPair{}, bitfield.BitField{}, err
	}

	return expirationQueueRoot, partitionActivePower, partitionFaultyPower, terminatedAsNonFaults, nil
}

func correctExpirationSet(exs *miner.ExpirationSet, sectors miner.Sectors,
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
