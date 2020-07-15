package miner

import (
	"errors"
	"sort"

	"github.com/filecoin-project/go-bitfield"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

// Deadlines contains Deadline objects, describing the sectors due at the given
// deadline and their state (faulty, terminated, recovering, etc.).
type Deadlines struct {
	// Note: we could inline part of the deadline struct (e.g., active/assigned sectors)
	// to make new sector assignment cheaper. At the moment, assigning a sector requires
	// loading all deadlines to figure out where best to assign new sectors.
	Due [WPoStPeriodDeadlines]cid.Cid // []Deadline
}

// Deadline holds the state for all sectors due at a specific deadline.
type Deadline struct {
	// Partitions in this deadline, in order.
	// The keys of this AMT are always sequential integers beginning with zero.
	Partitions cid.Cid // AMT[PartitionNumber]Partition

	// Maps epochs to partitions that _may_ have sectors that expire in or
	// before that epoch, either on-time or early as faults.
	// Keys are quantized to final epochs in each proving deadline.
	//
	// NOTE: Partitions MUST NOT be removed from this queue (until the
	// associated epoch has passed) even if they no longer have sectors
	// expiring at that epoch. Sectors expiring at this epoch may later be
	// recovered, and this queue will not be updated at that time.
	ExpirationsEpochs cid.Cid // AMT[ChainEpoch]BitField

	// Partitions numbers with PoSt submissions since the proving period started.
	PostSubmissions *abi.BitField

	// Partitions with sectors that terminated early.
	EarlyTerminations *abi.BitField

	// The number of non-terminated sectors in this deadline (incl faulty).
	LiveSectors uint64
}

//
// Deadlines (plural)
//

func ConstructDeadlines(emptyDeadlineCid cid.Cid) *Deadlines {
	d := new(Deadlines)
	for i := range d.Due {
		d.Due[i] = emptyDeadlineCid
	}
	return d
}

func (d *Deadlines) LoadDeadline(store adt.Store, dlIdx uint64) (*Deadline, error) {
	if dlIdx >= uint64(len(d.Due)) {
		return nil, xerrors.Errorf("invalid deadline %d", dlIdx)
	}
	deadline := new(Deadline)
	err := store.Get(store.Context(), d.Due[dlIdx], deadline)
	if err != nil {
		return nil, err
	}
	return deadline, nil
}

func (d *Deadlines) ForEach(store adt.Store, cb func(dlIdx uint64, dl *Deadline) error) error {
	for dlIdx := range d.Due {
		dl, err := d.LoadDeadline(store, uint64(dlIdx))
		if err != nil {
			return err
		}
		err = cb(uint64(dlIdx), dl)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Deadlines) UpdateDeadline(store adt.Store, dlIdx uint64, deadline *Deadline) error {
	if dlIdx >= uint64(len(d.Due)) {
		return xerrors.Errorf("invalid deadline %d", dlIdx)
	}
	dlCid, err := store.Put(store.Context(), deadline)
	if err != nil {
		return err
	}
	d.Due[dlIdx] = dlCid
	return nil
}

//
// Deadline (singular)
//

func ConstructDeadline(emptyArrayCid cid.Cid) *Deadline {
	return &Deadline{
		Partitions:        emptyArrayCid,
		ExpirationsEpochs: emptyArrayCid,
		PostSubmissions:   abi.NewBitField(),
		EarlyTerminations: abi.NewBitField(),
		LiveSectors:       0,
	}
}

func (d *Deadline) PartitionsArray(store adt.Store) (*adt.Array, error) {
	return adt.AsArray(store, d.Partitions)
}

func (d *Deadline) LoadPartition(store adt.Store, partIdx uint64) (*Partition, error) {
	partitions, err := d.PartitionsArray(store)
	if err != nil {
		return nil, err
	}
	var partition Partition
	found, err := partitions.Get(partIdx, &partition)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, xerrors.Errorf("no partition %d", partIdx)
	}
	return &partition, nil
}

// Adds some partition numbers to the set expiring at an epoch.
func (d *Deadline) AddExpirationPartitions(store adt.Store, expirationEpoch abi.ChainEpoch, partitions []uint64, quant QuantSpec) error {
	queue, err := LoadBitfieldQueue(store, d.ExpirationsEpochs, quant)
	if err != nil {
		return xerrors.Errorf("failed to load expiration queue: %w", err)
	}
	if err = queue.AddToQueueValues(expirationEpoch, partitions...); err != nil {
		return xerrors.Errorf("failed to mutate expiration queue: %w", err)
	}
	if d.ExpirationsEpochs, err = queue.Root(); err != nil {
		return xerrors.Errorf("failed to save expiration queue: %w", err)
	}
	return nil
}

// PopExpiredSectors terminates expired sectors from all partitions.
// Returns the expired sector aggregates.
func (dl *Deadline) PopExpiredSectors(store adt.Store, until abi.ChainEpoch, quant QuantSpec) (*ExpirationSet, error) {
	expiredPartitions, err := dl.popExpiredPartitions(store, until, quant)
	if err != nil {
		return nil, err
	} else if expiredPartitions == nil {
		return nil, nil // nothing to do.
	}

	partitions, err := dl.PartitionsArray(store)
	if err != nil {
		return nil, err
	}

	var onTimeSectors []*abi.BitField
	var earlySectors []*abi.BitField
	allOnTimePledge := big.Zero()
	allActivePower := NewPowerPairZero()
	allFaultyPower := NewPowerPairZero()
	var partitionsWithEarlyTerminations []uint64

	// For each partition with an expiry, remove and collect expirations from the partition queue.
	if err = expiredPartitions.ForEach(func(partIdx uint64) error {
		var partition Partition
		if found, err := partitions.Get(partIdx, &partition); err != nil {
			return err
		} else if !found {
			return xerrors.Errorf("missing expected partition %d", partIdx)
		}

		partExpiration, err := partition.PopExpiredSectors(store, until, quant)
		if err != nil {
			return xerrors.Errorf("failed to pop expired sectors from partition %d: %w", partIdx, err)
		}

		onTimeSectors = append(onTimeSectors, partExpiration.OnTimeSectors)
		earlySectors = append(earlySectors, partExpiration.EarlySectors)
		allActivePower = allActivePower.Add(partExpiration.ActivePower)
		allFaultyPower = allFaultyPower.Add(partExpiration.FaultyPower)
		allOnTimePledge = big.Add(allOnTimePledge, partExpiration.OnTimePledge)

		if empty, err := partExpiration.EarlySectors.IsEmpty(); err != nil {
			return xerrors.Errorf("failed to count early expirations from partition %d: %w", partIdx, err)
		} else if !empty {
			partitionsWithEarlyTerminations = append(partitionsWithEarlyTerminations, partIdx)
		}

		return partitions.Set(partIdx, &partition)
	}); err != nil {
		return nil, err
	}

	if dl.Partitions, err = partitions.Root(); err != nil {
		return nil, err
	}

	// Update early expiration bitmap, if relevant.
	if len(partitionsWithEarlyTerminations) > 0 {
		dl.EarlyTerminations, err = bitfield.MergeBitFields(dl.EarlyTerminations, bitfield.NewFromSet(partitionsWithEarlyTerminations))
		if err != nil {
			return nil, err
		}
	}

	allOnTimeSectors, err := bitfield.MultiMerge(onTimeSectors...)
	if err != nil {
		return nil, err
	}
	allEarlySectors, err := bitfield.MultiMerge(earlySectors...)
	if err != nil {
		return nil, err
	}

	// Update live sector count.
	onTimeCount, err := allOnTimeSectors.Count()
	if err != nil {
		return nil, xerrors.Errorf("failed to count on-time expired sectors: %w", err)
	}
	earlyCount, err := allEarlySectors.Count()
	if err != nil {
		return nil, xerrors.Errorf("failed to count early expired sectors: %w", err)
	}
	dl.LiveSectors -= onTimeCount + earlyCount

	return NewExpirationSet(allOnTimeSectors, allEarlySectors, allOnTimePledge, allActivePower, allFaultyPower), nil
}

// Adds sectors to a deadline. It's the caller's responsibility to make sure
// that this deadline isn't currently "open" (i.e., being proved at this point
// in time).
func (dl *Deadline) AddSectors(store adt.Store, partitionSize uint64, sectors []*SectorOnChainInfo,
	ssize abi.SectorSize, quant QuantSpec) error {
	// TODO: This function is ridiculous. We should try to break it into
	// smaller pieces.

	if len(sectors) == 0 {
		return nil
	}

	partitionDeadlineUpdates := make(map[abi.ChainEpoch][]uint64)

	// First update partitions

	{
		partitions, err := dl.PartitionsArray(store)
		if err != nil {
			return err
		}

		partIdx := partitions.Length()
		if partIdx > 0 {
			partIdx -= 1 // try filling up the last partition first.
		}

		for ; len(sectors) > 0; partIdx++ {
			// Get/create partition to update.
			partition := new(Partition)
			if found, err := partitions.Get(partIdx, partition); err != nil {
				return err
			} else if !found {
				// Calling this once per loop is fine. We're almost
				// never going to add more than one partition per call.
				emptyArray, err := adt.MakeEmptyArray(store).Root()
				if err != nil {
					return err
				}

				partition = ConstructPartition(emptyArray)
			}

			// Figure out which (if any) sectors we want to add to
			// this partition.
			sectorCount, err := partition.Sectors.Count()
			if sectorCount >= partitionSize {
				continue
			}

			size := min64(partitionSize-sectorCount, uint64(len(sectors)))
			partitionNewSectors := sectors[:size]
			sectors = sectors[size:]

			// Add sectors to partition.
			err = partition.AddSectors(store, partitionNewSectors, ssize, quant)
			if err != nil {
				return err
			}

			// Save partition back.
			err = partitions.Set(partIdx, partition)
			if err != nil {
				return err
			}

			// Record deadline -> partition mapping so we can later update the deadlines.
			for _, sector := range partitionNewSectors {
				partitionUpdate := partitionDeadlineUpdates[sector.Expiration]
				// Record each new partition once.
				if len(partitionUpdate) > 0 && partitionUpdate[len(partitionUpdate)-1] == partIdx {
					continue
				}
				partitionDeadlineUpdates[sector.Expiration] = append(partitionUpdate, partIdx)
			}
		}

		// Save partitions back.
		dl.Partitions, err = partitions.Root()
		if err != nil {
			return err
		}
	}

	// Next, update the per-deadline expiration queues.

	{
		deadlineExpirations, err := LoadBitfieldQueue(store, dl.ExpirationsEpochs, quant)
		if err != nil {
			return xerrors.Errorf("failed to load expiration epochs: %w", err)
		}

		if err = deadlineExpirations.AddManyToQueueValues(partitionDeadlineUpdates); err != nil {
			return xerrors.Errorf("failed to add expirations for new deadlines: %w", err)
		}

		if dl.ExpirationsEpochs, err = deadlineExpirations.Root(); err != nil {
			return err
		}
	}

	dl.LiveSectors += uint64(len(sectors))

	return nil
}

func (dl *Deadline) PopEarlyTerminations(store adt.Store, max uint64) (earlyTerminations []EpochSet, hasMore bool, err error) {
	stopErr := errors.New("stop error")

	partitions, err := dl.PartitionsArray(store)
	if err != nil {
		return nil, false, xerrors.Errorf("failed to load partitions: %w", err)
	}

	earlyTerminationsMap := make(map[abi.ChainEpoch][]*abi.BitField)

	var partitionsFinished []uint64
	err = dl.EarlyTerminations.ForEach(func(partIdx uint64) error {
		// Load partition.
		var partition Partition
		found, err := partitions.Get(partIdx, &partition)
		if err != nil {
			return xerrors.Errorf("failed to load partition %d: %w", partIdx, err)
		}

		if !found {
			// If the partition doesn't exist any more, no problem.
			// We don't remove partition indexes from this queue when sectors are re-scheduled,
			// terminated early, etc.
			// We do expect partition compaction to re-index the altered partitions, though.
			partitionsFinished = append(partitionsFinished, partIdx)
			return nil
		}

		// Pop early terminations.
		sectorTerminations, more, err := partition.PopEarlyTerminations(store, max)
		if err != nil {
			return xerrors.Errorf("failed to pop terminations from partition: %w", err)
		}

		// Sort through early terminations.
		for _, t := range sectorTerminations {
			earlyTerminationsMap[t.Epoch] = append(earlyTerminationsMap[t.Epoch], t.Sectors)
			count, err := t.Sectors.Count()
			if err != nil {
				return xerrors.Errorf("failed to count early terminations in partition %d from epoch %v: err", partIdx, t.Epoch, err)
			} else if count > max {
				return xerrors.Errorf("partition %d returned too many sectors when popping early terminations", partIdx)
			}
			max -= count
		}

		// If we've processed all of them for this partition, unmark it in the deadline.
		if !more {
			partitionsFinished = append(partitionsFinished, partIdx)
		}

		// Save partition
		err = partitions.Set(partIdx, &partition)
		if err != nil {
			return xerrors.Errorf("failed to store partition %v", partIdx)
		}

		if max == 0 {
			return stopErr
		}

		return nil
	})
	switch err {
	case nil:
	case stopErr:
		err = nil
	default:
		return nil, false, xerrors.Errorf("failed to walk early terminations bitfield for deadlines: %w", err)
	}

	// Removed finished partitions from the index.
	for _, finished := range partitionsFinished {
		dl.EarlyTerminations.Unset(finished)
	}

	// Save deadline's partitions
	dl.Partitions, err = partitions.Root()
	if err != nil {
		return nil, false, xerrors.Errorf("failed to update partitions")
	}

	// Update global early terminations bitfield.
	noEarlyTerminations, err := dl.EarlyTerminations.IsEmpty()
	if err != nil {
		return nil, false, xerrors.Errorf("failed to count remaining early terminations partitions: %w", err)
	}

	// This is safe because we sort immediately afterwards.
	for epoch, sectors := range earlyTerminationsMap { //nolint:nomaprange
		merged, err := bitfield.MultiMerge(sectors...)
		if err != nil {
			return nil, false, xerrors.Errorf("failed to merge early termination sector bitfields: %w", err)
		}
		earlyTerminations = append(earlyTerminations, EpochSet{epoch, merged})
	}

	sort.Slice(earlyTerminations, func(i, j int) bool {
		return earlyTerminations[i].Epoch < earlyTerminations[j].Epoch
	})

	return earlyTerminations, !noEarlyTerminations, nil
}

func (dl *Deadline) AddPoStSubmissions(idxs []uint64) {
	for _, pIdx := range idxs {
		dl.PostSubmissions.Set(pIdx)
	}
}

// Returns nil if nothing was popped.
func (dl *Deadline) popExpiredPartitions(store adt.Store, until abi.ChainEpoch, quant QuantSpec) (*abi.BitField, error) {
	expirations, err := LoadBitfieldQueue(store, dl.ExpirationsEpochs, quant)
	if err != nil {
		return nil, err
	}

	popped, modified, err := expirations.PopUntil(until)
	if err != nil {
		return nil, xerrors.Errorf("failed to pop expiring partitions: %w", err)
	}

	if modified {
		dl.ExpirationsEpochs, err = expirations.Root()
		if err != nil {
			return nil, err
		}
	}

	return popped, nil
}
