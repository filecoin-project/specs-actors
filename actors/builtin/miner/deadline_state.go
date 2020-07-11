package miner

import (
	"io"
	"sort"

	"github.com/filecoin-project/go-bitfield"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

// A bitfield of sector numbers due at each deadline.
// The sectors for each deadline are logically grouped into sequential partitions for proving.
type Deadlines struct {
	// TODO: Consider inlining part of the deadline struct (e.g.,
	// active/assigned sectors) to make sector assignment cheaper. At the
	// moment, assigning a sector requires loading all deadlines to figure
	// out where best to assign new sectors.
	Due [WPoStPeriodDeadlines]cid.Cid // []Deadline
}

type Deadline struct {
	// Partitions in this deadline, in order.
	// The keys of this AMT are always sequential integers beginning with zero.
	Partitions cid.Cid // AMT[PartitionNumber]Partition

	// Maps epochs to partitions with sectors that expire in that epoch, either on-time or early as faults.
	ExpirationsEpochs cid.Cid // AMT[ChainEpoch]BitField

	// Partitions numbers with PoSt submissions since the proving period started.
	PostSubmissions *abi.BitField

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
func (d *Deadline) AddExpirationPartitions(store adt.Store, expirationEpoch abi.ChainEpoch, partitions ...uint64) error {
	queue, err := loadUintQueue(store, d.ExpirationsEpochs)
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
func (dl *Deadline) PopExpiredSectors(store adt.Store, until abi.ChainEpoch) (*ExpirationSet, error) {
	expiredPartitions, err := dl.popExpiredPartitions(store, until)
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
	allActivePower := NewPowerPairZero()
	allFaultyPower := NewPowerPairZero()

	// For each partition with an expiry, remove and collect expirations from the partition queue.
	if err = expiredPartitions.ForEach(func(partIdx uint64) error {
		var partition Partition
		if found, err := partitions.Get(partIdx, &partition); err != nil {
			return err
		} else if !found {
			return xerrors.Errorf("missing expected partition %d", partIdx)
		}

		partExpiration, err := partition.PopExpiredSectors(store, until)
		if err != nil {
			return xerrors.Errorf("failed to pop expired sectors from partition %d: %w", partIdx, err)
		}

		onTimeSectors = append(onTimeSectors, partExpiration.OnTimeSectors)
		earlySectors = append(earlySectors, partExpiration.EarlySectors)
		allActivePower = allActivePower.Add(partExpiration.ActivePower)
		allFaultyPower = allFaultyPower.Add(partExpiration.FaultyPower)

		return partitions.Set(partIdx, &partition)
	}); err != nil {
		return nil, err
	}

	if dl.Partitions, err = partitions.Root(); err != nil {
		return nil, err
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

	return NewExpirationSet(allOnTimeSectors, allEarlySectors, allActivePower, allFaultyPower), nil
}

// Adds sectors to a deadline. It's the caller's responsibility to make sure
// that this deadline isn't currently "open" (i.e., being proved at this point
// in time).
func (dl *Deadline) AddSectors(
	store adt.Store,
	partitionSize uint64,
	sectorSize abi.SectorSize,
	sectors []*SectorOnChainInfo,
) error {
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

			//
			// Get/create partition to update.
			//

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

			//
			// Figure out which (if any) sectors we want to add to
			// this partition.
			//

			// See if there's room in this partition for more sectors.
			sectorCount, err := partition.Sectors.Count()
			if sectorCount >= partitionSize {
				continue
			}

			size := partitionSize - sectorCount
			if uint64(len(sectors)) < size {
				size = uint64(len(sectors))
			}

			partitionSectors := sectors[:size]
			sectors = sectors[size:]

			//
			// Add sectors to partition.
			//

			err = partition.AddSectors(store, partitionSectors, sectorSize)
			if err != nil {
				return err
			}

			//
			// Save partition back.
			//

			err = partitions.Set(partIdx, partition)
			if err != nil {
				return err
			}

			//
			// Record deadline -> partition mapping so we can later update the deadlines.
			//

			for _, sector := range partitionSectors {
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
		deadlineExpirations, err := loadUintQueue(store, dl.ExpirationsEpochs)
		if err != nil {
			return xerrors.Errorf("failed to load expiration epochs: %w", err)
		}

		// Update each epoch in-order to be deterministic.
		updatedEpochs := make([]abi.ChainEpoch, 0, len(partitionDeadlineUpdates))
		for epoch := range partitionDeadlineUpdates {
			updatedEpochs = append(updatedEpochs, epoch)
		}
		sort.Slice(updatedEpochs, func(i, j int) bool {
			return updatedEpochs[i] < updatedEpochs[j]
		})

		for _, epoch := range updatedEpochs {
			if err = deadlineExpirations.AddToQueueValues(epoch, partitionDeadlineUpdates[epoch]...); err != nil {
				return err
			}
		}

		if dl.ExpirationsEpochs, err = deadlineExpirations.Root(); err != nil {
			return err
		}
	}

	dl.LiveSectors += uint64(len(sectors))

	return nil
}

// Returns nil if nothing was popped.
func (dl *Deadline) popExpiredPartitions(store adt.Store, until abi.ChainEpoch) (*abi.BitField, error) {
	expirations, err := loadUintQueue(store, dl.ExpirationsEpochs)
	if err != nil {
		return nil, err
	}

	popped, err := expirations.PopUntil(until)
	if err != nil {
		return nil, xerrors.Errorf("failed to pop expiring partitions: %w", err)
	}

	if popped != nil {
		dl.ExpirationsEpochs, err = expirations.Root()
		if err != nil {
			return nil, err
		}
	}

	return popped, nil
}

func (dl *Deadline) MarshalCBOR(w io.Writer) error {
	panic("implement me")
}

func (dl *Deadline) UnmarshalCBOR(r io.Reader) error {
	panic("implement me")
}
