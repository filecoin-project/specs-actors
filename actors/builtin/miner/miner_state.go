package miner

import (
	"fmt"
	"reflect"
	"sort"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	cid "github.com/ipfs/go-cid"
	errors "github.com/pkg/errors"
	xerrors "golang.org/x/xerrors"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	. "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

// Balance of Miner Actor should be greater than or equal to
// the sum of PreCommitDeposits and LockedFunds.
// It is possible for balance to fall below the sum of
// PCD, LF and InitialPledgeRequirements, and this is a bad
// state (IP Debt) that limits a miner actor's behavior (i.e. no balance withdrawals)
// Excess balance as computed by st.GetAvailableBalance will be
// withdrawable or usable for pre-commit deposit or pledge lock-up.
type State struct {
	// Information not related to sectors.
	Info cid.Cid

	PreCommitDeposits abi.TokenAmount // Total funds locked as PreCommitDeposits
	LockedFunds       abi.TokenAmount // Total rewards and added funds locked in vesting table

	VestingFunds cid.Cid // Array, AMT[ChainEpoch]TokenAmount

	InitialPledgeRequirement abi.TokenAmount // Sum of initial pledge requirements of all active sectors

	// Sectors that have been pre-committed but not yet proven.
	PreCommittedSectors cid.Cid // Map, HAMT[SectorNumber]SectorPreCommitOnChainInfo

	// Information for all proven and not-yet-expired sectors.
	Sectors cid.Cid // Array, AMT[SectorNumber]SectorOnChainInfo (sparse)

	// The first epoch in this miner's current proving period. This is the first epoch in which a PoSt for a
	// partition at the miner's first deadline may arrive. Alternatively, it is after the last epoch at which
	// a PoSt for the previous window is valid.
	// Always greater than zero, this may be greater than the current epoch for genesis miners in the first
	// WPoStProvingPeriod epochs of the chain; the epochs before the first proving period starts are exempt from Window
	// PoSt requirements.
	// Updated at the end of every period by a cron callback.
	ProvingPeriodStart abi.ChainEpoch

	// Index of the deadline within the proving period beginning at ProvingPeriodStart that has not yet been
	// finalized.
	// Updated at the end of each deadline window by a cron callback.
	CurrentDeadline uint64

	// The sector numbers due for PoSt at each deadline in the current proving period, frozen at period start.
	// New sectors are added and expired ones removed at proving period boundary.
	// Faults are not subtracted from this in state, but on the fly.
	Deadlines cid.Cid

	// Deadlines with outstanding fees for early sector termination.
	EarlyTerminations *bitfield.BitField

	// Memoized power information
	FaultyPower PowerPair
}

type MinerInfo struct {
	// Account that owns this miner.
	// - Income and returned collateral are paid to this address.
	// - This address is also allowed to change the worker address for the miner.
	Owner addr.Address // Must be an ID-address.

	// Worker account for this miner.
	// The associated pubkey-type address is used to sign blocks and messages on behalf of this miner.
	Worker addr.Address // Must be an ID-address.

	PendingWorkerKey *WorkerKeyChange

	// Byte array representing a Libp2p identity that should be used when connecting to this miner.
	PeerId abi.PeerID

	// Slice of byte arrays representing Libp2p multi-addresses used for establishing a connection with this miner.
	Multiaddrs []abi.Multiaddrs

	// The proof type used by this miner for sealing sectors.
	SealProofType abi.RegisteredSealProof

	// Amount of space in each sector committed by this miner.
	// This is computed from the proof type and represented here redundantly.
	SectorSize abi.SectorSize

	// The number of sectors in each Window PoSt partition (proof).
	// This is computed from the proof type and represented here redundantly.
	WindowPoStPartitionSectors uint64
}

type WorkerKeyChange struct {
	NewWorker   addr.Address // Must be an ID address
	EffectiveAt abi.ChainEpoch
}

// Information provided by a miner when pre-committing a sector.
type SectorPreCommitInfo struct {
	SealProof       abi.RegisteredSealProof
	SectorNumber    abi.SectorNumber
	SealedCID       cid.Cid `checked:"true"` // CommR
	SealRandEpoch   abi.ChainEpoch
	DealIDs         []abi.DealID
	Expiration      abi.ChainEpoch
	ReplaceCapacity bool // Whether to replace a "committed capacity" no-deal sector (requires non-empty DealIDs)
	// The committed capacity sector to replace, and it's deadline/partition location
	ReplaceSectorDeadline  uint64
	ReplaceSectorPartition uint64
	ReplaceSectorNumber    abi.SectorNumber
}

// Information stored on-chain for a pre-committed sector.
type SectorPreCommitOnChainInfo struct {
	Info               SectorPreCommitInfo
	PreCommitDeposit   abi.TokenAmount
	PreCommitEpoch     abi.ChainEpoch
	DealWeight         abi.DealWeight // Integral of active deals over sector lifetime
	VerifiedDealWeight abi.DealWeight // Integral of active verified deals over sector lifetime
}

// Information stored on-chain for a proven sector.
type SectorOnChainInfo struct {
	SectorNumber       abi.SectorNumber
	SealProof          abi.RegisteredSealProof // The seal proof type implies the PoSt proof/s
	SealedCID          cid.Cid                 // CommR
	DealIDs            []abi.DealID
	Activation         abi.ChainEpoch  // Epoch during which the sector proof was accepted
	Expiration         abi.ChainEpoch  // Epoch during which the sector expires
	DealWeight         abi.DealWeight  // Integral of active deals over sector lifetime
	VerifiedDealWeight abi.DealWeight  // Integral of active verified deals over sector lifetime
	InitialPledge      abi.TokenAmount // Pledge collected to commit this sector
	ExpectedDayReward  abi.TokenAmount // Expect daily reward for sector computed at activation time
}

// Location of a specific sector
type SectorLocation struct {
	Deadline, Partition uint64
	SectorNumber        abi.SectorNumber
}

type SectorStatus int

const (
	SectorNotFound SectorStatus = iota
	SectorFaulty
	SectorTerminated
	SectorHealthy
)

func ConstructState(infoCid cid.Cid, periodStart abi.ChainEpoch, emptyArrayCid, emptyMapCid, emptyDeadlinesCid cid.Cid) (*State, error) {
	return &State{
		Info: infoCid,

		PreCommitDeposits:        abi.NewTokenAmount(0),
		LockedFunds:              abi.NewTokenAmount(0),
		VestingFunds:             emptyArrayCid,
		InitialPledgeRequirement: abi.NewTokenAmount(0),

		PreCommittedSectors: emptyMapCid,
		Sectors:             emptyArrayCid,
		ProvingPeriodStart:  periodStart,
		CurrentDeadline:     0,
		Deadlines:           emptyDeadlinesCid,

		FaultyPower:       NewPowerPairZero(),
		EarlyTerminations: abi.NewBitField(),
	}, nil
}

func ConstructMinerInfo(owner addr.Address, worker addr.Address, pid []byte, multiAddrs [][]byte, sealProofType abi.RegisteredSealProof) (*MinerInfo, error) {

	sectorSize, err := sealProofType.SectorSize()
	if err != nil {
		return nil, err
	}

	partitionSectors, err := sealProofType.WindowPoStPartitionSectors()
	if err != nil {
		return nil, err
	}
	return &MinerInfo{
		Owner:                      owner,
		Worker:                     worker,
		PendingWorkerKey:           nil,
		PeerId:                     pid,
		Multiaddrs:                 multiAddrs,
		SealProofType:              sealProofType,
		SectorSize:                 sectorSize,
		WindowPoStPartitionSectors: partitionSectors,
	}, nil
}

func (st *State) GetInfo(store adt.Store) (*MinerInfo, error) {
	var info MinerInfo
	if err := store.Get(store.Context(), st.Info, &info); err != nil {
		return nil, xerrors.Errorf("failed to get miner info %w", err)
	}
	return &info, nil
}

func (st *State) SaveInfo(store adt.Store, info *MinerInfo) error {
	c, err := store.Put(store.Context(), info)
	if err != nil {
		return err
	}
	st.Info = c
	return nil
}

// Returns deadline calculations for the current (according to state) proving period.
func (st *State) DeadlineInfo(currEpoch abi.ChainEpoch) *DeadlineInfo {
	return NewDeadlineInfo(st.ProvingPeriodStart, st.CurrentDeadline, currEpoch)
}

func (st *State) GetSectorCount(store adt.Store) (uint64, error) {
	arr, err := adt.AsArray(store, st.Sectors)
	if err != nil {
		return 0, err
	}

	return arr.Length(), nil
}

func (st *State) GetMaxAllowedFaults(store adt.Store) (uint64, error) {
	sectorCount, err := st.GetSectorCount(store)
	if err != nil {
		return 0, err
	}
	return 2 * sectorCount, nil
}

func (st *State) PutPrecommittedSector(store adt.Store, info *SectorPreCommitOnChainInfo) error {
	precommitted, err := adt.AsMap(store, st.PreCommittedSectors)
	if err != nil {
		return err
	}

	err = precommitted.Put(SectorKey(info.Info.SectorNumber), info)
	if err != nil {
		return errors.Wrapf(err, "failed to store precommitment for %v", info)
	}
	st.PreCommittedSectors, err = precommitted.Root()
	return err
}

func (st *State) GetPrecommittedSector(store adt.Store, sectorNo abi.SectorNumber) (*SectorPreCommitOnChainInfo, bool, error) {
	precommitted, err := adt.AsMap(store, st.PreCommittedSectors)
	if err != nil {
		return nil, false, err
	}

	var info SectorPreCommitOnChainInfo
	found, err := precommitted.Get(SectorKey(sectorNo), &info)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to load precommitment for %v", sectorNo)
	}
	return &info, found, nil
}

// This method gets and returns the requested pre-committed sectors, skipping
// missing sectors.
func (st *State) FindPrecommittedSectors(store adt.Store, sectorNos ...abi.SectorNumber) ([]*SectorPreCommitOnChainInfo, error) {
	precommitted, err := adt.AsMap(store, st.PreCommittedSectors)
	if err != nil {
		return nil, err
	}

	result := make([]*SectorPreCommitOnChainInfo, 0, len(sectorNos))

	for _, sectorNo := range sectorNos {
		var info SectorPreCommitOnChainInfo
		found, err := precommitted.Get(SectorKey(sectorNo), &info)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to load precommitment for %v", sectorNo)
		}
		if !found {
			// TODO #564 log: "failed to get precommitted sector on sector %d, dropping from prove commit set"
			continue
		}
		result = append(result, &info)
	}

	return result, nil
}

func (st *State) DeletePrecommittedSectors(store adt.Store, sectorNos ...abi.SectorNumber) error {
	precommitted, err := adt.AsMap(store, st.PreCommittedSectors)
	if err != nil {
		return err
	}

	for _, sectorNo := range sectorNos {
		err = precommitted.Delete(SectorKey(sectorNo))
		if err != nil {
			return xerrors.Errorf("failed to delete precommitment for %v: %w", sectorNo, err)
		}
	}
	st.PreCommittedSectors, err = precommitted.Root()
	return err
}

func (st *State) HasSectorNo(store adt.Store, sectorNo abi.SectorNumber) (bool, error) {
	sectors, err := adt.AsArray(store, st.Sectors)
	if err != nil {
		return false, err
	}

	var info SectorOnChainInfo
	found, err := sectors.Get(uint64(sectorNo), &info)
	if err != nil {
		return false, xerrors.Errorf("failed to get sector %v: %w", sectorNo, err)
	}
	return found, nil
}

func (st *State) PutSectors(store adt.Store, newSectors ...*SectorOnChainInfo) error {
	sectors, err := adt.AsArray(store, st.Sectors)
	if err != nil {
		return xerrors.Errorf("failed to load sectors: %w", err)
	}

	for _, sector := range newSectors {
		if err := sectors.Set(uint64(sector.SectorNumber), sector); err != nil {
			return xerrors.Errorf("failed to put sector %v: %w", sector, err)
		}
	}

	if sectors.Length() > SectorsMax {
		return xerrors.Errorf("too many sectors")
	}

	st.Sectors, err = sectors.Root()
	if err != nil {
		return xerrors.Errorf("failed to persist sectors: %w", err)
	}
	return nil
}

func (st *State) GetSector(store adt.Store, sectorNo abi.SectorNumber) (*SectorOnChainInfo, bool, error) {
	sectors, err := adt.AsArray(store, st.Sectors)
	if err != nil {
		return nil, false, err
	}

	var info SectorOnChainInfo
	found, err := sectors.Get(uint64(sectorNo), &info)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to get sector %v", sectorNo)
	}
	return &info, found, nil
}

func (st *State) DeleteSectors(store adt.Store, sectorNos *abi.BitField) error {
	sectors, err := adt.AsArray(store, st.Sectors)
	if err != nil {
		return err
	}
	err = sectorNos.ForEach(func(sectorNo uint64) error {
		if err = sectors.Delete(sectorNo); err != nil {
			return errors.Wrapf(err, "failed to delete sector %v", sectorNos)
		}
		return nil
	})
	if err != nil {
		return err
	}

	st.Sectors, err = sectors.Root()
	return err
}

// Iterates sectors.
// The pointer provided to the callback is not safe for re-use. Copy the pointed-to value in full to hold a reference.
func (st *State) ForEachSector(store adt.Store, f func(*SectorOnChainInfo)) error {
	sectors, err := adt.AsArray(store, st.Sectors)
	if err != nil {
		return err
	}
	var sector SectorOnChainInfo
	return sectors.ForEach(&sector, func(idx int64) error {
		f(&sector)
		return nil
	})
}

func (st *State) FindSector(store adt.Store, sno abi.SectorNumber) (uint64, uint64, error) {
	deadlines, err := st.LoadDeadlines(store)
	if err != nil {
		return 0, 0, err
	}
	return FindSector(store, deadlines, sno)
}

// Walks the given sectors, deadline by deadline, partition by partition,
// skipping missing partitions/sectors.
func (st *State) WalkSectors(
	store adt.Store,
	locations []SectorLocation,
	beforeDeadlineCb func(dlIdx uint64, dl *Deadline) (update bool, err error),
	partitionCb func(dl *Deadline, partition *Partition, dlIdx, partIdx uint64, sectors *bitfield.BitField) (update bool, err error),
	afterDeadlineCb func(dlIdx uint64, dl *Deadline) (update bool, err error),
) error {
	deadlines, err := st.LoadDeadlines(store)
	if err != nil {
		return err
	}

	var toVisit [WPoStPeriodDeadlines]map[uint64][]uint64
	for _, loc := range locations {
		dl := toVisit[loc.Deadline]
		if dl == nil {
			dl = make(map[uint64][]uint64, 1)
			toVisit[loc.Deadline] = dl
		}
		dl[loc.Partition] = append(dl[loc.Partition], uint64(loc.SectorNumber))
	}

	var deadlinesUpdated bool
	for dlIdx, partitions := range toVisit {
		if partitions == nil {
			continue
		}

		var deadlineUpdated bool
		dl, err := deadlines.LoadDeadline(store, uint64(dlIdx))
		if err != nil {
			return err
		}

		deadlineUpdated, err = beforeDeadlineCb(uint64(dlIdx), dl)
		if err != nil {
			return err
		}

		partitionsArr, err := dl.PartitionsArray(store)
		if err != nil {
			return err
		}

		partitionNumbers := make([]uint64, 0, len(partitions))
		for partIdx := range partitions { // nolint:nomaprange // subsequently sorted
			partitionNumbers = append(partitionNumbers, partIdx)
		}
		sort.Slice(partitionNumbers, func(i, j int) bool {
			return partitionNumbers[i] < partitionNumbers[j]
		})

		for _, partIdx := range partitionNumbers {
			var partition Partition
			found, err := partitionsArr.Get(partIdx, &partition)
			if err != nil {
				return err
			}
			if !found {
				continue
			}

			sectorNos := partitions[partIdx]
			foundSectors, err := bitfield.IntersectBitField(bitfield.NewFromSet(sectorNos), partition.Sectors)
			if err != nil {
				return err
			}

			// Anything to update?
			if empty, err := foundSectors.IsEmpty(); err != nil {
				return err
			} else if empty {
				// no.
				continue
			}

			if updated, err := partitionCb(dl, &partition, uint64(dlIdx), partIdx, foundSectors); err != nil {
				return err
			} else if updated {
				err = partitionsArr.Set(partIdx, &partition)
				if err != nil {
					return err
				}

				deadlineUpdated = true
			}
		}
		if deadlineUpdated {
			dl.Partitions, err = partitionsArr.Root()
			if err != nil {
				return err
			}
		}
		if updated, err := afterDeadlineCb(uint64(dlIdx), dl); err != nil {
			return err
		} else if updated || deadlineUpdated {
			if err := deadlines.UpdateDeadline(store, uint64(dlIdx), dl); err != nil {
				return err
			}
			deadlinesUpdated = true
		}
	}
	if deadlinesUpdated {
		if err := st.SaveDeadlines(store, deadlines); err != nil {
			return err
		}
	}
	return nil
}

// Schedule's each sector to expire at its next deadline end.
//
// If it can't find any given sector, it skips it.
//
// This method assumes that each sector's power has not changed, despite the rescheduling.
//
// TODO: distinguish bad arguments from invalid state https://github.com/filecoin-project/specs-actors/issues/597
func (st *State) RescheduleSectorExpirations(store adt.Store, currEpoch abi.ChainEpoch, sectors []SectorLocation,
	ssize abi.SectorSize, quant QuantSpec) error {
	var (
		newEpoch              abi.ChainEpoch
		rescheduledPartitions []uint64 // track partitions with moved expirations.
	)
	return st.WalkSectors(store, sectors,
		// Prepare to process deadline.
		func(dlIdx uint64, dl *Deadline) (bool, error) {
			rescheduledPartitions = nil

			// Figure out the sector's next deadline end.
			dlInfo := NewDeadlineInfo(st.ProvingPeriodStart, dlIdx, currEpoch).NextNotElapsed()
			newEpoch = dlInfo.Last()
			return false, nil
		},
		// Process partitions in deadline.
		func(dl *Deadline, partition *Partition, dlIdx, partIdx uint64, sectors *bitfield.BitField) (bool, error) {
			// Sectors are uniquified by being represented in a bitfield.
			// This is an important property for the reschedule logic to be correct.
			live, err := bitfield.SubtractBitField(sectors, partition.Terminated)
			if err != nil {
				return false, err
			}

			active, err := bitfield.SubtractBitField(live, partition.Faults)
			if err != nil {
				return false, err
			}

			sectorInfos, err := st.LoadSectorInfos(store, active)
			if err != nil {
				return false, err
			}

			if len(sectorInfos) == 0 {
				// Nothing to do
				return false, nil
			}

			// Note: The expiration stored in the sector info is not altered, but remains the initially-scheduled epoch.
			err = partition.RescheduleExpirations(store, newEpoch, sectorInfos, ssize, quant)
			if err != nil {
				return false, err
			}

			// Make sure we update the deadline's queue as well.
			rescheduledPartitions = append(rescheduledPartitions, partIdx)
			return true, nil
		},
		// Update deadline.
		func(dlIdx uint64, dl *Deadline) (bool, error) {
			if len(rescheduledPartitions) == 0 {
				return false, nil
			}

			// Record partitions that now expire at the new epoch.
			// Don't _remove_ anything from this queue, that's not safe.
			if err := dl.AddExpirationPartitions(store, newEpoch, rescheduledPartitions, quant); err != nil {
				return false, xerrors.Errorf("failed to add partition expirations: %w", err)
			}
			return true, nil
		},
	)
}

// Assign new sectors to deadlines.
func (st *State) AssignSectorsToDeadlines(
	store adt.Store,
	currentEpoch abi.ChainEpoch,
	sectors []*SectorOnChainInfo,
	partitionSize uint64,
	sectorSize abi.SectorSize,
	quant QuantSpec,
) (PowerPair, error) {
	deadlines, err := st.LoadDeadlines(store)
	if err != nil {
		return NewPowerPairZero(), err
	}

	// Sort sectors by number to get better runs in partition bitfields.
	sort.Slice(sectors, func(i, j int) bool {
		return sectors[i].SectorNumber < sectors[j].SectorNumber
	})

	var deadlineArr [WPoStPeriodDeadlines]*Deadline
	err = deadlines.ForEach(store, func(idx uint64, dl *Deadline) error {
		// Skip deadlines that aren't currently mutable.
		if deadlineIsMutable(st.ProvingPeriodStart, idx, currentEpoch) {
			deadlineArr[int(idx)] = dl
		}
		return nil
	})
	if err != nil {
		return NewPowerPairZero(), err
	}

	newPower := NewPowerPairZero()
	for dlIdx, newPartitions := range assignDeadlines(partitionSize, &deadlineArr, sectors) {
		if len(newPartitions) == 0 {
			continue
		}

		dl := deadlineArr[dlIdx]

		deadlineNewPower, err := dl.AddSectors(store, partitionSize, newPartitions, sectorSize, quant)
		if err != nil {
			return NewPowerPairZero(), err
		}

		newPower = newPower.Add(deadlineNewPower)

		err = deadlines.UpdateDeadline(store, uint64(dlIdx), dl)
		if err != nil {
			return NewPowerPairZero(), err
		}
	}

	err = st.SaveDeadlines(store, deadlines)
	if err != nil {
		return NewPowerPairZero(), err
	}
	return newPower, nil
}

// Pops up to max early terminated sectors from all deadlines.
//
// Returns hasMore if we still have more early terminations to process.
func (st *State) PopEarlyTerminations(store adt.Store, maxPartitions, maxSectors uint64) (result TerminationResult, hasMore bool, err error) {
	stopErr := errors.New("stop error")

	// Anything to do? This lets us avoid loading the deadlines if there's nothing to do.
	noEarlyTerminations, err := st.EarlyTerminations.IsEmpty()
	if err != nil {
		return TerminationResult{}, false, xerrors.Errorf("failed to count deadlines with early terminations: %w", err)
	} else if noEarlyTerminations {
		return TerminationResult{}, false, nil
	}

	// Load deadlines
	deadlines, err := st.LoadDeadlines(store)
	if err != nil {
		return TerminationResult{}, false, xerrors.Errorf("failed to load deadlines: %w", err)
	}

	// Process early terminations.
	if err = st.EarlyTerminations.ForEach(func(dlIdx uint64) error {
		// Load deadline + partitions.
		dl, err := deadlines.LoadDeadline(store, dlIdx)
		if err != nil {
			return xerrors.Errorf("failed to load deadline %d: %w", dlIdx, err)
		}

		deadlineResult, more, err := dl.PopEarlyTerminations(store, maxPartitions-result.PartitionsProcessed, maxSectors-result.SectorsProcessed)
		if err != nil {
			return xerrors.Errorf("failed to pop early terminations for deadline %d: %w", dlIdx, err)
		}

		err = result.Add(deadlineResult)
		if err != nil {
			return xerrors.Errorf("failed to merge result from popping early terminations from deadline: %w", err)
		}

		if !more {
			// safe to do while iterating.
			st.EarlyTerminations.Unset(dlIdx)
		}

		// Save the deadline
		err = deadlines.UpdateDeadline(store, dlIdx, dl)
		if err != nil {
			return xerrors.Errorf("failed to store deadline %d: %w", dlIdx, err)
		}

		if result.BelowLimit(maxPartitions, maxSectors) {
			return nil
		}

		return stopErr
	}); err != nil && err != stopErr {
		return TerminationResult{}, false, xerrors.Errorf("failed to walk early terminations bitfield for deadlines: %w", err)
	}

	// Save back the deadlines.
	err = st.SaveDeadlines(store, deadlines)
	if err != nil {
		return TerminationResult{}, false, xerrors.Errorf("failed to save deadlines: %w", err)
	}

	// Ok, check to see if we've handled all early terminations.
	noEarlyTerminations, err = st.EarlyTerminations.IsEmpty()
	if err != nil {
		return TerminationResult{}, false, xerrors.Errorf("failed to count remaining early terminations deadlines")
	}

	return result, !noEarlyTerminations, nil
}

// Returns a sector's status (healthy, faulty, missing, not found, terminated)
func (st *State) SectorStatus(store adt.Store, dlIdx, pIdx uint64, sector abi.SectorNumber) (SectorStatus, error) {
	dls, err := st.LoadDeadlines(store)
	if err != nil {
		return SectorNotFound, err
	}

	// Pre-check this because LoadDeadline will return an actual error.
	if dlIdx >= WPoStPeriodDeadlines {
		return SectorNotFound, nil
	}

	dl, err := dls.LoadDeadline(store, dlIdx)
	if err != nil {
		return SectorNotFound, err
	}

	// TODO: distinguish partition not found from state errors in exit code
	// https://github.com/filecoin-project/specs-actors/issues/597
	partition, err := dl.LoadPartition(store, pIdx)
	if err != nil {
		return SectorNotFound, xerrors.Errorf("in deadline %d: %w", dlIdx, err)
	}

	if exists, err := partition.Sectors.IsSet(uint64(sector)); err != nil {
		return SectorNotFound, err
	} else if !exists {
		return SectorNotFound, nil
	}

	if faulty, err := partition.Faults.IsSet(uint64(sector)); err != nil {
		return SectorNotFound, err
	} else if faulty {
		return SectorFaulty, nil
	}

	if terminated, err := partition.Terminated.IsSet(uint64(sector)); err != nil {
		return SectorNotFound, err
	} else if terminated {
		return SectorTerminated, nil
	}

	return SectorHealthy, nil
}

// Loads sector info for a sequence of sectors.
func (st *State) LoadSectorInfos(store adt.Store, sectors *abi.BitField) ([]*SectorOnChainInfo, error) {
	sectorsArr, err := adt.AsArray(store, st.Sectors)
	if err != nil {
		return nil, err
	}

	var sectorInfos []*SectorOnChainInfo
	err = sectors.ForEach(func(i uint64) error {
		var sectorOnChain SectorOnChainInfo
		found, err := sectorsArr.Get(i, &sectorOnChain)
		if err != nil {
			return xerrors.Errorf("failed to load sector %v: %w", abi.SectorNumber(i), err)
		} else if !found {
			return fmt.Errorf("can't find sector %d", i)
		}
		sectorInfos = append(sectorInfos, &sectorOnChain)
		return nil
	})
	return sectorInfos, err
}

// Loads info for a set of sectors to be proven.
// If any of the sectors are declared faulty and not to be recovered, info for the first non-faulty sector is substituted instead.
// If any of the sectors are declared recovered, they are returned from this method.
func (st *State) LoadSectorInfosForProof(store adt.Store, provenSectors, expectedFaults *abi.BitField) ([]*SectorOnChainInfo, error) {
	nonFaults, err := bitfield.SubtractBitField(provenSectors, expectedFaults)
	if err != nil {
		return nil, xerrors.Errorf("failed to diff bitfields: %w", err)
	}

	// Return empty if no non-faults
	if empty, err := nonFaults.IsEmpty(); err != nil {
		return nil, xerrors.Errorf("failed to check if bitfield was empty: %w", err)
	} else if empty {
		return nil, nil
	}

	// Select a non-faulty sector as a substitute for faulty ones.
	goodSectorNo, err := nonFaults.First()
	if err != nil {
		return nil, xerrors.Errorf("failed to get first good sector: %w", err)
	}

	// Load sector infos
	sectorInfos, err := st.LoadSectorInfosWithFaultMask(store, provenSectors, expectedFaults, abi.SectorNumber(goodSectorNo))
	if err != nil {
		return nil, xerrors.Errorf("failed to load sector infos: %w", err)
	}
	return sectorInfos, nil
}

// Loads sector info for a sequence of sectors, substituting info for a stand-in sector for any that are faulty.
func (st *State) LoadSectorInfosWithFaultMask(store adt.Store, sectors *abi.BitField, faults *abi.BitField, faultStandIn abi.SectorNumber) ([]*SectorOnChainInfo, error) {
	sectorArr, err := adt.AsArray(store, st.Sectors)
	if err != nil {
		return nil, xerrors.Errorf("failed to load sectors array: %w", err)
	}
	var standInInfo SectorOnChainInfo
	found, err := sectorArr.Get(uint64(faultStandIn), &standInInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to load stand-in sector %d: %v", faultStandIn, err)
	} else if !found {
		return nil, fmt.Errorf("can't find stand-in sector %d", faultStandIn)
	}

	// Expand faults into a map for quick lookups.
	// The faults bitfield should already be a subset of the sectors bitfield.
	sectorCount, err := sectors.Count()
	if err != nil {
		return nil, err
	}
	faultSet, err := faults.AllMap(sectorCount)
	if err != nil {
		return nil, fmt.Errorf("failed to expand faults: %w", err)
	}

	// Load the sector infos, masking out fault sectors with a good one.
	sectorInfos := make([]*SectorOnChainInfo, 0, sectorCount)
	err = sectors.ForEach(func(i uint64) error {
		sector := &standInInfo
		faulty := faultSet[i]
		if !faulty {
			var sectorOnChain SectorOnChainInfo
			found, err := sectorArr.Get(i, &sectorOnChain)
			if err != nil {
				return xerrors.Errorf("failed to load sector %d: %w", i, err)
			} else if !found {
				return fmt.Errorf("can't find sector %d", i)
			}
			sector = &sectorOnChain
		}
		sectorInfos = append(sectorInfos, sector)
		return nil
	})
	return sectorInfos, err
}

func (st *State) LoadDeadlines(store adt.Store) (*Deadlines, error) {
	var deadlines Deadlines
	if err := store.Get(store.Context(), st.Deadlines, &deadlines); err != nil {
		return nil, fmt.Errorf("failed to load deadlines (%s): %w", st.Deadlines, err)
	}

	return &deadlines, nil
}

func (st *State) SaveDeadlines(store adt.Store, deadlines *Deadlines) error {
	c, err := store.Put(store.Context(), deadlines)
	if err != nil {
		return err
	}
	st.Deadlines = c
	return nil
}

//
// Funds and vesting
//

func (st *State) AddPreCommitDeposit(amount abi.TokenAmount) {
	newTotal := big.Add(st.PreCommitDeposits, amount)
	AssertMsg(newTotal.GreaterThanEqual(big.Zero()), "negative pre-commit deposit %s after adding %s to prior %s",
		newTotal, amount, st.PreCommitDeposits)
	st.PreCommitDeposits = newTotal
}

func (st *State) AddInitialPledgeRequirement(amount abi.TokenAmount) {
	newTotal := big.Add(st.InitialPledgeRequirement, amount)
	AssertMsg(newTotal.GreaterThanEqual(big.Zero()), "negative initial pledge requirement %s after adding %s to prior %s",
		newTotal, amount, st.InitialPledgeRequirement)
	st.InitialPledgeRequirement = newTotal
}

func (st *State) AddLockedFunds(store adt.Store, currEpoch abi.ChainEpoch, vestingSum abi.TokenAmount, spec *VestSpec) error {
	AssertMsg(vestingSum.GreaterThanEqual(big.Zero()), "negative vesting sum %s", vestingSum)
	vestingFunds, err := adt.AsArray(store, st.VestingFunds)
	if err != nil {
		return err
	}

	// Quantization is aligned with when regular cron will be invoked, in the last epoch of deadlines.
	vestBegin := currEpoch + spec.InitialDelay // Nothing unlocks here, this is just the start of the clock.
	vestPeriod := big.NewInt(int64(spec.VestPeriod))
	vestedSoFar := big.Zero()
	for e := vestBegin + spec.StepDuration; vestedSoFar.LessThan(vestingSum); e += spec.StepDuration {
		vestEpoch := quantizeUp(e, spec.Quantization, st.ProvingPeriodStart)
		elapsed := vestEpoch - vestBegin

		targetVest := big.Zero() //nolint:ineffassign
		if elapsed < spec.VestPeriod {
			// Linear vesting, PARAM_FINISH
			targetVest = big.Div(big.Mul(vestingSum, big.NewInt(int64(elapsed))), vestPeriod)
		} else {
			targetVest = vestingSum
		}

		vestThisTime := big.Sub(targetVest, vestedSoFar)
		vestedSoFar = targetVest

		// Load existing entry, else set a new one
		key := EpochKey(vestEpoch)
		lockedFundEntry := big.Zero()
		_, err = vestingFunds.Get(key, &lockedFundEntry)
		if err != nil {
			return err
		}

		lockedFundEntry = big.Add(lockedFundEntry, vestThisTime)
		err = vestingFunds.Set(key, &lockedFundEntry)
		if err != nil {
			return err
		}
	}

	st.VestingFunds, err = vestingFunds.Root()
	if err != nil {
		return err
	}
	st.LockedFunds = big.Add(st.LockedFunds, vestingSum)

	return nil
}

// PenalizeFundsInPriorityOrder first unlocks unvested funds from the vesting table.
// If the target is not yet hit it deducts funds from the (new) available balance.
// Returns the amount unlocked from the vesting table and the amount taken from current balance.
// If the penalty exceeds the total amount available in the vesting table and unlocked funds
// the penalty is reduced to match.  This must be fixed when handling bankrupcy:
// https://github.com/filecoin-project/specs-actors/issues/627
func (st *State) PenalizeFundsInPriorityOrder(store adt.Store, currEpoch abi.ChainEpoch, target, unlockedBalance abi.TokenAmount) (fromVesting abi.TokenAmount, fromBalance abi.TokenAmount, err error) {
	fromVesting, err = st.UnlockUnvestedFunds(store, currEpoch, target)
	if err != nil {
		return abi.NewTokenAmount(0), abi.NewTokenAmount(0), err
	}
	if fromVesting.Equals(target) {
		return fromVesting, abi.NewTokenAmount(0), nil
	}

	// unlocked funds were just deducted from available, so track that
	remaining := big.Sub(target, fromVesting)

	fromBalance = big.Min(unlockedBalance, remaining)
	return fromVesting, fromBalance, nil
}

// Unlocks an amount of funds that have *not yet vested*, if possible.
// The soonest-vesting entries are unlocked first.
// Returns the amount actually unlocked.
func (st *State) UnlockUnvestedFunds(store adt.Store, currEpoch abi.ChainEpoch, target abi.TokenAmount) (abi.TokenAmount, error) {
	vestingFunds, err := adt.AsArray(store, st.VestingFunds)
	if err != nil {
		return big.Zero(), err
	}

	amountUnlocked := abi.NewTokenAmount(0)
	lockedEntry := abi.NewTokenAmount(0)
	var toDelete []uint64
	var finished = fmt.Errorf("finished")

	// Iterate vestingFunds are in order of release.
	err = vestingFunds.ForEach(&lockedEntry, func(k int64) error {
		if amountUnlocked.LessThan(target) {
			if k >= int64(currEpoch) {
				unlockAmount := big.Min(big.Sub(target, amountUnlocked), lockedEntry)
				amountUnlocked = big.Add(amountUnlocked, unlockAmount)
				lockedEntry = big.Sub(lockedEntry, unlockAmount)

				if lockedEntry.IsZero() {
					toDelete = append(toDelete, uint64(k))
				} else {
					if err = vestingFunds.Set(uint64(k), &lockedEntry); err != nil {
						return err
					}
				}
			}
			return nil
		} else {
			return finished
		}
	})

	if err != nil && err != finished {
		return big.Zero(), err
	}

	err = vestingFunds.BatchDelete(toDelete)
	if err != nil {
		return big.Zero(), errors.Wrapf(err, "failed to delete locked fund during slash: %v", err)
	}

	st.LockedFunds = big.Sub(st.LockedFunds, amountUnlocked)
	Assert(st.LockedFunds.GreaterThanEqual(big.Zero()))
	st.VestingFunds, err = vestingFunds.Root()
	if err != nil {
		return big.Zero(), err
	}

	return amountUnlocked, nil
}

// Unlocks all vesting funds that have vested before the provided epoch.
// Returns the amount unlocked.
func (st *State) UnlockVestedFunds(store adt.Store, currEpoch abi.ChainEpoch) (abi.TokenAmount, error) {
	vestingFunds, err := adt.AsArray(store, st.VestingFunds)
	if err != nil {
		return big.Zero(), err
	}

	amountUnlocked := abi.NewTokenAmount(0)
	lockedEntry := abi.NewTokenAmount(0)
	var toDelete []uint64
	var finished = fmt.Errorf("finished")

	// Iterate vestingFunds  in order of release.
	err = vestingFunds.ForEach(&lockedEntry, func(k int64) error {
		if k < int64(currEpoch) {
			amountUnlocked = big.Add(amountUnlocked, lockedEntry)
			toDelete = append(toDelete, uint64(k))
		} else {
			return finished // stop iterating
		}
		return nil
	})

	if err != nil && err != finished {
		return big.Zero(), err
	}

	err = vestingFunds.BatchDelete(toDelete)
	if err != nil {
		return big.Zero(), errors.Wrapf(err, "failed to delete locked fund during vest: %v", err)
	}

	st.LockedFunds = big.Sub(st.LockedFunds, amountUnlocked)
	Assert(st.LockedFunds.GreaterThanEqual(big.Zero()))
	st.VestingFunds, err = vestingFunds.Root()
	if err != nil {
		return big.Zero(), err
	}

	return amountUnlocked, nil
}

// CheckVestedFunds returns the amount of vested funds that have vested before the provided epoch.
func (st *State) CheckVestedFunds(store adt.Store, currEpoch abi.ChainEpoch) (abi.TokenAmount, error) {
	vestingFunds, err := adt.AsArray(store, st.VestingFunds)
	if err != nil {
		return big.Zero(), err
	}

	amountUnlocked := abi.NewTokenAmount(0)
	lockedEntry := abi.NewTokenAmount(0)
	var finished = fmt.Errorf("finished")

	// Iterate vestingFunds  in order of release.
	err = vestingFunds.ForEach(&lockedEntry, func(k int64) error {
		if k < int64(currEpoch) {
			amountUnlocked = big.Add(amountUnlocked, lockedEntry)
		} else {
			return finished // stop iterating
		}
		return nil
	})
	if err != nil && err != finished {
		return big.Zero(), err
	}

	return amountUnlocked, nil
}

// Unclaimed funds that are not locked -- includes funds used to cover initial pledge requirement
func (st *State) GetUnlockedBalance(actorBalance abi.TokenAmount) abi.TokenAmount {
	unlockedBalance := big.Subtract(actorBalance, st.LockedFunds, st.PreCommitDeposits)
	Assert(unlockedBalance.GreaterThanEqual(big.Zero()))
	return unlockedBalance
}

// Unclaimed funds.  Actor balance - (locked funds, precommit deposit, ip requirement)
// Can go negative if the miner is in IP debt
func (st *State) GetAvailableBalance(actorBalance abi.TokenAmount) abi.TokenAmount {
	availableBalance := st.GetUnlockedBalance(actorBalance)
	return big.Sub(availableBalance, st.InitialPledgeRequirement)
}

// Returns a quantization spec that quantizes values to the last epoch in each deadline.
func (st *State) QuantEndOfDeadline() QuantSpec {
	// Proving period start is the first epoch of the first deadline, so we want values that are earlier by one.
	return NewQuantSpec(WPoStChallengeWindow, st.ProvingPeriodStart-1)
}

func (st *State) AssertBalanceInvariants(balance abi.TokenAmount) {
	Assert(st.PreCommitDeposits.GreaterThanEqual(big.Zero()))
	Assert(st.LockedFunds.GreaterThanEqual(big.Zero()))
	Assert(balance.GreaterThanEqual(big.Sum(st.PreCommitDeposits, st.LockedFunds)))
}

func (st *State) MeetsInitialPledgeCondition(balance abi.TokenAmount) bool {
	available := st.GetUnlockedBalance(balance)
	return available.GreaterThanEqual(st.InitialPledgeRequirement)
}

//
// Misc helpers
//

func SectorKey(e abi.SectorNumber) adt.Keyer {
	return adt.UIntKey(uint64(e))
}

func EpochKey(e abi.ChainEpoch) uint64 {
	return uint64(e)
}

func init() {
	// Check that ChainEpoch is indeed an unsigned integer to confirm that SectorKey is making the right interpretation.
	var e abi.SectorNumber
	if reflect.TypeOf(e).Kind() != reflect.Uint64 {
		panic("incorrect sector number encoding")
	}
}
