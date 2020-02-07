package storage_miner

import (
	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
	errors "github.com/pkg/errors"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	storage_power "github.com/filecoin-project/specs-actors/actors/builtin/storage_power"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

// Balance of a StorageMinerActor should equal exactly the sum of PreCommit deposits
// that are not yet returned or burned.
type StorageMinerActorState struct {
	PreCommittedSectors cid.Cid // Map, HAMT[sectorNumber]SectorPreCommitOnChainInfo
	CommittedSet        cid.Cid // Array, AMT[]SectorOnChainInfo (sparse)
	ProvingSet          cid.Cid // Array, AMT[]SectorOnChainInfo (sparse)
	FaultSet            abi.BitField

	Info      MinerInfo // TODO: this needs to be a cid of the miner info struct
	PoStState MinerPoStState
}

type SetIdentifier int64

const (
	SetIdentifier_CommittedSet = SetIdentifier(0)
	SetIdentifier_ProvingSet   = SetIdentifier(1)
)

type MinerInfo struct {
	// Account that owns this miner.
	// - Income and returned collateral are paid to this address.
	// - This address is also allowed to change the worker address for the miner.
	Owner addr.Address // Must be an ID-address.

	// Worker account for this miner.
	// This will be the key that is used to sign blocks created by this miner, and
	// sign messages sent on behalf of this miner to commit sectors, submit PoSts, and
	// other day to day miner activities.

	// TODO: All addresses appearing on chain anywhere except the account actor are ID addresses
	// We want to enforce that the account actor referenced by this address carries a BLS key in its state.
	// This id -> pubkey lookup could be implemented now by asking the account actor for its pubkey address.
	Worker addr.Address // Must be an ID-address.

	PendingWorkerKey WorkerKeyChange

	// Libp2p identity that should be used when connecting to this miner.
	PeerId peer.ID

	// Amount of space in each sector committed to the network by this miner.
	SectorSize abi.SectorSize
}

type MinerPoStState struct {
	// Epoch of the last succesful PoSt, either election post or surprise post.
	LastSuccessfulPoSt abi.ChainEpoch

	// If >= 0 miner has been challenged and not yet responded successfully.
	// SurprisePoSt challenge state: The miner has not submitted timely ElectionPoSts,
	// and as a result, the system has fallen back to proving storage via SurprisePoSt.
	//  `epochUndefined` if not currently challeneged.
	SurpriseChallengeEpoch abi.ChainEpoch

	// True for challenged sector numbers when miner is challenged.
	ChallengedSectors cid.Cid // Map, HAMT[abi.SectorNumber]bool

	// Number of surprised post challenges that have been failed since last successful PoSt.
	// Indicates that the claimed storage power may not actually be proven. Recovery can proceed by
	// submitting a correct response to a subsequent SurprisePoSt challenge, up until
	// the limit of number of consecutive failures.
	NumConsecutiveFailures int64
}

type WorkerKeyChange struct {
	NewWorker   addr.Address // Must be an ID address
	EffectiveAt abi.ChainEpoch
}

type SectorPreCommitInfo struct {
	SectorNumber abi.SectorNumber
	SealedCID    cid.Cid // CommR
	SealEpoch    abi.ChainEpoch
	DealIDs      []abi.DealID
	Expiration   abi.ChainEpoch // Sector Expiration
}

type SectorPreCommitOnChainInfo struct {
	Info             SectorPreCommitInfo
	PreCommitDeposit abi.TokenAmount
	PreCommitEpoch   abi.ChainEpoch
}

type SectorOnChainInfo struct {
	Info                  SectorPreCommitInfo
	ActivationEpoch       abi.ChainEpoch // Epoch at which SectorProveCommit is accepted
	DeclaredFaultEpoch    abi.ChainEpoch // -1 if not currently declared faulted.
	DeclaredFaultDuration abi.ChainEpoch // -1 if not currently declared faulted.
	DealWeight            abi.DealWeight // integral of active deals over sector lifetime, 0 if CommittedCapacity sector
}

func ConstructState(store adt.Store, ownerAddr, workerAddr addr.Address, peerId peer.ID, sectorSize abi.SectorSize) (*StorageMinerActorState, error) {
	emptyMap, err := adt.MakeEmptyMap(store)
	if err != nil {
		return nil, err
	}

	emptyArray, err := adt.MakeEmptyArray(store)
	if err != nil {
		return nil, err
	}
	return &StorageMinerActorState{
		PreCommittedSectors: emptyMap.Root(),
		CommittedSet:        emptyArray.Root(),
		FaultSet:            abi.NewBitField(),
		ProvingSet:          emptyArray.Root(),
		Info: MinerInfo{
			Owner:            ownerAddr,
			Worker:           workerAddr,
			PendingWorkerKey: WorkerKeyChange{},
			PeerId:           peerId,
			SectorSize:       sectorSize,
		},
		PoStState: MinerPoStState{
			LastSuccessfulPoSt:     epochUndefined,
			SurpriseChallengeEpoch: epochUndefined,
			ChallengedSectors:      emptyMap.Root(),
			NumConsecutiveFailures: 0,
		},
	}, nil
}

func (st *StorageMinerActorState) getWorker() addr.Address {
	return st.Info.Worker
}

func (st *StorageMinerActorState) getSectorSize() abi.SectorSize {
	return st.Info.SectorSize
}

func (st *StorageMinerActorState) putPrecommittedSector(store adt.Store, info *SectorPreCommitOnChainInfo) error {
	precommitted := adt.AsMap(store, st.PreCommittedSectors)
	err := precommitted.Put(adt.IntKey(info.Info.SectorNumber), info)
	if err != nil {
		return errors.Wrapf(err, "failed to store precommitment for %v", info)
	}
	return nil
}

func (st *StorageMinerActorState) getPrecommittedSector(store adt.Store, sectorNo abi.SectorNumber) (*SectorPreCommitOnChainInfo, bool, error) {
	precommitted := adt.AsMap(store, st.PreCommittedSectors)
	var info SectorPreCommitOnChainInfo
	found, err := precommitted.Get(adt.IntKey(sectorNo), &info)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to load precommitment for %v", sectorNo)
	}
	return &info, found, nil
}

func (st *StorageMinerActorState) deletePrecommittedSector(store adt.Store, sectorNo abi.SectorNumber) error {
	precommitted := adt.AsMap(store, st.PreCommittedSectors)
	err := precommitted.Delete(adt.IntKey(sectorNo))
	if err != nil {
		return errors.Wrapf(err, "failed to delete precommitment for %v", sectorNo)
	}
	return nil
}

func (st *StorageMinerActorState) hasSectorNo(store adt.Store, ident SetIdentifier, sectorNo abi.SectorNumber) (bool, error) {
	sectors := getSet(ident)
	if sectors == nil {
		return false, errors.Wrapf(err, "invalid sector set requested %v", ident)
	}
	var info SectorOnChainInfo
	found, err := sectors.Get(uint64(sectorNo), &info)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get sector %v", sectorNo)
	}
	return found, nil
}

func (st *StorageMinerActorState) putSector(store adt.Store, ident SetIdentifier, sector *SectorOnChainInfo) error {
	sectors := getSet(ident)
	if sectors == nil {
		return errors.Wrapf(err, "invalid sector set requested %v", ident)
	}

	if err := sectors.Set(uint64(sector.Info.SectorNumber), sector); err != nil {
		return errors.Wrapf(err, "failed to put sector %v", sector)
	}
	return nil
}

func (st *StorageMinerActorState) getSectorFromSet(store adt.Store, ident SetIdentifier, sectorNo abi.SectorNumber) (*SectorOnChainInfo, bool, error) {
	sectors := getSet(ident)
	if sectors == nil {
		return nil, false, errors.Wrapf(err, "invalid sector set requested %v", ident)
	}

	var info SectorOnChainInfo
	found, err := sectors.Get(uint64(sectorNo), &info)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to get sector %v", sectorNo)
	}
	return &info, found, nil
}

func (st *StorageMinerActorState) getSector(store adt.Store, sectorNo abi.SectorNumber) (*SectorOnChainInfo, bool, SetIdentifier, error) {
	commSector, commFound, cErr := st.getSectorFromSet(store, SetIdentifier_CommittedSet, sectorNo)
	provSector, provFound, pErr := st.getSectorFromSet(store, SetIdentifier_CommittedSet, sectorNo)

	if cErr != nil {
		return nil, false, errors.Wrapf(cErr, "failed to get sector %v", sectorNo)
	}
	if pErr != nil {
		return nil, false, errors.Wrapf(pErr, "failed to get sector %v", sectorNo)
	}

	if commFound {
		return commSector, commFound, SetIdentifier_CommittedSet, nil
	}
	if provFound {
		return provSector, provFound, SetIdentifier_ProvingSet, nil
	}
	return nil, false, nil, nil
}

func (st *StorageMinerActorState) deleteSector(store adt.Store, ident SetIdentifier, sectorNo abi.SectorNumber) error {
	sectors := getSet(ident)
	if sectors == nil {
		return errors.Wrapf(err, "invalid sector set requested %v", ident)
	}

	if err := sectors.Delete(uint64(sectorNo)); err != nil {
		return errors.Wrapf(err, "failed to delete sector %v", sectorNo)
	}
	return nil
}

func (st *StorageMinerActorState) listSectors(store adt.Store, ident SetIdentifier) ([]abi.SectorNumber, error) {
	var ls []abi.SectorNumber
	err := st.forEachSector(store, ident, func(sector *SectorOnChainInfo) {
		ls = append(ls, sector.Info.SectorNumber)
	})

	if err != nil {
		return nil, err
	}
	return ls, nil
}

func (st *StorageMinerActorState) forEachSector(store adt.Store, ident SetIdentifier, f func(*SectorOnChainInfo)) error {
	sectors := getSet(ident)
	if sectors == nil {
		return errors.Wrapf(err, "invalid sector set requested %v", ident)
	}

	var sector SectorOnChainInfo
	return sectors.ForEach(&sector, func(idx int64) error {
		f(&sector)
		return nil
	})
}

func (st *StorageMinerActorState) GetStorageWeightDescForSector(store adt.Store, sectorNo abi.SectorNumber) (*storage_power.SectorStorageWeightDesc, error) {
	sectorInfo, found, _, err := st.getSector(store, sectorNo)
	if err != nil {
		return nil, err
	} else if !found {
		return nil, errors.Errorf("no such sector %v", sectorNo)
	}

	return asStorageWeightDesc(st.Info.SectorSize, sectorInfo), nil
}

// formatting helper
func asStorageWeightDesc(sectorSize abi.SectorSize, sectorInfo *SectorOnChainInfo) storage_power.SectorStorageWeightDesc {
	return storage_power.SectorStorageWeightDesc{
		SectorSize: sectorSize,
		DealWeight: sectorInfo.DealWeight,
		Duration:   sectorInfo.Info.Expiration - sectorInfo.ActivationEpoch,
	}
}

func getSet(ident SetIdentifier) *adt.Array {
	var sectors *adt.Array
	switch ident {
	case SetIdentifier_CommittedSet:
		sectors = adt.AsArray(store, st.CommittedSet)
	case SetIdentifier_ProvingSet:
		sectors = adt.AsArray(store, st.ProvingSet)
	case SetIdentifier_ChallengedSet:
		sectors = adt.AsArray(store, st.ChallengedSet)
	default:
		sectors = nil
	}
	return sectors
}

func (st *StorageMinerActorState) IsSectorInTemporaryFault(store adt.Store, sectorNo abi.SectorNumber) (bool, error) {
	sectorInfo, found, err := st.getSector(store, sectorNo)
	if err != nil {
		return false, err
	} else if !found {
		return false, errors.Errorf("no such sector %v", sectorNo)
	}
	ret, err := st.FaultSet.Has(uint64(sectorNo))
	AssertNoError(err)
	Assert(sectorInfo.DeclaredFaultEpoch != epochUndefined)
	Assert(sectorInfo.DeclaredFaultDuration != epochUndefined)
	return ret, nil
}

func (st *StorageMinerActorState) ComputeProvingSet() cid.Cid {
	// Current ProvingSet is a snapshot of the Sectors AMT subtracting sectors in the FaultSet
	// TODO: actually implement this
	var ret cid.Cid
	return ret
}

// Must be significantly larger than DeclaredFaultEpoch, since otherwise it may be possible
// to declare faults adaptively in order to exempt challenged sectors.
func (x *SectorOnChainInfo) EffectiveFaultBeginEpoch() abi.ChainEpoch {
	return x.DeclaredFaultEpoch + DeclaredFaultEffectiveDelay
}

func (x *SectorOnChainInfo) EffectiveFaultEndEpoch() abi.ChainEpoch {
	return x.EffectiveFaultBeginEpoch() + x.DeclaredFaultDuration
}

func (mps *MinerPoStState) isChallenged() bool {
	return mps.SurpriseChallengeEpoch != epochUndefined
}

func (mps *MinerPoStState) isPoStOk() bool {
	return !mps.hasFailedPost()
}

func (mps *MinerPoStState) hasFailedPost() bool {
	return mps.NumConsecutiveFailures > 0
}
