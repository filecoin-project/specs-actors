package storage_miner

import (
	"io"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
	errors "github.com/pkg/errors"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	storage_power "github.com/filecoin-project/specs-actors/actors/builtin/storage_power"
	indices "github.com/filecoin-project/specs-actors/actors/runtime/indices"
	autil "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

// Balance of a StorageMinerActor should equal exactly the sum of PreCommit deposits
// that are not yet returned or burned.
type StorageMinerActorState struct {
	PreCommittedSectors PreCommittedSectorsHAMT
	Sectors             cid.Cid                   // IntMap, AMT[]SectorOnChainInfo
	FaultSet            abi.BitField
	ProvingSet          cid.Cid                   // IntMap, AMT[]SectorOnChainInfo

	PoStState MinerPoStState
	Info      MinerInfo
}

// TODO HAMT
type PreCommittedSectorsHAMT map[abi.SectorNumber]*SectorPreCommitOnChainInfo

type MinerPoStState struct {
	// Epoch of the last succesful PoSt, either election post or surprise post.
	LastSuccessfulPoSt abi.ChainEpoch

	// If >= 0 miner has been challenged and not yet responded successfully.
	// SurprisePoSt challenge state: The miner has not submitted timely ElectionPoSts,
	// and as a result, the system has fallen back to proving storage via SurprisePoSt.
	//  `epochUndefined` if not currently challeneged.
	SurpriseChallengeEpoch abi.ChainEpoch

	// Not empty iff the miner is challenged.
	ChallengedSectors []abi.SectorNumber

	// Number of surprised post challenges that have been failed since last successful PoSt.
	// Indicates that the claimed storage power may not actually be proven. Recovery can proceed by
	// submitting a correct response to a subsequent SurprisePoSt challenge, up until
	// the limit of number of consecutive failures.
	NumConsecutiveFailures int64
}

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
	SectorSize             abi.SectorSize
}

type WorkerKeyChange struct {
	NewWorker   addr.Address // Must be an ID address
	EffectiveAt abi.ChainEpoch
}

type SectorPreCommitInfo struct {
	SectorNumber abi.SectorNumber
	SealedCID    abi.SealedSectorCID // CommR
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

type SectorProveCommitInfo struct {
	SectorNumber abi.SectorNumber
	Proof        abi.SealProof
}

func ConstructState(store adt.Store, ownerAddr, workerAddr addr.Address, peerId peer.ID, sectorSize abi.SectorSize) (*StorageMinerActorState, error) {
	emptyIntMap, err := adt.MakeEmptyIntMap(store)
	if err != nil {
		return nil, err
	}
	return &StorageMinerActorState{
		PreCommittedSectors: make(PreCommittedSectorsHAMT),
		Sectors:             emptyIntMap.Root(),
		FaultSet:            abi.NewBitField(),
		ProvingSet:          emptyIntMap.Root(),
		PoStState: MinerPoStState{
			LastSuccessfulPoSt:     epochUndefined,
			SurpriseChallengeEpoch: epochUndefined,
			ChallengedSectors:      []abi.SectorNumber{},
			NumConsecutiveFailures: 0,
		},
		Info: MinerInfo{
			Owner:                  ownerAddr,
			Worker:                 workerAddr,
			PendingWorkerKey:       WorkerKeyChange{},
			PeerId:                 peerId,
			SectorSize:             sectorSize,
		},
	}, nil
}

func (st *StorageMinerActorState) getWorker() addr.Address {
	return st.Info.Worker
}

func (st *StorageMinerActorState) getSectorSize() abi.SectorSize {
	return st.Info.SectorSize
}

func (st *StorageMinerActorState) putPrecommittedSector(sectorNo abi.SectorNumber, info *SectorPreCommitOnChainInfo) error {
	st.PreCommittedSectors[sectorNo] = info
	return nil
}

func (st *StorageMinerActorState) getPrecommittedSector(sectorNo abi.SectorNumber) (*SectorPreCommitOnChainInfo, bool, error) {
	info, found := st.PreCommittedSectors[sectorNo]
	return info, found, nil
}

func (st *StorageMinerActorState) deletePrecommitttedSector(sectorNo abi.SectorNumber) error {
	_, found := st.PreCommittedSectors[sectorNo]
	if !found {
		return errors.Errorf("no precommitted sector %v to delete", sectorNo)
	}
	delete(st.PreCommittedSectors, sectorNo)
	return nil
}

func (st *StorageMinerActorState) hasSectorNo(store adt.Store, sectorNo abi.SectorNumber) (bool, error) {
	sectors := adt.AsIntMap(store, st.Sectors)
	var info SectorOnChainInfo
	found, err := sectors.Get(uint64(sectorNo), &info)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get sector %v", sectorNo)
	}
	return found, nil
}

func (st *StorageMinerActorState) putSector(store adt.Store, sector *SectorOnChainInfo) error {
	sectors := adt.AsIntMap(store, st.Sectors)
	err := sectors.Put(uint64(sector.Info.SectorNumber), sector)
	if err != nil {
		return errors.Wrapf(err, "failed to put sector %v", sector)
	}
	return nil
}

func (st *StorageMinerActorState) getSector(store adt.Store, sectorNo abi.SectorNumber) (*SectorOnChainInfo, bool, error) {
	sectors := adt.AsIntMap(store, st.Sectors)
	var info SectorOnChainInfo
	found, err := sectors.Get(uint64(sectorNo), &info)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to get sector %v", sectorNo)
	}
	return &info, found, nil
}

func (st *StorageMinerActorState) deleteSector(store adt.Store, sectorNo abi.SectorNumber) error {
	sectors := adt.AsIntMap(store, st.Sectors)
	err := sectors.Delete(uint64(sectorNo))
	if err != nil {
		return errors.Wrapf(err, "failed to delete sector %v", sectorNo)
	}
	return nil
}

func (st *StorageMinerActorState) forEachSector(store adt.Store, f func(*SectorOnChainInfo)) error {
	sectors := adt.AsIntMap(store, st.Sectors)
	var sector SectorOnChainInfo
	err := sectors.ForEach(&sector, func(idx uint64) error {
		f(&sector)
		return nil
	})
	return err
}

func (st *StorageMinerActorState) GetStorageWeightDescForSector(store adt.Store, sectorNo abi.SectorNumber) (*storage_power.SectorStorageWeightDesc, error) {
	sectorInfo, found, err := st.getSector(store, sectorNo)
	if err != nil {
		return nil, err
	} else if !found {
		return nil, errors.Errorf("no such sector %v", sectorNo)
	}

	return asStorageWeightDesc(st.Info.SectorSize, sectorInfo), nil
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

func (st *StorageMinerActorState) VerifySurprisePoStMeetsTargetReq(candidate abi.PoStCandidate) bool {
	// TODO hs: Determine what should be the acceptance criterion for sector numbers proven in SurprisePoSt proofs.
	autil.TODO()
	panic("")
}

// Must be significantly larger than DeclaredFaultEpoch, since otherwise it may be possible
// to declare faults adaptively in order to exempt challenged sectors.
func (x *SectorOnChainInfo) EffectiveFaultBeginEpoch() abi.ChainEpoch {
	return x.DeclaredFaultEpoch + indices.StorageMining_DeclaredFaultEffectiveDelay()
}

func (x *SectorOnChainInfo) EffectiveFaultEndEpoch() abi.ChainEpoch {
	return x.EffectiveFaultBeginEpoch() + x.DeclaredFaultDuration
}

func (mps *MinerPoStState) isChallenged() bool {
	return mps.SurpriseChallengeEpoch != epochUndefined
}

func (mps *MinerPoStState) isPoStOk() bool {
	return !mps.isChallenged() && !mps.hasFailedPost()
}

func (mps *MinerPoStState) hasFailedPost() bool {
	return mps.NumConsecutiveFailures > 0
}

func asStorageWeightDesc(sectorSize abi.SectorSize, sectorInfo *SectorOnChainInfo) *storage_power.SectorStorageWeightDesc {
	return &storage_power.SectorStorageWeightDesc{
		SectorSize: sectorSize,
		DealWeight: sectorInfo.DealWeight,
		Duration:   sectorInfo.Info.Expiration - sectorInfo.ActivationEpoch,
	}
}

func (st *StorageMinerActorState) UnmarshalCBOR(r io.Reader) error {
	panic("replace with cbor-gen")
}

func (st *StorageMinerActorState) MarshalCBOR(w io.Writer) error {
	panic("replace with cbor-gen")
}

func (x *SectorOnChainInfo) UnmarshalCBOR(r io.Reader) error {
	panic("implement me")
}

func (x *SectorOnChainInfo) MarshalCBOR(w io.Writer) error {
	panic("implement me")
}
