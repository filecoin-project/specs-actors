package storage_miner

import (
	"io"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	indices "github.com/filecoin-project/specs-actors/actors/runtime/indices"
	autil "github.com/filecoin-project/specs-actors/actors/util"
)

// Balance of a StorageMinerActor should equal exactly the sum of PreCommit deposits
// that are not yet returned or burned.
type StorageMinerActorState struct {
	PreCommittedSectors PreCommittedSectorsAMT // PreCommitted Sectors
	Sectors             SectorsAMT             // Proven Sectors can be Active or in TemporaryFault
	FaultSet            SectorNumberSetHAMT    // TODO zx bitfield of Sectors in TemporaryFault
	ProvingSet          cid.Cid                // cid of SectorsAMT - sectors in FaultSet

	PoStState MinerPoStState
	Info      MinerInfo
}

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

func (mps *MinerPoStState) Is_Challenged() bool {
	return mps.SurpriseChallengeEpoch != epochUndefined
}

func (mps *MinerPoStState) Is_OK() bool {
	return !mps.Is_Challenged() && !mps.Is_DetectedFault()
}

func (mps *MinerPoStState) Is_DetectedFault() bool {
	return mps.NumConsecutiveFailures > 0
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

type SectorPreCommitInfo struct {
	SectorNumber abi.SectorNumber
	SealedCID    abi.SealedSectorCID // CommR
	SealEpoch    abi.ChainEpoch
	DealIDs      []abi.DealID
	Expiration   abi.ChainEpoch // Sector Expiration
}

type SectorProveCommitInfo struct {
	SectorNumber abi.SectorNumber
	Proof        abi.SealProof
}

// TODO AMT
type SectorsAMT map[abi.SectorNumber]SectorOnChainInfo
type PreCommittedSectorsAMT map[abi.SectorNumber]SectorPreCommitOnChainInfo

// TODO HAMT
type SectorNumberSetHAMT map[abi.SectorNumber]bool

type WorkerKeyChange struct {
	NewWorker   addr.Address // Must be an ID address
	EffectiveAt abi.ChainEpoch
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
	SealPartitions         int64
	ElectionPoStPartitions int64
	SurprisePoStPartitions int64
}

func (st *StorageMinerActorState) _getSectorOnChainInfo(sectorNo abi.SectorNumber) (info SectorOnChainInfo, ok bool) {
	sectorInfo, found := st.Sectors[sectorNo]
	if !found {
		return SectorOnChainInfo{}, false
	}
	return sectorInfo, true
}

func (st *StorageMinerActorState) _getSectorDealIDsAssert(sectorNo abi.SectorNumber) []abi.DealID {
	sectorInfo, found := st._getSectorOnChainInfo(sectorNo)
	Assert(found)
	return sectorInfo.Info.DealIDs
}

func SectorsAMT_Empty() SectorsAMT {
	IMPL_FINISH()
	panic("")
}

func (st *StorageMinerActorState) GetStorageWeightDescForSectorMaybe(sectorNumber abi.SectorNumber) (ret autil.SectorStorageWeightDesc, ok bool) {
	sectorInfo, found := st.Sectors[sectorNumber]
	if !found {
		ret = autil.SectorStorageWeightDesc{}
		ok = false
		return
	}

	ret = autil.SectorStorageWeightDesc{
		SectorSize: st.Info.SectorSize,
		DealWeight: sectorInfo.DealWeight,
		Duration:   sectorInfo.Info.Expiration - sectorInfo.ActivationEpoch,
	}
	ok = true
	return
}

func (st *StorageMinerActorState) _getStorageWeightDescForSector(sectorNumber abi.SectorNumber) autil.SectorStorageWeightDesc {
	ret, found := st.GetStorageWeightDescForSectorMaybe(sectorNumber)
	Assert(found)
	return ret
}

func (st *StorageMinerActorState) _getStorageWeightDescsForSectors(sectorNumbers []abi.SectorNumber) []autil.SectorStorageWeightDesc {
	ret := []autil.SectorStorageWeightDesc{}
	for _, sectorNumber := range sectorNumbers {
		ret = append(ret, st._getStorageWeightDescForSector(sectorNumber))
	}
	return ret
}

func (st *StorageMinerActorState) IsSectorInTemporaryFault(sectorNumber abi.SectorNumber) bool {
	checkSector, found := st.Sectors[sectorNumber]
	if !found {
		return false
	}
	_, ret := st.FaultSet[sectorNumber]
	Assert(checkSector.DeclaredFaultEpoch != epochUndefined)
	Assert(checkSector.DeclaredFaultDuration != epochUndefined)
	return ret
}

func (st *StorageMinerActorState) GetCurrentProvingSet() cid.Cid {
	// Current ProvingSet is a snapshot of the Sectors AMT subtracting sectors in the FaultSet
	var ret cid.Cid
	return ret
}

// Must be significantly larger than DeclaredFaultEpoch, since otherwise it may be possible
// to declare faults adaptively in order to exempt challenged sectors.
func (x *SectorOnChainInfo) EffectiveFaultBeginEpoch() abi.ChainEpoch {
	return x.DeclaredFaultEpoch + indices.StorageMining_DeclaredFaultEffectiveDelay()
}

func (x *SectorOnChainInfo) EffectiveFaultEndEpoch() abi.ChainEpoch {
	return x.EffectiveFaultBeginEpoch() + x.DeclaredFaultDuration
}

func MinerInfo_New(
	ownerAddr addr.Address, workerAddr addr.Address, sectorSize abi.SectorSize, peerId peer.ID) MinerInfo {

	ret := &MinerInfo{
		Owner:            ownerAddr,
		Worker:           workerAddr,
		PeerId:           peerId,
		SectorSize:       sectorSize,
		PendingWorkerKey: WorkerKeyChange{},
	}

	TODO() // TODO anorth: determine how to generate/validate VRF key and initialize other fields

	return *ret
}

func (st *StorageMinerActorState) VerifySurprisePoStMeetsTargetReq(candidate abi.PoStCandidate) bool {
	// TODO hs: Determine what should be the acceptance criterion for sector numbers proven in SurprisePoSt proofs.
	TODO()
	panic("")
}

func (st *StorageMinerActorState) UnmarshalCBOR(r io.Reader) error {
	panic("replace with cbor-gen")
}

func (st *StorageMinerActorState) MarshalCBOR(w io.Writer) error {
	panic("replace with cbor-gen")
}
