package miner

import (
	"reflect"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
	errors "github.com/pkg/errors"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	power "github.com/filecoin-project/specs-actors/actors/builtin/power"
	. "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

// Balance of a Actor should equal exactly the sum of PreCommit deposits
// that are not yet returned or burned.
type State struct {
	PreCommittedSectors cid.Cid // Map, HAMT[SectorNumber]SectorPreCommitOnChainInfo
	Sectors             cid.Cid // Array, AMT[]SectorOnChainInfo (sparse)
	FaultSet            abi.BitField
	ProvingSet          cid.Cid // Array, AMT[]SectorOnChainInfo (sparse)

	Info      MinerInfo // TODO: this needs to be a cid of the miner Info struct
	PoStState PoStState
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
	SectorSize abi.SectorSize
}

type PoStState struct {
	// Epoch of the last succesful PoSt, either election post or surprise post.
	LastSuccessfulPoSt abi.ChainEpoch

	// If >= 0 miner has been challenged and not yet responded successfully.
	// SurprisePoSt challenge state: The miner has not submitted timely ElectionPoSts,
	// and as a result, the system has fallen back to proving storage via SurprisePoSt.
	//  `epochUndefined` if not currently challeneged.
	SurpriseChallengeEpoch abi.ChainEpoch

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
	ActivationEpoch       abi.ChainEpoch  // Epoch at which SectorProveCommit is accepted
	DealWeight            abi.DealWeight  // Integral of active deals over sector lifetime, 0 if CommittedCapacity sector
	PledgeRequirement     abi.TokenAmount // Fixed pledge collateral requirement determined at activation
	DeclaredFaultEpoch    abi.ChainEpoch  // -1 if not currently declared faulted.
	DeclaredFaultDuration abi.ChainEpoch  // -1 if not currently declared faulted.
}

func ConstructState(store adt.Store, ownerAddr, workerAddr addr.Address, peerId peer.ID, sectorSize abi.SectorSize) (*State, error) {
	emptyMap, err := adt.MakeEmptyMap(store)
	if err != nil {
		return nil, err
	}

	emptyArray, err := adt.MakeEmptyArray(store)
	if err != nil {
		return nil, err
	}
	return &State{
		PreCommittedSectors: emptyMap.Root(),
		Sectors:             emptyArray.Root(),
		FaultSet:            abi.NewBitField(),
		ProvingSet:          emptyArray.Root(),
		Info: MinerInfo{
			Owner:            ownerAddr,
			Worker:           workerAddr,
			PendingWorkerKey: WorkerKeyChange{},
			PeerId:           peerId,
			SectorSize:       sectorSize,
		},
		PoStState: PoStState{
			LastSuccessfulPoSt:     epochUndefined,
			SurpriseChallengeEpoch: epochUndefined,
			NumConsecutiveFailures: 0,
		},
	}, nil
}

func (st *State) getWorker() addr.Address {
	return st.Info.Worker
}

func (st *State) getSectorSize() abi.SectorSize {
	return st.Info.SectorSize
}

func (st *State) putPrecommittedSector(store adt.Store, info *SectorPreCommitOnChainInfo) error {
	precommitted := adt.AsMap(store, st.PreCommittedSectors)
	err := precommitted.Put(sectorKey(info.Info.SectorNumber), info)
	if err != nil {
		return errors.Wrapf(err, "failed to store precommitment for %v", info)
	}
	return nil
}

func (st *State) getPrecommittedSector(store adt.Store, sectorNo abi.SectorNumber) (*SectorPreCommitOnChainInfo, bool, error) {
	precommitted := adt.AsMap(store, st.PreCommittedSectors)
	var info SectorPreCommitOnChainInfo
	found, err := precommitted.Get(sectorKey(sectorNo), &info)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to load precommitment for %v", sectorNo)
	}
	return &info, found, nil
}

func (st *State) deletePrecommittedSector(store adt.Store, sectorNo abi.SectorNumber) error {
	precommitted := adt.AsMap(store, st.PreCommittedSectors)
	err := precommitted.Delete(sectorKey(sectorNo))
	if err != nil {
		return errors.Wrapf(err, "failed to delete precommitment for %v", sectorNo)
	}
	return nil
}

func (st *State) hasSectorNo(store adt.Store, sectorNo abi.SectorNumber) (bool, error) {
	sectors := adt.AsArray(store, st.Sectors)
	var info SectorOnChainInfo
	found, err := sectors.Get(uint64(sectorNo), &info)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get sector %v", sectorNo)
	}
	return found, nil
}

func (st *State) putSector(store adt.Store, sector *SectorOnChainInfo) error {
	sectors := adt.AsArray(store, st.Sectors)
	if err := sectors.Set(uint64(sector.Info.SectorNumber), sector); err != nil {
		return errors.Wrapf(err, "failed to put sector %v", sector)
	}
	return nil
}

func (st *State) getSector(store adt.Store, sectorNo abi.SectorNumber) (*SectorOnChainInfo, bool, error) {
	sectors := adt.AsArray(store, st.Sectors)
	var info SectorOnChainInfo
	found, err := sectors.Get(uint64(sectorNo), &info)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to get sector %v", sectorNo)
	}
	return &info, found, nil
}

func (st *State) deleteSector(store adt.Store, sectorNo abi.SectorNumber) error {
	sectors := adt.AsArray(store, st.Sectors)
	if err := sectors.Delete(uint64(sectorNo)); err != nil {
		return errors.Wrapf(err, "failed to delete sector %v", sectorNo)
	}
	return nil
}

func (st *State) forEachSector(store adt.Store, f func(*SectorOnChainInfo)) error {
	sectors := adt.AsArray(store, st.Sectors)
	var sector SectorOnChainInfo
	return sectors.ForEach(&sector, func(idx int64) error {
		f(&sector)
		return nil
	})
}

func (st *State) GetStorageWeightDescForSector(store adt.Store, sectorNo abi.SectorNumber) (*power.SectorStorageWeightDesc, error) {
	sectorInfo, found, err := st.getSector(store, sectorNo)
	if err != nil {
		return nil, err
	} else if !found {
		return nil, errors.Errorf("no such sector %v", sectorNo)
	}

	return asStorageWeightDesc(st.Info.SectorSize, sectorInfo), nil
}

func (st *State) ComputeProvingSet() cid.Cid {
	// Current ProvingSet is a snapshot of the Sectors AMT subtracting sectors in the FaultSet
	// TODO: actually implement this
	var ret cid.Cid
	return ret
}

func (st *State) VerifySurprisePoStMeetsTargetReq(candidate abi.PoStCandidate) bool {
	// TODO hs: Determine what should be the acceptance criterion for sector numbers proven in SurprisePoSt proofs.
	TODO()
	panic("")
}

func (mps *PoStState) isChallenged() bool {
	return mps.SurpriseChallengeEpoch != epochUndefined
}

func (mps *PoStState) isPoStOk() bool {
	return !mps.hasFailedPost()
}

func (mps *PoStState) hasFailedPost() bool {
	return mps.NumConsecutiveFailures > 0
}

func asStorageWeightDesc(sectorSize abi.SectorSize, sectorInfo *SectorOnChainInfo) *power.SectorStorageWeightDesc {
	return &power.SectorStorageWeightDesc{
		SectorSize: sectorSize,
		DealWeight: sectorInfo.DealWeight,
		Duration:   sectorInfo.Info.Expiration - sectorInfo.ActivationEpoch,
	}
}

func sectorKey(e abi.SectorNumber) adt.Keyer {
	return adt.UIntKey(uint64(e))
}

func init() {
	// Check that ChainEpoch is indeed an unsigned integer to confirm that sectorKey is making the right interpretation.
	var e abi.SectorNumber
	if reflect.TypeOf(e).Kind() != reflect.Uint64 {
		panic("incorrect sector number encoding")
	}
}