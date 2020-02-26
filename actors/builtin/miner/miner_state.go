package miner

import (
	"reflect"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
	errors "github.com/pkg/errors"
	xerrors "golang.org/x/xerrors"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	power "github.com/filecoin-project/specs-actors/actors/builtin/power"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
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
	// The associated pubkey-type address is used to sign blocks and messages on behalf of this miner.
	Worker addr.Address // Must be an ID-address.

	PendingWorkerKey *WorkerKeyChange

	// Libp2p identity that should be used when connecting to this miner.
	PeerId peer.ID

	// Amount of space in each sector committed to the network by this miner.
	SectorSize abi.SectorSize
}

type PoStState struct {
	// Epoch that starts the current proving period
	ProvingPeriodStart abi.ChainEpoch

	// Number of surprised post challenges that have been failed since last successful PoSt.
	// Indicates that the claimed storage power may not actually be proven. Recovery can proceed by
	// submitting a correct response to a subsequent PoSt challenge, up until
	// the limit of number of consecutive failures.
	NumConsecutiveFailures int64
}

type WorkerKeyChange struct {
	NewWorker   addr.Address // Must be an ID address
	EffectiveAt abi.ChainEpoch
}

type SectorPreCommitInfo struct {
	RegisteredProof abi.RegisteredProof
	SectorNumber    abi.SectorNumber
	SealedCID       cid.Cid // CommR
	SealRandEpoch   abi.ChainEpoch
	DealIDs         []abi.DealID
	Expiration      abi.ChainEpoch // Sector Expiration
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

func ConstructState(emptyArrayCid, emptyMapCid cid.Cid, ownerAddr, workerAddr addr.Address, peerId peer.ID, sectorSize abi.SectorSize) *State {
	return &State{
		PreCommittedSectors: emptyMapCid,
		Sectors:             emptyArrayCid,
		FaultSet:            abi.NewBitField(),
		ProvingSet:          emptyArrayCid,
		Info: MinerInfo{
			Owner:            ownerAddr,
			Worker:           workerAddr,
			PendingWorkerKey: nil,
			PeerId:           peerId,
			SectorSize:       sectorSize,
		},
		PoStState: PoStState{
			ProvingPeriodStart:     epochUndefined,
			NumConsecutiveFailures: 0,
		},
	}
}

func (st *State) GetWorker() addr.Address {
	return st.Info.Worker
}

func (st *State) GetSectorSize() abi.SectorSize {
	return st.Info.SectorSize
}

func (st *State) PutPrecommittedSector(store adt.Store, info *SectorPreCommitOnChainInfo) error {
	precommitted := adt.AsMap(store, st.PreCommittedSectors)
	err := precommitted.Put(sectorKey(info.Info.SectorNumber), info)
	if err != nil {
		return errors.Wrapf(err, "failed to store precommitment for %v", info)
	}
	st.PreCommittedSectors = precommitted.Root()
	return nil
}

func (st *State) GetPrecommittedSector(store adt.Store, sectorNo abi.SectorNumber) (*SectorPreCommitOnChainInfo, bool, error) {
	precommitted := adt.AsMap(store, st.PreCommittedSectors)
	var info SectorPreCommitOnChainInfo
	found, err := precommitted.Get(sectorKey(sectorNo), &info)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to load precommitment for %v", sectorNo)
	}
	return &info, found, nil
}

func (st *State) DeletePrecommittedSector(store adt.Store, sectorNo abi.SectorNumber) error {
	precommitted := adt.AsMap(store, st.PreCommittedSectors)
	err := precommitted.Delete(sectorKey(sectorNo))
	if err != nil {
		return errors.Wrapf(err, "failed to delete precommitment for %v", sectorNo)
	}
	st.PreCommittedSectors = precommitted.Root()
	return nil
}

func (st *State) HasSectorNo(store adt.Store, sectorNo abi.SectorNumber) (bool, error) {
	sectors := adt.AsArray(store, st.Sectors)
	var info SectorOnChainInfo
	found, err := sectors.Get(uint64(sectorNo), &info)
	if err != nil {
		return false, xerrors.Errorf("failed to get sector %v: %w", sectorNo, err)
	}
	return found, nil
}

func (st *State) PutSector(store adt.Store, sector *SectorOnChainInfo) error {
	sectors := adt.AsArray(store, st.Sectors)
	if err := sectors.Set(uint64(sector.Info.SectorNumber), sector); err != nil {
		return errors.Wrapf(err, "failed to put sector %v", sector)
	}
	st.Sectors = sectors.Root()
	return nil
}

func (st *State) GetSector(store adt.Store, sectorNo abi.SectorNumber) (*SectorOnChainInfo, bool, error) {
	sectors := adt.AsArray(store, st.Sectors)
	var info SectorOnChainInfo
	found, err := sectors.Get(uint64(sectorNo), &info)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to get sector %v", sectorNo)
	}
	return &info, found, nil
}

func (st *State) DeleteSector(store adt.Store, sectorNo abi.SectorNumber) error {
	sectors := adt.AsArray(store, st.Sectors)
	if err := sectors.Delete(uint64(sectorNo)); err != nil {
		return errors.Wrapf(err, "failed to delete sector %v", sectorNo)
	}
	st.Sectors = sectors.Root()
	return nil
}

func (st *State) ForEachSector(store adt.Store, f func(*SectorOnChainInfo)) error {
	sectors := adt.AsArray(store, st.Sectors)
	var sector SectorOnChainInfo
	return sectors.ForEach(&sector, func(idx int64) error {
		f(&sector)
		return nil
	})
}

func (st *State) GetStorageWeightDescForSector(store adt.Store, sectorNo abi.SectorNumber) (*power.SectorStorageWeightDesc, error) {
	sectorInfo, found, err := st.GetSector(store, sectorNo)
	if err != nil {
		return nil, err
	} else if !found {
		return nil, errors.Errorf("no such sector %v", sectorNo)
	}

	return asStorageWeightDesc(st.Info.SectorSize, sectorInfo), nil
}

func (st *State) InChallengeWindow(rt Runtime) bool {
	return rt.CurrEpoch() > st.PoStState.ProvingPeriodStart // TODO: maybe also a few blocks beforehand?
}

func (st *State) ComputeProvingSet(store adt.Store) ([]abi.SectorInfo, error) {
	// ProvingSet is a snapshot of the Sectors AMT, must subtract sectors in the FaultSet

	provingSet := adt.AsArray(store, st.ProvingSet)

	var sectorInfos []abi.SectorInfo
	var ssinfo SectorOnChainInfo

	err := provingSet.ForEach(&ssinfo, func(sectorNum int64) error {
		// TODO: touch up faults
		fault, err := st.FaultSet.Has(uint64(sectorNum))
		if err != nil {
			return err
		}
		if ssinfo.DeclaredFaultEpoch != epochUndefined || ssinfo.DeclaredFaultDuration != epochUndefined {
			return errors.Errorf("sector faultEpoch or duration invalid %v", ssinfo.Info.SectorNumber)
		}

		// if not a temp fault sector, add to computed proving set
		if !fault {
			sectorInfos = append(sectorInfos, abi.SectorInfo{
				SealedCID:       ssinfo.Info.SealedCID,
				SectorNumber:    ssinfo.Info.SectorNumber,
				RegisteredProof: ssinfo.Info.RegisteredProof,
			})
		}
		return nil
	})

	if err != nil {
		return sectorInfos, errors.Wrapf(err, "failed to traverse sectors for proving set: %v", err)
	}

	return sectorInfos, nil
}

func (st *State) ComputeWindowedPoStChallengeCount(store adt.Store) (uint64, error) {
	// TODO: This is too slow. Subtract fault count from sector count when these
	// are maintained consistently.
	provingSet, err := st.ComputeProvingSet(store)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to compute proving set: %v", err)
	}

	num := uint64(len(provingSet)) * WindowedPoStSampleRateNumer
	den := uint64(WindowedPoStSampleRateDenom)

	return num/den + 1, nil
}

func (mps *PoStState) IsPoStOk() bool {
	return !mps.HasFailedPost()
}

func (mps *PoStState) HasFailedPost() bool {
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

func bitfieldToSectorNos(rt Runtime, bf *abi.BitField) []abi.SectorNumber {
	sns, err := bf.All(MaxFaultsCount)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalArgument, "failed to enumerate sector numbers from bitfield: %s", err)
	}

	var snums []abi.SectorNumber
	for _, sn := range sns {
		snums = append(snums, abi.SectorNumber(sn))
	}

	return snums
}
