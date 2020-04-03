package miner

import (
	"fmt"
	"io"
	"reflect"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
	errors "github.com/pkg/errors"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	power "github.com/filecoin-project/specs-actors/actors/builtin/power"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	. "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

// Balance of Miner Actor should be greater than or equal to
// the sum of PreCommitDeposits and LockedFunds.
// Excess balance as computed by st.getAvailableBalance will be
// withdrawable or usable for PreCommitDeposit.
type State struct {
	PreCommitDeposits   abi.TokenAmount // Total funds locked as PreCommitDeposits
	LockedFunds         abi.TokenAmount // Total unvested funds locked as pledge collateral
	VestingFunds        cid.Cid         // Array, AMT[ChainEpoch]TokenAmount
	PreCommittedSectors cid.Cid         // Map, HAMT[SectorNumber]SectorPreCommitOnChainInfo
	Sectors             cid.Cid         // Array, AMT[]SectorOnChainInfo (sparse)
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

type PeerID peer.ID

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
	ActivationEpoch       abi.ChainEpoch // Epoch at which SectorProveCommit is accepted
	DealWeight            abi.DealWeight // Integral of active deals over sector lifetime, 0 if CommittedCapacity sector
	DeclaredFaultEpoch    abi.ChainEpoch // -1 if not currently declared faulted.
	DeclaredFaultDuration abi.ChainEpoch // -1 if not currently declared faulted.
}

func ConstructState(emptyArrayCid, emptyMapCid cid.Cid, ownerAddr, workerAddr addr.Address, peerId peer.ID, sectorSize abi.SectorSize) *State {
	return &State{
		PreCommittedSectors: emptyMapCid,
		Sectors:             emptyArrayCid,
		FaultSet:            abi.NewBitField(),
		ProvingSet:          emptyArrayCid,
		PreCommitDeposits:   abi.NewTokenAmount(0),
		LockedFunds:         abi.NewTokenAmount(0),
		VestingFunds:        emptyArrayCid,
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

func (st *State) GetSectorCount(store adt.Store) (uint64, error) {
	arr := adt.AsArray(store, st.Sectors)
	return arr.Length()
}

func (st *State) GetMaxAllowedFaults(store adt.Store) (uint64, error) {
	sectorCount, err := st.GetSectorCount(store)
	if err != nil {
		return 0, err
	}
	return 2 * sectorCount, nil
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
	maxAllowedFaults, err := st.GetMaxAllowedFaults(store)
	if err != nil {
		return nil, err
	}
	faultsMap, err := st.FaultSet.AllMap(maxAllowedFaults)
	if err != nil {
		return nil, err
	}

	err = provingSet.ForEach(&ssinfo, func(sectorNum int64) error {
		_, fault := faultsMap[uint64(sectorNum)]
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

func computeVestAmount(total abi.TokenAmount, cliffStartEpoch abi.ChainEpoch, fullyVestedEpoch abi.ChainEpoch, quantizedEpoch abi.ChainEpoch) abi.TokenAmount {
	// PARAM_FINISH linear vesting placeholder
	numVestWindows := (fullyVestedEpoch - cliffStartEpoch) / VestIncrement
	_ = (quantizedEpoch - cliffStartEpoch) / VestIncrement // number of VestIncrement elapsed for other vest functions
	quantizedVestAmount := big.Div(total, big.NewInt(int64(numVestWindows)))
	return quantizedVestAmount
}

func getQuantizedEpoch(e abi.ChainEpoch) abi.ChainEpoch {
	// PARAM_FINISH
	// default is the nearest next epoch that is multiple of QuantizedGranularity after a VestIncrement offset
	// computeVestAmount can assume at least one VestIncrement has elapsed
	offsetEpoch := e + VestIncrement
	quantizedEpoch := offsetEpoch - (offsetEpoch % QuantizedGranularity) + QuantizedGranularity

	return quantizedEpoch
}

func (st *State) addLockedFund(store adt.Store, currEpoch abi.ChainEpoch, pledgeAmount abi.TokenAmount) error {
	Assert(pledgeAmount.GreaterThanEqual(big.Zero()))

	vestingFunds := adt.AsArray(store, st.VestingFunds)

	cliffStartEpoch := currEpoch + PledgeCliff
	fullyVestedEpoch := currEpoch + PledgeCliff + PledgeVestingPeriod

	// default vesting happens once a week at the quantized epoch.
	for i := cliffStartEpoch; i < fullyVestedEpoch; i += VestIncrement {
		lockedFundEntry := big.Zero()
		quantizedVestEpoch := getQuantizedEpoch(i)
		_, err := vestingFunds.Get(epochKey(quantizedVestEpoch), &lockedFundEntry)

		if err != nil {
			return err
		}

		vestAmount := computeVestAmount(pledgeAmount, cliffStartEpoch, fullyVestedEpoch, quantizedVestEpoch)
		newLockedFundEntry := big.Add(lockedFundEntry, vestAmount)
		vestingFunds.Set(epochKey(quantizedVestEpoch), &newLockedFundEntry)
	}

	st.VestingFunds = vestingFunds.Root()

	return nil
}

func (st *State) slashLockedFund(store adt.Store, currEpoch abi.ChainEpoch, amountToSlash abi.TokenAmount) (abi.TokenAmount, error) {
	var errorFinished = fmt.Errorf("finished")

	vestingFunds := adt.AsArray(store, st.VestingFunds)
	amountSlashed := big.Zero()

	var lockedFund abi.TokenAmount
	var toDelete []uint64
	// vestingFunds are in order of release from vesting.
	err := vestingFunds.ForEach(&lockedFund, func(k int64) error {
		if amountSlashed.LessThan(amountToSlash) {
			if k >= int64(currEpoch) {
				// slashable
				slashingAmount := big.Max(big.Sub(amountToSlash, amountSlashed), lockedFund)
				lockedFund = big.Sub(lockedFund, slashingAmount)
				amountSlashed = big.Add(amountSlashed, slashingAmount)

				if lockedFund.IsZero() {
					// we can delete this entry
					toDelete = append(toDelete, uint64(k))
				}
			}
			return nil
		} else {
			return errorFinished
		}
	})

	if err != nil && err != errorFinished {
		return big.Zero(), err
	}

	// If AMT exposed a batch delete, we could use that to save some writes here.
	for _, i := range toDelete {
		err = vestingFunds.Delete(i)
		if err != nil {
			return big.Zero(), errors.Wrapf(err, "failed to delete locked fund during slash: %v", err)
		}
	}

	st.VestingFunds = vestingFunds.Root()
	return amountSlashed, nil
}

// iterate over LockedFunds and delete fully vested locked funds and return newly unlocked funds
// update st.LockedFunds
func (st *State) vestNewFunds(store adt.Store, currEpoch abi.ChainEpoch) (abi.TokenAmount, error) {
	var errorFinished = fmt.Errorf("finished")

	vestingFunds := adt.AsArray(store, st.VestingFunds)
	newlyUnlockedFund := big.Zero()

	var lockedFund abi.TokenAmount
	var toDelete []uint64
	// vestingFunds are in order of release from vesting.
	err := vestingFunds.ForEach(&lockedFund, func(k int64) error {
		if k < int64(currEpoch) {
			// fully vested
			newlyUnlockedFund = big.Add(newlyUnlockedFund, lockedFund)
			toDelete = append(toDelete, uint64(k))
		} else {
			// stop iterating remaining funds
			return errorFinished
		}
		return nil
	})

	if err != nil && err != errorFinished {
		return big.Zero(), err
	}

	// If AMT exposed a batch delete, we could use that to save some writes here.
	for _, i := range toDelete {
		err = vestingFunds.Delete(i)
		if err != nil {
			return big.Zero(), errors.Wrapf(err, "failed to delete locked fund during vest new fund: %v", err)
		}
	}

	st.VestingFunds = vestingFunds.Root()
	st.LockedFunds = big.Sub(st.LockedFunds, newlyUnlockedFund)
	Assert(st.LockedFunds.GreaterThanEqual(big.Zero()))

	return newlyUnlockedFund, nil
}

func (st *State) getAvailableBalance(actorBalance abi.TokenAmount) abi.TokenAmount {
	availableBal := big.Sub(big.Sub(actorBalance, st.LockedFunds), st.PreCommitDeposits)
	Assert(availableBal.GreaterThanEqual(big.Zero()))

	return availableBal
}

func (st *State) assertBalanceInvariants(balance abi.TokenAmount) {
	Assert(st.PreCommitDeposits.GreaterThanEqual(big.Zero()))
	Assert(st.LockedFunds.GreaterThanEqual(big.Zero()))
	Assert(balance.GreaterThanEqual(big.Add(st.PreCommitDeposits, st.LockedFunds)))
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

func epochKey(e abi.ChainEpoch) uint64 {
	return uint64(e)
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

func (p *PeerID) MarshalCBOR(w io.Writer) error {
	if err := cbg.CborWriteHeader(w, cbg.MajByteString, uint64(len([]byte(*p)))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(*p)); err != nil {
		return err
	}
	return nil
}

func (p *PeerID) UnmarshalCBOR(r io.Reader) error {
	t, l, err := cbg.CborReadHeader(r)
	if err != nil {
		return err
	}

	if t != cbg.MajByteString {
		return fmt.Errorf("expected MajByteString when reading peer ID, got %d instead", t)
	}

	if l > cbg.MaxLength {
		return fmt.Errorf("peer ID in input was too long")
	}

	buf := make([]byte, l)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return fmt.Errorf("failed to read full peer ID: %w", err)
	}

	*p = PeerID(buf)
	return nil
}
