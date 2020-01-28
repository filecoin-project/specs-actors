package storage_power

import (
	"io"
	"sort"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	errors "github.com/pkg/errors"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	crypto "github.com/filecoin-project/specs-actors/actors/crypto"
	indices "github.com/filecoin-project/specs-actors/actors/runtime/indices"
	autil "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type StoragePowerActorState struct {
	TotalNetworkPower abi.StoragePower

	PowerTable  cid.Cid // HAMT[address]StoragePower
	MinerCount  int64
	EscrowTable autil.BalanceTableHAMT

	// Metadata cached for efficient processing of sector/challenge events.
	CronEventQueue           CronEventQueue
	PoStDetectedFaultMiners  cid.Cid // HAMT[addr.Address]struct{}
	ClaimedPower             cid.Cid // HAMT[address]StoragePower
	NominalPower             cid.Cid // HAMT[address]StoragePower
	NumMinersMeetingMinPower int
}

// A queue of events to be triggered by cron, indexed by epoch.
// TODO: HAMT/AMTs.
type CronEventQueue map[abi.ChainEpoch][]CronEvent

type CronEvent struct {
	MinerAddr       addr.Address
	CallbackPayload []byte
}

func (st *StoragePowerActorState) minerNominalPowerMeetsConsensusMinimum(s adt.Store, minerPower abi.StoragePower) (bool, error) {

	// if miner is larger than min power requirement, we're set
	if minerPower.GreaterThanEqual(indices.StoragePower_MinMinerSizeStor()) {
		return true, nil
	}

	// otherwise, if another miner meets min power requirement, return false
	if st.NumMinersMeetingMinPower > 0 {
		return false, nil
	}

	// else if none do, check whether in MIN_MINER_SIZE_TARG miners
	if st.MinerCount <= indices.StoragePower_MinMinerSizeTarg() {
		// miner should pass
		return true, nil
	}

	var minerSizes []abi.StoragePower
	var pwr abi.StoragePower
	if err := adt.AsMap(s, st.PowerTable).ForEach(&pwr, func(k string) error {
		minerSizes = append(minerSizes, pwr.Copy())
		return nil
	}); err != nil {
		return false, errors.Wrap(err, "failed to iterate power table")
	}

	// get size of MIN_MINER_SIZE_TARGth largest miner
	sort.Slice(minerSizes, func(i, j int) bool { return i > j })
	return minerPower.GreaterThanEqual(minerSizes[indices.StoragePower_MinMinerSizeTarg()-1]), nil
}

func (st *StoragePowerActorState) slashPledgeCollateral(
	minerAddr addr.Address, slashAmountRequested abi.TokenAmount) abi.TokenAmount {

	Assert(slashAmountRequested.GreaterThanEqual(big.Zero()))

	newTable, amountSlashed, ok := autil.BalanceTable_WithSubtractPreservingNonnegative(
		st.EscrowTable, minerAddr, slashAmountRequested)
	Assert(ok)
	st.EscrowTable = newTable

	autil.TODO()
	// Decide whether we can take any additional action if there is not enough
	// pledge collateral to be slashed.

	return amountSlashed
}

// selectMinersToSurprise implements the PoSt-Surprise sampling algorithm
func (st *StoragePowerActorState) selectMinersToSurprise(s adt.Store, challengeCount int, randomness abi.Randomness) ([]addr.Address, error) {
	var allMiners []addr.Address
	if err := adt.AsMap(s, st.PowerTable).ForEach(nil, func(k string) error {
		maddr, err := addr.NewFromBytes([]byte(k))
		if err != nil {
			return err
		}
		allMiners = append(allMiners, maddr)
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "failed to iterate PowerTable hamt when selecting miners to surprise")
	}

	selectedMiners := make([]addr.Address, 0)
	for chall := 0; chall < challengeCount; chall++ {
		minerIndex := crypto.RandomInt(randomness, chall, st.MinerCount)
		potentialChallengee := allMiners[minerIndex]
		// skip dups
		for addrInArray(potentialChallengee, selectedMiners) {
			minerIndex := crypto.RandomInt(randomness, chall, st.MinerCount)
			potentialChallengee = allMiners[minerIndex]
		}
		selectedMiners = append(selectedMiners, potentialChallengee)
	}

	return selectedMiners, nil
}

func (st *StoragePowerActorState) getMinerPower(s adt.Store, minerAddr addr.Address) (
	power abi.StoragePower, ok bool, err error) {
	return getStoragePower(s, st.PowerTable, minerAddr)
}

func (st *StoragePowerActorState) getMinerPledge(minerAddr addr.Address) (currPledge abi.TokenAmount, ok bool) {
	return autil.BalanceTable_GetEntry(st.EscrowTable, minerAddr)
}

func (st *StoragePowerActorState) addClaimedPowerForSector(s adt.Store, minerAddr addr.Address, weight autil.SectorStorageWeightDesc) error {
	// Note: The following computation does not use any of the dynamic information from CurrIndices();
	// it depends only on weight. This means that the power of a given weight
	// does not vary over time, so we can avoid continually updating it for each sector every epoch.
	//
	// The function is located in the indices module temporarily, until we find a better place for
	// global parameterization functions.
	sectorPower := indices.ConsensusPowerForStorageWeight(weight)

	currentPower, ok, err := getStoragePower(s, st.ClaimedPower, minerAddr)
	if err != nil {
		return err
	}
	if !ok {
		return errors.Errorf("no power for actor %v", minerAddr)
	}

	if err = st.setClaimedPower(s, minerAddr, big.Add(currentPower, sectorPower)); err != nil {
		return err
	}
	return st.updatePowerEntriesFromClaimed(s, minerAddr)
}

func (st *StoragePowerActorState) deductClaimedPowerForSector(s adt.Store, minerAddr addr.Address, weight autil.SectorStorageWeightDesc) error {
	sectorPower := indices.ConsensusPowerForStorageWeight(weight)

	currentPower, ok, err := getStoragePower(s, st.ClaimedPower, minerAddr)
	if err != nil {
		return errors.Wrap(err, "failed to get claimed miner power")
	}
	if !ok {
		return errors.Errorf("no power for actor %v", minerAddr)
	}

	if err = st.setClaimedPower(s, minerAddr, big.Sub(currentPower, sectorPower)); err != nil {
		return err
	}
	return st.updatePowerEntriesFromClaimed(s, minerAddr)
}

func (st *StoragePowerActorState) updatePowerEntriesFromClaimed(s adt.Store, minerAddr addr.Address) error {
	claimedPower, ok, err := getStoragePower(s, st.ClaimedPower, minerAddr)
	if err != nil {
		return errors.Wrap(err, "failed to get claimed miner power while setting claimed power table entry")
	}
	if !ok {
		return errors.Errorf("no power for actor %v", minerAddr)
	}

	// Compute nominal power: i.e., the power we infer the miner to have (based on the network's
	// PoSt queries), which may not be the same as the claimed power.
	// Currently, the only reason for these to differ is if the miner is in DetectedFault state
	// from a SurprisePoSt challenge.
	nominalPower := claimedPower
	if found, err := st.hasFault(s, minerAddr); err != nil {
		return err
	} else if found {
		nominalPower = big.Zero()
	}
	if err = st.setNominalPower(s, minerAddr, nominalPower); err != nil {
		return errors.Wrap(err, "failed to set nominal power while setting claimed power table entry")
	}

	// Compute actual (consensus) power, i.e., votes in leader election.
	power := nominalPower
	if found, err := st.minerNominalPowerMeetsConsensusMinimum(s, nominalPower); err != nil {
		return errors.Wrap(err, "failed to check miners nominal power against consensus minimum")

	} else if !found {
		power = big.Zero()
	}

	autil.TODO() // TODO: Decide effect of undercollateralization on (consensus) power.

	return st.setPowerEntry(s, minerAddr, power)
}

func (st *StoragePowerActorState) setClaimedPower(s adt.Store, minerAddr addr.Address, updatedMinerClaimedPower abi.StoragePower) error {
	Assert(updatedMinerClaimedPower.GreaterThanEqual(big.Zero()))
	var err error
	st.ClaimedPower, err = putStoragePower(s, st.ClaimedPower, minerAddr, updatedMinerClaimedPower)
	if err != nil {
		return errors.Wrap(err, "failed to set claimed power while setting claimed power table entry")
	}
	return nil
}

func (st *StoragePowerActorState) setNominalPower(s adt.Store, minerAddr addr.Address, updatedMinerNominalPower abi.StoragePower) error {
	Assert(updatedMinerNominalPower.GreaterThanEqual(big.Zero()))

	prevMinerNominalPower, ok, err := getStoragePower(s, st.NominalPower, minerAddr)
	if err != nil {
		return errors.Wrap(err, "failed to get previous nominal miner power while setting nominal power table entry")
	}
	if !ok {
		return errors.Errorf("no power for actor %v", minerAddr)
	}

	st.NominalPower, err = putStoragePower(s, st.NominalPower, minerAddr, updatedMinerNominalPower)
	if err != nil {
		return errors.Wrap(err, "failed to put updated nominal miner power while setting nominal power table entry")
	}

	wasMinMiner, _ := st.minerNominalPowerMeetsConsensusMinimum(s, prevMinerNominalPower)
	isMinMiner, _ := st.minerNominalPowerMeetsConsensusMinimum(s, updatedMinerNominalPower)

	if isMinMiner && !wasMinMiner {
		st.NumMinersMeetingMinPower += 1
	} else if !isMinMiner && wasMinMiner {
		st.NumMinersMeetingMinPower -= 1
	}
	return nil
}

func (st *StoragePowerActorState) setPowerEntry(s adt.Store, minerAddr addr.Address, updatedMinerPower abi.StoragePower) error {
	Assert(updatedMinerPower.GreaterThanEqual(big.Zero()))
	prevMinerPower, ok, err := getStoragePower(s, st.PowerTable, minerAddr)
	if err != nil {
		return errors.Wrap(err, "failed to get previous miner power while setting power table entry")
	}
	if !ok {
		return errors.Errorf("no power for actor %v", minerAddr)
	}
	st.PowerTable, err = putStoragePower(s, st.PowerTable, minerAddr, updatedMinerPower)
	if err != nil {
		return errors.Wrap(err, "failed to put new miner power while setting power table entry")
	}
	st.TotalNetworkPower = big.Add(st.TotalNetworkPower, big.Sub(updatedMinerPower, prevMinerPower))
	return nil
}

func (st *StoragePowerActorState) hasFault(s adt.Store, a addr.Address) (bool, error) {
	faultyMiners := adt.AsSet(s, st.PoStDetectedFaultMiners)
	found, err := faultyMiners.Has(asKey(a))
	if err != nil {
		return false, errors.Wrapf(err, "failed to get detected faults for address %v from set %s", a, st.PoStDetectedFaultMiners)
	}
	return found, nil
}

func (st *StoragePowerActorState) putFault(s adt.Store, a addr.Address) error {
	faultyMiners := adt.AsSet(s, st.PoStDetectedFaultMiners)
	if err := faultyMiners.Put(asKey(a)); err != nil {
		return errors.Wrapf(err, "failed to put detected fault for miner %s in set %s", a, st.PoStDetectedFaultMiners)
	}
	st.PoStDetectedFaultMiners = faultyMiners.Root()
	return nil
}

func (st *StoragePowerActorState) deleteFault(s adt.Store, a addr.Address) error {
	faultyMiners := adt.AsSet(s, st.PoStDetectedFaultMiners)
	if err := faultyMiners.Delete(asKey(a)); err != nil {
		return errors.Wrapf(err, "failed to delete storage power at address %s from set %s", a, st.PoStDetectedFaultMiners)
	}
	st.PoStDetectedFaultMiners = faultyMiners.Root()
	return nil
}

func getStoragePower(s adt.Store, root cid.Cid, a addr.Address) (abi.StoragePower, bool, error) {
	hm := adt.AsMap(s, root)

	var out abi.StoragePower
	found, err := hm.Get(asKey(a), &out)
	if err != nil {
		return abi.NewStoragePower(0), false, errors.Wrapf(err, "failed to get storage power for address %v from store %s", a, root)
	}
	if !found {
		return abi.NewStoragePower(0), false, nil
	}
	return out, true, nil
}

func putStoragePower(s adt.Store, root cid.Cid, a addr.Address, pwr abi.StoragePower) (cid.Cid, error) {
	hm := adt.AsMap(s, root)

	if err := hm.Put(asKey(a), &pwr); err != nil {
		return cid.Undef, errors.Wrapf(err, "failed to put storage power with address %s power %v in store %s", a, pwr, root)
	}
	return hm.Root(), nil
}

func deleteStoragePower(s adt.Store, root cid.Cid, a addr.Address) (cid.Cid, error) {
	hm := adt.AsMap(s, root)

	if err := hm.Delete(asKey(a)); err != nil {
		return cid.Undef, errors.Wrapf(err, "failed to delete storage power at address %s from store %s", a, root)
	}

	return hm.Root(), nil
}

func addrInArray(a addr.Address, list []addr.Address) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

type asKey addr.Address

func (kw asKey) Key() string {
	return string(addr.Address(kw).Bytes())
}

func (st *StoragePowerActorState) MarshalCBOR(w io.Writer) error {
	panic("replace with cbor-gen")
}

func (st *StoragePowerActorState) UnmarshalCBOR(r io.Reader) error {
	panic("replace with cbor-gen")
}
