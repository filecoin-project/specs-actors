package storage_power

import (
	"io"
	"sort"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	hamt "github.com/ipfs/go-hamt-ipld"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	crypto "github.com/filecoin-project/specs-actors/actors/crypto"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	indices "github.com/filecoin-project/specs-actors/actors/runtime/indices"
	autil "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

// TODO: HAMT
type PowerTableHAMT map[addr.Address]abi.StoragePower // TODO: convert address to ActorID

// TODO: HAMT
type MinerEventsHAMT map[abi.ChainEpoch]autil.MinerEventSetHAMT

type StoragePowerActorState struct {
	TotalNetworkPower abi.StoragePower

	PowerTable  PowerTableHAMT
	EscrowTable autil.BalanceTableHAMT

	// Metadata cached for efficient processing of sector/challenge events.
	CachedDeferredCronEvents MinerEventsHAMT
	PoStDetectedFaultMiners  autil.MinerSetHAMT
	ClaimedPower             cid.Cid
	NominalPower             cid.Cid
	NumMinersMeetingMinPower int
}

func (st *StoragePowerActorState) MarshalCBOR(w io.Writer) error {
	panic("replace with cbor-gen")
}

func (st *StoragePowerActorState) _minerNominalPowerMeetsConsensusMinimum(minerPower abi.StoragePower) bool {

	// if miner is larger than min power requirement, we're set
	if minerPower >= indices.StoragePower_MinMinerSizeStor() {
		return true
	}

	// otherwise, if another miner meets min power requirement, return false
	if st.NumMinersMeetingMinPower > 0 {
		return false
	}

	// else if none do, check whether in MIN_MINER_SIZE_TARG miners
	if len(st.PowerTable) <= indices.StoragePower_MinMinerSizeTarg() {
		// miner should pass
		return true
	}

	// get size of MIN_MINER_SIZE_TARGth largest miner
	minerSizes := make([]abi.StoragePower, 0, len(st.PowerTable))
	for _, v := range st.PowerTable {
		minerSizes = append(minerSizes, v)
	}
	sort.Slice(minerSizes, func(i, j int) bool { return int(i) > int(j) })
	return minerPower >= minerSizes[indices.StoragePower_MinMinerSizeTarg()-1]
}

func (st *StoragePowerActorState) _slashPledgeCollateral(
	minerAddr addr.Address, slashAmountRequested abi.TokenAmount) abi.TokenAmount {

	Assert(slashAmountRequested.GreaterThanEqual(big.Zero()))

	newTable, amountSlashed, ok := autil.BalanceTable_WithSubtractPreservingNonnegative(
		st.EscrowTable, minerAddr, slashAmountRequested)
	Assert(ok)
	st.EscrowTable = newTable

	TODO()
	// Decide whether we can take any additional action if there is not enough
	// pledge collateral to be slashed.

	return amountSlashed
}

func addrInArray(a addr.Address, list []addr.Address) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

// _selectMinersToSurprise implements the PoSt-Surprise sampling algorithm
func (st *StoragePowerActorState) _selectMinersToSurprise(challengeCount int, randomness abi.Randomness) []addr.Address {
	// this wont quite work -- a.PowerTable is a HAMT by actor address, doesn't
	// support enumerating by int index. maybe we need that as an interface too,
	// or something similar to an iterator (or iterator over the keys)
	// or even a seeded random call directly in the HAMT: myhamt.GetRandomElement(seed []byte, idx int) using the ticket as a seed

	ptSize := len(st.PowerTable)
	allMiners := make([]addr.Address, len(st.PowerTable))
	index := 0

	for address := range st.PowerTable {
		allMiners[index] = address
		index++
	}

	selectedMiners := make([]addr.Address, 0)
	for chall := 0; chall < challengeCount; chall++ {
		minerIndex := crypto.RandomInt(randomness, chall, ptSize)
		potentialChallengee := allMiners[minerIndex]
		// skip dups
		for addrInArray(potentialChallengee, selectedMiners) {
			minerIndex := crypto.RandomInt(randomness, chall, ptSize)
			potentialChallengee = allMiners[minerIndex]
		}
		selectedMiners = append(selectedMiners, potentialChallengee)
	}

	return selectedMiners
}

func (st *StoragePowerActorState) _getPowerTotalForMiner(minerAddr addr.Address) (
	power abi.StoragePower, ok bool) {

	minerPower, found := st.PowerTable[minerAddr]
	if !found {
		return abi.StoragePower(0), found
	}

	return minerPower, true
}

func (st *StoragePowerActorState) _getCurrPledgeForMiner(minerAddr addr.Address) (currPledge abi.TokenAmount, ok bool) {
	return autil.BalanceTable_GetEntry(st.EscrowTable, minerAddr)
}

func (st *StoragePowerActorState) _addClaimedPowerForSector(rt vmr.Runtime, minerAddr addr.Address, storageWeightDesc SectorStorageWeightDesc) {
	// Note: The following computation does not use any of the dynamic information from CurrIndices();
	// it depends only on storageWeightDesc. This means that the power of a given storageWeightDesc
	// does not vary over time, so we can avoid continually updating it for each sector every epoch.
	//
	// The function is located in the indices module temporarily, until we find a better place for
	// global parameterization functions.
	sectorPower := indices.ConsensusPowerForStorageWeight(storageWeightDesc)

	currentPower, ok := getStoragePower(rt, st.ClaimedPower, minerAddr)
	Assert(ok)

	st._setClaimedPowerEntryInternal(rt, minerAddr, currentPower+sectorPower)
	st._updatePowerEntriesFromClaimedPower(rt, minerAddr)
}

func (st *StoragePowerActorState) _deductClaimedPowerForSectorAssert(rt vmr.Runtime, minerAddr addr.Address, storageWeightDesc SectorStorageWeightDesc) {
	// Note: The following computation does not use any of the dynamic information from CurrIndices();
	// it depends only on storageWeightDesc. This means that the power of a given storageWeightDesc
	// does not vary over time, so we can avoid continually updating it for each sector every epoch.
	//
	// The function is located in the indices module temporarily, until we find a better place for
	// global parameterization functions.
	sectorPower := indices.ConsensusPowerForStorageWeight(storageWeightDesc)

	currentPower, ok := getStoragePower(rt, st.ClaimedPower, minerAddr)
	Assert(ok)

	st._setClaimedPowerEntryInternal(rt, minerAddr, currentPower-sectorPower)
	st._updatePowerEntriesFromClaimedPower(rt, minerAddr)
}

func (st *StoragePowerActorState) _updatePowerEntriesFromClaimedPower(rt vmr.Runtime, minerAddr addr.Address) {
	claimedPower, ok := getStoragePower(rt, st.ClaimedPower, minerAddr)
	Assert(ok)

	// Compute nominal power: i.e., the power we infer the miner to have (based on the network's
	// PoSt queries), which may not be the same as the claimed power.
	// Currently, the only reason for these to differ is if the miner is in DetectedFault state
	// from a SurprisePoSt challenge.
	nominalPower := claimedPower
	if st.PoStDetectedFaultMiners[minerAddr] {
		nominalPower = 0
	}
	st._setNominalPowerEntryInternal(rt, minerAddr, nominalPower)

	// Compute actual (consensus) power, i.e., votes in leader election.
	power := nominalPower
	if !st._minerNominalPowerMeetsConsensusMinimum(nominalPower) {
		power = 0
	}

	TODO() // TODO: Decide effect of undercollateralization on (consensus) power.

	st._setPowerEntryInternal(minerAddr, power)
}

func (st *StoragePowerActorState) _setClaimedPowerEntryInternal(rt vmr.Runtime, minerAddr addr.Address, updatedMinerClaimedPower abi.StoragePower) {
	Assert(updatedMinerClaimedPower >= 0)
	putStoragePower(rt, st.ClaimedPower, minerAddr, updatedMinerClaimedPower)
}

func (st *StoragePowerActorState) _setNominalPowerEntryInternal(rt vmr.Runtime, minerAddr addr.Address, updatedMinerNominalPower abi.StoragePower) {
	Assert(updatedMinerNominalPower >= 0)

	prevMinerNominalPower, ok := getStoragePower(rt, st.NominalPower, minerAddr)
	Assert(ok)
	st.NominalPower = putStoragePower(rt, st.NominalPower, minerAddr, updatedMinerNominalPower)

	wasMinMiner := st._minerNominalPowerMeetsConsensusMinimum(prevMinerNominalPower)
	isMinMiner := st._minerNominalPowerMeetsConsensusMinimum(updatedMinerNominalPower)

	if isMinMiner && !wasMinMiner {
		st.NumMinersMeetingMinPower += 1
	} else if !isMinMiner && wasMinMiner {
		st.NumMinersMeetingMinPower -= 1
	}
}

func (st *StoragePowerActorState) _setPowerEntryInternal(minerAddr addr.Address, updatedMinerPower abi.StoragePower) {
	Assert(updatedMinerPower >= 0)
	prevMinerPower, ok := st.PowerTable[minerAddr]
	Assert(ok)
	st.PowerTable[minerAddr] = updatedMinerPower
	st.TotalNetworkPower += (updatedMinerPower - prevMinerPower)
}

func (st *StoragePowerActorState) _getPledgeSlashForConsensusFault(currPledge abi.TokenAmount, faultType ConsensusFaultType) abi.TokenAmount {
	// default is to slash all pledge collateral for all consensus fault
	TODO()
	switch faultType {
	case DoubleForkMiningFault:
		return currPledge
	case ParentGrindingFault:
		return currPledge
	case TimeOffsetMiningFault:
		return currPledge
	default:
		panic("Unsupported case for pledge collateral consensus fault slashing")
	}
}

func asKeyer(a addr.Address) adt.Keyer {
	return addrKeyWrapper{a}
}

type addrKeyWrapper struct {
	addr.Address
}

func (kw addrKeyWrapper) Key() string {
	return string(kw.Bytes())
}

func getStoragePower(rt vmr.Runtime, root cid.Cid, a addr.Address) (abi.StoragePower, bool) {
	hm := adt.NewMap(vmr.AsStore(rt), root)

	var out abi.StoragePower
	err := hm.Get(asKeyer(a), &out)
	if err == hamt.ErrNotFound {
		return abi.StoragePower(0), false
	}
	requireNoStateErr(rt, err, "failed to get storage power for address %v from claimed power HAMT", a)

	return out, true
}

func putStoragePower(rt vmr.Runtime, root cid.Cid, a addr.Address, pwr abi.StoragePower) cid.Cid {
	hm := adt.NewMap(vmr.AsStore(rt), root)

	err := hm.Put(asKeyer(a), &pwr)
	requireNoStateErr(rt, err, "failed to put claimed power for address %v into claimed power HAMT", a)
	return hm.Root()
}

func deleteStoragePower(rt vmr.Runtime, root cid.Cid, a addr.Address) cid.Cid {
	hm := adt.NewMap(vmr.AsStore(rt), root)

	err := hm.Delete(asKeyer(a))
	requireNoStateErr(rt, err, "failed to remove claimed power for address %v from claimed power HAMT", a)
	return hm.Root()
}

func requireNoStateErr(rt vmr.Runtime, err error, msg string, args ...interface{}) {
	if err != nil {
		errMsg := msg + " :" + err.Error()
		rt.Abort(exitcode.ErrIllegalState, errMsg, args...)
	}
}

func _getConsensusFaultSlasherReward(elapsedEpoch abi.ChainEpoch, collateralToSlash abi.TokenAmount) abi.TokenAmount {
	TODO()
	// BigInt Operation
	// var growthRate = builtin.SLASHER_SHARE_GROWTH_RATE_NUM / builtin.SLASHER_SHARE_GROWTH_RATE_DENOM
	// var multiplier = growthRate^elapsedEpoch
	// var slasherProportion = min(INITIAL_SLASHER_SHARE * multiplier, 1.0)
	// return collateralToSlash * slasherProportion
	return abi.NewTokenAmount(0)
}

func PowerTableHAMT_Empty() PowerTableHAMT {
	IMPL_FINISH()
	panic("")
}

func MinerEventsHAMT_Empty() MinerEventsHAMT {
	IMPL_FINISH()
	panic("")
}
