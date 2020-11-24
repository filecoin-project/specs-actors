package miner

import (
	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/util"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type DealSummary struct {
	SectorStart      abi.ChainEpoch
	SectorExpiration abi.ChainEpoch
}

type StateSummary struct {
	LivePower     PowerPair
	ActivePower   PowerPair
	FaultyPower   PowerPair
	SealProofType abi.RegisteredSealProof
}

// Checks internal invariants of init state.
func CheckStateInvariants(st *State, store adt.Store, balance abi.TokenAmount) (*StateSummary, *builtin.MessageAccumulator) {
	acc := &builtin.MessageAccumulator{}
	sectorSize := abi.SectorSize(0)
	minerSummary := &StateSummary{
		LivePower:     NewPowerPairZero(),
		ActivePower:   NewPowerPairZero(),
		FaultyPower:   NewPowerPairZero(),
		SealProofType: 0,
	}

	//Load data from linked structures.
	if info, err := st.GetInfo(store); err != nil {
		acc.Addf("error loading miner info: %v", err)
		// Stop here, it's too hard to make other useful checks.
		return minerSummary, acc
	} else {
		minerSummary.SealProofType = info.SealProofType
		sectorSize = info.SectorSize
		CheckMinerInfo(info, acc)
	}

	// Check deadlines
	acc.Require(st.CurrentDeadline < WPoStPeriodDeadlines,
		"current deadline index is greater than deadlines per period(%d): %d", WPoStPeriodDeadlines, st.CurrentDeadline)

	deadlines, err := st.LoadDeadlines(store)
	if err != nil {
		acc.Addf("error loading deadlines: %v", err)
		deadlines = nil
	}

	if deadlines != nil {
		err = deadlines.ForEach(store, func(dlIdx uint64, dl *Deadline) error {
			acc := acc.WithPrefix("deadline %d: ", dlIdx) // Shadow
			quant := st.QuantSpecForDeadline(dlIdx)
			dlSummary := CheckDeadlineStateInvariants(dl, store, quant, sectorSize, acc)

			minerSummary.LivePower = minerSummary.LivePower.Add(dlSummary.LivePower)
			minerSummary.ActivePower = minerSummary.ActivePower.Add(dlSummary.ActivePower)
			minerSummary.FaultyPower = minerSummary.FaultyPower.Add(dlSummary.FaultyPower)
			return nil
		})
		acc.RequireNoError(err, "error iterating deadlines")
	}

	return minerSummary, acc
}

type DeadlineStateSummary struct {
	LivePower   PowerPair
	ActivePower PowerPair
	FaultyPower PowerPair
}

func CheckDeadlineStateInvariants(deadline *Deadline, store adt.Store, quant QuantSpec, ssize abi.SectorSize,
	acc *builtin.MessageAccumulator) *DeadlineStateSummary {

	// Load linked structures.
	partitions, err := deadline.PartitionsArray(store)
	if err != nil {
		acc.Addf("error loading partitions: %v", err)
		// Hard to do any useful checks.
		return &DeadlineStateSummary{
			LivePower:   NewPowerPairZero(),
			ActivePower: NewPowerPairZero(),
			FaultyPower: NewPowerPairZero(),
		}
	}

	allLivePower := NewPowerPairZero()
	allActivePower := NewPowerPairZero()
	allFaultyPower := NewPowerPairZero()

	// Check partitions.

	partitionCount := uint64(0)
	var partition Partition
	err = partitions.ForEach(&partition, func(i int64) error {
		pIdx := uint64(i)
		// Check sequential partitions.
		acc.Require(pIdx == partitionCount, "Non-sequential partitions, expected index %d, found %d", partitionCount, pIdx)
		partitionCount++

		acc := acc.WithPrefix("partition %d: ", pIdx) // Shadow
		summary := CheckPartitionStateInvariants(&partition, store, quant, ssize, acc)

		allLivePower = allLivePower.Add(summary.LivePower)
		allActivePower = allActivePower.Add(summary.ActivePower)
		allFaultyPower = allFaultyPower.Add(summary.FaultyPower)
		return nil
	})
	acc.RequireNoError(err, "error iterating partitions")

	return &DeadlineStateSummary{
		LivePower:   allLivePower,
		ActivePower: allActivePower,
		FaultyPower: allFaultyPower,
	}
}

type PartitionStateSummary struct {
	LivePower             PowerPair
	ActivePower           PowerPair
	FaultyPower           PowerPair
	RecoveringPower       PowerPair
	ExpirationEpochs      []abi.ChainEpoch // Epochs at which some sector is scheduled to expire.
	EarlyTerminationCount int
}

func CheckPartitionStateInvariants(
	partition *Partition,
	store adt.Store,
	quant QuantSpec,
	sectorSize abi.SectorSize,
	acc *builtin.MessageAccumulator,
) *PartitionStateSummary {

	return &PartitionStateSummary{
		LivePower:       partition.LivePower,
		ActivePower:     partition.ActivePower(),
		FaultyPower:     partition.FaultyPower,
		RecoveringPower: partition.RecoveringPower,
	}
}

func CheckMinerInfo(info *MinerInfo, acc *builtin.MessageAccumulator) {
	acc.Require(info.Owner.Protocol() == addr.ID, "owner address %v is not an ID address", info.Owner)
	acc.Require(info.Worker.Protocol() == addr.ID, "worker address %v is not an ID address", info.Worker)
	for _, a := range info.ControlAddresses {
		acc.Require(a.Protocol() == addr.ID, "control address %v is not an ID address", a)
	}

	if info.PendingWorkerKey != nil {
		acc.Require(info.PendingWorkerKey.NewWorker.Protocol() == addr.ID,
			"pending worker address %v is not an ID address", info.PendingWorkerKey.NewWorker)
		acc.Require(info.PendingWorkerKey.NewWorker != info.Worker,
			"pending worker key %v is same as existing worker %v", info.PendingWorkerKey.NewWorker, info.Worker)
	}

	if info.PendingOwnerAddress != nil {
		acc.Require(info.PendingOwnerAddress.Protocol() == addr.ID,
			"pending owner address %v is not an ID address", info.PendingOwnerAddress)
		acc.Require(*info.PendingOwnerAddress != info.Owner,
			"pending owner address %v is same as existing owner %v", info.PendingOwnerAddress, info.Owner)
	}

	sealProofInfo, found := abi.SealProofInfos[info.SealProofType]
	acc.Require(found, "miner has unrecognized seal proof type %d", info.SealProofType)
	if found {
		acc.Require(sealProofInfo.SectorSize == info.SectorSize,
			"sector size %d is wrong for seal proof type %d: %d", info.SectorSize, info.SealProofType, sealProofInfo.SectorSize)
	}
	windowPoStProof := sealProofInfo.WindowPoStProof
	poStProofPolicy, found := builtin.PoStProofPolicies[windowPoStProof]
	acc.Require(found, "no seal proof policy exists for proof type %d", info.SealProofType)
	if found {
		acc.Require(poStProofPolicy.WindowPoStPartitionSectors == info.WindowPoStPartitionSectors,
			"miner partition sectors %d does not match partition sectors %d for seal proof type %d",
			info.WindowPoStPartitionSectors, poStProofPolicy.WindowPoStPartitionSectors, info.SealProofType)
	}
}

// Selects a subset of sectors from a map by sector number.
// Returns the selected sectors, and a slice of any sector numbers not found.
func selectSectorsMap(sectors map[abi.SectorNumber]*SectorOnChainInfo, include bitfield.BitField) (map[abi.SectorNumber]*SectorOnChainInfo, []abi.SectorNumber, error) {
	included := map[abi.SectorNumber]*SectorOnChainInfo{}
	missing := []abi.SectorNumber{}
	if err := include.ForEach(func(n uint64) error {
		if s, ok := sectors[abi.SectorNumber(n)]; ok {
			included[abi.SectorNumber(n)] = s
		} else {
			missing = append(missing, abi.SectorNumber(n))
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return included, missing, nil
}

func powerForSectors(sectors map[abi.SectorNumber]*SectorOnChainInfo, ssize abi.SectorSize) PowerPair {
	qa := big.Zero()
	for _, s := range sectors { // nolint:nomaprange
		qa = big.Add(qa, QAPowerForSector(ssize, s))
	}

	return PowerPair{
		Raw: big.Mul(big.NewIntUnsigned(uint64(ssize)), big.NewIntUnsigned(uint64(len(sectors)))),
		QA:  qa,
	}
}

func requireContainsAll(superset, subset bitfield.BitField, acc *builtin.MessageAccumulator, msg string) {
	if contains, err := util.BitFieldContainsAll(superset, subset); err != nil {
		acc.Addf("error in BitfieldContainsAll(): %v", err)
	} else if !contains {
		acc.Addf(msg+": %v, %v", superset, subset)
		// Verbose output for debugging
		//sup, err := superset.All(1 << 20)
		//if err != nil {
		//	acc.Addf("error in Bitfield.All(): %v", err)
		//	return
		//}
		//sub, err := subset.All(1 << 20)
		//if err != nil {
		//	acc.Addf("error in Bitfield.All(): %v", err)
		//	return
		//}
		//acc.Addf(msg+": %v, %v", sup, sub)
	}
}

func requireContainsNone(superset, subset bitfield.BitField, acc *builtin.MessageAccumulator, msg string) {
	if contains, err := util.BitFieldContainsAny(superset, subset); err != nil {
		acc.Addf("error in BitfieldContainsAny(): %v", err)
	} else if contains {
		acc.Addf(msg+": %v, %v", superset, subset)
		// Verbose output for debugging
		//sup, err := superset.All(1 << 20)
		//if err != nil {
		//	acc.Addf("error in Bitfield.All(): %v", err)
		//	return
		//}
		//sub, err := subset.All(1 << 20)
		//if err != nil {
		//	acc.Addf("error in Bitfield.All(): %v", err)
		//	return
		//}
		//acc.Addf(msg+": %v, %v", sup, sub)
	}
}

func requireEqual(a, b bitfield.BitField, acc *builtin.MessageAccumulator, msg string) {
	requireContainsAll(a, b, acc, msg)
	requireContainsAll(b, a, acc, msg)
}
