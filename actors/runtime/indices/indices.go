package indices

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
)

// Data in Indices are populated at instantiation with data from the state tree
// Indices itself has no state tree or access to the runtime
// it is a passive data structure that allows for convenience access to network indices
// and pure functions in implementing economic policies given states
type Indices interface {
	SectorWeight(
		sectorSize abi.SectorSize,
		startEpoch abi.ChainEpoch,
		endEpoch abi.ChainEpoch,
		dealWeight abi.DealWeight,
	) abi.SectorWeight
	PledgeCollateralReq(minerNominalPower abi.StoragePower) abi.TokenAmount
	StoragePower(
		minerNominalPower abi.StoragePower,
		minerPledgeCollateral abi.TokenAmount,
	) abi.StoragePower
	CurrEpochBlockReward(
		networkIndicator big.Int,
		networkKPI big.Int,
		networkTime big.Int,
	) abi.TokenAmount
	GetCurrBlockRewardForMiner(
		minerStoragePower abi.StoragePower,
		minerPledgeCollateral abi.TokenAmount,
	) abi.TokenAmount
}

type IndicesImpl struct {
	// these fields are computed from StateTree upon construction
	// they are treated as globally available states
	Epoch                      abi.ChainEpoch
	NetworkKPI                 big.Int
	NetworkIndicator           big.Int
	NetworkTime                big.Int
	TotalNetworkSectorWeight   abi.SectorWeight
	TotalPledgeCollateral      abi.TokenAmount
	TotalNetworkEffectivePower abi.StoragePower // power above minimum miner size
	TotalNetworkPower          abi.StoragePower // total network power irrespective of meeting minimum miner size

	TotalMinedFIL   abi.TokenAmount
	TotalUnminedFIL abi.TokenAmount
	TotalBurnedFIL  abi.TokenAmount
	LastEpochReward abi.TokenAmount
}

var _ Indices = &IndicesImpl{}

func (inds *IndicesImpl) SectorWeight(
	sectorSize abi.SectorSize,
	startEpoch abi.ChainEpoch,
	endEpoch abi.ChainEpoch,
	dealWeight abi.DealWeight,
) abi.SectorWeight {
	// for every sector, given its size, start, end, and deals within the sector
	// assign sector power for the duration of its lifetime
	// PARAM_FINISH
	return abi.SectorWeight(big.NewInt(int64(sectorSize)))
}

func (inds *IndicesImpl) PledgeCollateralReq(minerNominalPower abi.StoragePower) abi.TokenAmount {
	// PARAM_FINISH
	return abi.NewTokenAmount(0)
}

func (inds *IndicesImpl) StoragePower(
	minerNominalPower abi.StoragePower,
	minerPledgeCollateral abi.TokenAmount,
) abi.StoragePower {
	// return StoragePower based on inputs
	// StoragePower for miner = func(NominalPower for miner, PledgeCollateral for miner, global indices)
	// Tentatively, miners dont get ConsensusPower for the share of power that they dont have collateral for
	requiredPledge := inds.PledgeCollateralReq(minerNominalPower)
	if minerPledgeCollateral.GreaterThanEqual(requiredPledge) {
		return minerNominalPower
	}

	// minerNominalPower * minerPledgeCollateral / requiredPledge
	// this is likely to change
	// PARAM_FINISH
	return big.Div(big.Mul(minerNominalPower, minerPledgeCollateral), requiredPledge)
}

func (inds *IndicesImpl) CurrEpochBlockReward(
	networkIndicator big.Int,
	networkKPI big.Int,
	networkTime big.Int,
) abi.TokenAmount {
	// total block reward allocated for CurrEpoch
	// each expected winner get an equal share of this reward
	// computed as a function of NetworkKPI, NetworkIndicator, LastEpochReward, TotalUnmminedFIL, etc
	// PARAM_FINISH
	return abi.NewTokenAmount(1)
}

func (inds *IndicesImpl) GetCurrBlockRewardForMiner(
	minerStoragePower abi.StoragePower,
	minerPledgeCollateral abi.TokenAmount,
) abi.TokenAmount {
	// PARAM_FINISH
	return abi.NewTokenAmount(1)
}
