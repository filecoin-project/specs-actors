package miner

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	power "github.com/filecoin-project/specs-actors/actors/builtin/power"
)

// An approximation to chain state finality.
const ChainFinalityish = abi.ChainEpoch(500) // PARAM_FINISH

// Lookback from current epoch from which to obtain a PoRep challenge.
// TODO: HS why is this value unused?
const PoRepLookback = ChainFinalityish // Should be approximately chain ~finality. PARAM_FINISH

// Minimum and maximum delay (inclusive) between a sector pre-commitment and corresponding proof of commitment.
const PoRepMinDelay = abi.ChainEpoch(5)  // PARAM_FINISH
const PoRepMaxDelay = abi.ChainEpoch(10) // PARAM_FINISH

// Maximum duration to allow for the sealing process for seal algorithms.
var MaxSealDuration = map[abi.RegisteredProof]abi.ChainEpoch{
	abi.RegisteredProofStackedDRG32GiBSeal:    abi.ChainEpoch(1), // PARAM_FINISH
	abi.RegisteredProofWinStackedDRG32GiBSeal: abi.ChainEpoch(1), // PARAM_FINISH
}

// Lookback from the current epoch from which to obtain a PoSt challenge.
const PoStLookback = abi.ChainEpoch(1) // PARAM_FINISH

// Lookback from the current epoch for state view for elections; for Election PoSt, same as the PoSt lookback.
const ElectionLookback = PoStLookback // PARAM_FINISH

// Number of sectors to be sampled as part of surprise PoSt
const NumWindowedPoStSectors = 200 // PARAM_FINISH

// Delay between declaration of a temporary sector fault and effectiveness of reducing the active proving set for PoSts.
const DeclaredFaultEffectiveDelay = abi.ChainEpoch(20) // PARAM_FINISH

// Staging period for a miner worker key change.
const WorkerKeyChangeDelay = 2 * ElectionLookback // PARAM_FINISH

// Deposit per sector required at pre-commitment, refunded after the commitment is proven (else burned).
func precommitDeposit(sectorSize abi.SectorSize, duration abi.ChainEpoch) abi.TokenAmount {
	depositPerByte := abi.NewTokenAmount(0) // PARAM_FINISH
	return big.Mul(depositPerByte, big.NewInt(int64(sectorSize)))
}

func temporaryFaultFee(weights []*power.SectorStorageWeightDesc, duration abi.ChainEpoch) abi.TokenAmount {
	return big.Zero() // PARAM_FINISH
}

// MaxFaultsCount is the maximum number of faults that can be declared
const MaxFaultsCount = 32 << 20

// ProvingPeriod defines the frequency of PoSt challenges that a miner will have to respond to
const ProvingPeriod = 300
