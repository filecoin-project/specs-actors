package builtin

import (
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/go-state-types/big"
)

const SecondsInHour = 60 * 60
const SecondsInDay = 24 * SecondsInHour

type NetworkPolicy struct {
	// The duration of a chain epoch.
	// Motivation: It guarantees that a block is propagated and WinningPoSt can be successfully done in time all supported miners.
	// Usage: It is used for deriving epoch-denominated periods that are more naturally expressed in clock time.
	epochDurationSeconds uint64
	epochsInHour         abi.ChainEpoch
	epochsInDay          abi.ChainEpoch
	epochsInYear         abi.ChainEpoch

	// Expected number of block quality in an epoch (e.g. 1 block with block quality 5, or 5 blocks with quality 1)
	// Motivation: It ensures that there is enough on-chain throughput
	// Usage: It is used to calculate the block reward.
	ExpectedLeadersPerEpoch int64
}

func MakeNetworkPolicy(epochDurationSeconds uint64, expectedLeadersPerEpoch int64) NetworkPolicy {
	if SecondsInHour%epochDurationSeconds != 0 {
		// This even division is an assumption that other code might unwittingly make.
		// Don't rely on it on purpose, though.
		// While we're pretty sure everything will still work fine, we're safer maintaining this invariant anyway.
		panic(fmt.Sprintf("epoch duration %d does not evenly divide one hour (%d)", epochDurationSeconds, SecondsInHour))
	}

	n := NetworkPolicy{
		ExpectedLeadersPerEpoch: expectedLeadersPerEpoch,
	}
	n.SetEpochDurationSeconds(epochDurationSeconds)

	return n
}
func (np *NetworkPolicy) SetEpochDurationSeconds(epochDurationSeconds uint64) {
	np.epochDurationSeconds = epochDurationSeconds
	np.epochsInHour = SecondsInHour / abi.ChainEpoch(np.epochDurationSeconds)
	np.epochsInDay = 24 * np.epochsInHour
	np.epochsInYear = 365 * np.epochsInDay
}
func (np NetworkPolicy) EpochDurationSeconds() uint64 {
	return np.epochDurationSeconds
}
func (np NetworkPolicy) EpochsInHour() abi.ChainEpoch {
	return np.epochsInHour
}
func (np NetworkPolicy) EpochsInDay() abi.ChainEpoch {
	return np.epochsInDay
}
func (np NetworkPolicy) EpochsInYear() abi.ChainEpoch {
	return np.epochsInYear
}

var DefaultNetworkPolicy = MakeNetworkPolicy(30, 5)

var CurrentNetworkPolicy = DefaultNetworkPolicy

func EpochDurationSeconds() uint64 {
	return CurrentNetworkPolicy.EpochDurationSeconds()
}
func EpochsInHour() abi.ChainEpoch {
	return CurrentNetworkPolicy.EpochsInHour()
}
func EpochsInDay() abi.ChainEpoch {
	return CurrentNetworkPolicy.EpochsInDay()
}
func EpochsInYear() abi.ChainEpoch {
	return CurrentNetworkPolicy.EpochsInYear()
}

func ExpectedLeadersPerEpoch() int64 {
	return CurrentNetworkPolicy.ExpectedLeadersPerEpoch
}

// Number of token units in an abstract "FIL" token.
// The network works purely in the indivisible token amounts. This constant converts to a fixed decimal with more
// human-friendly scale.
var TokenPrecision = big.NewIntUnsigned(1_000_000_000_000_000_000)

// The maximum supply of Filecoin that will ever exist (in token units)
var TotalFilecoin = big.Mul(big.NewIntUnsigned(2_000_000_000), TokenPrecision)

// Quality multiplier for committed capacity (no deals) in a sector
var QualityBaseMultiplier = big.NewInt(10)

// Quality multiplier for unverified deals in a sector
var DealWeightMultiplier = big.NewInt(10)

// Quality multiplier for verified deals in a sector
var VerifiedDealWeightMultiplier = big.NewInt(100)

// Precision used for making QA power calculations
const SectorQualityPrecision = 20

// 1 NanoFIL
var OneNanoFIL = big.NewInt(1_000_000_000)
