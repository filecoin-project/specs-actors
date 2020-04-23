package reward

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

// The block reward is the sum of two different exponential decays.
// A simple exponential decay and a target-driven decay.
// The simple one emits an exponentially decreasing amount every epoch, defined from a total amount, a time period,
// and a decay rate corresponding to a six-year half-life.
// The "baseline" one is also defined by an expoential decay, but "time" is defined as the network's
// progress toward a target amount of raw-byte-power-time
// The power actor calls a method to advertise the "network KPI" value (which is just the raw byte power) at this
// epoch. This is assumed to be called at every epoch, so a sum of all these values is the integral of
// power over time.
// The baseline is defined as a function over time of a power amount at each epoch.
// The baseline has been "met" if at any epoch, the sum of byte-epochs up to this point equals or exceeds
// the sum of the baseline values up to that point.

type State struct {
	// TODO: consider removing the unused state variables.
	// The most recent value of the baseline function; i.e. the network raw-byte power target for
	// maximum block reward minting.
	// This value is never used, it's just stored for information.
	BaselinePower abi.StoragePower
	// The most recent value of the actual network power reported to this actor.
	// This value is never used, just for information.
	RealizedPower abi.StoragePower

	// The sum of the baseline power values for all epochs until present.
	CumsumBaseline abi.Spacetime
	// The sum of the actual power values for all epochs until present.
	CumsumRealized abi.Spacetime
	// The most recent value of the progress of the actual power against the baseline power.
	// This is expressed as epochs elapsed, such that if the baseline has been met, this value will
	// equal the epoch at which it was evaluated.
	// Never exceeds the current epoch.
	EffectiveNetworkTime abi.ChainEpoch

	// How much token has been minted and distributed to miners.
	SimpleSupply   abi.TokenAmount
	BaselineSupply abi.TokenAmount

	// The total block reward distributed in the previous epoch (maybe split between many miners).
	LastPerEpochReward abi.TokenAmount
}

type AddrKey = adt.AddrKey

const BlockTimeSeconds = 30 // FIXME
const SecondsInYear = 31556925
const FixedPoint = 97 // bit of precision for the minting function

// Exponential decay rate.
var LambdaNumBase, _ = big.FromString("6931471805599453094172321215")
var LambdaNum = big.Mul(big.NewInt(BlockTimeSeconds), LambdaNumBase)
var LambdaDenBase, _ = big.FromString("10000000000000000000000000000")
var LambdaDen = big.Mul(big.NewInt(6*SecondsInYear), LambdaDenBase)

// The total reward allocation to the simple minting function.
// FIXME these must be provided to the reward actor in genesis, not written as constants here.
// These values aren't right yet. Express this as a fraction instead.
var SimpleTotal, _ = big.FromString("1000000000000000000000000")
var BaselineTotal, _ = big.FromString("1000000000000000000000000")

func ConstructState() *State {
	return &State{
		BaselinePower:        big.Zero(),
		RealizedPower:        big.Zero(),
		CumsumBaseline:       big.Zero(),
		CumsumRealized:       big.Zero(),
		EffectiveNetworkTime: abi.ChainEpoch(0),

		SimpleSupply:       big.Zero(),
		BaselineSupply:     big.Zero(),
		LastPerEpochReward: big.Zero(),
	}
}

// Return taylor series expansion of 1-e^(-lambda*t)
// except that it is multiplied by 2^FixedPoint
// divide by 2^FixedPoint before using the return value
func taylorSeriesExpansion(t abi.ChainEpoch) big.Int {
	numeratorBase := big.Mul(LambdaNum.Neg(), big.NewInt(int64(t)))
	numerator := numeratorBase.Neg()
	denominator := LambdaDen
	ret := big.Zero()

	for n := int64(1); n < int64(25); n++ {
		denominator = big.Mul(denominator, big.NewInt(n))
		term := big.Div(big.Lsh(numerator, FixedPoint), denominator)
		ret = big.Add(ret, term)

		denominator = big.Mul(denominator, LambdaDen)
		numerator = big.Mul(numerator, numeratorBase)

		denominatorLen := big.BitLen(denominator)
		unnecessaryBits := denominatorLen - FixedPoint
		if unnecessaryBits < 0 {
			unnecessaryBits = 0
		}

		numerator = big.Rsh(numerator, unnecessaryBits)
		denominator = big.Rsh(denominator, unnecessaryBits)
	}

	return ret
}
