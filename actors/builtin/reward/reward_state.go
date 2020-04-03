package reward

import (
	cid "github.com/ipfs/go-cid"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type State struct {
	BaselinePower        abi.StoragePower
	RealizedPower        abi.StoragePower
	CumsumBaseline       abi.Spacetime
	CumsumRealized       abi.Spacetime
	EffectiveNetworkTime abi.ChainEpoch

	SimpleSupply   abi.TokenAmount // current supply
	BaselineSupply abi.TokenAmount // current supply

	LastPerEpochReward abi.TokenAmount
}

type AddrKey = adt.AddrKey

const BlockTimeSeconds = 30
const SecondsInYear = 31556925
const FixedPoint = 97 // bit of precision for the minting function

var LambdaNumBase, _ = big.FromString("6931471805599453094172321215")
var LambdaNum = big.Mul(big.NewInt(BlockTimeSeconds), LambdaNumBase)
var LambdaDenBase, _ = big.FromString("10000000000000000000000000000")
var LambdaDen = big.Mul(big.NewInt(6*SecondsInYear), LambdaDenBase)

// These numbers are placeholders, but should be in units of attoFIL
var SimpleTotal, _ = big.FromString("1000000000000000000000000")
var BaselineTotal, _ = big.FromString("1000000000000000000000000")

func ConstructState(emptyMultiMapCid cid.Cid) *State {
	return &State{
		BaselinePower:        big.Zero(),
		RealizedPower:        big.Zero(),
		CumsumBaseline:       big.Zero(),
		CumsumRealized:       big.Zero(),
		EffectiveNetworkTime: abi.ChainEpoch(int64(0)),

		SimpleSupply:   big.Zero(),
		BaselineSupply: big.Zero(),
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
