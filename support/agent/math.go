package agent

import (
	"math"
	big2 "math/big"
	"math/rand"

	"github.com/filecoin-project/go-state-types/abi"
)

type RateIterator struct {
	rnd            *rand.Rand
	rate           float64
	nextOccurrence float64
}

func NewRateIterator(rate float64, seed int64) *RateIterator {
	rnd := rand.New(rand.NewSource(seed))
	return &RateIterator{
		rnd:  rnd,
		rate: rate,

		// choose first event in next tick
		nextOccurrence: 1.0 - math.Log(1.0-rnd.Float64())/rate,
	}
}

// simulate random occurrences by calling the given function once for each event that would land in this epoch.
// The function will be called `rate` times on average, but may be called zero or many times in any Tick.
//
func (ri *RateIterator) Tick(f func() error) error {
	// next tick becomes this tick
	ri.nextOccurrence -= 1.0

	// choose events can call function until event occurs in next tick
	for ri.nextOccurrence < 1.0 {
		err := f()
		if err != nil {
			return err
		}

		// Choose next event
		// Note the argument to Log is <= 1, so the right side is always negative and nextOccurrence increases
		ri.nextOccurrence -= math.Log(1.0-ri.rnd.Float64()) / ri.rate
	}
	return nil
}

// convenience method for when rate depends on changing variables
func (ri *RateIterator) TickWithRate(rate float64, f func() error) error {
	ri.rate = rate
	return ri.Tick(f)
}

///////////////////////////////////////
//
//  Win Count
//
///////////////////////////////////////

// This is the Filecoin algorithm for winning a ticket within a block with the tickets replaced
// with random numbers. It lets miners win according to a Poisson distribution with rate
// proportional to the miner's fraction of network power.
func WinCount(minerPower abi.StoragePower, totalPower abi.StoragePower, random float64) uint64 {
	E := big2.NewRat(5, 1)
	lambdaR := new(big2.Rat)
	lambdaR.SetFrac(minerPower.Int, totalPower.Int)
	lambdaR.Mul(lambdaR, E)
	lambda, _ := lambdaR.Float64()

	rhs := 1 - poissonPMF(lambda, 0)

	winCount := uint64(0)
	for rhs > random {
		winCount++
		rhs -= poissonPMF(lambda, winCount)
	}
	return winCount
}

func poissonPMF(lambda float64, k uint64) float64 {
	fk := float64(k)
	return (math.Exp(-lambda) * math.Pow(lambda, fk)) / fact(fk)
}

func fact(k float64) float64 {
	fact := 1.0
	for i := 2.0; i <= k; i += 1.0 {
		fact *= i
	}
	return fact
}
