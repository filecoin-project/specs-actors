package agent

import (
	"math"
	"math/rand"
)

type RateIterator struct {
	rnd           *rand.Rand
	rate          float64
	nextOccurence float64
}

func NewRateIterator(rate float64, seed int64) *RateIterator {
	ri := &RateIterator{
		rnd:           rand.New(rand.NewSource(seed)),
		rate:          rate,
		nextOccurence: 1.0, // next occurrence should happen next tick
	}
	ri.chooseNext() // randomize first occurrence
	return ri
}

// simulate random occurrences by calling the given function once for each event that would land in this epoch.
// The function will be called `rate` times on average, but may be called zero or many times in any Tick.
func (ri *RateIterator) Tick(f func() error) error {
	ri.nextOccurence -= 1.0
	for ri.nextOccurence < 1.0 {
		err := f()
		if err != nil {
			return err
		}
		ri.chooseNext()
	}
	return nil
}

// convenience method for when rate depends on changing variables
func (ri *RateIterator) TickWithRate(rate float64, f func() error) error {
	ri.rate = rate
	return ri.Tick(f)
}

// Choose next event according to a poisson distribution for the rate
func (ri *RateIterator) chooseNext() {
	ri.nextOccurence += -math.Log(1-ri.rnd.Float64()) / ri.rate
}
