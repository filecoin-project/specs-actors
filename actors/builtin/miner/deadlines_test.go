package miner_test

import (
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/v4/actors/builtin/miner"
)

func TestProvingPeriodDeadlines(t *testing.T) {

	t.Run("quantization spec rounds to the next deadline", func(t *testing.T) {
		periodStart := abi.ChainEpoch(2)
		curr := periodStart + miner.WPoStProvingPeriod
		d := miner.NewDeadlineInfo(periodStart, 10, curr)
		quant := miner.QuantSpecForDeadline(d)
		assert.Equal(t, d.NextNotElapsed().Last(), quant.QuantizeUp(curr))
	})
}

func TestDeadlineInfoFromOffsetAndEpoch(t *testing.T) {
	// All proving periods equivalent mod WPoStProving period should give equivalent
	// dlines for a given epoch. Only the offset property should matter

	pp := abi.ChainEpoch(1972)
	ppThree := abi.ChainEpoch(1972 + 2880*3)
	ppMillion := abi.ChainEpoch(1972 + 2880*10e6)

	epochs := []abi.ChainEpoch{4, 2000, 400000, 5000000}
	for _, epoch := range epochs {
		dlineA := miner.NewDeadlineInfoFromOffsetAndEpoch(pp, epoch)
		dlineB := miner.NewDeadlineInfoFromOffsetAndEpoch(ppThree, epoch)
		dlineC := miner.NewDeadlineInfoFromOffsetAndEpoch(ppMillion, epoch)

		assert.Equal(t, *dlineA, *dlineB)
		assert.Equal(t, *dlineB, *dlineC)

	}

}
