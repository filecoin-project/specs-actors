package miner_test

import (
	"testing"

	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
)

func TestWPoStPeriodDeadlines(t *testing.T) {
	if (uint64(miner.WPoStProvingPeriod / miner.WPoStChallengeWindow)) != miner.WPoStPeriodDeadlines {
		t.Fatal("WPoStPeriodDeadlines must be equal to WPoStProvingPeriod / WPoStChallengeWindow")
	}
}
