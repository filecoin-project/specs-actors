package miner_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
)

func TestProvingPeriodDeadlines(t *testing.T) {
	PP := miner.WPoStProvingPeriod
	CW := miner.WPoStChallengeWindow

	t.Run("pre-open", func(t *testing.T) {
		curr := abi.ChainEpoch(0) // Current is before the period opens.
		{
			periodStart := miner.FaultDeclarationCutoff + 1
			di := miner.NewDeadlineInfo(periodStart, 0, curr)
			assert.Equal(t, curr, di.CurrentEpoch)
			assert.Equal(t, periodStart, di.PeriodStart)
			assert.Equal(t, uint64(0), di.Index)
			assert.Equal(t, periodStart, di.Open)
			assert.Equal(t, periodStart+CW, di.Close)
			assert.Equal(t, periodStart-miner.WPoStChallengeLookback, di.Challenge)
			assert.Equal(t, periodStart-miner.FaultDeclarationCutoff, di.FaultCutoff)

			assert.False(t, di.PeriodStarted())
			assert.False(t, di.PeriodElapsed())
			assert.False(t, di.IsOpen())
			assert.False(t, di.HasElapsed())
			assert.Equal(t, periodStart+CW-1, di.Last())
			assert.Equal(t, periodStart+CW, di.NextOpen())
			assert.False(t, di.FaultCutoffPassed())
			assert.Equal(t, periodStart+PP-1, di.PeriodEnd())
			assert.Equal(t, periodStart+PP, di.NextPeriodStart())
		}
		{
			periodStart := miner.FaultDeclarationCutoff - 1
			di := miner.NewDeadlineInfo(periodStart, 0, curr)
			assert.True(t, di.FaultCutoffPassed())
		}
	})

	t.Run("proving period boundary", func(t *testing.T) {
		periodStart := abi.ChainEpoch(50000)
		{
			// Period not yet started
			curr := periodStart - 1
			di := miner.NewDeadlineInfo(periodStart, 0, curr)
			assert.False(t, di.PeriodStarted()) // Not yet started
			assert.False(t, di.PeriodElapsed())
			assert.Equal(t, periodStart+PP-1, di.PeriodEnd())
			assert.Equal(t, periodStart+PP, di.NextPeriodStart())
		}
		{
			// Period started
			curr := periodStart
			di := miner.NewDeadlineInfo(periodStart, 0, curr)
			assert.True(t, di.PeriodStarted())
			assert.False(t, di.PeriodElapsed())
			assert.Equal(t, periodStart+PP-1, di.PeriodEnd())
			assert.Equal(t, periodStart+PP, di.NextPeriodStart())
		}
		{
			// Period elapsed
			curr := periodStart + PP
			di := miner.NewDeadlineInfo(periodStart, miner.WPoStPeriodDeadlines-1, curr)
			assert.True(t, di.PeriodStarted())
			assert.True(t, di.PeriodElapsed())
			assert.Equal(t, periodStart+PP-1, di.PeriodEnd())
			assert.Equal(t, periodStart+PP, di.NextPeriodStart())

			assert.False(t, di.IsOpen())
			assert.True(t, di.HasElapsed())
		}
	})

	t.Run("deadline boundaries", func(t *testing.T) {
		periodStart := abi.ChainEpoch(50000)
		{
			// First epoch of deadline zero
			curr := periodStart
			di := miner.NewDeadlineInfo(periodStart, 0, curr)

			assert.Equal(t, periodStart, di.Open)
			assert.Equal(t, periodStart+CW, di.Close)
			assert.Equal(t, periodStart-miner.WPoStChallengeLookback, di.Challenge)
			assert.Equal(t, periodStart-miner.FaultDeclarationCutoff, di.FaultCutoff)

			assert.True(t, di.IsOpen())
			assert.False(t, di.HasElapsed())
			assert.Equal(t, periodStart+CW-1, di.Last())
			assert.Equal(t, periodStart+CW, di.NextOpen())
			assert.True(t, di.FaultCutoffPassed())

			// The last invalid epoch of a deadline is the first valid epoch for the next.
			assert.Equal(t, di.Last()+1, di.NextOpen())
			assert.Equal(t, di.Close, di.NextOpen())
		}
		{
			// Before deadline zero opens
			curr := periodStart - 1
			di := miner.NewDeadlineInfo(periodStart, 0, curr)

			assert.False(t, di.IsOpen()) // Not yet open
			assert.False(t, di.HasElapsed())
			assert.True(t, di.FaultCutoffPassed())

			// The next not-elapsed is this one, because it hasn't even started yet.
			nxt := di.NextNotElapsed()
			assert.Equal(t, periodStart, nxt.PeriodStart)
			assert.Equal(t, uint64(0), nxt.Index)
		}
		{
			// During deadline zero, deadline one isn't open
			curr := periodStart
			di0 := miner.NewDeadlineInfo(periodStart, 0, curr)
			assert.True(t, di0.IsOpen()) // Now open
			assert.False(t, di0.HasElapsed())
			assert.True(t, di0.FaultCutoffPassed())

			// The next not-elapsed is this one, which is not yet
			// open, but not elapsed either.
			nxt0 := di0.NextNotElapsed()
			assert.Equal(t, periodStart, nxt0.PeriodStart)
			assert.Equal(t, uint64(0), nxt0.Index)

			di1 := miner.NewDeadlineInfo(periodStart, 1, curr)
			assert.False(t, di1.IsOpen())
			assert.False(t, di1.HasElapsed())
			// The fault cutoff is more than one deadline into the future.
			assert.True(t, di1.FaultCutoffPassed())

			// The next not-elapsed is the upcoming one
			nxt1 := di1.NextNotElapsed()
			assert.Equal(t, periodStart, nxt1.PeriodStart)
			assert.Equal(t, uint64(1), nxt1.Index)
		}
		{
			// Last epoch of deadline zero
			curr := periodStart + miner.WPoStChallengeWindow - 1
			di := miner.NewDeadlineInfo(periodStart, 0, curr)

			assert.True(t, di.IsOpen())
			assert.False(t, di.HasElapsed())
			assert.True(t, di.FaultCutoffPassed())

			// The next not-elapsed is this one still
			nxt := di.NextNotElapsed()
			assert.Equal(t, periodStart, nxt.PeriodStart)
			assert.Equal(t, uint64(0), nxt.Index)
		}
		{
			// Deadline zero expired
			curr := periodStart + miner.WPoStChallengeWindow
			di := miner.NewDeadlineInfo(periodStart, 0, curr)

			assert.False(t, di.IsOpen())
			assert.True(t, di.HasElapsed())
			assert.True(t, di.FaultCutoffPassed())

			// The next not-elapsed is the subsequent proving period
			nxt := di.NextNotElapsed()
			assert.Equal(t, periodStart+miner.WPoStProvingPeriod, nxt.PeriodStart)
			assert.Equal(t, uint64(0), nxt.Index)
		}
	})

	t.Run("period expired", func(t *testing.T) {
		periodStart := abi.ChainEpoch(0)
		curr := periodStart + miner.WPoStProvingPeriod
		d := miner.NewDeadlineInfo(periodStart, miner.WPoStPeriodDeadlines, curr)
		assert.True(t, d.PeriodStarted())
		assert.True(t, d.PeriodElapsed())
		assert.Equal(t, miner.WPoStPeriodDeadlines, d.Index)
		assert.False(t, d.IsOpen())
		assert.True(t, d.HasElapsed())
		assert.True(t, d.FaultCutoffPassed())
		assert.Equal(t, miner.WPoStProvingPeriod-1, d.PeriodEnd())
		assert.Equal(t, miner.WPoStProvingPeriod, d.NextPeriodStart())
	})

	t.Run("quantization spec rounds to the next deadline", func(t *testing.T) {
		periodStart := abi.ChainEpoch(2)
		curr := periodStart + miner.WPoStProvingPeriod
		d := miner.NewDeadlineInfo(periodStart, 10, curr)
		quant := d.QuantSpec()
		assert.Equal(t, d.NextNotElapsed().Last(), quant.QuantizeUp(curr))
	})
}
