package migration

import (
	"context"

	reward0 "github.com/filecoin-project/specs-actors/actors/builtin/reward"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	reward2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/reward"
	smoothing2 "github.com/filecoin-project/specs-actors/v2/actors/util/smoothing"
)

type rewardMigrator struct {
}

func (m *rewardMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid) (cid.Cid, error) {
	var inState reward0.State
	if err := store.Get(ctx, head, &inState); err != nil {
		return cid.Undef, err
	}

	// The baseline function initial value and growth rate are changed from v0.
	// As an approximation to working out what the baseline value would be at the migration epoch
	// had the baseline parameters been this way all along, this just sets the immediately value to the
	// new (higher) initial value, essentially restarting its calculation.
	// The baseline and realized cumsums, and effective network time, are not changed.
	// This boils down to a step change in the baseline function, as if it had been defined piecewise.
	// This will be a bit annoying for external analytical calculations of the baseline function.

	if inState.ThisEpochBaselinePower.GreaterThan(reward2.BaselineInitialValue) {
		return cid.Undef, xerrors.Errorf("unexpected baseline power %v higher than new initial value %v",
			inState.ThisEpochBaselinePower, reward2.BaselineInitialValue)
	}

	outState := reward2.State{
		CumsumBaseline:          inState.CumsumBaseline,
		CumsumRealized:          inState.CumsumRealized,
		EffectiveNetworkTime:    inState.EffectiveNetworkTime,
		EffectiveBaselinePower:  inState.EffectiveBaselinePower,
		ThisEpochReward:         inState.ThisEpochReward,
		ThisEpochRewardSmoothed: smoothing2.FilterEstimate(*inState.ThisEpochRewardSmoothed),
		ThisEpochBaselinePower:  reward2.BaselineInitialValue,
		Epoch:                   inState.Epoch,
		TotalMined:              inState.TotalMined,
	}
	return store.Put(ctx, &outState)
}

