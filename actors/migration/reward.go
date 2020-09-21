package migration

import (
	"context"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	reward0 "github.com/filecoin-project/specs-actors/actors/builtin/reward"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	reward2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/reward"
	smoothing2 "github.com/filecoin-project/specs-actors/v2/actors/util/smoothing"
)

type rewardMigrator struct {
}

func (m *rewardMigrator) MigrateState(ctx context.Context, store cbor.IpldStore, head cid.Cid, _ abi.TokenAmount) (cid.Cid, abi.TokenAmount, error) {
	var inState reward0.State
	if err := store.Get(ctx, head, &inState); err != nil {
		return cid.Undef, big.Zero(), err
	}

	// The baseline function initial value and growth rate are changed from v0.
	// As an approximation to working out what the baseline value would be at the migration epoch
	// had the baseline parameters been this way all along, this just sets the immediately value to the
	// new (higher) initial value, essentially restarting its calculation.
	// The baseline and realized cumsums, and effective network time, are not changed.
	// This boils down to a step change in the baseline function, as if it had been defined piecewise.
	// This will be a bit annoying for external analytical calculations of the baseline function.

	if inState.ThisEpochBaselinePower.GreaterThan(reward2.BaselineInitialValue) {
		return cid.Undef, big.Zero(), xerrors.Errorf("unexpected baseline power %v higher than new initial value %v",
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
		TotalStoragePowerReward: inState.TotalMined,
	}
	newHead, err := store.Put(ctx, &outState)
	return newHead, big.Zero(), err
}
