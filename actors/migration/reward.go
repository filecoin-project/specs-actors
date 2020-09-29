package migration

import (
	"context"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	reward0 "github.com/filecoin-project/specs-actors/actors/builtin/reward"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

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

	outState := reward2.State{
		CumsumBaseline:          inState.CumsumBaseline,
		CumsumRealized:          inState.CumsumRealized,
		EffectiveNetworkTime:    inState.EffectiveNetworkTime,
		EffectiveBaselinePower:  inState.EffectiveBaselinePower,
		ThisEpochReward:         inState.ThisEpochReward,
		ThisEpochRewardSmoothed: smoothing2.FilterEstimate(*inState.ThisEpochRewardSmoothed),
		ThisEpochBaselinePower:  inState.ThisEpochBaselinePower,
		Epoch:                   inState.Epoch,
		TotalStoragePowerReward: inState.TotalMined,
	}
	newHead, err := store.Put(ctx, &outState)
	return newHead, big.Zero(), err
}
