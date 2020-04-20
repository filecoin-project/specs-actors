package reward_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

func TestExports(t *testing.T) {
	mock.CheckActorExports(t, reward.Actor{})
}

func TestConstructor(t *testing.T) {
	actor := rewardHarness{reward.Actor{}, t}

	rt := mock.NewBuilder(context.Background(), builtin.RewardActorAddr).
		WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID).
		Build(t)

	actor.constructAndVerify(rt)
}

func TestAwardBlockReward(t *testing.T) {
	actor := rewardHarness{reward.Actor{}, t}
	builder := mock.NewBuilder(context.Background(), builtin.RewardActorAddr).
		WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("assertion failure when current balance is less than gas reward", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)
		miner := tutil.NewIDAddr(t, 1000)

		gasreward := abi.NewTokenAmount(10)
		rt.SetBalance(abi.NewTokenAmount(0))

		rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
		rt.ExpectAssertionFailure("actor current balance 0 insufficient to pay gas reward 10", func() {
			rt.Call(actor.AwardBlockReward, &reward.AwardBlockRewardParams{
				Miner:     miner,
				Penalty:   big.Zero(),
				GasReward: gasreward,
			})
		})
		rt.Verify()
	})
}

type rewardHarness struct {
	reward.Actor
	t testing.TB
}

func (h *rewardHarness) constructAndVerify(rt *mock.Runtime) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	ret := rt.Call(h.Constructor, nil)
	assert.Nil(h.t, ret)
	rt.Verify()

}
