package reward_test

import (
	"context"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

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

func TestWithdrawReward(t *testing.T) {
	actor := rewardHarness{reward.Actor{}, t}
	owner := tutil.NewIDAddr(t, 101)
	m := tutil.NewIDAddr(t, 1000)
	builder := mock.NewBuilder(context.Background(), builtin.RewardActorAddr).
		WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID).
		WithActorType(m, builtin.StorageMinerActorCodeID).
		WithActorType(owner, builtin.AccountActorCodeID)

	t.Run("successfully withdraw reward total", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		gasreward := abi.NewTokenAmount(10)
		rt.SetBalance(abi.NewTokenAmount(100000000))

		rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, big.Zero(), nil, 0)
		rt.Call(actor.AwardBlockReward, &reward.AwardBlockRewardParams{
			Miner:       m,
			Penalty:     big.Zero(),
			GasReward:   gasreward,
			TicketCount: 50,
		})
		rt.SetCaller(owner, builtin.AccountActorCodeID)
		rt.ExpectValidateCallerAddr(owner, owner)
		rt.ExpectSend(m, builtin.MethodsMiner.ControlAddresses, nil, big.Zero(), &mock.ReturnWrapper{V: &miner.GetControlAddressesReturn{owner, owner}}, 0)
		rt.ExpectSend(owner, builtin.MethodSend, nil, big.NewInt(100000000), nil, 0)
		rt.Call(actor.WithdrawReward, &m)

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

	var st reward.State
	rt.GetState(&st)
	emptyMap := adt.AsMultimap(adt.AsStore(rt), st.RewardMap)
	assert.Equal(h.t, emptyMap.Root(), st.RewardMap)
	assert.Equal(h.t, big.Zero(), st.RewardTotal)
}
