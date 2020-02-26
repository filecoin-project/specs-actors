package reward_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/stretchr/testify/assert"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
)

func TestConstructor(t *testing.T) {
	actor := rewardHarness{reward.Actor{}, t}

	receiver := tutil.NewIDAddr(t, 1000)
	builder := mock.NewBuilder(context.Background(), receiver).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)
	rt := builder.Build(t)
	actor.constructAndVerify(rt)
}

func TestAwardBlockReward(t *testing.T) {
	actor := rewardHarness{reward.Actor{}, t}

	receiver := tutil.NewIDAddr(t, 1000)
	builder := mock.NewBuilder(context.Background(), receiver).WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("happy path block reward", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		miner := tutil.NewIDAddr(t, 1000)
		penalty := abi.NewTokenAmount(10)
		gasreward := abi.NewTokenAmount(10)
		nominalpwr := abi.NewStoragePower(10)
		rt.SetBalance(gasreward)
		actor.blockRewardAndVerify(rt, miner, penalty, gasreward, nominalpwr)
	})
}

type rewardHarness struct {
	reward.Actor
	t testing.TB
}

func (h *rewardHarness) constructAndVerify(rt *mock.Runtime) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	ret := rt.Call(h.Constructor, &adt.EmptyValue{})
	assert.Equal(h.t, &adt.EmptyValue{}, ret)
	rt.Verify()

	var st reward.State
	rt.GetState(&st)
	emptyMap := adt.AsMultimap(adt.AsStore(rt), st.RewardMap)
	assert.Equal(h.t, emptyMap.Root(), st.RewardMap)
	assert.Equal(h.t, big.Zero(), st.RewardTotal)
}

func (h *rewardHarness) blockRewardAndVerify(rt *mock.Runtime, miner address.Address,
	penalty, gasReward abi.TokenAmount, nominal abi.StoragePower) *adt.EmptyValue {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, penalty, nil, exitcode.Ok)
	ret := rt.Call(h.AwardBlockReward, &reward.AwardBlockRewardParams{
		Miner:        miner,
		Penalty:      penalty,
		GasReward:    gasReward,
		NominalPower: nominal,
	}).(*adt.EmptyValue)
	rt.Verify()
	return ret
}
