package reward_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

	t.Run("block reward with no payable reward", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		miner := tutil.NewIDAddr(t, 1000)
		penalty := abi.NewTokenAmount(10)
		gasreward := abi.NewTokenAmount(10)
		nominalpwr := abi.NewStoragePower(10)
		rt.SetBalance(gasreward)
		actor.blockRewardAndVerify(rt, miner, penalty, gasreward, nominalpwr)

		var st reward.State
		rt.GetState(&st)
		rewardMap := adt.AsMultimap(adt.AsStore(rt), st.RewardMap)
		assert.Equal(t, big.Zero(), st.RewardTotal)
		var r []reward.Reward
		var rwd reward.Reward
		err := rewardMap.ForEach(reward.AddrKey(miner), &rwd, func(i int64) error {
			r = append(r, rwd)
			return nil
		})
		require.NoError(t, err)
		// there are 0 rewards in the actors state
		assert.Equal(t, 0, len(r))
	})

	t.Run("block reward with reward payable", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		miner := tutil.NewIDAddr(t, 1000)
		penalty := abi.NewTokenAmount(10)
		gasreward := abi.NewTokenAmount(10)
		nominalpwr := abi.NewStoragePower(10)
		rt.SetBalance(big.NewInt(20))
		actor.blockRewardAndVerify(rt, miner, penalty, gasreward, nominalpwr)

		var st reward.State
		rt.GetState(&st)
		rewardMap := adt.AsMultimap(adt.AsStore(rt), st.RewardMap)
		assert.Equal(t, big.NewInt(10), st.RewardTotal)
		var r []reward.Reward
		var rwd reward.Reward
		err := rewardMap.ForEach(reward.AddrKey(miner), &rwd, func(i int64) error {
			r = append(r, rwd)
			return nil
		})
		require.NoError(t, err)
		// there is a single reward in the actor state.
		require.Equal(t, 1, len(r))

		assert.Equal(t, abi.ChainEpoch(0), r[0].StartEpoch)
		assert.Equal(t, 0+reward.RewardVestingPeriod, r[0].EndEpoch)
		assert.Equal(t, big.NewInt(10), r[0].Value)
		assert.Equal(t, big.Zero(), r[0].AmountWithdrawn)
		assert.Equal(t, reward.RewardVestingFunction, r[0].VestingFunction)

	})

	t.Run("block reward send failure", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		miner := tutil.NewIDAddr(t, 1000)
		penalty := abi.NewTokenAmount(10)
		gasreward := abi.NewTokenAmount(10)
		nominalpwr := abi.NewStoragePower(10)
		rt.SetBalance(gasreward)
		rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, penalty, nil, exitcode.ErrForbidden)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			rt.Call(actor.AwardBlockReward, &reward.AwardBlockRewardParams{
				Miner:        miner,
				Penalty:      penalty,
				GasReward:    gasreward,
				NominalPower: nominalpwr,
			})
		})
		rt.Verify()
	})

	t.Run("assertion failure when miner is not ID-address protocol", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		miner := tutil.NewSECP256K1Addr(t, "bleepbloob")
		penalty := abi.NewTokenAmount(10)
		gasreward := abi.NewTokenAmount(10)
		nominalpwr := abi.NewStoragePower(10)
		rt.SetBalance(gasreward)
		rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
		rt.ExpectAssertionFailure("miner address must be ID-address protocol (0), was protocol 1", func() {
			rt.Call(actor.AwardBlockReward, &reward.AwardBlockRewardParams{
				Miner:        miner,
				Penalty:      penalty,
				GasReward:    gasreward,
				NominalPower: nominalpwr,
			})
		})
		rt.Verify()
	})

	t.Run("assertion failure when current balance is less than gas reward", func(t *testing.T) {
		rt := builder.Build(t)
		actor.constructAndVerify(rt)

		miner := tutil.NewIDAddr(t, 1000)
		penalty := abi.NewTokenAmount(10)
		gasreward := abi.NewTokenAmount(10)
		nominalpwr := abi.NewStoragePower(10)
		rt.SetBalance(abi.NewTokenAmount(0))
		rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
		rt.ExpectAssertionFailure("actor current balance 0 insufficient to pay gas reward 10", func() {
			rt.Call(actor.AwardBlockReward, &reward.AwardBlockRewardParams{
				Miner:        miner,
				Penalty:      penalty,
				GasReward:    gasreward,
				NominalPower: nominalpwr,
			})
		})
		rt.Verify()
	})
	// TODO test reward payable + penalty exceeds balance assertion failure when blockreward function is complete
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
