package reward_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/stretchr/testify/assert"

	address "github.com/filecoin-project/go-address"
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

	t.Run("construct with 0 power", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), builtin.RewardActorAddr).
			WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID).
			Build(t)
		startRealizedPower := abi.NewStoragePower(0)
		actor.constructAndVerify(rt, &startRealizedPower)
		st := getState(rt)
		assert.Equal(t, abi.ChainEpoch(0), st.Epoch)
		assert.Equal(t, abi.NewStoragePower(0), st.CumsumRealized)
		assert.Equal(t, big.MustFromString("36266264293777134739"), st.ThisEpochReward)
		epochZeroBaseline := big.Sub(reward.BaselineInitialValue, big.NewInt(1)) // account for rounding error of one byte during construction
		assert.Equal(t, epochZeroBaseline, st.ThisEpochBaselinePower)
		assert.Equal(t, reward.BaselineInitialValue, st.EffectiveBaselinePower)
	})
	t.Run("construct with less power than baseline", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), builtin.RewardActorAddr).
			WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID).
			Build(t)
		startRealizedPower := big.Lsh(abi.NewStoragePower(1), 39)
		actor.constructAndVerify(rt, &startRealizedPower)
		st := getState(rt)
		assert.Equal(t, abi.ChainEpoch(0), st.Epoch)
		assert.Equal(t, startRealizedPower, st.CumsumRealized)

		assert.NotEqual(t, big.Zero(), st.ThisEpochReward)
	})
	t.Run("construct with more power than baseline", func(t *testing.T) {
		rt := mock.NewBuilder(context.Background(), builtin.RewardActorAddr).
			WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID).
			Build(t)
		startRealizedPower := big.Lsh(abi.NewStoragePower(1), 60)
		actor.constructAndVerify(rt, &startRealizedPower)
		st := getState(rt)
		rwrd := st.ThisEpochReward

		// start with 2x power
		rt = mock.NewBuilder(context.Background(), builtin.RewardActorAddr).
			WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID).
			Build(t)
		startRealizedPower = big.Lsh(abi.NewStoragePower(2), 60)
		actor.constructAndVerify(rt, &startRealizedPower)
		newSt := getState(rt)
		// Reward value is the same; realized power impact on reward is capped at baseline
		assert.Equal(t, rwrd, newSt.ThisEpochReward)
	})

}

func TestAwardBlockReward(t *testing.T) {
	actor := rewardHarness{reward.Actor{}, t}
	builder := mock.NewBuilder(context.Background(), builtin.RewardActorAddr).
		WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)

	t.Run("assertion failure when current balance is less than gas reward", func(t *testing.T) {
		rt := builder.Build(t)
		startRealizedPower := abi.NewStoragePower(0)
		actor.constructAndVerify(rt, &startRealizedPower)
		miner := tutil.NewIDAddr(t, 1000)

		gasreward := abi.NewTokenAmount(10)
		rt.SetBalance(abi.NewTokenAmount(0))

		rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
		rt.ExpectAssertionFailure("actor current balance 0 insufficient to pay gas reward 10", func() {
			rt.Call(actor.AwardBlockReward, &reward.AwardBlockRewardParams{
				Miner:     miner,
				Penalty:   big.Zero(),
				GasReward: gasreward,
				WinCount:  1,
			})
		})
		rt.Verify()
	})

	t.Run("pays out current balance when reward exceeds total balance", func(t *testing.T) {
		rt := builder.Build(t)
		startRealizedPower := abi.NewStoragePower(1)
		actor.constructAndVerify(rt, &startRealizedPower)
		miner := tutil.NewIDAddr(t, 1000)

		// Total reward is a huge number, upon writing ~1e18, so 300 should be way less
		smallReward := abi.NewTokenAmount(300)
		rt.SetBalance(smallReward)
		rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
		rt.ExpectSend(miner, builtin.MethodsMiner.AddLockedFund, &smallReward, smallReward, nil, 0)
		rt.Call(actor.AwardBlockReward, &reward.AwardBlockRewardParams{
			Miner:     miner,
			Penalty:   big.Zero(),
			GasReward: big.Zero(),
			WinCount:  1,
		})
		rt.Verify()
	})

	t.Run("TotalMined tracks correctly", func(t *testing.T) {
		rt := builder.Build(t)
		startRealizedPower := abi.NewStoragePower(1)
		actor.constructAndVerify(rt, &startRealizedPower)
		miner := tutil.NewIDAddr(t, 1000)

		st := getState(rt)
		assert.Equal(t, big.Zero(), st.TotalMined)
		st.ThisEpochReward = abi.NewTokenAmount(5000)
		rt.ReplaceState(st)
		// enough balance to pay 3 full rewards and one partial
		totalPayout := abi.NewTokenAmount(3500)
		rt.SetBalance(totalPayout)

		// award normalized by expected leaders is 1000
		actor.awardBlockReward(rt, miner, big.Zero(), big.Zero(), 1, big.NewInt(1000))
		actor.awardBlockReward(rt, miner, big.Zero(), big.Zero(), 1, big.NewInt(1000))
		actor.awardBlockReward(rt, miner, big.Zero(), big.Zero(), 1, big.NewInt(1000))
		actor.awardBlockReward(rt, miner, big.Zero(), big.Zero(), 1, big.NewInt(500)) // partial payout when balance below reward

		newState := getState(rt)
		assert.Equal(t, totalPayout, newState.TotalMined)

	})

	t.Run("funds are sent to the burnt funds actor if sending locked funds to miner fails", func(t *testing.T) {
		rt := builder.Build(t)
		startRealizedPower := abi.NewStoragePower(1)
		actor.constructAndVerify(rt, &startRealizedPower)
		miner := tutil.NewIDAddr(t, 1000)
		st := getState(rt)
		assert.Equal(t, big.Zero(), st.TotalMined)
		st.ThisEpochReward = abi.NewTokenAmount(5000)
		rt.ReplaceState(st)
		// enough balance to pay 3 full rewards and one partial
		totalPayout := abi.NewTokenAmount(3500)
		rt.SetBalance(totalPayout)

		rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
		expectedReward := big.NewInt(1000)
		rt.ExpectSend(miner, builtin.MethodsMiner.AddLockedFund, &expectedReward, expectedReward, nil, exitcode.ErrForbidden)
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, expectedReward, nil, exitcode.Ok)

		rt.Call(actor.AwardBlockReward, &reward.AwardBlockRewardParams{
			Miner:     miner,
			Penalty:   big.Zero(),
			GasReward: big.Zero(),
			WinCount:  1,
		})

		rt.Verify()
	})
}

func TestSuccessiveKPIUpdates(t *testing.T) {
	actor := rewardHarness{reward.Actor{}, t}
	builder := mock.NewBuilder(context.Background(), builtin.RewardActorAddr).
		WithCaller(builtin.SystemActorAddr, builtin.SystemActorCodeID)
	rt := builder.Build(t)
	power := abi.NewStoragePower(1 << 50)
	actor.constructAndVerify(rt, &power)

	rt.SetEpoch(abi.ChainEpoch(1))
	actor.updateNetworkKPI(rt, &power)

	rt.SetEpoch(abi.ChainEpoch(2))
	actor.updateNetworkKPI(rt, &power)

	rt.SetEpoch(abi.ChainEpoch(3))
	actor.updateNetworkKPI(rt, &power)

}

type rewardHarness struct {
	reward.Actor
	t testing.TB
}

func (h *rewardHarness) constructAndVerify(rt *mock.Runtime, currRawPower *abi.StoragePower) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	ret := rt.Call(h.Constructor, currRawPower)
	assert.Nil(h.t, ret)
	rt.Verify()

}

func (h *rewardHarness) updateNetworkKPI(rt *mock.Runtime, currRawPower *abi.StoragePower) {
	rt.SetCaller(builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID)
	rt.ExpectValidateCallerAddr(builtin.StoragePowerActorAddr)
	ret := rt.Call(h.UpdateNetworkKPI, currRawPower)
	assert.Nil(h.t, ret)
	rt.Verify()
}

func (h *rewardHarness) awardBlockReward(rt *mock.Runtime, miner address.Address, penalty, gasReward abi.TokenAmount, winCount int64, expectedTotalReward abi.TokenAmount) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	rt.ExpectSend(miner, builtin.MethodsMiner.AddLockedFund, &expectedTotalReward, expectedTotalReward, nil, 0)
	rt.Call(h.AwardBlockReward, &reward.AwardBlockRewardParams{
		Miner:     miner,
		Penalty:   big.Zero(),
		GasReward: big.Zero(),
		WinCount:  1,
	})
	if penalty.GreaterThan(big.Zero()) {
		rt.ExpectSend(builtin.BurntFundsActorAddr, builtin.MethodSend, &penalty, penalty, nil, 0)
	}
	rt.Verify()
}

func getState(rt *mock.Runtime) *reward.State {
	var st reward.State
	rt.GetState(&st)
	return &st
}
