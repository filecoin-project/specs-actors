package test

import (
	"context"
	"testing"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
	vm "github.com/filecoin-project/specs-actors/support/vm"

	"github.com/filecoin-project/go-bitfield"
	"github.com/stretchr/testify/require"
)

func TestCommitPoStFlow(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t)
	addrs := vm.CreateAccounts(ctx, t, v, 1, big.Mul(big.NewInt(10_000), big.NewInt(1e18)), 93837778)

	minerBalance := big.Mul(big.NewInt(10_000), vm.FIL)
	sectorNumber := abi.SectorNumber(100)
	sealedCid := tutil.MakeCID("100", &miner.SealedCIDPrefix)
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1

	// create miner
	params := power.CreateMinerParams{
		Owner:         addrs[0],
		Worker:        addrs[0],
		SealProofType: sealProof,
		Peer:          abi.PeerID("not really a peer id"),
	}
	ret, code := v.ApplyMessage(addrs[0], builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)
	require.Equal(t, exitcode.Ok, code)

	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	// advance vm so we can have seal randomness epoch in the past
	v, err := v.WithEpoch(200)
	require.NoError(t, err)

	// precommit sector
	preCommitParams := miner.SectorPreCommitInfo{
		SealProof:     sealProof,
		SectorNumber:  sectorNumber,
		SealedCID:     sealedCid,
		SealRandEpoch: v.GetEpoch() - 1,
		DealIDs:       nil,
		Expiration:    v.GetEpoch() + miner.MinSectorExpiration + miner.MaxSealDuration[sealProof] + 100,
	}
	_, code = v.ApplyMessage(addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)
	require.Equal(t, exitcode.Ok, code)

	// assert successful precommit invocation
	vm.ExpectInvocation{
		// Original send to storage power actor
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.PreCommitSector,
		Params: vm.ExpectObject(&preCommitParams),
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.VerifyDealsForActivation}},
	}.Matches(t, v.Invocations()[0])

	// advance time to max seal duration
	proveTime := v.GetEpoch() + miner.MaxSealDuration[sealProof]
	v, _ = vm.AdvanceTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	// Prove commit sector after max seal duration
	v, err = v.WithEpoch(proveTime)
	require.NoError(t, err)
	proveCommitParams := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}
	_, code = v.ApplyMessage(addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)
	require.Equal(t, exitcode.Ok, code)

	vm.ExpectInvocation{
		// Original send to storage power actor
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.ProveCommitSector,
		Params: vm.ExpectObject(&proveCommitParams),
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.ComputeDataCommitment},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.SubmitPoRepForBulkVerify},
		},
	}.Matches(t, v.Invocations()[0])

	// In the same epoch, trigger cron to validate prove commit
	_, code = v.ApplyMessage(builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
	require.Equal(t, exitcode.Ok, code)

	vm.ExpectInvocation{
		// Original send to storage power actor
		To:     builtin.CronActorAddr,
		Method: builtin.MethodsCron.EpochTick,
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.OnEpochTickEnd, SubInvocations: []vm.ExpectInvocation{
				{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.OnDeferredCronEvent, SubInvocations: []vm.ExpectInvocation{
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
					{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
					{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
				}},
				{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.ConfirmSectorProofsValid, SubInvocations: []vm.ExpectInvocation{
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
					{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
					{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.ActivateDeals},
					{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdateClaimedPower},
					{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdatePledgeTotal},
				}},
				{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
			}},
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick},
		},
	}.Matches(t, v.Invocations()[1])

	// advance time to next proving period
	var minerState miner.State
	err = v.GetState(minerAddrs.IDAddress, &minerState)
	require.NoError(t, err)

	dlIdx, pIdx, err := minerState.FindSector(v.Store(), sectorNumber)
	require.NoError(t, err)
	v, dlInfo := vm.AdvanceTillIndex(t, v, minerAddrs.IDAddress, dlIdx)
	v, err = v.WithEpoch(dlInfo.Open)
	require.NoError(t, err)

	// Submit PoSt
	submitParams := miner.SubmitWindowedPoStParams{
		Deadline: dlIdx,
		Partitions: []miner.PoStPartition{{
			Index:   pIdx,
			Skipped: bitfield.New(),
		}},
		Proofs: []abi.PoStProof{{
			PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		}},
		ChainCommitEpoch: v.GetEpoch() - 1,
		ChainCommitRand:  []byte("not really random"),
	}
	_, code = v.ApplyMessage(addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)
	require.Equal(t, exitcode.Ok, code)

	vm.ExpectInvocation{
		// Original send to storage power actor
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.SubmitWindowedPoSt,
		Params: vm.ExpectObject(&submitParams),
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
		},
	}.Matches(t, v.Invocations()[0])
}
