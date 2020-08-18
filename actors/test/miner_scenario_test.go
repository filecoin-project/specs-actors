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

	//
	// precommit sector
	//

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

	// find information about precommited sector
	var minerState miner.State
	err = v.GetState(minerAddrs.IDAddress, &minerState)
	require.NoError(t, err)

	precommit, found, err := minerState.GetPrecommittedSector(v.Store(), sectorNumber)
	require.NoError(t, err)
	require.True(t, found)

	// advance time to max seal duration
	proveTime := v.GetEpoch() + miner.MaxSealDuration[sealProof]
	v, dlInfo := vm.AdvanceTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	//
	// overdue precommit
	//

	t.Run("missed prove commit results in precommit expiry", func(t *testing.T) {
		// advanced one more deadline so precommit is late
		tv, _ := v.WithEpoch(dlInfo.Close)
		require.NoError(t, err)

		// run cron which should expire precommit
		_, code = tv.ApplyMessage(builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
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

						// The call to burnt funds indicates the overdue precommit has been penalized
						{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend, Value: vm.ExpectAttoFil(precommit.PreCommitDeposit)},
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
					}},
					//{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.ConfirmSectorProofsValid},
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
				}},
				{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick},
			},
		}.Matches(t, tv.Invocations()[0])
	})

	//
	// prove and verify
	//

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
				// expect confirm sector proofs valid because we prove committed,
				// but not an on deferred cron event because this is not a deadline boundary
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

	//
	// Submit PoSt
	//

	// find information about committed sector
	err = v.GetState(minerAddrs.IDAddress, &minerState)
	require.NoError(t, err)

	dlIdx, pIdx, err := minerState.FindSector(v.Store(), sectorNumber)
	require.NoError(t, err)
	sector, found, err := minerState.GetSector(v.Store(), sectorNumber)
	require.NoError(t, err)
	require.True(t, found)

	// advance time to next proving period
	v, dlInfo = vm.AdvanceTillIndex(t, v, minerAddrs.IDAddress, dlIdx)
	v, err = v.WithEpoch(dlInfo.Open)
	require.NoError(t, err)

	t.Run("submit PoSt succeeds", func(t *testing.T) {
		tv, err := v.WithEpoch(v.GetEpoch())
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
			ChainCommitEpoch: tv.GetEpoch() - 1,
			ChainCommitRand:  []byte("not really random"),
		}
		_, code = tv.ApplyMessage(addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)
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
		}.Matches(t, tv.Invocations()[0])
	})

	t.Run("skip sector", func(t *testing.T) {
		tv, err := v.WithEpoch(v.GetEpoch())
		require.NoError(t, err)

		// Submit PoSt
		submitParams := miner.SubmitWindowedPoStParams{
			Deadline: dlIdx,
			Partitions: []miner.PoStPartition{{
				Index:   pIdx,
				Skipped: bitfield.NewFromSet([]uint64{uint64(sectorNumber)}),
			}},
			Proofs: []abi.PoStProof{{
				PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			}},
			ChainCommitEpoch: tv.GetEpoch() - 1,
			ChainCommitRand:  []byte("not really random"),
		}
		_, code = tv.ApplyMessage(addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)
		require.Equal(t, exitcode.Ok, code)

		sectorSize, err := sector.SealProof.SectorSize()
		require.NoError(t, err)
		sectorPower := miner.PowerForSector(sectorSize, sector)
		updatePowerParams := &power.UpdateClaimedPowerParams{
			RawByteDelta:         sectorPower.Raw.Neg(),
			QualityAdjustedDelta: sectorPower.QA.Neg(),
		}

		vm.ExpectInvocation{
			// Original send to storage power actor
			To:     minerAddrs.IDAddress,
			Method: builtin.MethodsMiner.SubmitWindowedPoSt,
			Params: vm.ExpectObject(&submitParams),
			SubInvocations: []vm.ExpectInvocation{
				{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
				{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},

				// This call to the power actor indicates power has been removed for the sector
				{
					To:     builtin.StoragePowerActorAddr,
					Method: builtin.MethodsPower.UpdateClaimedPower,
					Params: vm.ExpectObject(updatePowerParams),
				},
				// This call to the burnt funds actor indicates miner has been penalized for missing PoSt
				{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
			},
		}.Matches(t, tv.Invocations()[0])
	})

	t.Run("missed first PoSt deadline", func(t *testing.T) {
		// move to proving period end
		tv, err := v.WithEpoch(dlInfo.Last())
		require.NoError(t, err)

		// Run cron to detect missing PoSt
		_, code = tv.ApplyMessage(builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
		require.Equal(t, exitcode.Ok, code)

		sectorSize, err := sector.SealProof.SectorSize()
		require.NoError(t, err)
		sectorPower := miner.PowerForSector(sectorSize, sector)
		updatePowerParams := &power.UpdateClaimedPowerParams{
			RawByteDelta:         sectorPower.Raw.Neg(),
			QualityAdjustedDelta: sectorPower.QA.Neg(),
		}

		vm.ExpectInvocation{
			// Original send to storage power actor
			To:     builtin.CronActorAddr,
			Method: builtin.MethodsCron.EpochTick,
			SubInvocations: []vm.ExpectInvocation{
				{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.OnEpochTickEnd, SubInvocations: []vm.ExpectInvocation{
					{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.OnDeferredCronEvent, SubInvocations: []vm.ExpectInvocation{
						{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},

						// This call to the power actor indicates power has been removed for the sector
						{
							To:     builtin.StoragePowerActorAddr,
							Method: builtin.MethodsPower.UpdateClaimedPower,
							Params: vm.ExpectObject(updatePowerParams),
						},
						// This call to the burnt funds actor indicates miner has been penalized for missing PoSt
						{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},

						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
					}},
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
				}},
				{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick},
			},
		}.Matches(t, tv.Invocations()[0])
	})
}
