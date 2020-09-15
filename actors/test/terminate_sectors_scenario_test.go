package test_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/verifreg"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"
	tutil "github.com/filecoin-project/specs-actors/v2/support/testing"
	vm "github.com/filecoin-project/specs-actors/v2/support/vm"
)

// This scenario hits all Market Actor methods.
func TestTerminateSectors(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t)
	addrs := vm.CreateAccounts(ctx, t, v, 4, big.Mul(big.NewInt(10_000), vm.FIL), 93837778)
	worker, verifier, unverifiedClient, verifiedClient := addrs[0], addrs[1], addrs[2], addrs[3]

	minerBalance := big.Mul(big.NewInt(1_000), vm.FIL)
	sectorNumber := abi.SectorNumber(100)
	sealedCid := tutil.MakeCID("100", &miner.SealedCIDPrefix)
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1

	// create miner
	params := power.CreateMinerParams{
		Owner:         worker,
		Worker:        worker,
		SealProofType: sealProof,
		Peer:          abi.PeerID("not really a peer id"),
	}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)

	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	//
	// publish verified and unverified deals
	//

	// register verifier then verified client
	addVerifierParams := verifreg.AddVerifierParams{
		Address:   verifier,
		Allowance: abi.NewStoragePower(32 << 40),
	}
	vm.ApplyOk(t, v, vm.VerifregRoot, builtin.VerifiedRegistryActorAddr, big.Zero(), builtin.MethodsVerifiedRegistry.AddVerifier, &addVerifierParams)

	addClientParams := verifreg.AddVerifiedClientParams{
		Address:   verifiedClient,
		Allowance: abi.NewStoragePower(32 << 40),
	}
	vm.ApplyOk(t, v, verifier, builtin.VerifiedRegistryActorAddr, big.Zero(), builtin.MethodsVerifiedRegistry.AddVerifiedClient, &addClientParams)

	// add market collateral for clients and miner
	collateral := big.Mul(big.NewInt(3), vm.FIL)
	vm.ApplyOk(t, v, unverifiedClient, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &unverifiedClient)
	vm.ApplyOk(t, v, verifiedClient, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &verifiedClient)
	minerCollateral := big.Mul(big.NewInt(64), vm.FIL)
	vm.ApplyOk(t, v, worker, builtin.StorageMarketActorAddr, minerCollateral, builtin.MethodsMarket.AddBalance, &minerAddrs.IDAddress)

	// create 3 deals, some verified and some not
	dealIDs := []abi.DealID{}
	dealStart := v.GetEpoch() + miner.PreCommitChallengeDelay + 1
	deals := publishDeal(t, v, worker, verifiedClient, minerAddrs.IDAddress, "deal1", 1<<30, true, dealStart, 181*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)
	deals = publishDeal(t, v, worker, verifiedClient, minerAddrs.IDAddress, "deal2", 1<<32, true, dealStart, 200*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)
	deals = publishDeal(t, v, worker, unverifiedClient, minerAddrs.IDAddress, "deal3", 1<<34, false, dealStart, 210*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)

	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
	for _, id := range dealIDs {
		// deals are pending and don't yet have deal states
		_, found := vm.GetDealState(t, v, id)
		require.False(t, found)
	}

	//
	// Precommit, Prove, Verify and PoSt sector with deals
	//

	// precommit sector with deals
	preCommitParams := miner.PreCommitSectorParams{
		SealProof:       sealProof,
		SectorNumber:    sectorNumber,
		SealedCID:       sealedCid,
		SealRandEpoch:   v.GetEpoch() - 1,
		DealIDs:         dealIDs,
		Expiration:      v.GetEpoch() + 220*builtin.EpochsInDay,
		ReplaceCapacity: false,
	}
	vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)

	// advance time to min seal duration
	proveTime := v.GetEpoch() + miner.PreCommitChallengeDelay + 1
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	// Prove commit sector after max seal duration
	v, err := v.WithEpoch(proveTime)
	require.NoError(t, err)
	proveCommitParams := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}
	vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)

	// In the same epoch, trigger cron to validate prove commit
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// advance to proving period and submit post
	dlInfo, pIdx, v := vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)
	submitParams := miner.SubmitWindowedPoStParams{
		Deadline: dlInfo.Index,
		Partitions: []miner.PoStPartition{{
			Index:   pIdx,
			Skipped: bitfield.New(),
		}},
		Proofs: []proof.PoStProof{{
			PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		}},
		ChainCommitEpoch: dlInfo.Challenge,
		ChainCommitRand:  []byte("not really random"),
	}
	vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)

	// proving period cron adds miner power
	v, err = v.WithEpoch(dlInfo.Last())
	require.NoError(t, err)
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// market cron updates deal states indicating deals are no longer pending.
	for _, id := range dealIDs {
		state, found := vm.GetDealState(t, v, id)
		require.True(t, found)
		// non-zero
		assert.Greater(t, uint64(state.LastUpdatedEpoch), uint64(0))
		// deal has not been slashed
		assert.Equal(t, abi.ChainEpoch(-1), state.SlashEpoch)
	}

	//
	// Terminate Sector
	//

	v, err = v.WithEpoch(v.GetEpoch() + 1)
	require.NoError(t, err)

	terminateParams := miner.TerminateSectorsParams{
		Terminations: []miner.TerminationDeclaration{{
			Deadline:  dlInfo.Index,
			Partition: pIdx,
			Sectors:   bitfield.NewFromSet([]uint64{uint64(sectorNumber)}),
		}},
	}

	vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.TerminateSectors, &terminateParams)

	noSubinvocations := []vm.ExpectInvocation{}
	vm.ExpectInvocation{
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.TerminateSectors,
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward, SubInvocations: noSubinvocations},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower, SubInvocations: noSubinvocations},
			{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend, SubInvocations: noSubinvocations},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdatePledgeTotal, SubInvocations: noSubinvocations},
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.OnMinerSectorsTerminate, SubInvocations: noSubinvocations},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdateClaimedPower, SubInvocations: noSubinvocations},
		},
	}.Matches(t, v.LastInvocation())

	// expect power, market and miner to be in base state
	minerBalances := vm.GetMinerBalances(t, v, minerAddrs.IDAddress)
	assert.Equal(t, big.Zero(), minerBalances.InitialPledge)
	assert.Equal(t, big.Zero(), minerBalances.PreCommitDeposit)

	// expect network stats to reflect power has been removed from sector
	stats := vm.GetNetworkStats(t, v)
	assert.Equal(t, int64(0), stats.MinerAboveMinPowerCount)
	assert.Equal(t, big.Zero(), stats.TotalRawBytePower)
	assert.Equal(t, big.Zero(), stats.TotalQualityAdjPower)
	assert.Equal(t, big.Zero(), stats.TotalBytesCommitted)
	assert.Equal(t, big.Zero(), stats.TotalQABytesCommitted)
	assert.Equal(t, big.Zero(), stats.TotalPledgeCollateral)

	// market cron slashes deals because sector has been terminated
	for _, id := range dealIDs {
		state, found := vm.GetDealState(t, v, id)
		require.True(t, found)
		// non-zero
		assert.Greater(t, uint64(state.LastUpdatedEpoch), uint64(0))
		// deal has not been slashed
		assert.Equal(t, v.GetEpoch(), state.SlashEpoch)

	}

	// advance and run cron to complete processing of termination
	v, err = v.WithEpoch(v.GetEpoch() + 1000)
	require.NoError(t, err)
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// Verified client should be able to withdraw all all deal collateral.
	// Client added 3 FIL balance and had 2 deals with 1 FIL collateral apiece.
	// Should only be able to withdraw the full 2 FIL if deals have been slashed and balance was unlocked.
	withdrawal := big.Mul(big.NewInt(2), vm.FIL)
	withdrawParams := &market.WithdrawBalanceParams{
		ProviderOrClientAddress: verifiedClient,
		Amount:                  withdrawal,
	}
	vm.ApplyOk(t, v, verifiedClient, builtin.StorageMarketActorAddr, big.Zero(), builtin.MethodsMarket.WithdrawBalance, withdrawParams)

	verifiedIDAddr, found := v.NormalizeAddress(verifiedClient)
	require.True(t, found)
	vm.ExpectInvocation{
		To:     builtin.StorageMarketActorAddr,
		Method: builtin.MethodsMarket.WithdrawBalance,
		SubInvocations: []vm.ExpectInvocation{
			{To: verifiedIDAddr, Method: builtin.MethodSend, Value: vm.ExpectAttoFil(withdrawal)},
		},
	}.Matches(t, v.LastInvocation())
}
