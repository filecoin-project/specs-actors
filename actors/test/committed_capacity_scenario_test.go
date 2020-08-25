package test_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/builtin/verifreg"
	"github.com/filecoin-project/specs-actors/actors/crypto"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
	vm "github.com/filecoin-project/specs-actors/support/vm"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplaceCommittedCapacitySectorWithDealLadenSector(t *testing.T) {
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
	ret, code := v.ApplyMessage(addrs[0], builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)
	require.Equal(t, exitcode.Ok, code)

	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	//
	// Precommit, prove and PoSt empty sector (more fully tested in TestCommitPoStFlow)
	//

	// precommit sector
	preCommitParams := miner.SectorPreCommitInfo{
		SealProof:     sealProof,
		SectorNumber:  sectorNumber,
		SealedCID:     sealedCid,
		SealRandEpoch: v.GetEpoch() - 1,
		DealIDs:       nil,
		Expiration:    v.GetEpoch() + 200*builtin.EpochsInDay,
	}
	_, code = v.ApplyMessage(addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)
	require.Equal(t, exitcode.Ok, code)

	// advance time to max seal duration
	proveTime := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	// Prove commit sector after max seal duration
	v, err := v.WithEpoch(proveTime)
	require.NoError(t, err)
	proveCommitParams := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}
	_, code = v.ApplyMessage(worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)
	require.Equal(t, exitcode.Ok, code)

	// In the same epoch, trigger cron to validate prove commit
	_, code = v.ApplyMessage(builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
	require.Equal(t, exitcode.Ok, code)

	// advance to proving period and submit post
	dlInfo, pIdx, v := vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)

	submitParams := miner.SubmitWindowedPoStParams{
		Deadline: dlInfo.Index,
		Partitions: []miner.PoStPartition{{
			Index:   pIdx,
			Skipped: bitfield.New(),
		}},
		Proofs: []abi.PoStProof{{
			PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		}},
		ChainCommitRand: []byte("not really random"),
	}

	_, code = v.ApplyMessage(addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)
	require.Equal(t, exitcode.Ok, code)

	// check power table
	sectorPower := vm.PowerForMinerSector(t, v, minerAddrs.IDAddress, sectorNumber)
	minerPower := vm.MinerPower(t, v, minerAddrs.IDAddress)
	networkStats := vm.GetNetworkStats(t, v)
	assert.Equal(t, sectorPower.Raw, minerPower.Raw)
	assert.Equal(t, sectorPower.QA, minerPower.Raw)
	assert.Equal(t, sectorPower.Raw, networkStats.TotalBytesCommitted)
	assert.Equal(t, sectorPower.QA, networkStats.TotalQABytesCommitted)
	// miner does not meet consensus minimum so actual power is not added
	assert.Equal(t, big.Zero(), networkStats.TotalRawBytePower)
	assert.Equal(t, big.Zero(), networkStats.TotalQualityAdjPower)

	//
	// publish verified and unverified deals
	//

	// register verifier then verified client
	addVerifierParams := verifreg.AddVerifierParams{
		Address:   verifier,
		Allowance: abi.NewStoragePower(32 << 40),
	}
	_, code = v.ApplyMessage(vm.VerifregRoot, builtin.VerifiedRegistryActorAddr, big.Zero(), builtin.MethodsVerifiedRegistry.AddVerifier, &addVerifierParams)
	require.Equal(t, exitcode.Ok, code)

	addClientParams := verifreg.AddVerifiedClientParams{
		Address:   verifiedClient,
		Allowance: abi.NewStoragePower(32 << 40),
	}
	_, code = v.ApplyMessage(verifier, builtin.VerifiedRegistryActorAddr, big.Zero(), builtin.MethodsVerifiedRegistry.AddVerifiedClient, &addClientParams)
	require.Equal(t, exitcode.Ok, code)

	// add market collateral for clients and miner
	collateral := big.Mul(big.NewInt(3), vm.FIL)
	_, code = v.ApplyMessage(unverifiedClient, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &unverifiedClient)
	require.Equal(t, exitcode.Ok, code)
	_, code = v.ApplyMessage(verifiedClient, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &verifiedClient)
	require.Equal(t, exitcode.Ok, code)
	collateral = big.Mul(big.NewInt(64), vm.FIL)
	_, code = v.ApplyMessage(worker, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &minerAddrs.IDAddress)
	require.Equal(t, exitcode.Ok, code)

	// create 3 deals, some verified and some not
	dealIDs := []abi.DealID{}
	dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	deals := publishDeal(t, v, worker, verifiedClient, minerAddrs.IDAddress, "deal1", 1<<30, true, dealStart, 181*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)
	deals = publishDeal(t, v, worker, verifiedClient, minerAddrs.IDAddress, "deal2", 1<<32, true, dealStart, 200*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)
	deals = publishDeal(t, v, worker, unverifiedClient, minerAddrs.IDAddress, "deal3", 1<<34, false, dealStart, 210*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)

	//
	// Precommit, Prove, Verify and PoSt committed capacity sector
	//

	// precommit capacity upgrade sector with deals
	ccSectorNumber := abi.SectorNumber(101)
	ccSealedCid := tutil.MakeCID("101", &miner.SealedCIDPrefix)
	preCommitParams = miner.SectorPreCommitInfo{
		SealProof:              sealProof,
		SectorNumber:           ccSectorNumber,
		SealedCID:              ccSealedCid,
		SealRandEpoch:          v.GetEpoch() - 1,
		DealIDs:                dealIDs,
		Expiration:             v.GetEpoch() + 220*builtin.EpochsInDay,
		ReplaceCapacity:        true,
		ReplaceSectorDeadline:  dlInfo.Index,
		ReplaceSectorPartition: pIdx,
		ReplaceSectorNumber:    sectorNumber,
	}
	_, code = v.ApplyMessage(addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)
	require.Equal(t, exitcode.Ok, code)

	// assert successful precommit invocation
	none := []vm.ExpectInvocation{}
	vm.ExpectInvocation{
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.PreCommitSector,
		Params: vm.ExpectObject(&preCommitParams),
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward, SubInvocations: none},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower, SubInvocations: none},
			// addtion of deal ids prompts call to verify deals for activation
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.VerifyDealsForActivation, SubInvocations: none},
		},
	}.Matches(t, v.LastInvocation())

	t.Run("verified registry bytes are restored when verified deals are not proven", func(t *testing.T) {
		tv, err := v.WithEpoch(dealStart + market.DealUpdatesInterval)
		require.NoError(t, err)

		// run cron and check for deal expiry in market actor
		_, code = tv.ApplyMessage(builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
		require.Equal(t, exitcode.Ok, code)

		vm.ExpectInvocation{
			To:     builtin.CronActorAddr,
			Method: builtin.MethodsCron.EpochTick,
			SubInvocations: []vm.ExpectInvocation{
				{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.OnEpochTickEnd, SubInvocations: []vm.ExpectInvocation{
					{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.OnDeferredCronEvent, SubInvocations: []vm.ExpectInvocation{
						{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
						// pre-commit deposit is burnt
						{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
					}},
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
				}},
				{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick, SubInvocations: []vm.ExpectInvocation{
					// notify verified registry that used bytes are released
					{To: builtin.VerifiedRegistryActorAddr, Method: builtin.MethodsVerifiedRegistry.RestoreBytes},
					{To: builtin.VerifiedRegistryActorAddr, Method: builtin.MethodsVerifiedRegistry.RestoreBytes},
					// slash funds
					{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
				}},
			},
		}.Matches(t, tv.LastInvocation())
	})

	// advance time to min seal duration
	proveTime = v.GetEpoch() + miner.PreCommitChallengeDelay + 1
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	// Prove commit sector after max seal duration
	v, err = v.WithEpoch(proveTime)
	require.NoError(t, err)
	proveCommitParams = miner.ProveCommitSectorParams{
		SectorNumber: ccSectorNumber,
	}
	_, code = v.ApplyMessage(worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)
	require.Equal(t, exitcode.Ok, code)

	vm.ExpectInvocation{
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.ProveCommitSector,
		Params: vm.ExpectObject(&proveCommitParams),
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.ComputeDataCommitment, SubInvocations: none},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.SubmitPoRepForBulkVerify, SubInvocations: none},
		},
	}.Matches(t, v.LastInvocation())

	// In the same epoch, trigger cron to validate prove commit
	_, code = v.ApplyMessage(builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
	require.Equal(t, exitcode.Ok, code)

	vm.ExpectInvocation{
		To:     builtin.CronActorAddr,
		Method: builtin.MethodsCron.EpochTick,
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.OnEpochTickEnd, SubInvocations: []vm.ExpectInvocation{
				{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.ConfirmSectorProofsValid, SubInvocations: []vm.ExpectInvocation{
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
					{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
					// deals are now activated
					{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.ActivateDeals},
					{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdatePledgeTotal},
				}},
				{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
			}},
			{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick},
		},
	}.Matches(t, v.LastInvocation())

	// miner still has power for old sector
	minerPower = vm.MinerPower(t, v, minerAddrs.IDAddress)
	networkStats = vm.GetNetworkStats(t, v)
	assert.Equal(t, sectorPower.Raw, minerPower.Raw)
	assert.Equal(t, sectorPower.QA, minerPower.Raw)
	assert.Equal(t, sectorPower.Raw, networkStats.TotalBytesCommitted)
	assert.Equal(t, sectorPower.QA, networkStats.TotalQABytesCommitted)

	t.Run("miner misses first PoSt of replacement sector", func(t *testing.T) {
		// advance to proving period end of new sector
		dlInfo, _, tv := vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)
		tv, err = tv.WithEpoch(dlInfo.Last())
		require.NoError(t, err)

		// run cron to penalize missing PoSt
		_, code = tv.ApplyMessage(builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
		require.Equal(t, exitcode.Ok, code)

		vm.ExpectInvocation{
			To:     builtin.CronActorAddr,
			Method: builtin.MethodsCron.EpochTick,
			SubInvocations: []vm.ExpectInvocation{
				{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.OnEpochTickEnd, SubInvocations: []vm.ExpectInvocation{
					{To: minerAddrs.IDAddress, Method: builtin.MethodsMiner.OnDeferredCronEvent, SubInvocations: []vm.ExpectInvocation{
						{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
						// power is removed for old sector and pledge is burnt
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdateClaimedPower},
						{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdatePledgeTotal},
						{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.EnrollCronEvent},
					}},
					{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.UpdateNetworkKPI},
				}},
				{To: builtin.StorageMarketActorAddr, Method: builtin.MethodsMarket.CronTick, SubInvocations: []vm.ExpectInvocation{}},
			},
		}.Matches(t, tv.LastInvocation())

		// miner's power is removed for old sector (and not added for new)
		minerPower = vm.MinerPower(t, tv, minerAddrs.IDAddress)
		networkStats = vm.GetNetworkStats(t, tv)
		assert.Equal(t, big.Zero(), minerPower.Raw)
		assert.Equal(t, big.Zero(), minerPower.Raw)
		assert.Equal(t, big.Zero(), networkStats.TotalBytesCommitted)
		assert.Equal(t, big.Zero(), networkStats.TotalQABytesCommitted)
	})

	// advance to proving period and submit post
	dlInfo, pIdx, v = vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, ccSectorNumber)

	t.Run("miner skips replacing sector in first PoSt", func(t *testing.T) {
		tv, err := v.WithEpoch(v.GetEpoch()) // create vm copy
		require.NoError(t, err)

		submitParams = miner.SubmitWindowedPoStParams{
			Deadline: dlInfo.Index,
			Partitions: []miner.PoStPartition{{
				Index: pIdx,
				// skip cc upgrade
				Skipped: bitfield.NewFromSet([]uint64{uint64(ccSectorNumber)}),
			}},
			Proofs: []abi.PoStProof{{
				PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			}},
			ChainCommitRand: []byte("not really random"),
		}
		_, code = tv.ApplyMessage(worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)
		require.Equal(t, exitcode.Ok, code)

		vm.ExpectInvocation{
			To:     minerAddrs.IDAddress,
			Method: builtin.MethodsMiner.SubmitWindowedPoSt,
			Params: vm.ExpectObject(&submitParams),
			SubInvocations: []vm.ExpectInvocation{
				{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
				{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
				// Skipped sector is penalized as undeclared fault
				{To: builtin.BurntFundsActorAddr, Method: builtin.MethodSend},
			},
		}.Matches(t, tv.LastInvocation())

		// old sector power remains (until its proving deadline)
		minerPower = vm.MinerPower(t, tv, minerAddrs.IDAddress)
		networkStats = vm.GetNetworkStats(t, tv)
		assert.Equal(t, sectorPower.Raw, minerPower.Raw)
		assert.Equal(t, sectorPower.QA, minerPower.Raw)
		assert.Equal(t, sectorPower.Raw, networkStats.TotalBytesCommitted)
		assert.Equal(t, sectorPower.QA, networkStats.TotalQABytesCommitted)
	})

	submitParams = miner.SubmitWindowedPoStParams{
		Deadline: dlInfo.Index,
		Partitions: []miner.PoStPartition{{
			Index:   pIdx,
			Skipped: bitfield.New(),
		}},
		Proofs: []abi.PoStProof{{
			PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		}},
		ChainCommitRand: []byte("not really random"),
	}
	_, code = v.ApplyMessage(worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)
	require.Equal(t, exitcode.Ok, code)

	vm.ExpectInvocation{
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.SubmitWindowedPoSt,
		Params: vm.ExpectObject(&submitParams),
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward},
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower},
			// This call to the power actor indicates power has been added for the replaced sector
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdateClaimedPower},
		},
	}.Matches(t, v.LastInvocation())

	// power is upgraded for new sector
	// Until the old sector is terminated at its proving period, miner gets combined power for new and old sectors
	ccSectorPower := vm.PowerForMinerSector(t, v, minerAddrs.IDAddress, ccSectorNumber)
	combinedPower := ccSectorPower.Add(sectorPower)
	minerPower = vm.MinerPower(t, v, minerAddrs.IDAddress)
	networkStats = vm.GetNetworkStats(t, v)
	assert.Equal(t, combinedPower.Raw, minerPower.Raw)
	assert.Equal(t, combinedPower.QA, minerPower.QA)
	assert.Equal(t, combinedPower.Raw, networkStats.TotalBytesCommitted)
	assert.Equal(t, combinedPower.QA, networkStats.TotalQABytesCommitted)

	// advance to proving period of old sector
	dlInfo, _, v = vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)

	// proving period cron removes sector reducing the miner's power to that of the new sector
	v, err = v.WithEpoch(dlInfo.Last())
	require.NoError(t, err)
	_, code = v.ApplyMessage(builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
	require.Equal(t, exitcode.Ok, code)

	// power is removed
	// Until the old sector is terminated at its proving period, miner gets combined power for new and old sectors
	minerPower = vm.MinerPower(t, v, minerAddrs.IDAddress)
	networkStats = vm.GetNetworkStats(t, v)
	assert.Equal(t, ccSectorPower.Raw, minerPower.Raw)
	assert.Equal(t, ccSectorPower.QA, minerPower.QA)
	assert.Equal(t, ccSectorPower.Raw, networkStats.TotalBytesCommitted)
	assert.Equal(t, ccSectorPower.QA, networkStats.TotalQABytesCommitted)
}

func publishDeal(t *testing.T, v *vm.VM, provider, dealClient, minerID addr.Address, dealLabel string,
	pieceSize abi.PaddedPieceSize, verifiedDeal bool, dealStart abi.ChainEpoch, dealLifetime abi.ChainEpoch,
) *market.PublishStorageDealsReturn {
	deal := market.DealProposal{
		PieceCID:             tutil.MakeCID(dealLabel, &market.PieceCIDPrefix),
		PieceSize:            pieceSize,
		VerifiedDeal:         verifiedDeal,
		Client:               dealClient,
		Provider:             minerID,
		Label:                dealLabel,
		StartEpoch:           dealStart,
		EndEpoch:             dealStart + dealLifetime,
		StoragePricePerEpoch: abi.NewTokenAmount(1 << 20),
		ProviderCollateral:   big.Mul(big.NewInt(2), vm.FIL),
		ClientCollateral:     big.Mul(big.NewInt(1), vm.FIL),
	}

	publishDealParams := market.PublishStorageDealsParams{
		Deals: []market.ClientDealProposal{{
			Proposal:        deal,
			ClientSignature: crypto.Signature{},
		}},
	}
	ret, code := v.ApplyMessage(provider, builtin.StorageMarketActorAddr, big.Zero(), builtin.MethodsMarket.PublishStorageDeals, &publishDealParams)
	require.Equal(t, exitcode.Ok, code)

	expectedPublishSubinvocations := []vm.ExpectInvocation{
		{To: minerID, Method: builtin.MethodsMiner.ControlAddresses, SubInvocations: []vm.ExpectInvocation{}},
		{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward, SubInvocations: []vm.ExpectInvocation{}},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower, SubInvocations: []vm.ExpectInvocation{}},
	}

	if verifiedDeal {
		expectedPublishSubinvocations = append(expectedPublishSubinvocations, vm.ExpectInvocation{
			To:             builtin.VerifiedRegistryActorAddr,
			Method:         builtin.MethodsVerifiedRegistry.UseBytes,
			SubInvocations: []vm.ExpectInvocation{},
		})
	}

	vm.ExpectInvocation{
		To:             builtin.StorageMarketActorAddr,
		Method:         builtin.MethodsMarket.PublishStorageDeals,
		SubInvocations: expectedPublishSubinvocations,
	}.Matches(t, v.LastInvocation())

	return ret.(*market.PublishStorageDealsReturn)
}
