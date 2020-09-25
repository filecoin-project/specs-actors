package nv3_test

import (
	"context"
	"fmt"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/migration/nv3"
	"github.com/filecoin-project/specs-actors/actors/runtime/proof"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
	vm "github.com/filecoin-project/specs-actors/support/vm"
)

func TestMigrationCorrectsCCThenFaultIssue(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t)
	addrs := vm.CreateAccounts(ctx, t, v, 2, big.Mul(big.NewInt(100_000), vm.FIL), 93837778)
	worker, unverifiedClient := addrs[0], addrs[1]
	numSectors := uint64(2349)

	minerBalance := big.Mul(big.NewInt(10_000), vm.FIL)
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
	// Precommit, prove and PoSt empty sector (more fully tested in TestCommitPoStFlow)
	//

	for i := uint64(0); i < numSectors; i++ {
		// precommit sector
		preCommitParams := miner.SectorPreCommitInfo{
			SealProof:     sealProof,
			SectorNumber:  sectorNumber + abi.SectorNumber(i),
			SealedCID:     sealedCid,
			SealRandEpoch: v.GetEpoch() - 1,
			DealIDs:       nil,
			Expiration:    v.GetEpoch() + 200*builtin.EpochsInDay,
		}
		vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)
	}

	// advance time to seal duration
	proveTime := v.GetEpoch() + miner.PreCommitChallengeDelay + 1
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	// miner should have no power yet
	assert.Equal(t, uint64(0), vm.MinerPower(t, v, minerAddrs.IDAddress).Raw.Uint64())

	// Prove commit sector after max seal duration
	v, err := v.WithEpoch(proveTime)
	require.NoError(t, err)

	for i := uint64(0); i < numSectors; i += 200 {
		for j := uint64(0); j < 200 && i+j < numSectors; j++ {
			proveCommitParams := miner.ProveCommitSectorParams{
				SectorNumber: sectorNumber + abi.SectorNumber(i+j),
			}
			vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)
		}

		// In the same epoch, trigger cron to validate prove commits
		vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

		v, err = v.WithEpoch(v.GetEpoch() + 1)
		require.NoError(t, err)
	}

	// all sectors should have power
	assert.Equal(t, numSectors*32<<30, vm.MinerPower(t, v, minerAddrs.IDAddress).Raw.Uint64())

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

	vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)

	//
	// publish verified and unverified deals
	//

	// add market collateral for clients and miner
	collateral := big.Mul(big.NewInt(3), vm.FIL)
	vm.ApplyOk(t, v, unverifiedClient, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &unverifiedClient)
	collateral = big.Mul(big.NewInt(64), vm.FIL)
	vm.ApplyOk(t, v, worker, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &minerAddrs.IDAddress)

	// create 3 deals, some verified and some not
	dealStart := v.GetEpoch() + 2*builtin.EpochsInDay
	deals := publishDeal(t, v, worker, unverifiedClient, minerAddrs.IDAddress, "deal1", 1<<34, false, dealStart, 210*builtin.EpochsInDay)
	dealIDs := deals.IDs

	//
	// Precommit, Prove, Verify and PoSt committed capacity sector
	//

	// precommit capacity upgrade sector with deals
	upgradeSectorNumber := abi.SectorNumber(3000)
	upgradeSealedCid := tutil.MakeCID("101", &miner.SealedCIDPrefix)
	preCommitParams := miner.SectorPreCommitInfo{
		SealProof:              sealProof,
		SectorNumber:           upgradeSectorNumber,
		SealedCID:              upgradeSealedCid,
		SealRandEpoch:          v.GetEpoch() - 1,
		DealIDs:                dealIDs,
		Expiration:             v.GetEpoch() + 220*builtin.EpochsInDay,
		ReplaceCapacity:        true,
		ReplaceSectorDeadline:  dlInfo.Index,
		ReplaceSectorPartition: pIdx,
		ReplaceSectorNumber:    sectorNumber,
	}
	vm.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)

	// advance time to min seal duration
	proveTime = v.GetEpoch() + miner.PreCommitChallengeDelay + 1
	v, _ = vm.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	// Prove commit sector after max seal duration
	v, err = v.WithEpoch(proveTime)
	require.NoError(t, err)
	proveCommitParams := miner.ProveCommitSectorParams{
		SectorNumber: upgradeSectorNumber,
	}
	vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)

	// In the same epoch, trigger cron to validate prove commit
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// original and upgrade sector should have power, so total power is for num sectors + 1
	assert.Equal(t, (numSectors+1)*32<<30, vm.MinerPower(t, v, minerAddrs.IDAddress).Raw.Uint64())

	// advance into next deadline
	v, err = v.WithEpoch(v.GetEpoch() + 1)
	require.NoError(t, err)

	// now declare original sector as a fault
	vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.DeclareFaults, &miner.DeclareFaultsParams{
		Faults: []miner.FaultDeclaration{{
			Deadline:  dlInfo.Index,
			Partition: pIdx,
			Sectors:   bitfield.NewFromSet([]uint64{uint64(sectorNumber)}),
		}},
	})

	// original sector should lose power bringing us back down to num sectors
	assert.Equal(t, numSectors*32<<30, vm.MinerPower(t, v, minerAddrs.IDAddress).Raw.Uint64())

	// advance to original sector's period and submit post
	oDlInfo, oPIdx, v := vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber)

	vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &miner.SubmitWindowedPoStParams{
		Deadline: oDlInfo.Index,
		Partitions: []miner.PoStPartition{{
			Index:   oPIdx,
			Skipped: bitfield.New(),
		}},
		Proofs: []proof.PoStProof{{
			PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		}},
		ChainCommitEpoch: oDlInfo.Challenge,
		ChainCommitRand:  []byte("not really random"),
	})

	v, err = v.WithEpoch(oDlInfo.Last())
	require.NoError(t, err)
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// original sector's expires dropping its power again. This is a symptom of bad behavior.
	assert.Equal(t, (numSectors-1)*32<<30, vm.MinerPower(t, v, minerAddrs.IDAddress).Raw.Uint64())

	// advance to upgrade period and submit post
	dlInfo, pIdx, v = vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, upgradeSectorNumber)

	vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &miner.SubmitWindowedPoStParams{
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
	})

	fmt.Println(vm.MinerPower(t, v, minerAddrs.IDAddress))

	v, err = v.WithEpoch(dlInfo.Last())
	require.NoError(t, err)
	vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	fmt.Println(vm.MinerPower(t, v, minerAddrs.IDAddress))

	// advance 14 proving periods submitting PoSts along the way
	for i := 0; i < 14; i++ {
		// advance to original sector's period and submit post
		oDlInfo, oPIdx, v = vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber+1)

		vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &miner.SubmitWindowedPoStParams{
			Deadline: oDlInfo.Index,
			Partitions: []miner.PoStPartition{{
				Index:   oPIdx,
				Skipped: bitfield.New(),
			}},
			Proofs: []proof.PoStProof{{
				PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			}},
			ChainCommitEpoch: oDlInfo.Challenge,
			ChainCommitRand:  []byte("not really random"),
		})

		v, err = v.WithEpoch(oDlInfo.Last())
		require.NoError(t, err)
		vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

		// once miner is borked, our logic to advance through deadlines stops working, so exit now.
		power := vm.MinerPower(t, v, minerAddrs.IDAddress)
		if power.IsZero() {
			break
		}

		// advance to upgrade period and submit post
		dlInfo, pIdx, v = vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, upgradeSectorNumber)
		require.True(t, dlInfo.Last() > oDlInfo.Last())

		vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &miner.SubmitWindowedPoStParams{
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
		})

		v, err = v.WithEpoch(dlInfo.Last())
		require.NoError(t, err)
		vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

		fmt.Println(vm.MinerPower(t, v, minerAddrs.IDAddress))
	}

	// advance a few proving periods and deadlines before running the migration
	v, err = v.WithEpoch(v.GetEpoch() + 5*miner.WPoStProvingPeriod + 4*miner.WPoStChallengeWindow)
	require.NoError(t, err)

	//
	// assert values that are WRONG due to cc upgrade then fault problem and show we can fix them
	//

	// miner has lost all power
	assert.Equal(t, miner.NewPowerPairZero(), vm.MinerPower(t, v, minerAddrs.IDAddress))

	// miner's proving period and deadline are stale
	var st miner.State
	err = v.GetState(minerAddrs.IDAddress, &st)
	require.NoError(t, err)
	assert.True(t, st.ProvingPeriodStart+miner.WPoStProvingPeriod < v.GetEpoch())

	//
	// migrate miner
	//

	nextRoot, err := nv3.MigrateStateTree(ctx, v.Store(), v.StateRoot(), v.GetEpoch())
	require.NoError(t, err)

	v, err = v.WithRoot(nextRoot)
	require.NoError(t, err)

	//
	// Test correction
	//

	// Raw power is num sectors * sector size (the upgrade exactly replaces the original)
	assert.Equal(t, numSectors*32<<30, vm.MinerPower(t, v, minerAddrs.IDAddress).Raw.Uint64())

	// run a few more proving periods to show miner is back in shape
	for i := 0; i < 5; i++ {
		// advance to original sector's period and submit post
		oDlInfo, oPIdx, v = vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, sectorNumber+1)

		vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &miner.SubmitWindowedPoStParams{
			Deadline: oDlInfo.Index,
			Partitions: []miner.PoStPartition{{
				Index:   oPIdx,
				Skipped: bitfield.New(),
			}},
			Proofs: []proof.PoStProof{{
				PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
			}},
			ChainCommitEpoch: oDlInfo.Challenge,
			ChainCommitRand:  []byte("not really random"),
		})

		v, err = v.WithEpoch(oDlInfo.Last())
		require.NoError(t, err)
		vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

		// advance to upgrade period and submit post
		dlInfo, pIdx, v = vm.AdvanceTillProvingDeadline(t, v, minerAddrs.IDAddress, upgradeSectorNumber)
		require.True(t, dlInfo.Last() > oDlInfo.Last())

		vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &miner.SubmitWindowedPoStParams{
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
		})

		v, err = v.WithEpoch(dlInfo.Last())
		require.NoError(t, err)
		vm.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

		fmt.Println(vm.MinerPower(t, v, minerAddrs.IDAddress))
	}

	// miner still has power
	assert.Equal(t, numSectors*32<<30, vm.MinerPower(t, v, minerAddrs.IDAddress).Raw.Uint64())

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
