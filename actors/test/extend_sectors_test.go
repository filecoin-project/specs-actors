package test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/verifreg"
	"github.com/filecoin-project/specs-actors/v7/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/v7/support/ipld"
	tutil "github.com/filecoin-project/specs-actors/v7/support/testing"
	"github.com/filecoin-project/specs-actors/v7/support/vm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtendSectorWithDeals(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	addrs := vm.CreateAccounts(ctx, t, v, 4, big.Mul(big.NewInt(10_000), vm.FIL), 93837778)
	worker, verifier, unverifiedClient, verifiedClient := addrs[0], addrs[1], addrs[2], addrs[3]

	minerBalance := big.Mul(big.NewInt(1_000), vm.FIL)
	sectorNumber := abi.SectorNumber(100)
	sealedCid := tutil.MakeCID("100", &miner.SealedCIDPrefix)
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1

	// create miner
	params := power.CreateMinerParams{
		Owner:               worker,
		Worker:              worker,
		WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		Peer:                abi.PeerID("not really a peer id"),
	}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)

	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	//
	// publish verified deals
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
	collateral = big.Mul(big.NewInt(64), vm.FIL)
	vm.ApplyOk(t, v, worker, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &minerAddrs.IDAddress)

	// create 1 verified deal for total sector capacity for 6 months
	dealIDs := []abi.DealID{}
	dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	deals := publishDeal(t, v, worker, verifiedClient, minerAddrs.IDAddress, "deal1", 32<<30, true, dealStart, 180*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)

	//
	// Precommit, prove and PoSt empty sector (more fully tested in TestCommitPoStFlow)
	//

	// precommit sector
	preCommitParams := miner.PreCommitSectorParams{
		SealProof:     sealProof,
		SectorNumber:  sectorNumber,
		SealedCID:     sealedCid,
		SealRandEpoch: v.GetEpoch() - 1,
		DealIDs:       dealIDs,
		Expiration:    dealStart + 180*builtin.EpochsInDay,
	}
	vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)

	// advance time to max seal duration
	proveTime := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
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

	// inspect sector info

	var mState miner.State
	require.NoError(t, v.GetState(minerAddrs.IDAddress, &mState))
	info, found, err := mState.GetSector(v.Store(), sectorNumber)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, abi.ChainEpoch(180*builtin.EpochsInDay), info.Expiration-info.Activation)
	assert.Equal(t, big.Zero(), info.DealWeight)                                           // 0 space time
	assert.Equal(t, big.NewInt(180*builtin.EpochsInDay*(32<<30)), info.VerifiedDealWeight) // (180 days *2880 epochs per day) * 32 GiB
	initialVerifiedDealWeight := info.VerifiedDealWeight
	initialDealWeight := info.DealWeight

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
		ChainCommitRand:  []byte(vm.RandString),
	}

	expectPowerDelta := power.UpdateClaimedPowerParams{
		RawByteDelta:         abi.NewStoragePower(32 << 30),        // 32 GiB
		QualityAdjustedDelta: abi.NewStoragePower(10 * (32 << 30)), // 32 GiB x 10 since sector entirely verified
	}
	vm.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)
	vm.ExpectInvocation{
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.SubmitWindowedPoSt,
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdateClaimedPower, SubInvocations: []vm.ExpectInvocation{},
				Params: vm.ExpectObject(&expectPowerDelta)},
		},
	}.Matches(t, v.LastInvocation())
	// move forward one deadline so advanceWhileProving doesn't fail double submitting posts
	v, _ = vm.AdvanceByDeadlineTillIndex(t, v, minerAddrs.IDAddress, dlInfo.Index+2%miner.WPoStPeriodDeadlines)

	// advance halfway through life and extend another 6 months
	// verified deal weight /= 2
	// power multiplier = (1/4)*10 + (3/4)*1 = 3.25
	// power delta = (10-3.25)*32GiB = 6.75*32GiB
	v = vm.AdvanceByDeadlineTillEpochWhileProving(t, v, minerAddrs.IDAddress, worker, sectorNumber, dealStart+90*builtin.EpochsInDay)
	dlIdx, pIdx := vm.SectorDeadline(t, v, minerAddrs.IDAddress, sectorNumber)
	v, err = v.WithEpoch(dealStart + 90*builtin.EpochsInDay) // for getting epoch exactly halfway through lifetime
	require.NoError(t, err)

	extensionParams := &miner.ExtendSectorExpirationParams{
		Extensions: []miner.ExpirationExtension{{
			Deadline:      dlIdx,
			Partition:     pIdx,
			Sectors:       bitfield.NewFromSet([]uint64{uint64(sectorNumber)}),
			NewExpiration: abi.ChainEpoch(dealStart + 2*180*builtin.EpochsInDay),
		}},
	}
	vm.ApplyOk(t, v, worker, minerAddrs.IDAddress, big.Zero(), builtin.MethodsMiner.ExtendSectorExpiration, extensionParams)
	expectPowerDelta = power.UpdateClaimedPowerParams{
		RawByteDelta:         big.Zero(),
		QualityAdjustedDelta: abi.NewStoragePower(-1 * 675 * (32 << 30) / 100),
	}
	vm.ExpectInvocation{
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.ExtendSectorExpiration,
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdateClaimedPower, SubInvocations: []vm.ExpectInvocation{},
				Params: vm.ExpectObject(&expectPowerDelta)},
		},
	}.Matches(t, v.LastInvocation())

	// advance to 6 months (original expiration) and extend another 6 months
	// verified deal weight /= 2
	// power multiplier = (1/3)*3.25 + (2/3)*1 = 1.75
	// power delta = (3.25 - 1.75)*32GiB = 1.5*32GiB

	// move forward one deadline so advanceWhileProving doesn't fail double submitting posts
	v, _ = vm.AdvanceByDeadlineTillIndex(t, v, minerAddrs.IDAddress, dlInfo.Index+2%miner.WPoStPeriodDeadlines)
	v = vm.AdvanceByDeadlineTillEpochWhileProving(t, v, minerAddrs.IDAddress, worker, sectorNumber, dealStart+180*builtin.EpochsInDay)
	dlIdx, pIdx = vm.SectorDeadline(t, v, minerAddrs.IDAddress, sectorNumber)
	v, err = v.WithEpoch(dealStart + 180*builtin.EpochsInDay) // for getting epoch exactly halfway through lifetime
	require.NoError(t, err)

	extensionParamsTwo := &miner.ExtendSectorExpirationParams{
		Extensions: []miner.ExpirationExtension{{
			Deadline:      dlIdx,
			Partition:     pIdx,
			Sectors:       bitfield.NewFromSet([]uint64{uint64(sectorNumber)}),
			NewExpiration: abi.ChainEpoch(dealStart + 3*180*builtin.EpochsInDay),
		}},
	}
	vm.ApplyOk(t, v, worker, minerAddrs.IDAddress, big.Zero(), builtin.MethodsMiner.ExtendSectorExpiration, extensionParamsTwo)
	expectPowerDeltaTwo := power.UpdateClaimedPowerParams{
		RawByteDelta:         big.Zero(),
		QualityAdjustedDelta: abi.NewStoragePower(-1 * 15 * (32 << 30) / 10),
	}
	vm.ExpectInvocation{
		To:     minerAddrs.IDAddress,
		Method: builtin.MethodsMiner.ExtendSectorExpiration,
		SubInvocations: []vm.ExpectInvocation{
			{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.UpdateClaimedPower, SubInvocations: []vm.ExpectInvocation{},
				Params: vm.ExpectObject(&expectPowerDeltaTwo)},
		},
	}.Matches(t, v.LastInvocation())

	var mStateFinal miner.State
	require.NoError(t, v.GetState(minerAddrs.IDAddress, &mStateFinal))
	infoFinal, found, err := mStateFinal.GetSector(v.Store(), sectorNumber)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, abi.ChainEpoch(180*3*builtin.EpochsInDay), infoFinal.Expiration-infoFinal.Activation)
	assert.Equal(t, initialDealWeight, infoFinal.DealWeight)                                         // 0 space time, unchanged
	assert.Equal(t, big.Div(initialVerifiedDealWeight, big.NewInt(4)), infoFinal.VerifiedDealWeight) // two halvings => 1/4 initial verified deal weight
}
