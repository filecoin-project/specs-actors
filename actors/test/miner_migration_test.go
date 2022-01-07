package test

import (
	"context"
	"fmt"
	ipld2 "github.com/filecoin-project/specs-actors/v2/support/ipld"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/stretchr/testify/require"

	market6 "github.com/filecoin-project/specs-actors/v6/actors/builtin/market"
	vm6 "github.com/filecoin-project/specs-actors/v6/support/vm"

	builtin6 "github.com/filecoin-project/specs-actors/v6/actors/builtin"
	miner6 "github.com/filecoin-project/specs-actors/v6/actors/builtin/miner"
	power6 "github.com/filecoin-project/specs-actors/v6/actors/builtin/power"

	tutil6 "github.com/filecoin-project/specs-actors/v6/support/testing"
	"github.com/filecoin-project/specs-actors/v7/support/vm6Util"

	addr "github.com/filecoin-project/go-address"
)


func publishDealv6(t *testing.T, v *vm6.VM, provider, dealClient, minerID addr.Address, dealLabel string,
	pieceSize abi.PaddedPieceSize, verifiedDeal bool, dealStart abi.ChainEpoch, dealLifetime abi.ChainEpoch,
) *market6.PublishStorageDealsReturn {
	deal := market6.DealProposal{
		PieceCID:             tutil6.MakeCID(dealLabel, &market6.PieceCIDPrefix),
		PieceSize:            pieceSize,
		VerifiedDeal:         verifiedDeal,
		Client:               dealClient,
		Provider:             minerID,
		Label:                dealLabel,
		StartEpoch:           dealStart,
		EndEpoch:             dealStart + dealLifetime,
		StoragePricePerEpoch: abi.NewTokenAmount(1 << 20),
		ProviderCollateral:   big.Mul(big.NewInt(2), vm6.FIL),
		ClientCollateral:     big.Mul(big.NewInt(1), vm6.FIL),
	}

	publishDealParams := market6.PublishStorageDealsParams{
		Deals: []market6.ClientDealProposal{{
			Proposal: deal,
			ClientSignature: crypto.Signature{
				Type: crypto.SigTypeBLS,
			},
		}},
	}
	result := vm6.RequireApplyMessage(t, v, provider, builtin6.StorageMarketActorAddr, big.Zero(), builtin6.MethodsMarket.PublishStorageDeals, &publishDealParams, t.Name())
	require.Equal(t, exitcode.Ok, result.Code)

	expectedPublishSubinvocations := []vm6.ExpectInvocation{
		{To: minerID, Method: builtin6.MethodsMiner.ControlAddresses, SubInvocations: []vm6.ExpectInvocation{}},
		{To: builtin6.RewardActorAddr, Method: builtin6.MethodsReward.ThisEpochReward, SubInvocations: []vm6.ExpectInvocation{}},
		{To: builtin6.StoragePowerActorAddr, Method: builtin6.MethodsPower.CurrentTotalPower, SubInvocations: []vm6.ExpectInvocation{}},
	}

	if verifiedDeal {
		expectedPublishSubinvocations = append(expectedPublishSubinvocations, vm6.ExpectInvocation{
			To:             builtin6.VerifiedRegistryActorAddr,
			Method:         builtin6.MethodsVerifiedRegistry.UseBytes,
			SubInvocations: []vm6.ExpectInvocation{},
		})
	}

	vm6.ExpectInvocation{
		To:             builtin6.StorageMarketActorAddr,
		Method:         builtin6.MethodsMarket.PublishStorageDeals,
		SubInvocations: expectedPublishSubinvocations,
	}.Matches(t, v.LastInvocation())

	return result.Ret.(*market6.PublishStorageDealsReturn)
}

func createMinersAndSectorsV6(t *testing.T, ctx context.Context, v *vm6.VM, firstSectorNo int, numMiners int, numSectors int) (*vm6.VM, [][]abi.SectorNumber) {
	workerAddrs := vm6.CreateAccounts(ctx, t, v, numMiners, big.Mul(big.NewInt(200_000), big.NewInt(1e18)), 93837778)

	// create miner
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)

	var minerAddresses []addr.Address
	for _, address := range workerAddrs {
		params := power6.CreateMinerParams{
			Owner:               address,
			Worker:              address,
			WindowPoStProofType: wPoStProof,
			Peer:                abi.PeerID("not really a peer id"),
		}
		ret := vm6.ApplyOk(t, v, address, builtin6.StoragePowerActorAddr, big.Mul(big.NewInt(100_000), vm6.FIL), builtin6.MethodsPower.CreateMiner, &params)
		minerAddress, ok := ret.(*power6.CreateMinerReturn)
		require.True(t, ok)
		minerAddresses = append(minerAddresses, minerAddress.IDAddress)
	}
	assert.Equal(t, len(minerAddresses), numMiners)

	var sectorNumbers [][]abi.SectorNumber
	for i, minerAddress := range minerAddresses {
		worker := workerAddrs[i]
		fmt.Printf("MINER %d\n", i)
		fmt.Printf("BALANCE %d\n", vm6.GetMinerBalances(t, v, minerAddress).AvailableBalance)
		precommits := vm6Util.PreCommitSectors(t, v, numSectors, miner6.PreCommitSectorBatchMaxSize, workerAddrs[i], minerAddress, sealProof, abi.SectorNumber(firstSectorNo + i * numSectors), true, v.GetEpoch()+miner6.MaxSectorExpirationExtension)
		fmt.Printf("BALANCE %d\n", vm6.GetMinerBalances(t, v, minerAddress).AvailableBalance)

		assert.Equal(t, len(precommits), numSectors)
		balances := vm6.GetMinerBalances(t, v, minerAddress)
		assert.True(t, balances.PreCommitDeposit.GreaterThan(big.Zero()))

		// advance time to max seal duration
		proveTime := v.GetEpoch() + miner6.MaxProveCommitDuration[sealProof]
		v, _ = vm6.AdvanceByDeadlineTillEpoch(t, v, minerAddress, proveTime)

		v, err = v.WithEpoch(proveTime)
		require.NoError(t, err)

		var sectorNums []abi.SectorNumber

		vm6Util.ProveCommitSectors(t, v, worker, minerAddress, precommits, 256)
		// proveCommit the sector
		for i, _ := range precommits {
			sectorNums = append(sectorNums, precommits[i].Info.SectorNumber)
			sectorNumber := sectorNums[i]

			// In the same epoch, trigger cron to validate prove commit
			vm6.ApplyOk(t, v, builtin6.SystemActorAddr, builtin6.CronActorAddr, big.Zero(), builtin6.MethodsCron.EpochTick, nil)

			// advance to proving period and submit post
			dlInfo, pIdx, v := vm6.AdvanceTillProvingDeadline(t, v, minerAddress, sectorNumber)

			// sector shouldn't be active until PoSt
			require.False(t, vm6Util.CheckSectorActive(t, v, minerAddress, dlInfo.Index, pIdx, sectorNumber))
			vm6Util.SubmitPoSt(t, v, minerAddress, worker, dlInfo, pIdx)

			// move into the next deadline so that the created sector is mutable
			v, _ = vm6.AdvanceByDeadlineTillEpoch(t, v, minerAddress, v.GetEpoch()+miner6.WPoStChallengeWindow)
			v = vm6Util.AdvanceOneEpochWithCron(t, v)

			// hooray, sector is now active
			require.True(t, vm6Util.CheckSectorActive(t, v, minerAddress, dlInfo.Index, pIdx, sectorNumber))
		}
		assert.Equal(t, len(sectorNums), numSectors)
		sectorNumbers = append(sectorNumbers, sectorNums)
	}
	assert.Equal(t, len(sectorNumbers), numMiners)

	return v, sectorNumbers
}

func TestCreateMiners(t *testing.T) {
	ctx := context.Background()
	bs := ipld2.NewSyncBlockStoreInMemory()
	v := vm6.NewVMWithSingletons(ctx, t, bs)

	createMinersAndSectorsV6(t, ctx, v, 100, 10, 100)
}