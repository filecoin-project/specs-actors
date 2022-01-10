package test

import (
	"context"
	ipld2 "github.com/filecoin-project/specs-actors/v2/support/ipld"
	"github.com/filecoin-project/specs-actors/v6/actors/util/adt"
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

	"github.com/filecoin-project/go-address"
)

const sealProof = abi.RegisteredSealProof_StackedDrg32GiBV1_1


func publishDealv6(t *testing.T, v *vm6.VM, provider, dealClient, minerID address.Address, dealLabel string,
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

func createMiners(t *testing.T, ctx context.Context, v *vm6.VM, numMiners int) []vm6Util.MinerInfo {
	wPoStProof, err := sealProof.RegisteredWindowPoStProof()
	require.NoError(t, err)

	workerAddresses := vm6.CreateAccounts(ctx, t, v, numMiners, big.Mul(big.NewInt(200_000_000), vm6.FIL), 93837778)
	assert.Equal(t, len(workerAddresses), numMiners)

	var minerInfos []vm6Util.MinerInfo
	for _, workerAddress := range workerAddresses {
		params := power6.CreateMinerParams{
			Owner:               workerAddress,
			Worker:              workerAddress,
			WindowPoStProofType: wPoStProof,
			Peer:                abi.PeerID("not really a peer id"),
		}
		ret := vm6.ApplyOk(t, v, workerAddress, builtin6.StoragePowerActorAddr, big.Mul(big.NewInt(100_000_000), vm6.FIL), builtin6.MethodsPower.CreateMiner, &params)
		minerAddress, ok := ret.(*power6.CreateMinerReturn)
		require.True(t, ok)
		minerInfos = append(minerInfos, vm6Util.MinerInfo{ WorkerAddress: workerAddress, MinerAddress: minerAddress.IDAddress })
	}
	assert.Equal(t, len(minerInfos), numMiners)
	return minerInfos
}

func precommits(t *testing.T, v *vm6.VM, firstSectorNo int, numSectors int, minerInfos []vm6Util.MinerInfo) [][]*miner6.SectorPreCommitOnChainInfo {
	var precommitInfo [][]*miner6.SectorPreCommitOnChainInfo
	for _, minerInfo := range minerInfos {

		precommits := vm6Util.PreCommitSectors(t, v, numSectors, miner6.PreCommitSectorBatchMaxSize, minerInfo.WorkerAddress, minerInfo.MinerAddress, sealProof, abi.SectorNumber(firstSectorNo), true, v.GetEpoch()+miner6.MaxSectorExpirationExtension)

		assert.Equal(t, len(precommits), numSectors)
		balances := vm6.GetMinerBalances(t, v, minerInfo.MinerAddress)
		assert.True(t, balances.PreCommitDeposit.GreaterThan(big.Zero()))
		precommitInfo = append(precommitInfo, precommits)
	}
	return precommitInfo
}

func createMinersAndSectorsV6(t *testing.T, ctx context.Context, v *vm6.VM, firstSectorNo int, numMiners int, numSectors int) []vm6Util.MinerInfo {
	minerInfos := createMiners(t, ctx, v, numMiners)

	precommitInfo := precommits(t, v , firstSectorNo, numSectors, minerInfos)

	// advance time to when we can prove-commit
	proveTime := v.GetEpoch() + miner6.PreCommitChallengeDelay + 1
	v, err := v.WithEpoch(proveTime)
	require.NoError(t, err)

	for i, minerInfo := range minerInfos {
		vm6Util.ProveCommitSectors(t, v, minerInfo.WorkerAddress, minerInfo.MinerAddress, precommitInfo[i])
	}
	v = vm6Util.AdvanceOneEpochWithCron(t, v)

	//networkStats := vm6.GetNetworkStats(t, v)
	//fmt.Printf("BYTES COMMITTED %s\n", networkStats.TotalBytesCommitted)
	return minerInfos
}

func TestCreateMiners(t *testing.T) {
	ctx := context.Background()
	bs := ipld2.NewSyncBlockStoreInMemory()
	ctxStore := adt.WrapBlockStore(ctx, bs)
	v := vm6.NewVMWithSingletons(ctx, t, bs)

	v = vm6Util.AdvanceToEpochWithCron(t, v, 200)
	minerInfos := createMinersAndSectorsV6(t, ctx, v, 100, 11, 10_000)

	v = vm6Util.AdvanceOneDayWhileProving(t, v, ctxStore, minerInfos)
	v = vm6Util.AdvanceOneDayWhileProving(t, v, ctxStore, minerInfos)
}