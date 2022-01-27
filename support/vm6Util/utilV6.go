package vm6Util

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/dline"
	"github.com/filecoin-project/go-state-types/exitcode"
	xc "github.com/filecoin-project/go-state-types/exitcode"
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin"
	initactor "github.com/filecoin-project/specs-actors/v6/actors/builtin/init"
	market6 "github.com/filecoin-project/specs-actors/v6/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v6/actors/builtin/miner"
	power6 "github.com/filecoin-project/specs-actors/v6/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v6/actors/runtime/proof"
	"github.com/filecoin-project/specs-actors/v6/actors/util/adt"
	tutil "github.com/filecoin-project/specs-actors/v6/support/testing"
	vm6 "github.com/filecoin-project/specs-actors/v6/support/vm"
	"github.com/stretchr/testify/require"
)

func MinerPower(t *testing.T, vm *vm6.VM, store adt.Store, minerIdAddr address.Address) miner.PowerPair {
	var state power6.State
	err := vm.GetState(builtin.StoragePowerActorAddr, &state)
	require.NoError(t, err)

	claim, found, err := state.GetClaim(store, minerIdAddr)
	require.NoError(t, err)
	require.True(t, found)

	return miner.NewPowerPair(claim.RawBytePower, claim.QualityAdjPower)
}

func SubmitPoStForDeadline(t *testing.T, v *vm6.VM, ctxStore adt.Store, minerAddress, workerAddress address.Address) {
	dlInfo := MinerDLInfo(t, v, minerAddress)
	var minerState miner.State
	err := v.GetState(minerAddress, &minerState)
	require.NoError(t, err)
	deadlines, err := minerState.LoadDeadlines(ctxStore)
	require.NoError(t, err)
	deadline, err := deadlines.LoadDeadline(ctxStore, dlInfo.Index)
	require.NoError(t, err)
	if deadline.TotalSectors == 0 {
		return
	}
	partitionArray, err := deadline.PartitionsArray(ctxStore)
	require.NoError(t, err)
	var partitions []miner.PoStPartition
	for i := uint64(0); i < partitionArray.Length(); i++ {
		var part miner.Partition
		_, err = partitionArray.Get(i, &part)
		require.NoError(t, err)

		partitions = append(partitions, miner.PoStPartition{
			Index:   i,
			Skipped: bitfield.New(),
		})
	}

	submitParams := miner.SubmitWindowedPoStParams{
		Deadline:   dlInfo.Index,
		Partitions: partitions,
		Proofs: []proof.PoStProof{{
			PoStProof: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		}},
		ChainCommitEpoch: dlInfo.Challenge,
		ChainCommitRand:  []byte(vm6.RandString),
	}

	vm6.ApplyOk(t, v, workerAddress, minerAddress, big.Zero(), builtin.MethodsMiner.SubmitWindowedPoSt, &submitParams)
}

func PreCommitSectors(t *testing.T, v *vm6.VM, count, batchSize int, worker, mAddr address.Address, sealProof abi.RegisteredSealProof, sectorNumberBase abi.SectorNumber, expectCronEnrollment bool, expiration abi.ChainEpoch, dealIDs []abi.DealID) []*miner.SectorPreCommitOnChainInfo {
	sectorIndex := 0
	for sectorIndex < count {
		// Prepare message.
		params := miner.PreCommitSectorBatchParams{Sectors: make([]miner0.SectorPreCommitInfo, batchSize)}
		if expiration < 0 {
			expiration = v.GetEpoch() + miner.MinSectorExpiration + miner.MaxProveCommitDuration[sealProof] + 100
		}

		for j := 0; j < batchSize && sectorIndex < count; j++ {
			var deals []abi.DealID = nil
			if j == 0 {
				deals = dealIDs
			}
			sectorNumber := sectorNumberBase + abi.SectorNumber(sectorIndex)
			sealedCid := tutil.MakeCID(fmt.Sprintf("%d", sectorNumber), &miner.SealedCIDPrefix)
			params.Sectors[j] = miner0.SectorPreCommitInfo{
				SealProof:     sealProof,
				SectorNumber:  sectorNumber,
				SealedCID:     sealedCid,
				SealRandEpoch: v.GetEpoch() - 1,
				DealIDs:       deals,
				Expiration:    expiration,
			}
			sectorIndex++
		}
		if sectorIndex == count && sectorIndex%batchSize != 0 {
			// Trim the last, partial batch.
			params.Sectors = params.Sectors[:sectorIndex%batchSize]
		}

		vm6.ApplyOk(t, v, worker, mAddr, big.Zero(), builtin.MethodsMiner.PreCommitSectorBatch, &params)
		vm6.ExpectInvocation{
			To:     mAddr,
			Method: builtin.MethodsMiner.PreCommitSectorBatch,
			Params: vm6.ExpectObject(&params),
		}.Matches(t, v.LastInvocation())
	}

	// Extract chain state.
	var minerState miner.State
	err := v.GetState(mAddr, &minerState)
	require.NoError(t, err)

	precommits := make([]*miner.SectorPreCommitOnChainInfo, count)
	for i := 0; i < count; i++ {
		precommit, found, err := minerState.GetPrecommittedSector(v.Store(), sectorNumberBase+abi.SectorNumber(i))
		require.NoError(t, err)
		require.True(t, found)
		precommits[i] = precommit
	}
	return precommits
}

// Returns an error if the target sector cannot be found, or some other bad state is reached.
// Returns false if the target sector is faulty, terminated, or unproven
// Returns true otherwise
func CheckMinerSectorActive(st *miner.State, store adt.Store, dlIdx, pIdx uint64, sector abi.SectorNumber, requireProven bool) (bool, error) {
	dls, err := st.LoadDeadlines(store)
	if err != nil {
		return false, err
	}

	dl, err := dls.LoadDeadline(store, dlIdx)
	if err != nil {
		return false, err
	}

	partition, err := dl.LoadPartition(store, pIdx)
	if err != nil {
		return false, err
	}

	if exists, err := partition.Sectors.IsSet(uint64(sector)); err != nil {
		return false, xc.ErrIllegalState.Wrapf("failed to decode sectors bitfield (deadline %d, partition %d): %w", dlIdx, pIdx, err)
	} else if !exists {
		return false, xc.ErrNotFound.Wrapf("sector %d not a member of partition %d, deadline %d", sector, pIdx, dlIdx)
	}

	if faulty, err := partition.Faults.IsSet(uint64(sector)); err != nil {
		return false, xc.ErrIllegalState.Wrapf("failed to decode faults bitfield (deadline %d, partition %d): %w", dlIdx, pIdx, err)
	} else if faulty {
		return false, nil
	}

	if terminated, err := partition.Terminated.IsSet(uint64(sector)); err != nil {
		return false, xc.ErrIllegalState.Wrapf("failed to decode terminated bitfield (deadline %d, partition %d): %w", dlIdx, pIdx, err)
	} else if terminated {
		return false, nil
	}

	if unproven, err := partition.Unproven.IsSet(uint64(sector)); err != nil {
		return false, xc.ErrIllegalState.Wrapf("failed to decode unproven bitfield (deadline %d, partition %d): %w", dlIdx, pIdx, err)
	} else if unproven && requireProven {
		return false, nil
	}

	return true, nil
}

// Returns a bitfield of the sector numbers from a collection pre-committed sectors.
func PrecommitSectorNumbers(precommits []*miner.SectorPreCommitOnChainInfo) bitfield.BitField {
	intSectorNumbers := make([]uint64, len(precommits))
	for i := range precommits {
		intSectorNumbers[i] = uint64(precommits[i].Info.SectorNumber)
	}
	return bitfield.NewFromSet(intSectorNumbers)
}

// Proves pre-committed sectors as batches of aggSize.
func ProveCommitSectors(t *testing.T, v *vm6.VM, worker, actor address.Address, precommits []*miner.SectorPreCommitOnChainInfo, includesDeals bool) {
	for len(precommits) > 0 {
		batchSize := min(819, len(precommits)) // 819 is max aggregation size
		toProve := precommits[:batchSize]
		precommits = precommits[batchSize:]

		sectorNosBf := PrecommitSectorNumbers(toProve)
		proveCommitAggregateParams := miner.ProveCommitAggregateParams{
			SectorNumbers: sectorNosBf,
		}
		vm6.ApplyOk(t, v, worker, actor, big.Zero(), builtin.MethodsMiner.ProveCommitAggregate, &proveCommitAggregateParams)
		vm6.ExpectInvocation{
			To:     actor,
			Method: builtin.MethodsMiner.ProveCommitAggregate,
			Params: vm6.ExpectObject(&proveCommitAggregateParams),
		}.Matches(t, v.LastInvocation())
	}
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func MinerDLInfo(t *testing.T, v *vm6.VM, minerIDAddr address.Address) *dline.Info {
	var minerState miner.State
	err := v.GetState(minerIDAddr, &minerState)
	require.NoError(t, err)

	return miner.NewDeadlineInfoFromOffsetAndEpoch(minerState.ProvingPeriodStart, v.GetEpoch())
}

// Advances to the next epoch, running cron.
func AdvanceOneEpochWithCron(t *testing.T, v *vm6.VM) *vm6.VM {
	vm6.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	v, err := v.WithEpoch(v.GetEpoch() + 1)
	require.NoError(t, err)
	return v
}

// Advances by deadline until e is contained within the deadline period represented by the returned deadline info.
// The VM returned will be set to the last deadline close, not at e.
func AdvanceToEpochWithCron(t *testing.T, v *vm6.VM, e abi.ChainEpoch) *vm6.VM {
	for v.GetEpoch() < e {
		v = AdvanceOneEpochWithCron(t, v)
	}
	return v
}

func AdvanceOneDeadlineWithCron(t *testing.T, v *vm6.VM) *vm6.VM {
	for i := abi.ChainEpoch(0); i < miner.WPoStChallengeWindow; i++ {
		v = AdvanceOneEpochWithCron(t, v)
	}
	return v
}

func PrintMinerInfos(t *testing.T, v *vm6.VM, ctxStore adt.Store, minerInfos []MinerInfo) {
	var initState initactor.State
	err := v.GetState(builtin.InitActorAddr, &initState)
	require.NoError(t, err)
	for _, minerInfo := range minerInfos {
		workerId, _, err := initState.ResolveAddress(ctxStore, minerInfo.WorkerAddress)
		require.NoError(t, err)
		fmt.Printf("WORKER, MINER %s %s\n", workerId, minerInfo.MinerAddress.String())
	}
}

func ProveThenAdvanceOneDeadlineWithCron(t *testing.T, v *vm6.VM, ctxStore adt.Store, minerInfos []MinerInfo) *vm6.VM {
	for _, minerInfo := range minerInfos {
		SubmitPoStForDeadline(t, v, ctxStore, minerInfo.MinerAddress, minerInfo.WorkerAddress)
	}
	return AdvanceOneDeadlineWithCron(t, v)
}

func AdvanceOneDayWhileProving(t *testing.T, v *vm6.VM, ctxStore adt.Store, minerInfos []MinerInfo) *vm6.VM {
	for i := uint64(0); i < miner.WPoStPeriodDeadlines; i++ {
		v = ProveThenAdvanceOneDeadlineWithCron(t, v, ctxStore, minerInfos)
	}
	return v
}

type MinerInfo struct {
	WorkerAddress address.Address
	MinerAddress  address.Address
}

func CreateDeals(t *testing.T, numberOfDeals int, v *vm6.VM, clientAddress address.Address, workerAddress address.Address, minerAddress address.Address, sealProof abi.RegisteredSealProof) []abi.DealID {
	// add market collateral for client and miner
	collateral := big.Mul(big.NewInt(int64(3*numberOfDeals)), vm6.FIL)
	vm6.ApplyOk(t, v, clientAddress, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &clientAddress)
	collateral = big.Mul(big.NewInt(int64(64*numberOfDeals)), vm6.FIL)
	vm6.ApplyOk(t, v, workerAddress, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &minerAddress)

	var dealIDs []abi.DealID
	for i := 0; i < numberOfDeals; i++ {
		dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
		deals := publishDeal(t, v, workerAddress, workerAddress, minerAddress, "dealLabel"+strconv.Itoa(i), 32<<30, false, dealStart, 180*builtin.EpochsInDay)
		dealIDs = append(dealIDs, deals.IDs...)
	}

	return dealIDs
}

func publishDeal(t *testing.T, v *vm6.VM, provider, dealClient, minerID address.Address, dealLabel string,
	pieceSize abi.PaddedPieceSize, verifiedDeal bool, dealStart abi.ChainEpoch, dealLifetime abi.ChainEpoch,
) *market6.PublishStorageDealsReturn {
	deal := market6.DealProposal{
		PieceCID:             tutil.MakeCID(dealLabel, &market6.PieceCIDPrefix),
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
	result := vm6.RequireApplyMessage(t, v, provider, builtin.StorageMarketActorAddr, big.Zero(), builtin.MethodsMarket.PublishStorageDeals, &publishDealParams, t.Name())
	require.Equal(t, exitcode.Ok, result.Code)

	expectedPublishSubinvocations := []vm6.ExpectInvocation{
		{To: minerID, Method: builtin.MethodsMiner.ControlAddresses, SubInvocations: []vm6.ExpectInvocation{}},
		{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward, SubInvocations: []vm6.ExpectInvocation{}},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower, SubInvocations: []vm6.ExpectInvocation{}},
	}

	if verifiedDeal {
		expectedPublishSubinvocations = append(expectedPublishSubinvocations, vm6.ExpectInvocation{
			To:             builtin.VerifiedRegistryActorAddr,
			Method:         builtin.MethodsVerifiedRegistry.UseBytes,
			SubInvocations: []vm6.ExpectInvocation{},
		})
	}

	vm6.ExpectInvocation{
		To:             builtin.StorageMarketActorAddr,
		Method:         builtin.MethodsMarket.PublishStorageDeals,
		SubInvocations: expectedPublishSubinvocations,
	}.Matches(t, v.LastInvocation())

	return result.Ret.(*market6.PublishStorageDealsReturn)
}
