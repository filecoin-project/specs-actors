package test

import (
	"bytes"
	"context"
	"testing"
	"unicode/utf8"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-state-types/rt"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"

	ipld2 "github.com/filecoin-project/specs-actors/v2/support/ipld"
	market7 "github.com/filecoin-project/specs-actors/v7/actors/builtin/market"
	power7 "github.com/filecoin-project/specs-actors/v7/actors/builtin/power"

	vm7 "github.com/filecoin-project/specs-actors/v7/support/vm"

	"github.com/filecoin-project/specs-actors/v8/actors/builtin"
	"github.com/filecoin-project/specs-actors/v8/actors/builtin/exported"
	"github.com/filecoin-project/specs-actors/v8/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v8/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v8/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v8/actors/migration/nv16"
	"github.com/filecoin-project/specs-actors/v8/actors/util/adt"

	tutil "github.com/filecoin-project/specs-actors/v8/support/testing"
	"github.com/filecoin-project/specs-actors/v8/support/vm"

	addr "github.com/filecoin-project/go-address"
)

func TestDealLabelMigration(t *testing.T) {
	ctx := context.Background()
	log := nv16.TestLogger{TB: t}
	bs := ipld2.NewSyncBlockStoreInMemory()
	v := vm7.NewVMWithSingletons(ctx, t, bs)

	addrs := vm7.CreateAccounts(ctx, t, v, 2, big.Mul(big.NewInt(10_000), vm.FIL), 93837778)
	worker, client := addrs[0], addrs[1]

	minerBalance := big.Mul(big.NewInt(1_000), vm.FIL)
	sealProof := abi.RegisteredSealProof_StackedDrg32GiBV1_1

	// create miner
	params := power7.CreateMinerParams{
		Owner:               worker,
		Worker:              worker,
		WindowPoStProofType: abi.RegisteredPoStProof_StackedDrgWindow32GiBV1,
		Peer:                abi.PeerID("not really a peer id"),
	}
	ret := vm7.ApplyOk(t, v, addrs[0], builtin.StoragePowerActorAddr, minerBalance, builtin.MethodsPower.CreateMiner, &params)

	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	//
	// publish deals
	//

	// add market collateral for clients and miner
	collateral := big.Mul(big.NewInt(64), vm.FIL)
	vm7.ApplyOk(t, v, client, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &client)
	vm7.ApplyOk(t, v, worker, builtin.StorageMarketActorAddr, collateral, builtin.MethodsMarket.AddBalance, &minerAddrs.IDAddress)

	// create 6 deals
	dealIDs := []abi.DealID{}
	dealStart := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]

	invalidLabel1 := "deal2-activated-cronned-invalidUTF8" + string([]byte{0xff})
	invalidLabel2 := "deal4-activateduncronned-invalidUTF8" + string([]byte{0xff})
	invalidLabel3 := "deal6-unactivated-invalidUTF8" + string([]byte{0xff})
	require.True(t, !utf8.ValidString(invalidLabel1) && !utf8.ValidString(invalidLabel2) && !utf8.ValidString(invalidLabel3))

	deals := publishDealv7(t, v, worker, client, minerAddrs.IDAddress, "deal1-activatedcronned-validUTF8", 1<<30, false, dealStart, 365*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)
	deals = publishDealv7(t, v, worker, client, minerAddrs.IDAddress, invalidLabel1, 1<<30, false, dealStart, 365*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)
	deals = publishDealv7(t, v, worker, client, minerAddrs.IDAddress, "deal3-activateduncronned-validUTF8", 1<<30, false, dealStart, 365*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)
	deals = publishDealv7(t, v, worker, client, minerAddrs.IDAddress, invalidLabel2, 1<<30, false, dealStart, 365*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)
	deals = publishDealv7(t, v, worker, client, minerAddrs.IDAddress, "deal5-unactivated-validUTF8", 1<<30, false, dealStart, 365*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)
	deals = publishDealv7(t, v, worker, client, minerAddrs.IDAddress, invalidLabel3, 1<<30, false, dealStart, 365*builtin.EpochsInDay)
	dealIDs = append(dealIDs, deals.IDs...)

	deal1ID := dealIDs[0]
	deal2ID := dealIDs[1]
	deal3ID := dealIDs[2]
	deal4ID := dealIDs[3]
	deal5ID := dealIDs[4]
	deal6ID := dealIDs[5]

	deal1CronTime := market.GenRandNextEpoch(dealStart, deal1ID)
	deal2CronTime := market.GenRandNextEpoch(dealStart, deal2ID)
	deal3CronTime := market.GenRandNextEpoch(dealStart, deal3ID)
	deal4CronTime := market.GenRandNextEpoch(dealStart, deal4ID)
	deal5CronTime := market.GenRandNextEpoch(dealStart, deal5ID)
	deal6CronTime := market.GenRandNextEpoch(dealStart, deal6ID)
	require.True(t, deal1CronTime < deal2CronTime && deal2CronTime < deal3CronTime)
	require.True(t, deal3CronTime < deal4CronTime && deal4CronTime < deal5CronTime && deal5CronTime < deal6CronTime)
	require.True(t, v.GetEpoch() < dealStart && dealStart < deal1CronTime)

	// precommit sector with deals
	sectorNumber := abi.SectorNumber(100)
	sealedCid := tutil.MakeCID("100", &miner.SealedCIDPrefix)
	preCommitParams := miner.PreCommitSectorParams{
		SealProof:     sealProof,
		SectorNumber:  sectorNumber,
		SealedCID:     sealedCid,
		SealRandEpoch: v.GetEpoch() - 1,
		DealIDs:       dealIDs[:4],
		Expiration:    v.GetEpoch() + 400*builtin.EpochsInDay,
	}
	vm7.ApplyOk(t, v, addrs[0], minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.PreCommitSector, &preCommitParams)

	// advance time to max seal duration
	proveTime := v.GetEpoch() + miner.MaxProveCommitDuration[sealProof]
	v, _ = vm7.AdvanceByDeadlineTillEpoch(t, v, minerAddrs.IDAddress, proveTime)

	// Prove commit sector after max seal duration- deal1, deal2, deal3 and deal4 get activated here
	v, err := v.WithEpoch(proveTime)
	require.NoError(t, err)
	proveCommitParams := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}
	vm7.ApplyOk(t, v, worker, minerAddrs.RobustAddress, big.Zero(), builtin.MethodsMiner.ProveCommitSector, &proveCommitParams)

	// In the same epoch, trigger cron to validate prove commit
	vm7.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// advance time to when deal1 will be cronned and cron deal 1
	v, err = v.WithEpoch(deal1CronTime)
	require.NoError(t, err)
	vm7.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
	// advance time to when deal2 will be cronned and cron deal 2.  deals 3 and above uncronned
	v, err = v.WithEpoch(deal2CronTime)
	require.NoError(t, err)
	vm7.ApplyOk(t, v, builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)

	// now do some assertions about what's in pendingproposals/proposals
	// getting various AMTs out of things
	adtStore := adt.WrapStore(ctx, cbor.NewCborStore(bs))
	var market7State market7.State
	require.NoError(t, v.GetState(builtin.StorageMarketActorAddr, &market7State))
	oldProposals, err := adt.AsArray(adtStore, market7State.Proposals, market7.ProposalsAmtBitwidth)
	require.NoError(t, err)
	oldStates, err := adt.AsArray(adtStore, market7State.States, market7.StatesAmtBitwidth)
	require.NoError(t, err)
	oldPendingProposals, err := adt.AsSet(adtStore, market7State.PendingProposals, builtin.DefaultHamtBitwidth)
	require.NoError(t, err)
	// deal1, deal2 will just be in states and proposals and not pendingproposals
	checkMarketProposalsEtcState(t, oldProposals, oldStates, oldPendingProposals, deal1ID, true, true, false, false)
	checkMarketProposalsEtcState(t, oldProposals, oldStates, oldPendingProposals, deal2ID, true, true, false, false)
	// deal3, deal4 will be in proposals pendingproposals and in states
	checkMarketProposalsEtcState(t, oldProposals, oldStates, oldPendingProposals, deal3ID, true, true, true, false)
	checkMarketProposalsEtcState(t, oldProposals, oldStates, oldPendingProposals, deal4ID, true, true, true, false)
	// deal5, deal6 will be in proposals and pendingproposals but not in states
	checkMarketProposalsEtcState(t, oldProposals, oldStates, oldPendingProposals, deal5ID, true, false, true, false)
	checkMarketProposalsEtcState(t, oldProposals, oldStates, oldPendingProposals, deal6ID, true, false, true, false)

	oldMarketActor, found, err := v.GetActor(builtin.StorageMarketActorAddr)
	require.NoError(t, err)
	require.True(t, found)

	startRoot := v.StateRoot()
	manifestCid := makeTestManifest(t, adtStore)
	nextRoot, err := nv16.MigrateStateTree(ctx, adtStore, manifestCid, startRoot, abi.ChainEpoch(0), nv16.Config{MaxWorkers: 1}, log, nv16.NewMemMigrationCache())
	require.NoError(t, err)

	lookup := map[cid.Cid]rt.VMActor{}
	for _, ba := range exported.BuiltinActors() {
		lookup[ba.Code()] = ba
	}

	v8, err := vm.NewVMAtEpoch(ctx, lookup, v.Store(), nextRoot, v.GetEpoch()+1)
	require.NoError(t, err)

	// now do the assertions again about what's in pendingproposals/proposals
	// getting various AMTs out of things
	var marketState market.State
	require.NoError(t, v8.GetState(builtin.StorageMarketActorAddr, &marketState))
	proposals, err := adt.AsArray(adtStore, marketState.Proposals, market.ProposalsAmtBitwidth)
	require.NoError(t, err)
	states, err := adt.AsArray(adtStore, marketState.States, market.StatesAmtBitwidth)
	require.NoError(t, err)
	pendingProposals, err := adt.AsSet(adtStore, marketState.PendingProposals, builtin.DefaultHamtBitwidth)
	require.NoError(t, err)
	// deal1, deal2 will just be in states and proposals and not pendingproposals
	checkMarketProposalsEtcState(t, proposals, states, pendingProposals, deal1ID, true, true, false, true)
	checkMarketProposalsEtcState(t, proposals, states, pendingProposals, deal2ID, true, true, false, true)
	// deal3, deal4 will be in proposals pendingproposals and in states
	checkMarketProposalsEtcState(t, proposals, states, pendingProposals, deal3ID, true, true, true, true)
	checkMarketProposalsEtcState(t, proposals, states, pendingProposals, deal4ID, true, true, true, true)
	// deal5, deal6 will be in proposals and pendingproposals but not in states
	checkMarketProposalsEtcState(t, proposals, states, pendingProposals, deal5ID, true, false, true, true)
	checkMarketProposalsEtcState(t, proposals, states, pendingProposals, deal6ID, true, false, true, true)

	// check that labels are the same, just with potentially changed types, before and after.
	require.NoError(t, checkLabel(oldProposals, proposals, deal1ID))
	require.NoError(t, checkLabel(oldProposals, proposals, deal2ID))
	require.NoError(t, checkLabel(oldProposals, proposals, deal3ID))
	require.NoError(t, checkLabel(oldProposals, proposals, deal4ID))
	require.NoError(t, checkLabel(oldProposals, proposals, deal5ID))
	require.NoError(t, checkLabel(oldProposals, proposals, deal6ID))

	var market8State market.State
	require.NoError(t, v8.GetState(builtin.StorageMarketActorAddr, &market8State))
	market.CheckStateInvariants(&market8State, v8.Store(), oldMarketActor.Balance, v.GetEpoch()+1)

}

func publishDealv7(t *testing.T, v *vm7.VM, provider, dealClient, minerID addr.Address, dealLabel string,
	pieceSize abi.PaddedPieceSize, verifiedDeal bool, dealStart abi.ChainEpoch, dealLifetime abi.ChainEpoch,
) *market7.PublishStorageDealsReturn {
	deal := market7.DealProposal{
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

	// fake signature syscall wants sig data == provided data
	buf := bytes.Buffer{}
	require.NoError(t, deal.MarshalCBOR(&buf))
	publishDealParams := market7.PublishStorageDealsParams{
		Deals: []market7.ClientDealProposal{{
			Proposal: deal,
			ClientSignature: crypto.Signature{
				Type: crypto.SigTypeBLS,
				Data: buf.Bytes(),
			},
		}},
	}

	result := vm7.RequireApplyMessage(t, v, provider, builtin.StorageMarketActorAddr, big.Zero(), builtin.MethodsMarket.PublishStorageDeals, &publishDealParams, t.Name())
	require.Equal(t, exitcode.Ok, result.Code)

	expectedPublishSubinvocations := []vm7.ExpectInvocation{
		{To: minerID, Method: builtin.MethodsMiner.ControlAddresses, SubInvocations: []vm7.ExpectInvocation{}},
		{To: builtin.RewardActorAddr, Method: builtin.MethodsReward.ThisEpochReward, SubInvocations: []vm7.ExpectInvocation{}},
		{To: builtin.StoragePowerActorAddr, Method: builtin.MethodsPower.CurrentTotalPower, SubInvocations: []vm7.ExpectInvocation{}},
	}

	if verifiedDeal {
		expectedPublishSubinvocations = append(expectedPublishSubinvocations, vm7.ExpectInvocation{
			To:             builtin.VerifiedRegistryActorAddr,
			Method:         builtin.MethodsVerifiedRegistry.UseBytes,
			SubInvocations: []vm7.ExpectInvocation{},
		})
	}

	vm7.ExpectInvocation{
		To:             builtin.StorageMarketActorAddr,
		Method:         builtin.MethodsMarket.PublishStorageDeals,
		SubInvocations: expectedPublishSubinvocations,
	}.Matches(t, v.LastInvocation())

	return result.Ret.(*market7.PublishStorageDealsReturn)
}

func checkMarketProposalsEtcState(t *testing.T, proposals *adt.Array, states *adt.Array, pendingProposals *adt.Set,
	dealID abi.DealID, inProposals bool, inStates bool, inPendingProposals bool, isv8 bool) {
	var dealprop8 market.DealProposal
	var dealprop7 market7.DealProposal
	var found bool
	var err error
	if isv8 {
		found, err = proposals.Get(uint64(dealID), &dealprop8)
	} else {
		found, err = proposals.Get(uint64(dealID), &dealprop7)
	}
	require.NoError(t, err)
	require.Equal(t, inProposals, found)
	found, err = states.Get(uint64(dealID), nil)
	require.NoError(t, err)
	require.Equal(t, inStates, found)
	var dealpropcid cid.Cid
	if isv8 {
		dealpropcid, err = dealprop8.Cid()
	} else {
		dealpropcid, err = dealprop7.Cid()
	}
	require.NoError(t, err)
	found, err = pendingProposals.Has(abi.CidKey(dealpropcid))
	require.NoError(t, err)
	require.Equal(t, inPendingProposals, found)
}

func checkLabel(v7Proposals *adt.Array, v8Proposals *adt.Array, dealID abi.DealID) error {
	var dealprop7 market7.DealProposal
	var dealprop8 market.DealProposal
	found, err := v7Proposals.Get(uint64(dealID), &dealprop7)
	if !found || err != nil {
		return xerrors.Errorf("failed to look up dealID %v in validating deal label", dealID)
	}
	found, err = v8Proposals.Get(uint64(dealID), &dealprop8)
	if !found || err != nil {
		return xerrors.Errorf("failed to look up dealID %v in validating deal label", dealID)
	}
	if dealprop8.Label.IsString() {
		prop8LabelString, err := dealprop8.Label.ToString()
		if err != nil {
			return err
		}
		if !utf8.ValidString(prop8LabelString) {
			return xerrors.Errorf("migration incorrectly kept invalid utf8 string as string in label")
		}
		if dealprop7.Label != prop8LabelString {
			return xerrors.Errorf("deal labels were not the same after migration.")
		}
	} else { // dealprop8.Label.IsBytes()
		bs, err := dealprop8.Label.ToBytes()
		if err != nil {
			return err
		}
		if utf8.ValidString(string(bs)) {
			return xerrors.Errorf("label converted to bytes in migration even though it is valid utf8")
		}
		if dealprop7.Label != string(bs) {
			return xerrors.Errorf("deal labels were not the same after migration")
		}
	}

	return nil
}
