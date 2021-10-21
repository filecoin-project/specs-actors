package test

import (
	"testing"

	"github.com/filecoin-project/go-address"
	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v7/actors/states"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v7/actors/builtin"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/market"
	tutil "github.com/filecoin-project/specs-actors/v7/support/testing"
	"github.com/filecoin-project/specs-actors/v7/support/vm"
)

func createMiner(t *testing.T, v *vm.VM, owner, worker addr.Address, wPoStProof abi.RegisteredPoStProof, balance abi.TokenAmount) *power.CreateMinerReturn {
	params := power.CreateMinerParams{
		Owner:               owner,
		Worker:              worker,
		WindowPoStProofType: wPoStProof,
		Peer:                abi.PeerID("not really a peer id"),
	}
	ret := vm.ApplyOk(t, v, worker, builtin.StoragePowerActorAddr, balance, builtin.MethodsPower.CreateMiner, &params)
	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)
	return minerAddrs
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
			Proposal: deal,
			ClientSignature: crypto.Signature{
				Type: crypto.SigTypeBLS,
			},
		}},
	}
	result := vm.RequireApplyMessage(t, v, provider, builtin.StorageMarketActorAddr, big.Zero(), builtin.MethodsMarket.PublishStorageDeals, &publishDealParams, t.Name())
	require.Equal(t, exitcode.Ok, result.Code)

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

	return result.Ret.(*market.PublishStorageDealsReturn)
}

type dealBatcher struct {
	deals []market.DealProposal
	v     *vm.VM
}

func newDealBatcher(v *vm.VM) *dealBatcher {
	return &dealBatcher{
		deals: make([]market.DealProposal, 0),
		v:     v,
	}
}

func (db *dealBatcher) stage(t *testing.T, dealClient, dealProvider addr.Address, dealLabel string, pieceSize abi.PaddedPieceSize, verifiedDeal bool, dealStart,
	dealLifetime abi.ChainEpoch, pricePerEpoch, providerCollateral, clientCollateral abi.TokenAmount) {
	deal := market.DealProposal{
		PieceCID:             tutil.MakeCID(dealLabel, &market.PieceCIDPrefix),
		PieceSize:            pieceSize,
		VerifiedDeal:         verifiedDeal,
		Client:               dealClient,
		Provider:             dealProvider,
		Label:                dealLabel,
		StartEpoch:           dealStart,
		EndEpoch:             dealStart + dealLifetime,
		StoragePricePerEpoch: pricePerEpoch,
		ProviderCollateral:   providerCollateral,
		ClientCollateral:     clientCollateral,
	}

	db.deals = append(db.deals, deal)
}

func (db *dealBatcher) publishOK(t *testing.T, sender addr.Address) *market.PublishStorageDealsReturn {
	publishDealParams := market.PublishStorageDealsParams{}
	for _, deal := range db.deals {
		publishDealParams.Deals = append(publishDealParams.Deals, market.ClientDealProposal{
			Proposal: deal,
			ClientSignature: crypto.Signature{
				Type: crypto.SigTypeBLS,
			},
		})
	}

	result := vm.RequireApplyMessage(t, db.v, sender, builtin.StorageMarketActorAddr, big.Zero(), builtin.MethodsMarket.PublishStorageDeals, &publishDealParams, t.Name())
	require.Equal(t, exitcode.Ok, result.Code)

	return result.Ret.(*market.PublishStorageDealsReturn)
}

func (db *dealBatcher) publishFail(t *testing.T, sender addr.Address) {
	publishDealParams := market.PublishStorageDealsParams{}
	for _, deal := range db.deals {
		publishDealParams.Deals = append(publishDealParams.Deals, market.ClientDealProposal{
			Proposal: deal,
			ClientSignature: crypto.Signature{
				Type: crypto.SigTypeBLS,
			},
		})
	}

	result := vm.RequireApplyMessage(t, db.v, sender, builtin.StorageMarketActorAddr, big.Zero(), builtin.MethodsMarket.PublishStorageDeals, &publishDealParams, t.Name())
	require.Equal(t, exitcode.ErrIllegalArgument, result.Code) // because we can't return multiple codes for batch failures we return 16 in all cases
}

func requireActor(t *testing.T, v *vm.VM, addr address.Address) *states.Actor {
	a, found, err := v.GetActor(addr)
	require.NoError(t, err)
	require.True(t, found)
	return a
}
