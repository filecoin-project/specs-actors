package storage_market

import (
	addr "github.com/filecoin-project/go-address"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	. "github.com/filecoin-project/specs-actors/actors/util"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

type StorageMarketActor struct{}

type Runtime = vmr.Runtime

////////////////////////////////////////////////////////////////////////////////
// Actor methods
////////////////////////////////////////////////////////////////////////////////

func (a *StorageMarketActor) Constructor(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)

	rt.State().Construct(func() vmr.CBORMarshaler {
		st, err := ConstructState(adt.AsStore(rt))
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "failed to create storage market state map: %v", err)
		}
		return st
	})
	return &adt.EmptyValue{}
}

type WithdrawBalanceParams struct {
	Address addr.Address
	Amount  abi.TokenAmount
}

// Attempt to withdraw the specified amount from the balance held in escrow.
// If less than the specified amount is available, yields the entire available balance.
func (a *StorageMarketActor) WithdrawBalance(rt Runtime, params *WithdrawBalanceParams) *adt.EmptyValue {
	amountSlashedTotal := abi.NewTokenAmount(0)

	if params.Amount.LessThan(big.Zero()) {
		rt.Abort(exitcode.ErrIllegalArgument, "negative amount %v", params.Amount)
	}

	recipientAddr := builtin.MarketAddress(rt, params.Address)

	var amountExtracted abi.TokenAmount
	var st StorageMarketActorState
	rt.State().Transaction(&st, func() interface{} {
		// Before any operations that check the balance tables for funds, execute all deferred
		// deal state updates.
		amountSlashedTotal = big.Add(amountSlashedTotal, st.updatePendingDealStatesForParty(rt, params.Address))

		minBalance := st.getLockedBalance(rt, params.Address)

		et := adt.AsBalanceTable(adt.AsStore(rt), st.EscrowTable)
		ex, err := et.SubtractWithMinimum(params.Address, params.Amount, minBalance)
		if err != nil {
			rt.Abort(exitcode.ErrIllegalState, "subtract form escrow table: %v", err)
		}

		st.EscrowTable = et.Root()
		amountExtracted = ex
		return nil
	})

	_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, amountSlashedTotal)
	builtin.RequireSuccess(rt, code, "failed to burn slashed funds")
	_, code = rt.Send(recipientAddr, builtin.MethodSend, nil, amountExtracted)
	builtin.RequireSuccess(rt, code, "failed to send funds")
	return &adt.EmptyValue{}
}

// Deposits the specified amount into the balance held in escrow.
// Note: the amount is included implicitly in the message.
func (a *StorageMarketActor) AddBalance(rt Runtime, address *addr.Address) *adt.EmptyValue {
	builtin.MarketAddress(rt, *address)

	var st StorageMarketActorState
	rt.State().Transaction(&st, func() interface{} {
		msgValue := rt.ValueReceived()

		{
			et := adt.AsBalanceTable(adt.AsStore(rt), st.EscrowTable)
			err := et.AddCreate(*address, msgValue)
			if err != nil {
				rt.Abort(exitcode.ErrIllegalState, "adding to escrow table: %v", err)
			}
			st.EscrowTable = et.Root()
		}

		{
			lt := adt.AsBalanceTable(adt.AsStore(rt), st.LockedTable)
			err := lt.AddCreate(*address, big.NewInt(0))
			if err != nil {
				rt.Abort(exitcode.ErrIllegalArgument, "adding to locked table: %v", err)
			}
			st.LockedTable = lt.Root()
		}

		return nil
	})
	return &adt.EmptyValue{}
}

type PublishStorageDealsParams struct {
	Deals []StorageDealProposal
}

// Publish a new set of storage deals (not yet included in a sector).
func (a *StorageMarketActor) PublishStorageDeals(rt Runtime, params *PublishStorageDealsParams) *adt.EmptyValue {
	amountSlashedTotal := abi.NewTokenAmount(0)

	// Deal message must have a From field identical to the provider of all the deals.
	// This allows us to retain and verify only the client's signature in each deal proposal itself.
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)

	var st StorageMarketActorState
	rt.State().Transaction(&st, func() interface{} {
		// All storage deals will be added in an atomic transaction; this operation will be unrolled if any of them fails.
		for _, deal := range params.Deals {
			if deal.Provider != rt.ImmediateCaller() {
				rt.Abort(exitcode.ErrForbidden, "caller is not provider %v", deal.Provider)
			}

			validateDeal(rt, deal)

			// Before any operations that check the balance tables for funds, execute all deferred
			// deal state updates.
			//
			// Note: as an optimization, implementations may cache efficient data structures indicating
			// which of the following set of updates are redundant and can be skipped.
			amountSlashedTotal = big.Add(amountSlashedTotal, st.updatePendingDealStatesForParty(rt, deal.Client))
			amountSlashedTotal = big.Add(amountSlashedTotal, st.updatePendingDealStatesForParty(rt, deal.Provider))

			st.lockBalanceOrAbort(rt, deal.Client, deal.ClientBalanceRequirement())
			st.lockBalanceOrAbort(rt, deal.Provider, deal.ProviderBalanceRequirement())

			id := st.generateStorageDealID()

			onchainDeal := &OnChainDeal{
				ID:               id,
				Proposal:         deal,
				SectorStartEpoch: epochUndefined,
			}

			deals := AsDealArray(adt.AsStore(rt), st.Deals)
			err := deals.Set(id, onchainDeal)
			if err != nil {
				rt.Abort(exitcode.ErrIllegalState, "set deal %v", err)
			}
			st.Deals = deals.Root()
		}

		return nil
	})

	_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, amountSlashedTotal)
	builtin.RequireSuccess(rt, code, "failed to burn funds")
	return &adt.EmptyValue{}
}

type VerifyDealsOnSectorProveCommitParams struct {
	DealIDs      []abi.DealID
	SectorExpiry abi.ChainEpoch
}

// Verify that a given set of storage deals is valid for a sector currently being ProveCommitted,
// update the market's internal state accordingly, and return DealWeight of the set of storage deals given.
// Note: in the case of a capacity-commitment sector (one with zero deals), this function should succeed vacuously.
// The weight is defined as the sum, over all deals in the set, of the product of its size
// with its duration. This quantity may be an input into the functions specifying block reward,
// sector power, collateral, and/or other parameters.
func (a *StorageMarketActor) VerifyDealsOnSectorProveCommit(rt Runtime, params *VerifyDealsOnSectorProveCommitParams) *abi.DealWeight {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.ImmediateCaller()
	totalWeight := big.Zero()

	var st StorageMarketActorState
	rt.State().Transaction(&st, func() interface{} {
		// if there are no dealIDs, it is a CommittedCapacity sector
		// and the totalWeight should be zero
		for _, dealID := range params.DealIDs {
			deals := AsDealArray(adt.AsStore(rt), st.Deals)
			deal, err := deals.Get(dealID)
			if err != nil {
				rt.Abort(exitcode.ErrIllegalState, "get deal %v", err)
			}

			validateDealCanActivate(rt, minerAddr, params.SectorExpiry, deal)

			deal.SectorStartEpoch = rt.CurrEpoch()
			err = deals.Set(dealID, deal)
			if err != nil {
				rt.Abort(exitcode.ErrIllegalState, "set deal %v", err)
			}
			st.Deals = deals.Root()

			// Compute deal weight
			dur := big.NewInt(int64(deal.Proposal.Duration()))
			siz := big.NewInt(deal.Proposal.PieceSize.Total())
			weight := big.Mul(dur, siz)
			totalWeight = big.Add(totalWeight, weight)
		}
		return nil
	})
	return &totalWeight
}

type GetPieceInfosForDealIDsParams struct {
	DealIDs []abi.DealID
}

type GetPieceInfosForDealIDsReturn struct {
	Pieces []abi.PieceInfo
}

func (a *StorageMarketActor) GetPieceInfosForDealIDs(rt Runtime, params *GetPieceInfosForDealIDsParams) *GetPieceInfosForDealIDsReturn {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)

	ret := make([]abi.PieceInfo, 0)
	var st StorageMarketActorState
	rt.State().Transaction(&st, func() interface{} {
		for _, dealID := range params.DealIDs {
			deal := st.mustGetDeal(rt, dealID)
			ret = append(ret, abi.PieceInfo{
				PieceCID: deal.Proposal.PieceCID,
				Size:     deal.Proposal.PieceSize.Total(),
			})
		}
		return nil
	})

	return &GetPieceInfosForDealIDsReturn{Pieces: ret}
}

type OnMinerSectorsTerminateParams struct {
	DealIDs []abi.DealID
}

// Terminate a set of deals in response to their containing sector being terminated.
// Slash provider collateral, refund client collateral, and refund partial unpaid escrow
// amount to client.
func (a *StorageMarketActor) OnMinerSectorsTerminate(rt Runtime, params *OnMinerSectorsTerminateParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.ImmediateCaller()

	var st StorageMarketActorState
	rt.State().Transaction(&st, func() interface{} {
		deals := AsDealArray(adt.AsStore(rt), st.Deals)

		for _, dealID := range params.DealIDs {
			deal, err := deals.Get(dealID)
			if err != nil {
				rt.Abort(exitcode.ErrIllegalState, "get deal: %v", err)
			}
			Assert(deal.Proposal.Provider == minerAddr)

			// Note: we do not perform the balance transfers here, but rather simply record the flag
			// to indicate that processDealSlashed should be called when the deferred state computation
			// is performed. // TODO: Do that here

			deal.SlashEpoch = rt.CurrEpoch()
			err = deals.Set(dealID, deal)
			if err != nil {
				rt.Abort(exitcode.ErrIllegalState, "set deal: %v", err)
			}
		}

		st.Deals = deals.Root()
		return nil
	})
	return &adt.EmptyValue{}
}

type HandleExpiredDealsParams struct {
	Deals []abi.DealID // TODO: RLE
}

func (a *StorageMarketActor) HandleExpiredDeals(rt Runtime, params *HandleExpiredDealsParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	var slashed abi.TokenAmount
	var st StorageMarketActorState
	rt.State().Transaction(&st, func() interface{} {
		slashed = st.updatePendingDealStates(rt, params.Deals, rt.CurrEpoch())
		return nil
	})

	// TODO: award some small portion of slashed to caller as incentive

	_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, slashed)
	builtin.RequireSuccess(rt, code, "failed to burn funds")
	return &adt.EmptyValue{}
}

////////////////////////////////////////////////////////////////////////////////
// Checks
////////////////////////////////////////////////////////////////////////////////

func validateDealCanActivate(rt Runtime, minerAddr addr.Address, sectorExpiration abi.ChainEpoch, deal *OnChainDeal) {
	if deal.Proposal.Provider != minerAddr {
		rt.Abort(exitcode.ErrIllegalArgument, "Deal has incorrect miner as its provider.")
	}

	if deal.SectorStartEpoch != epochUndefined {
		rt.Abort(exitcode.ErrIllegalArgument, "Deal has already appeared in proven sector.")
	}

	if rt.CurrEpoch() > deal.Proposal.StartEpoch {
		rt.Abort(exitcode.ErrIllegalArgument, "Deal start epoch has already elapsed.")
	}

	if deal.Proposal.EndEpoch > sectorExpiration {
		rt.Abort(exitcode.ErrIllegalArgument, "Deal would outlive its containing sector.")
	}
}

func validateDeal(rt Runtime, deal StorageDealProposal) {
	if !dealProposalIsInternallyValid(rt, deal) {
		rt.Abort(exitcode.ErrIllegalArgument, "Invalid deal proposal.")
	}

	if rt.CurrEpoch() > deal.StartEpoch {
		rt.Abort(exitcode.ErrIllegalArgument, "Deal start epoch has already elapsed.")
	}

	inds := rt.CurrIndices()

	minDuration, maxDuration := inds.StorageDeal_DurationBounds(deal.PieceSize, deal.StartEpoch)
	if deal.Duration() < minDuration || deal.Duration() > maxDuration {
		rt.Abort(exitcode.ErrIllegalArgument, "Deal duration out of bounds.")
	}

	minPrice, maxPrice := inds.StorageDeal_StoragePricePerEpochBounds(deal.PieceSize, deal.StartEpoch, deal.EndEpoch)
	if deal.StoragePricePerEpoch.LessThan(minPrice) || deal.StoragePricePerEpoch.GreaterThan(maxPrice) {
		rt.Abort(exitcode.ErrIllegalArgument, "Storage price out of bounds.")
	}

	minProviderCollateral, maxProviderCollateral := inds.StorageDeal_ProviderCollateralBounds(deal.PieceSize, deal.StartEpoch, deal.EndEpoch)
	if deal.ProviderCollateral.LessThan(minProviderCollateral) || deal.ProviderCollateral.GreaterThan(maxProviderCollateral) {
		rt.Abort(exitcode.ErrIllegalArgument, "Provider collateral out of bounds.")
	}

	minClientCollateral, maxClientCollateral := inds.StorageDeal_ClientCollateralBounds(deal.PieceSize, deal.StartEpoch, deal.EndEpoch)
	if deal.ClientCollateral.LessThan(minClientCollateral) || deal.ClientCollateral.GreaterThan(maxClientCollateral) {
		rt.Abort(exitcode.ErrIllegalArgument, "Client collateral out of bounds.")
	}
}
