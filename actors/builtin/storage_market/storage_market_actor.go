package storage_market

import (
	addr "github.com/filecoin-project/go-address"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	autil "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type StorageMarketActor struct{}

type BalanceTableHAMT = autil.BalanceTableHAMT
type Runtime = vmr.Runtime

var Assert = autil.Assert
var IMPL_FINISH = autil.IMPL_FINISH

////////////////////////////////////////////////////////////////////////////////
// Actor methods
////////////////////////////////////////////////////////////////////////////////

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

	recipientAddr := builtin.RT_MinerEntry_ValidateCaller_DetermineFundsLocation(rt, params.Address, builtin.MinerEntrySpec_MinerOrSignable)

	var amountExtracted abi.TokenAmount
	var st StorageMarketActorState
	rt.State().Transaction(&st, func() interface{} {
		st._rtAbortIfAddressEntryDoesNotExist(rt, params.Address)

		// Before any operations that check the balance tables for funds, execute all deferred
		// deal state updates.
		//
		// Note: as an optimization, implementations may cache efficient data structures indicating
		// which of the following set of updates are redundant and can be skipped.
		amountSlashedTotal = big.Add(amountSlashedTotal, st._rtUpdatePendingDealStatesForParty(rt, params.Address))

		minBalance := st._getLockedReqBalanceInternal(params.Address)
		newTable, ex, ok := autil.BalanceTable_WithExtractPartial(st.EscrowTable, params.Address, params.Amount, minBalance)
		Assert(ok)
		st.EscrowTable = newTable
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
	builtin.RT_MinerEntry_ValidateCaller_DetermineFundsLocation(rt, *address, builtin.MinerEntrySpec_MinerOrSignable)

	var st StorageMarketActorState
	rt.State().Transaction(&st, func() interface{} {
		st._rtAbortIfAddressEntryDoesNotExist(rt, *address)

		msgValue := rt.ValueReceived()
		newTable, ok := autil.BalanceTable_WithAdd(st.EscrowTable, *address, msgValue)
		if !ok {
			// Entry not found; create implicitly.
			newTable, ok = autil.BalanceTable_WithNewAddressEntry(st.EscrowTable, *address, msgValue)
			Assert(ok)
		}
		st.EscrowTable = newTable
		return nil
	})
	return &adt.EmptyValue{}
}

type PublishStorageDealsParams struct {
	Deals []StorageDeal
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
		for _, newDeal := range params.Deals {
			p := newDeal.Proposal

			if p.Provider != rt.ImmediateCaller() {
				rt.Abort(exitcode.ErrForbidden, "caller is not provider %v", p.Provider)
			}

			_rtAbortIfNewDealInvalid(rt, newDeal)

			// Before any operations that check the balance tables for funds, execute all deferred
			// deal state updates.
			//
			// Note: as an optimization, implementations may cache efficient data structures indicating
			// which of the following set of updates are redundant and can be skipped.
			amountSlashedTotal = big.Add(amountSlashedTotal, st._rtUpdatePendingDealStatesForParty(rt, p.Client))
			amountSlashedTotal = big.Add(amountSlashedTotal, st._rtUpdatePendingDealStatesForParty(rt, p.Provider))

			st._rtLockBalanceOrAbort(rt, p.Client, p.ClientBalanceRequirement())
			st._rtLockBalanceOrAbort(rt, p.Provider, p.ProviderBalanceRequirement())

			id := st._generateStorageDealID()

			onchainDeal := OnChainDeal{
				ID:               id,
				Deal:             newDeal,
				SectorStartEpoch: epochUndefined,
			}

			st.Deals[id] = onchainDeal

			if _, found := st.CachedExpirationsPending[p.EndEpoch]; !found {
				st.CachedExpirationsPending[p.EndEpoch] = NewDealIDQueue()
			}
			cep := st.CachedExpirationsPending[p.EndEpoch]
			cep.Enqueue(id)
		}

		st.CurrEpochNumDealsPublished += len(params.Deals)
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
			deal, dealP := st._rtGetOnChainDealOrAbort(rt, dealID)
			_rtAbortIfDealInvalidForNewSectorSeal(rt, minerAddr, params.SectorExpiry, deal)
			ocd := st.Deals[dealID]
			ocd.SectorStartEpoch = rt.CurrEpoch()
			st.Deals[dealID] = ocd

			// Compute deal weight
			dur := big.NewInt(int64(dealP.Duration()))
			siz := big.NewInt(dealP.PieceSize.Total())
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

	ret := []abi.PieceInfo{}
	var st StorageMarketActorState
	rt.State().Transaction(&st, func() interface{} {
		for _, dealID := range params.DealIDs {
			_, dealP := st._rtGetOnChainDealOrAbort(rt, dealID)
			ret = append(ret, abi.PieceInfo{
				PieceCID: dealP.PieceCID,
				Size:     dealP.PieceSize.Total(),
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
		for _, dealID := range params.DealIDs {
			_, dealP := st._rtGetOnChainDealOrAbort(rt, dealID)
			Assert(dealP.Provider == minerAddr)

			// Note: we do not perform the balance transfers here, but rather simply record the flag
			// to indicate that _processDealSlashed should be called when the deferred state computation
			// is performed.
			ocd := st.Deals[dealID]
			ocd.SlashEpoch = rt.CurrEpoch()
			st.Deals[dealID] = ocd
		}
		return nil
	})
	return &adt.EmptyValue{}
}

func (a *StorageMarketActor) OnEpochTickEnd(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.CronActorAddr)
	var amountSlashedTotal abi.TokenAmount
	var st StorageMarketActorState
	rt.State().Transaction(&st, func() interface{} {
		// Some deals may never be affected by the normal calls to _rtUpdatePendingDealStatesForParty
		// (notably, if the relevant party never checks its balance).
		// Without some cleanup mechanism, these deals may gradually accumulate and cause
		// the StorageMarketActor state to grow without bound.
		// To prevent this, we amortize the cost of this cleanup by processing a relatively
		// small number of deals every epoch, independent of the calls above.
		//
		// More specifically, we process deals:
		//   (a) In priority order of expiration epoch, up until the current epoch
		//   (b) Within a given expiration epoch, in order of original publishing.
		//
		// We stop once we have exhausted this valid set, or when we have hit a certain target
		// (DEAL_PROC_AMORTIZED_SCALE_FACTOR times the number of deals freshly published in the
		// current epoch) of deals dequeued, whichever comes first.

		const DEAL_PROC_AMORTIZED_SCALE_FACTOR = 2
		numDequeuedTarget := st.CurrEpochNumDealsPublished * DEAL_PROC_AMORTIZED_SCALE_FACTOR

		numDequeued := 0
		extractedDealIDs := []abi.DealID{}

		for {
			if st.CachedExpirationsNextProcEpoch > rt.CurrEpoch() {
				break
			}

			if numDequeued >= numDequeuedTarget {
				break
			}

			queue, found := st.CachedExpirationsPending[st.CachedExpirationsNextProcEpoch]
			if !found {
				st.CachedExpirationsNextProcEpoch += 1
				continue
			}

			queueDepleted := false
			for {
				dealID, ok := queue.Dequeue()
				if !ok {
					queueDepleted = true
					break
				}
				numDequeued += 1
				if _, found = st.Deals[dealID]; found {
					// May have already processed expiration, independently, via _rtUpdatePendingDealStatesForParty.
					// If not, add it to the list to be processed.
					extractedDealIDs = append(extractedDealIDs, dealID)
				}
			}

			if !queueDepleted {
				Assert(numDequeued >= numDequeuedTarget)
				break
			}

			delete(st.CachedExpirationsPending, st.CachedExpirationsNextProcEpoch)
			st.CachedExpirationsNextProcEpoch += 1
		}

		amountSlashedTotal = st._updatePendingDealStates(extractedDealIDs, rt.CurrEpoch())

		// Reset for next epoch.
		st.CurrEpochNumDealsPublished = 0
		return nil
	})

	_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, amountSlashedTotal)
	builtin.RequireSuccess(rt, code, "failed to burn funds")
	return &adt.EmptyValue{}
}

func (a *StorageMarketActor) Constructor(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	var st StorageMarketActorState
	rt.State().Transaction(&st, func() interface{} {
		st.Deals = DealsAMT_Empty()
		st.EscrowTable = autil.BalanceTableHAMT_Empty()
		st.LockedReqTable = autil.BalanceTableHAMT_Empty()
		st.NextID = abi.DealID(0)
		st.CachedDealIDsByParty = CachedDealIDsByPartyHAMT_Empty()
		st.CachedExpirationsPending = CachedExpirationsPendingHAMT_Empty()
		st.CachedExpirationsNextProcEpoch = abi.ChainEpoch(0)
		return nil
	})
	return &adt.EmptyValue{}
}

////////////////////////////////////////////////////////////////////////////////
// Method utility functions
////////////////////////////////////////////////////////////////////////////////

func _rtAbortIfDealAlreadyProven(rt Runtime, deal OnChainDeal) {
	if deal.SectorStartEpoch != epochUndefined {
		rt.Abort(exitcode.ErrIllegalArgument, "Deal has already appeared in proven sector.")
	}
}

func _rtAbortIfDealNotFromProvider(rt Runtime, dealP StorageDealProposal, minerAddr addr.Address) {
	if dealP.Provider != minerAddr {
		rt.Abort(exitcode.ErrIllegalArgument, "Deal has incorrect miner as its provider.")
	}
}

func _rtAbortIfDealStartElapsed(rt Runtime, dealP StorageDealProposal) {
	if rt.CurrEpoch() > dealP.StartEpoch {
		rt.Abort(exitcode.ErrIllegalArgument, "Deal start epoch has already elapsed.")
	}
}

func _rtAbortIfDealEndElapsed(rt Runtime, dealP StorageDealProposal) {
	if dealP.EndEpoch > rt.CurrEpoch() {
		rt.Abort(exitcode.ErrIllegalArgument, "Deal end epoch has already elapsed.")
	}
}

func _rtAbortIfDealExceedsSectorLifetime(rt Runtime, dealP StorageDealProposal, sectorExpiration abi.ChainEpoch) {
	if dealP.EndEpoch > sectorExpiration {
		rt.Abort(exitcode.ErrIllegalArgument, "Deal would outlive its containing sector.")
	}
}

func _rtAbortIfDealInvalidForNewSectorSeal(
	rt Runtime, minerAddr addr.Address, sectorExpiration abi.ChainEpoch, deal OnChainDeal) {

	dealP := deal.Deal.Proposal

	_rtAbortIfDealNotFromProvider(rt, dealP, minerAddr)
	_rtAbortIfDealAlreadyProven(rt, deal)
	_rtAbortIfDealStartElapsed(rt, dealP)
	_rtAbortIfDealExceedsSectorLifetime(rt, dealP, sectorExpiration)
}

func _rtAbortIfNewDealInvalid(rt Runtime, deal StorageDeal) {
	dealP := deal.Proposal

	if !_rtDealProposalIsInternallyValid(rt, dealP) {
		rt.Abort(exitcode.ErrIllegalArgument, "Invalid deal proposal.")
	}

	_rtAbortIfDealStartElapsed(rt, dealP)
	_rtAbortIfDealFailsParamBounds(rt, dealP)
}

func _rtAbortIfDealFailsParamBounds(rt Runtime, dealP StorageDealProposal) {
	inds := rt.CurrIndices()

	minDuration, maxDuration := inds.StorageDeal_DurationBounds(dealP.PieceSize, dealP.StartEpoch)
	if dealP.Duration() < minDuration || dealP.Duration() > maxDuration {
		rt.Abort(exitcode.ErrIllegalArgument, "Deal duration out of bounds.")
	}

	minPrice, maxPrice := inds.StorageDeal_StoragePricePerEpochBounds(dealP.PieceSize, dealP.StartEpoch, dealP.EndEpoch)
	if dealP.StoragePricePerEpoch.LessThan(minPrice) || dealP.StoragePricePerEpoch.GreaterThan(maxPrice) {
		rt.Abort(exitcode.ErrIllegalArgument, "Storage price out of bounds.")
	}

	minProviderCollateral, maxProviderCollateral := inds.StorageDeal_ProviderCollateralBounds(
		dealP.PieceSize, dealP.StartEpoch, dealP.EndEpoch)
	if dealP.ProviderCollateral.LessThan(minProviderCollateral) || dealP.ProviderCollateral.GreaterThan(maxProviderCollateral) {
		rt.Abort(exitcode.ErrIllegalArgument, "Provider collateral out of bounds.")
	}

	minClientCollateral, maxClientCollateral := inds.StorageDeal_ClientCollateralBounds(
		dealP.PieceSize, dealP.StartEpoch, dealP.EndEpoch)
	if dealP.ClientCollateral.LessThan(minClientCollateral) || dealP.ClientCollateral.GreaterThan(maxClientCollateral) {
		rt.Abort(exitcode.ErrIllegalArgument, "Client collateral out of bounds.")
	}
}
