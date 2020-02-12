package market

import (
	addr "github.com/filecoin-project/go-address"

	cbg "github.com/whyrusleeping/cbor-gen"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	. "github.com/filecoin-project/specs-actors/actors/util"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

type Actor struct{}

type Runtime = vmr.Runtime

func (a Actor) Exports() []interface{} {
	return []interface{}{
		builtin.MethodConstructor: a.Constructor,
		2:                         a.AddBalance,
		3:                         a.WithdrawBalance,
		4:                         a.HandleExpiredDeals,
		5:                         a.PublishStorageDeals,
		6:                         a.VerifyDealsOnSectorProveCommit,
		7:                         a.OnMinerSectorsTerminate,
		8:                         a.ComputeDataCommitment,
		//9: a.GetWeightForDealSet,
	}
}

var _ abi.Invokee = Actor{}

////////////////////////////////////////////////////////////////////////////////
// Actor methods
////////////////////////////////////////////////////////////////////////////////

func (a Actor) Constructor(rt Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)
	st, err := ConstructState(adt.AsStore(rt))
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to create storage market state map: %v", err)
	}
	rt.State().Create(st)
	return &adt.EmptyValue{}
}

type WithdrawBalanceParams struct {
	ProviderOrClientAddress addr.Address
	Amount                  abi.TokenAmount
}

// Attempt to withdraw the specified amount from the balance held in escrow.
// If less than the specified amount is available, yields the entire available balance.
func (a Actor) WithdrawBalance(rt Runtime, params *WithdrawBalanceParams) *adt.EmptyValue {
	amountSlashedTotal := abi.NewTokenAmount(0)

	if params.Amount.LessThan(big.Zero()) {
		rt.Abortf(exitcode.ErrIllegalArgument, "negative amount %v", params.Amount)
	}

	recipientAddr := escrowAddress(rt, params.ProviderOrClientAddress)

	var amountExtracted abi.TokenAmount
	var st State
	rt.State().Transaction(&st, func() interface{} {
		// Before any operations that check the balance tables for funds, execute all deferred
		// deal state updates.
		amountSlashedTotal = big.Add(amountSlashedTotal, st.updatePendingDealStatesForParty(rt, params.ProviderOrClientAddress))

		minBalance := st.getLockedBalance(rt, params.ProviderOrClientAddress)

		et := adt.AsBalanceTable(adt.AsStore(rt), st.EscrowTable)
		ex, err := et.SubtractWithMinimum(params.ProviderOrClientAddress, params.Amount, minBalance)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "subtract form escrow table: %v", err)
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

// Deposits the received value into the balance held in escrow.
func (a Actor) AddBalance(rt Runtime, providerOrClientAdrress *addr.Address) *adt.EmptyValue {
	escrowAddress(rt, *providerOrClientAdrress)

	var st State
	rt.State().Transaction(&st, func() interface{} {
		msgValue := rt.Message().ValueReceived()

		{
			et := adt.AsBalanceTable(adt.AsStore(rt), st.EscrowTable)
			err := et.AddCreate(*providerOrClientAdrress, msgValue)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "adding to escrow table: %v", err)
			}
			st.EscrowTable = et.Root()
		}

		{
			lt := adt.AsBalanceTable(adt.AsStore(rt), st.LockedTable)
			err := lt.AddCreate(*providerOrClientAdrress, big.NewInt(0))
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalArgument, "adding to locked table: %v", err)
			}
			st.LockedTable = lt.Root()
		}

		return nil
	})
	return &adt.EmptyValue{}
}

type PublishStorageDealsParams struct {
	Deals []ClientDealProposal
}

type PublishStorageDealsReturn struct {
	IDs []abi.DealID
}

// Publish a new set of storage deals (not yet included in a sector).
func (a Actor) PublishStorageDeals(rt Runtime, params *PublishStorageDealsParams) *PublishStorageDealsReturn {
	amountSlashedTotal := abi.NewTokenAmount(0)

	// Deal message must have a From field identical to the provider of all the deals.
	// This allows us to retain and verify only the client's signature in each deal proposal itself.
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)

	newDealIds := []abi.DealID{}
	var st State
	rt.State().Transaction(&st, func() interface{} {
		proposals := AsDealProposalArray(adt.AsStore(rt), st.Proposals)
		dbp := AsSetMultimap(adt.AsStore(rt), st.DealIDsByParty)
		// All storage proposals will be added in an atomic transaction; this operation will be unrolled if any of them fails.
		for _, deal := range params.Deals {
			if deal.Proposal.Provider != rt.Message().Caller() {
				rt.Abortf(exitcode.ErrForbidden, "caller is not provider %v", deal.Proposal.Provider)
			}

			validateDeal(rt, deal)

			// Before any operations that check the balance tables for funds, execute all deferred
			// deal state updates.
			//
			// Note: as an optimization, implementations may cache efficient data structures indicating
			// which of the following set of updates are redundant and can be skipped.
			amountSlashedTotal = big.Add(amountSlashedTotal, st.updatePendingDealStatesForParty(rt, deal.Proposal.Client))
			amountSlashedTotal = big.Add(amountSlashedTotal, st.updatePendingDealStatesForParty(rt, deal.Proposal.Provider))

			st.lockBalanceOrAbort(rt, deal.Proposal.Client, deal.Proposal.ClientBalanceRequirement())
			st.lockBalanceOrAbort(rt, deal.Proposal.Provider, deal.Proposal.ProviderBalanceRequirement())

			id := st.generateStorageDealID()

			err := proposals.Set(id, &deal.Proposal)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "set deal: %v", err)
			}

			if err = dbp.Put(deal.Proposal.Client, id); err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "set client deal id: %v", err)
			}
			if err = dbp.Put(deal.Proposal.Provider, id); err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "set provider deal id: %v", err)
			}

			newDealIds = append(newDealIds, id)
		}
		st.Proposals = proposals.Root()
		st.DealIDsByParty = dbp.Root()
		return nil
	})

	_, code := rt.Send(builtin.BurntFundsActorAddr, builtin.MethodSend, nil, amountSlashedTotal)
	builtin.RequireSuccess(rt, code, "failed to burn funds")
	return &PublishStorageDealsReturn{newDealIds}
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
func (a Actor) VerifyDealsOnSectorProveCommit(rt Runtime, params *VerifyDealsOnSectorProveCommitParams) *abi.DealWeight {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.Message().Caller()
	totalWeight := big.Zero()

	var st State
	rt.State().Transaction(&st, func() interface{} {
		// if there are no dealIDs, it is a CommittedCapacity sector
		// and the totalWeight should be zero
		states := AsDealStateArray(adt.AsStore(rt), st.States)
		proposals := AsDealProposalArray(adt.AsStore(rt), st.Proposals)

		for _, dealID := range params.DealIDs {
			deal, err := states.Get(dealID)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "get deal %v", err)
			}
			proposal, err := proposals.Get(dealID)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "get deal %v", err)
			}

			validateDealCanActivate(rt, minerAddr, params.SectorExpiry, deal, proposal)

			deal.SectorStartEpoch = rt.CurrEpoch()
			err = states.Set(dealID, deal)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "set deal %v", err)
			}

			// Compute deal weight
			dur := big.NewInt(int64(proposal.Duration()))
			siz := big.NewInt(int64(proposal.PieceSize))
			weight := big.Mul(dur, siz)
			totalWeight = big.Add(totalWeight, weight)
		}
		st.States = states.Root()

		return nil
	})
	return &totalWeight
}

type ComputeDataCommitmentParams struct {
	DealIDs    []abi.DealID
	SectorSize abi.SectorSize
}

func (a Actor) ComputeDataCommitment(rt Runtime, params *ComputeDataCommitmentParams) *cbg.CborCid {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)

	pieces := make([]abi.PieceInfo, 0)
	var st State
	rt.State().Transaction(&st, func() interface{} {
		for _, dealID := range params.DealIDs {
			deal := st.mustGetDeal(rt, dealID)
			pieces = append(pieces, abi.PieceInfo{
				PieceCID: deal.PieceCID,
				Size:     deal.PieceSize,
			})
		}
		return nil
	})

	commd, err := rt.Syscalls().ComputeUnsealedSectorCID(params.SectorSize, pieces)
	if err != nil {
		rt.Abortf(exitcode.SysErrorIllegalArgument, "failed to compute unsealed sector CID: %s", err)
	}

	return (*cbg.CborCid)(&commd)
}

type OnMinerSectorsTerminateParams struct {
	DealIDs []abi.DealID
}

// Terminate a set of deals in response to their containing sector being terminated.
// Slash provider collateral, refund client collateral, and refund partial unpaid escrow
// amount to client.
func (a Actor) OnMinerSectorsTerminate(rt Runtime, params *OnMinerSectorsTerminateParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.StorageMinerActorCodeID)
	minerAddr := rt.Message().Caller()

	var st State
	rt.State().Transaction(&st, func() interface{} {
		proposals := AsDealProposalArray(adt.AsStore(rt), st.Proposals)
		states := AsDealStateArray(adt.AsStore(rt), st.States)

		for _, dealID := range params.DealIDs {
			deal, err := proposals.Get(dealID)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "get deal: %v", err)
			}
			Assert(deal.Provider == minerAddr)

			state, err := states.Get(dealID)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "get deal: %v", err)
			}

			// Note: we do not perform the balance transfers here, but rather simply record the flag
			// to indicate that processDealSlashed should be called when the deferred state computation
			// is performed. // TODO: Do that here

			state.SlashEpoch = rt.CurrEpoch()
			err = states.Set(dealID, state)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "set deal: %v", err)
			}
		}

		st.States = states.Root()
		return nil
	})
	return &adt.EmptyValue{}
}

type HandleExpiredDealsParams struct {
	Deals []abi.DealID // TODO: RLE
}

func (a Actor) HandleExpiredDeals(rt Runtime, params *HandleExpiredDealsParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	var slashed abi.TokenAmount
	var st State
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

func validateDealCanActivate(rt Runtime, minerAddr addr.Address, sectorExpiration abi.ChainEpoch, deal *DealState, proposal *DealProposal) {
	if proposal.Provider != minerAddr {
		rt.Abortf(exitcode.ErrIllegalArgument, "Deal has incorrect miner as its provider.")
	}

	if deal.SectorStartEpoch != epochUndefined {
		rt.Abortf(exitcode.ErrIllegalArgument, "Deal has already appeared in proven sector.")
	}

	if rt.CurrEpoch() > proposal.StartEpoch {
		rt.Abortf(exitcode.ErrIllegalArgument, "Deal start epoch has already elapsed.")
	}

	if proposal.EndEpoch > sectorExpiration {
		rt.Abortf(exitcode.ErrIllegalArgument, "Deal would outlive its containing sector.")
	}
}

func validateDeal(rt Runtime, deal ClientDealProposal) {
	if !dealProposalIsInternallyValid(rt, deal) {
		rt.Abortf(exitcode.ErrIllegalArgument, "Invalid deal proposal.")
	}

	proposal := deal.Proposal

	if rt.CurrEpoch() > proposal.StartEpoch {
		rt.Abortf(exitcode.ErrIllegalArgument, "Deal start epoch has already elapsed.")
	}

	minDuration, maxDuration := dealDurationBounds(proposal.PieceSize)
	if proposal.Duration() < minDuration || proposal.Duration() > maxDuration {
		rt.Abortf(exitcode.ErrIllegalArgument, "Deal duration out of bounds.")
	}

	minPrice, maxPrice := dealPricePerEpochBounds(proposal.PieceSize, proposal.Duration())
	if proposal.StoragePricePerEpoch.LessThan(minPrice) || proposal.StoragePricePerEpoch.GreaterThan(maxPrice) {
		rt.Abortf(exitcode.ErrIllegalArgument, "Storage price out of bounds.")
	}

	minProviderCollateral, maxProviderCollateral := dealProviderCollateralBounds(proposal.PieceSize, proposal.Duration())
	if proposal.ProviderCollateral.LessThan(minProviderCollateral) || proposal.ProviderCollateral.GreaterThan(maxProviderCollateral) {
		rt.Abortf(exitcode.ErrIllegalArgument, "Provider collateral out of bounds.")
	}

	minClientCollateral, maxClientCollateral := dealClientCollateralBounds(proposal.PieceSize, proposal.Duration())
	if proposal.ClientCollateral.LessThan(minClientCollateral) || proposal.ClientCollateral.GreaterThan(maxClientCollateral) {
		rt.Abortf(exitcode.ErrIllegalArgument, "Client collateral out of bounds.")
	}
}

func escrowAddress(rt Runtime, addr addr.Address) addr.Address {
	codeID, ok := rt.GetActorCodeCID(addr)
	if !ok {
		rt.Abortf(exitcode.ErrIllegalArgument, "no code for address %v", addr)
	}

	if codeID.Equals(builtin.StorageMinerActorCodeID) {
		// Storage miner actor entry; implied funds recipient is the associated owner address.
		ownerAddr, workerAddr := builtin.RequestMinerControlAddrs(rt, addr)
		rt.ValidateImmediateCallerIs(ownerAddr, workerAddr)
		return ownerAddr
	}

	// Ordinary account-style actor entry; funds recipient is just the entry address itself.
	rt.ValidateImmediateCallerType(builtin.CallerTypesSignable...)
	return addr
}
