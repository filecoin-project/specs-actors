package market

import (
	addr "github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	. "github.com/filecoin-project/specs-actors/actors/util"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
)

const epochUndefined = abi.ChainEpoch(-1)

// Market mutations
// add / rm balance
// pub deal (always provider)
// activate deal (miner)
// end deal (miner terminate, expire(no activation))

type State struct {
	Proposals cid.Cid // AMT[DealID]DealProposal
	States    cid.Cid // AMT[DealID]DealState

	// Total amount held in escrow, indexed by actor address (including both locked and unlocked amounts).
	EscrowTable cid.Cid // BalanceTable

	// Amount locked, indexed by actor address.
	// Note: the amounts in this table do not affect the overall amount in escrow:
	// only the _portion_ of the total escrow amount that is locked.
	LockedTable cid.Cid // BalanceTable

	NextID abi.DealID

	// Metadata cached for efficient iteration over deals.
	DealIDsByParty cid.Cid // SetMultimap, HAMT[addr]Set
}

func ConstructState(store adt.Store) (*State, error) {
	emptyArray, err := adt.MakeEmptyArray(store)
	if err != nil {
		return nil, err
	}

	emptyMap, err := adt.MakeEmptyMap(store)
	if err != nil {
		return nil, err
	}

	emptyMSet, err := MakeEmptySetMultimap(store)
	if err != nil {
		return nil, err
	}

	return &State{
		Proposals:      emptyArray.Root(),
		States:         emptyArray.Root(),
		EscrowTable:    emptyMap.Root(),
		LockedTable:    emptyMap.Root(),
		NextID:         abi.DealID(0),
		DealIDsByParty: emptyMSet.Root(),
	}, nil
}

////////////////////////////////////////////////////////////////////////////////
// Deal state operations
////////////////////////////////////////////////////////////////////////////////

func (st *State) updatePendingDealStatesForParty(rt Runtime, addr addr.Address) (amountSlashedTotal abi.TokenAmount) {
	// For consistency with HandleExpiredDeals, only process updates up to the end of the _previous_ epoch.
	epoch := rt.CurrEpoch() - 1

	dbp := AsSetMultimap(adt.AsStore(rt), st.DealIDsByParty)
	var extractedDealIDs []abi.DealID
	err := dbp.ForEach(addr, func(id abi.DealID) error {
		extractedDealIDs = append(extractedDealIDs, id)
		return nil
	})
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "foreach error %v", err)
	}

	amountSlashedTotal = st.updatePendingDealStates(rt, extractedDealIDs, epoch)
	return
}

func (st *State) updatePendingDealStates(rt Runtime, dealIDs []abi.DealID, epoch abi.ChainEpoch) abi.TokenAmount {
	amountSlashedTotal := abi.NewTokenAmount(0)

	for _, dealID := range dealIDs {
		amountSlashedTotal = big.Add(amountSlashedTotal, st.updatePendingDealState(rt, dealID, epoch))
	}

	return amountSlashedTotal
}

// TODO: This does waaaay too many redundant hamt reads
func (st *State) updatePendingDealState(rt Runtime, dealID abi.DealID, epoch abi.ChainEpoch) (amountSlashed abi.TokenAmount) {
	amountSlashed = abi.NewTokenAmount(0)

	deal := st.mustGetDeal(rt, dealID)
	state := st.mustGetDealState(rt, dealID)

	everUpdated := state.LastUpdatedEpoch != epochUndefined
	everSlashed := state.SlashEpoch != epochUndefined

	Assert(!everUpdated || (state.LastUpdatedEpoch <= epoch)) // if the deal was ever updated, make sure it didn't happen in the future

	if state.LastUpdatedEpoch == epoch { // TODO: This looks fishy, check all places that set LastUpdatedEpoch
		return
	}

	if state.SectorStartEpoch == epochUndefined {
		// Not yet appeared in proven sector; check for timeout.
		if deal.StartEpoch >= epoch {
			return st.processDealInitTimedOut(rt, dealID)
		}
		return
	}

	Assert(deal.StartEpoch <= epoch)

	dealEnd := deal.EndEpoch
	if everSlashed {
		Assert(state.SlashEpoch <= dealEnd)
		dealEnd = state.SlashEpoch
	}

	elapsedStart := deal.StartEpoch
	if everUpdated && state.LastUpdatedEpoch > elapsedStart {
		elapsedStart = state.LastUpdatedEpoch
	}

	elapsedEnd := dealEnd
	if epoch < elapsedEnd {
		elapsedEnd = epoch
	}

	numEpochsElapsed := elapsedEnd - elapsedStart

	{
		// Process deal payment for the elapsed epochs.
		totalPayment := big.Mul(big.NewInt(int64(numEpochsElapsed)), deal.StoragePricePerEpoch)
		st.transferBalance(rt, deal.Client, deal.Provider, totalPayment)
	}

	if everSlashed {
		// unlock client collateral and locked storage fee
		clientCollateral := deal.ClientCollateral
		paymentRemaining := dealGetPaymentRemaining(deal, state.SlashEpoch)
		st.unlockBalance(rt, deal.Client, big.Add(clientCollateral, paymentRemaining))

		// slash provider collateral
		amountSlashed = deal.ProviderCollateral
		st.slashBalance(rt, deal.Provider, amountSlashed)

		st.deleteDeal(rt, dealID)
		return
	}

	if epoch >= deal.EndEpoch {
		st.processDealExpired(rt, dealID)
		return
	}

	state.LastUpdatedEpoch = epoch

	states := AsDealStateArray(adt.AsStore(rt), st.States)
	if err := states.Set(dealID, state); err != nil {
		rt.Abortf(exitcode.ErrPlaceholder, "failed to get deal: %v", err)
	}
	st.States = states.Root()
	return
}

func (st *State) deleteDeal(rt Runtime, dealID abi.DealID) {
	dealP := st.mustGetDeal(rt, dealID)

	proposals := AsDealProposalArray(adt.AsStore(rt), st.Proposals)
	if err := proposals.Delete(uint64(dealID)); err != nil {
		rt.Abortf(exitcode.ErrPlaceholder, "failed to delete deal: %v", err)
	}
	st.Proposals = proposals.Root()

	dbp := AsSetMultimap(adt.AsStore(rt), st.DealIDsByParty)
	if err := dbp.Remove(dealP.Client, dealID); err != nil {
		rt.Abortf(exitcode.ErrPlaceholder, "failed to delete deal from DealIDsByParty: %v", err)
	}
	st.DealIDsByParty = dbp.Root()
}

// Deal start deadline elapsed without appearing in a proven sector.
// Delete deal, slash a portion of provider's collateral, and unlock remaining collaterals
// for both provider and client.
func (st *State) processDealInitTimedOut(rt Runtime, dealID abi.DealID) (amountSlashed abi.TokenAmount) {
	deal := st.mustGetDeal(rt, dealID)
	state := st.mustGetDealState(rt, dealID)

	Assert(state.SectorStartEpoch == epochUndefined)

	st.unlockBalance(rt, deal.Client, deal.ClientBalanceRequirement())

	amountSlashed = collateralPenaltyForDealActivationMissed(deal.ProviderCollateral)
	amountRemaining := big.Sub(deal.ProviderBalanceRequirement(), amountSlashed)

	st.slashBalance(rt, deal.Provider, amountSlashed)
	st.unlockBalance(rt, deal.Provider, amountRemaining)

	st.deleteDeal(rt, dealID)
	return
}

// Normal expiration. Delete deal and unlock collaterals for both miner and client.
func (st *State) processDealExpired(rt Runtime, dealID abi.DealID) {
	deal := st.mustGetDeal(rt, dealID)
	state := st.mustGetDealState(rt, dealID)

	Assert(state.SectorStartEpoch != epochUndefined)

	// Note: payment has already been completed at this point (_rtProcessDealPaymentEpochsElapsed)
	st.unlockBalance(rt, deal.Provider, deal.ProviderCollateral)
	st.unlockBalance(rt, deal.Client, deal.ClientCollateral)

	st.deleteDeal(rt, dealID)
}

func (st *State) generateStorageDealID() abi.DealID {
	ret := st.NextID
	st.NextID = st.NextID + abi.DealID(1)
	return ret
}

////////////////////////////////////////////////////////////////////////////////
// Balance table operations
////////////////////////////////////////////////////////////////////////////////

func (st *State) getEscrowBalance(rt Runtime, a addr.Address) abi.TokenAmount {
	ret, err := adt.AsBalanceTable(adt.AsStore(rt), st.EscrowTable).Get(a)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "get escrow balance: %v", err)
	}
	return ret
}

func (st *State) getLockedBalance(rt Runtime, a addr.Address) abi.TokenAmount {
	ret, err := adt.AsBalanceTable(adt.AsStore(rt), st.LockedTable).Get(a)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "get locked balance: %v", err)
	}
	return ret
}

func (st *State) maybeLockBalance(rt Runtime, addr addr.Address, amount abi.TokenAmount) (lockBalanceOK bool) {
	Assert(amount.GreaterThanEqual(big.Zero()))

	prevLocked := st.getLockedBalance(rt, addr)
	if big.Add(prevLocked, amount).GreaterThan(st.getEscrowBalance(rt, addr)) {
		lockBalanceOK = false
		return
	}

	lt := adt.AsBalanceTable(adt.AsStore(rt), st.LockedTable)
	err := lt.Add(addr, amount)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "adding locked balance: %v", err)
	}

	st.LockedTable = lt.Root()

	lockBalanceOK = true
	return
}

func (st *State) unlockBalance(rt Runtime, addr addr.Address, amount abi.TokenAmount) {
	Assert(amount.GreaterThanEqual(big.Zero()))

	lt := adt.AsBalanceTable(adt.AsStore(rt), st.LockedTable)
	err := lt.MustSubtract(addr, amount)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "subtracting from locked balance: %v", err)
	}
	st.LockedTable = lt.Root()
}

// move funds from locked in client to available in provider
func (st *State) transferBalance(rt Runtime, fromAddr addr.Address, toAddr addr.Address, amount abi.TokenAmount) {
	Assert(amount.GreaterThanEqual(big.Zero()))

	et := adt.AsBalanceTable(adt.AsStore(rt), st.EscrowTable)
	lt := adt.AsBalanceTable(adt.AsStore(rt), st.LockedTable)

	err := et.MustSubtract(fromAddr, amount)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "subtract from escrow: %v", err)
	}

	err = lt.MustSubtract(fromAddr, amount)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "subtract from locked: %v", err)
	}

	err = et.Add(toAddr, amount)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "add to escrow: %v", err)
	}

	st.LockedTable = lt.Root()
	st.EscrowTable = et.Root()
}

func (st *State) slashBalance(rt Runtime, addr addr.Address, amount abi.TokenAmount) {
	Assert(amount.GreaterThanEqual(big.Zero()))

	et := adt.AsBalanceTable(adt.AsStore(rt), st.EscrowTable)
	lt := adt.AsBalanceTable(adt.AsStore(rt), st.LockedTable)

	err := et.MustSubtract(addr, amount)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "subtract from escrow: %v", err)
	}
	err = lt.MustSubtract(addr, amount)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "subtract from locked: %v", err)
	}

	st.LockedTable = lt.Root()
	st.EscrowTable = et.Root()
}

////////////////////////////////////////////////////////////////////////////////
// Method utility functions
////////////////////////////////////////////////////////////////////////////////

func (st *State) mustGetDeal(rt Runtime, dealID abi.DealID) *DealProposal {
	proposals := AsDealProposalArray(adt.AsStore(rt), st.Proposals)
	proposal, err := proposals.Get(dealID)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "get proposal: %v", err)
	}

	return proposal
}

func (st *State) mustGetDealState(rt Runtime, dealID abi.DealID) *DealState {
	states := AsDealStateArray(adt.AsStore(rt), st.States)
	state, err := states.Get(dealID)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "get state state: %v", err)
	}

	return state
}

func (st *State) lockBalanceOrAbort(rt Runtime, addr addr.Address, amount abi.TokenAmount) {
	if amount.LessThan(big.Zero()) {
		rt.Abortf(exitcode.ErrIllegalArgument, "negative amount %v", amount)
	}

	if !st.maybeLockBalance(rt, addr, amount) {
		rt.Abortf(exitcode.ErrInsufficientFunds, "Insufficient funds available to lock")
	}
}

////////////////////////////////////////////////////////////////////////////////
// State utility functions
////////////////////////////////////////////////////////////////////////////////

func dealProposalIsInternallyValid(rt Runtime, proposal ClientDealProposal) bool {
	if proposal.Proposal.EndEpoch <= proposal.Proposal.StartEpoch {
		return false
	}

	if proposal.Proposal.Duration() != proposal.Proposal.EndEpoch-proposal.Proposal.StartEpoch {
		return false
	}

	TODO()
	// Determine which subset of DealProposal to use as the message to be signed by the client.
	var m []byte

	// Note: we do not verify the provider signature here, since this is implicit in the
	// authenticity of the on-chain message publishing the deal.
	sigVerified := rt.Syscalls().VerifySignature(proposal.ClientSignature, proposal.Proposal.Client, m)
	if !sigVerified {
		return false
	}

	return true
}

func dealGetPaymentRemaining(deal *DealProposal, epoch abi.ChainEpoch) abi.TokenAmount {
	Assert(epoch <= deal.EndEpoch)

	durationRemaining := deal.EndEpoch - (epoch - 1)
	Assert(durationRemaining > 0)

	return big.Mul(big.NewInt(int64(durationRemaining)), deal.StoragePricePerEpoch)
}
