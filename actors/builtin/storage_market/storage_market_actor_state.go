package storage_market

import (
	"fmt"
	"strconv"

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

type DealSet struct{}

type PartyDeals struct {
	Deals map[string]DealSet
}

type StorageMarketActorState struct {
	Deals cid.Cid // AMT[]OnChainDeal

	// Total amount held in escrow, indexed by actor address (including both locked and unlocked amounts).
	EscrowTable cid.Cid // BalanceTable

	// Amount locked, indexed by actor address.
	// Note: the amounts in this table do not affect the overall amount in escrow:
	// only the _portion_ of the total escrow amount that is locked.
	LockedTable cid.Cid // BalanceTable

	NextID abi.DealID

	// Metadata cached for efficient iteration over deals.
	DealIDsByParty map[string]PartyDeals // TODO: figure out a way to drop this
}

func ConstructState(store adt.Store) (*StorageMarketActorState, error) {
	emptyArray, err := adt.MakeEmptyArray(store)
	if err != nil {
		return nil, err
	}

	return &StorageMarketActorState{
		Deals:          emptyArray.Root(),
		EscrowTable:    emptyArray.Root(),
		LockedTable:    emptyArray.Root(),
		NextID:         abi.DealID(0),
		DealIDsByParty: map[string]PartyDeals{},
	}, nil
}

////////////////////////////////////////////////////////////////////////////////
// Deal state operations
////////////////////////////////////////////////////////////////////////////////

func (st *StorageMarketActorState) updatePendingDealStatesForParty(rt Runtime, addr addr.Address) (amountSlashedTotal abi.TokenAmount) {
	// For consistency with HandleExpiredDeals, only process updates up to the end of the _previous_ epoch.
	epoch := rt.CurrEpoch() - 1

	deals, ok := st.DealIDsByParty[addr.String()]
	Assert(ok)
	var extractedDealIDs []abi.DealID
	for cachedDealID := range deals.Deals {
		id, err := strconv.ParseUint(cachedDealID, 10, 64)
		AssertNoError(err)
		extractedDealIDs = append(extractedDealIDs, abi.DealID(id))
	}

	amountSlashedTotal = st.updatePendingDealStates(rt, extractedDealIDs, epoch)
	return
}

func (st *StorageMarketActorState) updatePendingDealStates(rt Runtime, dealIDs []abi.DealID, epoch abi.ChainEpoch) abi.TokenAmount {
	amountSlashedTotal := abi.NewTokenAmount(0)

	for _, dealID := range dealIDs {
		amountSlashedTotal = big.Add(amountSlashedTotal, st.updatePendingDealState(rt, dealID, epoch))
	}

	return amountSlashedTotal
}

// TODO: This does waaaay too many redundant hamt reads
func (st *StorageMarketActorState) updatePendingDealState(rt Runtime, dealID abi.DealID, epoch abi.ChainEpoch) (amountSlashed abi.TokenAmount) {
	amountSlashed = abi.NewTokenAmount(0)

	deal := st.mustGetDeal(rt, dealID)
	dealP := deal.Proposal

	everUpdated := deal.LastUpdatedEpoch != epochUndefined
	everSlashed := deal.SlashEpoch != epochUndefined

	Assert(!everUpdated || (deal.LastUpdatedEpoch <= epoch)) // if the deal was ever updated, make sure it didn't happen in the future

	if deal.LastUpdatedEpoch == epoch { // TODO: This looks fishy, check all places that set LastUpdatedEpoch
		return
	}

	if deal.SectorStartEpoch == epochUndefined {
		// Not yet appeared in proven sector; check for timeout.
		if dealP.StartEpoch >= epoch {
			return st.processDealInitTimedOut(rt, dealID)
		}
		return
	}

	Assert(dealP.StartEpoch <= epoch)

	dealEnd := dealP.EndEpoch
	if everSlashed {
		Assert(deal.SlashEpoch <= dealEnd)
		dealEnd = deal.SlashEpoch
	}

	elapsedStart := dealP.StartEpoch
	if everUpdated && deal.LastUpdatedEpoch > elapsedStart {
		elapsedStart = deal.LastUpdatedEpoch
	}

	elapsedEnd := dealEnd
	if epoch < elapsedEnd {
		elapsedEnd = epoch
	}

	numEpochsElapsed := elapsedEnd - elapsedStart
	st.processDealPaymentEpochsElapsed(rt, dealID, numEpochsElapsed)

	if everSlashed {
		amountSlashed = st.processDealSlashed(rt, dealID)
		return
	}

	if epoch >= dealP.EndEpoch {
		st.processDealExpired(rt, dealID)
		return
	}

	var ocd *OnChainDeal
	deals := AsDealArray(adt.AsStore(rt), st.Deals)
	ocd, err := deals.Get(dealID)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "failed to get deal: %v", err)
	}
	ocd.LastUpdatedEpoch = epoch
	if err := deals.Set(dealID, ocd); err != nil {
		rt.Abort(exitcode.ErrPlaceholder, "failed to get deal: %v", err)
	}
	st.Deals = deals.Root()
	return
}

func (st *StorageMarketActorState) deleteDeal(rt Runtime, dealID abi.DealID) {
	dealP := st.mustGetDeal(rt, dealID).Proposal

	deals := AsDealArray(adt.AsStore(rt), st.Deals)
	if err := deals.Delete(uint64(dealID)); err != nil {
		rt.Abort(exitcode.ErrPlaceholder, "failed to delete deal: %v", err)
	}

	delete(st.DealIDsByParty[dealP.Client.String()].Deals, fmt.Sprint(dealID))
}

// Note: only processes deal payments, not deal expiration (even if the deal has expired).
func (st *StorageMarketActorState) processDealPaymentEpochsElapsed(rt Runtime, dealID abi.DealID, numEpochsElapsed abi.ChainEpoch) {
	deal := st.mustGetDeal(rt, dealID)

	Assert(deal.SectorStartEpoch != epochUndefined)

	// Process deal payment for the elapsed epochs.
	totalPayment := big.Mul(big.NewInt(int64(numEpochsElapsed)), deal.Proposal.StoragePricePerEpoch)
	st.transferBalance(rt, deal.Proposal.Client, deal.Proposal.Provider, totalPayment)
}

func (st *StorageMarketActorState) processDealSlashed(rt Runtime, dealID abi.DealID) (amountSlashed abi.TokenAmount) {
	deal := st.mustGetDeal(rt, dealID)

	Assert(deal.SectorStartEpoch != epochUndefined)

	slashEpoch := deal.SlashEpoch
	Assert(slashEpoch != epochUndefined)

	// unlock client collateral and locked storage fee
	clientCollateral := deal.Proposal.ClientCollateral
	paymentRemaining := dealGetPaymentRemaining(deal, slashEpoch)
	st.unlockBalance(rt, deal.Proposal.Client, big.Add(clientCollateral, paymentRemaining))

	// slash provider collateral
	amountSlashed = deal.Proposal.ProviderCollateral
	st.slashBalance(rt, deal.Proposal.Provider, amountSlashed)

	st.deleteDeal(rt, dealID)
	return
}

// Deal start deadline elapsed without appearing in a proven sector.
// Delete deal, slash a portion of provider's collateral, and unlock remaining collaterals
// for both provider and client.
func (st *StorageMarketActorState) processDealInitTimedOut(rt Runtime, dealID abi.DealID) (amountSlashed abi.TokenAmount) {
	deal := st.mustGetDeal(rt, dealID)

	Assert(deal.SectorStartEpoch == epochUndefined)

	st.unlockBalance(rt, deal.Proposal.Client, deal.Proposal.ClientBalanceRequirement())

	amountSlashed = collateralPenaltyForDealActivationMissed(deal.Proposal.ProviderCollateral)
	amountRemaining := big.Sub(deal.Proposal.ProviderBalanceRequirement(), amountSlashed)

	st.slashBalance(rt, deal.Proposal.Provider, amountSlashed)
	st.unlockBalance(rt, deal.Proposal.Provider, amountRemaining)

	st.deleteDeal(rt, dealID)
	return
}

// Normal expiration. Delete deal and unlock collaterals for both miner and client.
func (st *StorageMarketActorState) processDealExpired(rt Runtime, dealID abi.DealID) {
	deal := st.mustGetDeal(rt, dealID)

	Assert(deal.SectorStartEpoch != epochUndefined)

	// Note: payment has already been completed at this point (_rtProcessDealPaymentEpochsElapsed)
	st.unlockBalance(rt, deal.Proposal.Provider, deal.Proposal.ProviderCollateral)
	st.unlockBalance(rt, deal.Proposal.Client, deal.Proposal.ClientCollateral)

	st.deleteDeal(rt, dealID)
}

func (st *StorageMarketActorState) generateStorageDealID() abi.DealID {
	ret := st.NextID
	st.NextID = st.NextID + abi.DealID(1)
	return ret
}

////////////////////////////////////////////////////////////////////////////////
// Balance table operations
////////////////////////////////////////////////////////////////////////////////

func (st *StorageMarketActorState) getEscrowBalance(rt Runtime, a addr.Address) abi.TokenAmount {
	ret, err := adt.AsBalanceTable(adt.AsStore(rt), st.EscrowTable).Get(a)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "get escrow balance: %v", err)
	}
	return ret
}

func (st *StorageMarketActorState) getLockedBalance(rt Runtime, a addr.Address) abi.TokenAmount {
	ret, err := adt.AsBalanceTable(adt.AsStore(rt), st.LockedTable).Get(a)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "get locked balance: %v", err)
	}
	return ret
}

func (st *StorageMarketActorState) maybeLockBalance(rt Runtime, addr addr.Address, amount abi.TokenAmount) (lockBalanceOK bool) {
	Assert(amount.GreaterThanEqual(big.Zero()))

	prevLocked := st.getLockedBalance(rt, addr)
	if big.Add(prevLocked, amount).GreaterThan(st.getEscrowBalance(rt, addr)) {
		lockBalanceOK = false
		return
	}

	lt := adt.AsBalanceTable(adt.AsStore(rt), st.LockedTable)
	err := lt.Add(addr, amount)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "adding locked balance: %v", err)
	}

	st.LockedTable = lt.Root()

	lockBalanceOK = true
	return
}

func (st *StorageMarketActorState) unlockBalance(rt Runtime, addr addr.Address, amount abi.TokenAmount) {
	Assert(amount.GreaterThanEqual(big.Zero()))

	lt := adt.AsBalanceTable(adt.AsStore(rt), st.LockedTable)
	err := lt.MustSubtract(addr, amount)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "subtracting from locked balance: %v", err)
	}
	st.LockedTable = lt.Root()
}

// move funds from locked in client to available in provider
func (st *StorageMarketActorState) transferBalance(rt Runtime, fromAddr addr.Address, toAddr addr.Address, amount abi.TokenAmount) {
	Assert(amount.GreaterThanEqual(big.Zero()))

	et := adt.AsBalanceTable(adt.AsStore(rt), st.EscrowTable)
	lt := adt.AsBalanceTable(adt.AsStore(rt), st.LockedTable)

	err := et.MustSubtract(fromAddr, amount)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "subtract from escrow: %v", err)
	}

	err = lt.MustSubtract(fromAddr, amount)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "subtract from locked: %v", err)
	}

	err = et.Add(toAddr, amount)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "add to escrow: %v", err)
	}

	st.LockedTable = lt.Root()
	st.EscrowTable = et.Root()
}

func (st *StorageMarketActorState) slashBalance(rt Runtime, addr addr.Address, amount abi.TokenAmount) {
	Assert(amount.GreaterThanEqual(big.Zero()))

	et := adt.AsBalanceTable(adt.AsStore(rt), st.EscrowTable)
	lt := adt.AsBalanceTable(adt.AsStore(rt), st.LockedTable)

	err := et.MustSubtract(addr, amount)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "subtract from escrow: %v", err)
	}
	err = lt.MustSubtract(addr, amount)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "subtract from locked: %v", err)
	}

	st.LockedTable = lt.Root()
	st.EscrowTable = et.Root()
}

////////////////////////////////////////////////////////////////////////////////
// Method utility functions
////////////////////////////////////////////////////////////////////////////////

func (st *StorageMarketActorState) mustGetDeal(rt Runtime, dealID abi.DealID) *OnChainDeal {
	deals := AsDealArray(adt.AsStore(rt), st.Deals)
	deal, err := deals.Get(dealID)
	if err != nil {
		rt.Abort(exitcode.ErrIllegalState, "get deal: %v", err)
	}

	return deal
}

func (st *StorageMarketActorState) lockBalanceOrAbort(rt Runtime, addr addr.Address, amount abi.TokenAmount) {
	if amount.LessThan(big.Zero()) {
		rt.Abort(exitcode.ErrIllegalArgument, "negative amount %v", amount)
	}

	if !st.maybeLockBalance(rt, addr, amount) {
		rt.Abort(exitcode.ErrInsufficientFunds, "Insufficient funds available to lock")
	}
}

////////////////////////////////////////////////////////////////////////////////
// State utility functions
////////////////////////////////////////////////////////////////////////////////

func dealProposalIsInternallyValid(rt Runtime, dealP StorageDealProposal) bool {
	if dealP.EndEpoch <= dealP.StartEpoch {
		return false
	}

	if dealP.Duration() != dealP.EndEpoch-dealP.StartEpoch {
		return false
	}

	TODO()
	// Determine which subset of DealProposal to use as the message to be signed by the client.
	var m []byte

	// Note: we do not verify the provider signature here, since this is implicit in the
	// authenticity of the on-chain message publishing the deal.
	sigVerified := rt.Syscalls().VerifySignature(dealP.ClientSignature, dealP.Client, m)
	if !sigVerified {
		return false
	}

	return true
}

func dealGetPaymentRemaining(deal *OnChainDeal, epoch abi.ChainEpoch) abi.TokenAmount {
	Assert(epoch <= deal.Proposal.EndEpoch)

	durationRemaining := deal.Proposal.EndEpoch - (epoch - 1)
	Assert(durationRemaining > 0)

	return big.Mul(big.NewInt(int64(durationRemaining)), deal.Proposal.StoragePricePerEpoch)
}
