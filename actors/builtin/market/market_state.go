package market

import (
	"bytes"

	addr "github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
	xerrors "golang.org/x/xerrors"

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
	DealOpsByEpoch cid.Cid // SetMultimap, HAMT[epoch]Set
	LastCron       abi.ChainEpoch
}

func ConstructState(emptyArrayCid, emptyMapCid, emptyMSetCid cid.Cid) *State {
	return &State{
		Proposals:      emptyArrayCid,
		States:         emptyArrayCid,
		EscrowTable:    emptyMapCid,
		LockedTable:    emptyMapCid,
		NextID:         abi.DealID(0),
		DealOpsByEpoch: emptyMSetCid,
	}
}

////////////////////////////////////////////////////////////////////////////////
// Deal state operations
////////////////////////////////////////////////////////////////////////////////

func (st *State) updatePendingDealState(rt Runtime, dealID abi.DealID, epoch abi.ChainEpoch) (abi.TokenAmount, abi.ChainEpoch) {
	amountSlashed := abi.NewTokenAmount(0)

	states, err := AsDealStateArray(adt.AsStore(rt), st.States)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "get state state: %v", err)
	}

	state, found, err := states.Get(dealID)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to get deal: %d", dealID)
	}
	if !found {
		return abi.NewTokenAmount(0), epochUndefined
	}

	everUpdated := state.LastUpdatedEpoch != epochUndefined
	everSlashed := state.SlashEpoch != epochUndefined

	Assert(!everUpdated || (state.LastUpdatedEpoch <= epoch)) // if the deal was ever updated, make sure it didn't happen in the future

	deal := st.mustGetDeal(rt, dealID)

	if state.SectorStartEpoch == epochUndefined {
		// Not yet appeared in proven sector; check for timeout.
		if epoch > deal.StartEpoch {
			return st.processDealInitTimedOut(rt, dealID, deal, state), epochUndefined
		}
		// This should not be able to happen
		return amountSlashed, epochUndefined
	}

	// This would be the case that the first callback somehow triggers before it is scheduled to
	// This is expected not to be able to happen
	if deal.StartEpoch > epoch {
		return amountSlashed, epochUndefined
	}

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
		return amountSlashed, epochUndefined
	}

	if epoch >= deal.EndEpoch {
		st.processDealExpired(rt, dealID)
		return amountSlashed, epochUndefined
	}

	next := epoch + DealUpdatesInterval
	if next > deal.EndEpoch {
		next = deal.EndEpoch
	}

	// TODO: can we avoid having this field?
	state.LastUpdatedEpoch = epoch

	st.mutateDealStates(rt, func(states *DealMetaArray) {
		if err := states.Set(dealID, state); err != nil {
			rt.Abortf(exitcode.ErrPlaceholder, "failed to get deal: %v", err)
		}
	})

	return amountSlashed, next
}

func (st *State) mutateDealStates(rt Runtime, f func(*DealMetaArray)) {
	states, err := AsDealStateArray(adt.AsStore(rt), st.States)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to load deal states: %s", err)
	}

	f(states)

	rcid, err := states.Root()
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "flushing deal states failed: %s", err)
	}

	st.States = rcid
}

func (st *State) mutateDealProposals(rt Runtime, f func(*DealArray)) {
	proposals, err := AsDealProposalArray(adt.AsStore(rt), st.Proposals)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to load deal proposals array: %s", err)
	}

	f(proposals)

	rcid, err := proposals.Root()
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "flushing deal proposals set failed: %s", err)
	}

	st.Proposals = rcid
}

func (st *State) deleteDeal(rt Runtime, dealID abi.DealID) {
	st.mutateDealProposals(rt, func(proposals *DealArray) {
		if err := proposals.Delete(uint64(dealID)); err != nil {
			rt.Abortf(exitcode.ErrPlaceholder, "failed to delete deal: %v", err)
		}
	})
}

// Deal start deadline elapsed without appearing in a proven sector.
// Delete deal, slash a portion of provider's collateral, and unlock remaining collaterals
// for both provider and client.
func (st *State) processDealInitTimedOut(rt Runtime, dealID abi.DealID, deal *DealProposal, state *DealState) (amountSlashed abi.TokenAmount) {
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

func (st *State) MutateBalanceTable(s adt.Store, c *cid.Cid, f func(t *adt.BalanceTable) error) error {
	t, err := adt.AsBalanceTable(s, *c)
	if err != nil {
		return err
	}

	if err := f(t); err != nil {
		return err
	}

	rc, err := t.Root()
	if err != nil {
		return err
	}

	*c = rc
	return nil
}

func (st *State) AddEscrowBalance(s adt.Store, a addr.Address, amount abi.TokenAmount) error {
	return st.MutateBalanceTable(s, &st.EscrowTable, func(et *adt.BalanceTable) error {
		err := et.AddCreate(a, amount)
		if err != nil {
			return xerrors.Errorf("failed to add %s to balance table: %w", a, err)
		}
		return nil
	})
}

func (st *State) AddLockedBalance(s adt.Store, a addr.Address, amount abi.TokenAmount) error {
	return st.MutateBalanceTable(s, &st.LockedTable, func(lt *adt.BalanceTable) error {
		err := lt.AddCreate(a, amount)
		if err != nil {
			return err
		}
		return nil
	})
}

func (st *State) GetEscrowBalance(rt Runtime, a addr.Address) abi.TokenAmount {
	bt, err := adt.AsBalanceTable(adt.AsStore(rt), st.EscrowTable)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "get escrow balance: %v", err)
	}
	ret, err := bt.Get(a)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "get escrow balance: %v", err)
	}
	return ret
}

func (st *State) GetLockedBalance(rt Runtime, a addr.Address) abi.TokenAmount {
	lt, err := adt.AsBalanceTable(adt.AsStore(rt), st.LockedTable)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "get locked balance: %v", err)
	}
	ret, err := lt.Get(a)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "get locked balance: %v", err)
	}
	return ret
}

func (st *State) maybeLockBalance(rt Runtime, addr addr.Address, amount abi.TokenAmount) error {
	Assert(amount.GreaterThanEqual(big.Zero()))

	prevLocked := st.GetLockedBalance(rt, addr)
	escrowBalance := st.GetEscrowBalance(rt, addr)
	if big.Add(prevLocked, amount).GreaterThan(st.GetEscrowBalance(rt, addr)) {
		return xerrors.Errorf("not enough balance to lock for addr %s: %s <  %s + %s", addr, escrowBalance, prevLocked, amount)
	}

	return st.MutateBalanceTable(adt.AsStore(rt), &st.LockedTable, func(lt *adt.BalanceTable) error {
		err := lt.Add(addr, amount)
		if err != nil {
			return xerrors.Errorf("adding locked balance: %w", err)
		}
		return nil
	})
}

// TODO: all these balance table mutations need to happen at the top level and be batched (no flushing after each!)
func (st *State) unlockBalance(rt Runtime, addr addr.Address, amount abi.TokenAmount) {
	Assert(amount.GreaterThanEqual(big.Zero()))

	st.MutateBalanceTable(adt.AsStore(rt), &st.LockedTable, func(lt *adt.BalanceTable) error {
		err := lt.MustSubtract(addr, amount)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "subtracting from locked balance: %v", err)
		}
		return nil
	})
}

// move funds from locked in client to available in provider
func (st *State) transferBalance(rt Runtime, fromAddr addr.Address, toAddr addr.Address, amount abi.TokenAmount) {
	Assert(amount.GreaterThanEqual(big.Zero()))

	et, err := adt.AsBalanceTable(adt.AsStore(rt), st.EscrowTable)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "loading escrow table: %s", err)
	}
	lt, err := adt.AsBalanceTable(adt.AsStore(rt), st.LockedTable)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "loading locked balance table: %s", err)
	}

	if err := et.MustSubtract(fromAddr, amount); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "subtract from escrow: %v", err)
	}

	if err := lt.MustSubtract(fromAddr, amount); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "subtract from locked: %v", err)
	}

	if err := et.Add(toAddr, amount); err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "add to escrow: %v", err)
	}

	ltc, err := lt.Root()
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to flush locked table: %s", err)
	}
	etc, err := et.Root()
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to flush escrow table: %s", err)
	}

	st.LockedTable = ltc
	st.EscrowTable = etc
}

func (st *State) slashBalance(rt Runtime, addr addr.Address, amount abi.TokenAmount) {
	Assert(amount.GreaterThanEqual(big.Zero()))

	et, err := adt.AsBalanceTable(adt.AsStore(rt), st.EscrowTable)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "loading escrow table: %s", err)
	}
	lt, err := adt.AsBalanceTable(adt.AsStore(rt), st.LockedTable)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "loading locked balance table: %s", err)
	}

	err = et.MustSubtract(addr, amount)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "subtract from escrow: %v", err)
	}
	err = lt.MustSubtract(addr, amount)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "subtract from locked: %v", err)
	}

	ltc, err := lt.Root()
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to flush locked table: %s", err)
	}
	etc, err := et.Root()
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to flush escrow table: %s", err)
	}

	st.LockedTable = ltc
	st.EscrowTable = etc
}

////////////////////////////////////////////////////////////////////////////////
// Method utility functions
////////////////////////////////////////////////////////////////////////////////

func (st *State) mustGetDeal(rt Runtime, dealID abi.DealID) *DealProposal {
	proposals, err := AsDealProposalArray(adt.AsStore(rt), st.Proposals)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "get proposal: %v", err)
	}

	proposal, err := proposals.Get(dealID)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "get proposal: %v", err)
	}

	return proposal
}

func (st *State) mustGetDealState(rt Runtime, dealID abi.DealID) *DealState {
	states, err := AsDealStateArray(adt.AsStore(rt), st.States)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "get state state: %v", err)
	}
	state, found, err := states.Get(dealID)
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "get state state: %v", err)
	}

	if !found {
		rt.Abortf(exitcode.ErrIllegalState, "deal %d not found", dealID)
	}

	return state
}

func (st *State) lockBalanceOrAbort(rt Runtime, addr addr.Address, amount abi.TokenAmount) {
	if amount.LessThan(big.Zero()) {
		rt.Abortf(exitcode.ErrIllegalArgument, "negative amount %v", amount)
	}

	if err := st.maybeLockBalance(rt, addr, amount); err != nil {
		rt.Abortf(exitcode.ErrInsufficientFunds, "Insufficient funds available to lock: %s", err)
	}
}

////////////////////////////////////////////////////////////////////////////////
// State utility functions
////////////////////////////////////////////////////////////////////////////////

func dealProposalIsInternallyValid(rt Runtime, proposal ClientDealProposal) error {
	if proposal.Proposal.EndEpoch <= proposal.Proposal.StartEpoch {
		return xerrors.Errorf("proposal end epoch before start epoch")
	}

	// Note: we do not verify the provider signature here, since this is implicit in the
	// authenticity of the on-chain message publishing the deal.
	buf := bytes.Buffer{}
	err := proposal.Proposal.MarshalCBOR(&buf)
	if err != nil {
		return xerrors.Errorf("proposal signature verification failed to marshal proposal: %w", err)
	}
	err = rt.Syscalls().VerifySignature(proposal.ClientSignature, proposal.Proposal.Client, buf.Bytes())
	if err != nil {
		return xerrors.Errorf("signature proposal invalid: %w", err)
	}
	return nil
}

func dealGetPaymentRemaining(deal *DealProposal, epoch abi.ChainEpoch) abi.TokenAmount {
	Assert(epoch <= deal.EndEpoch)

	durationRemaining := deal.EndEpoch - (epoch - 1)
	Assert(durationRemaining > 0)

	return big.Mul(big.NewInt(int64(durationRemaining)), deal.StoragePricePerEpoch)
}
