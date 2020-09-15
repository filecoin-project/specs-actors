package migration

import (
	"context"

	market0 "github.com/filecoin-project/specs-actors/actors/builtin/market"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	"golang.org/x/xerrors"

	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
)

type marketMigrator struct {
}

func (m *marketMigrator) MigrateState(ctx context.Context, storeIn, storeOut cbor.IpldStore, head cid.Cid) (cid.Cid, error) {
	var inState market0.State
	if err := storeIn.Get(ctx, head, &inState); err != nil {
		return cid.Undef, err
	}

	proposalsRoot, err := m.migrateProposals(ctx, storeIn, storeOut, inState.Proposals)
	if err != nil {
		return cid.Undef, xerrors.Errorf("proposals: %w", err)
	}

	statesRoot, err := m.migrateStates(ctx, storeIn, storeOut, inState.States)
	if err != nil {
		return cid.Undef, xerrors.Errorf("states: %w", err)
	}

	pendingRoot, err := m.migratePendingProposals(ctx, storeIn, storeOut, inState.PendingProposals)
	if err != nil {
		return cid.Undef, xerrors.Errorf("pending proposals: %w", err)
	}

	escrowRoot, err := m.migrateBalanceTable(ctx, storeIn, storeOut, inState.EscrowTable)
	if err != nil {
		return cid.Undef, xerrors.Errorf("escrow table: %w", err)
	}

	lockedRoot, err := m.migrateBalanceTable(ctx, storeIn, storeOut, inState.LockedTable)
	if err != nil {
		return cid.Undef, xerrors.Errorf("locked table: %w", err)
	}

	dealOpsRoot, err := m.migrateDealOps(ctx, storeIn, storeOut, inState.DealOpsByEpoch)
	if err != nil {
		return cid.Undef, xerrors.Errorf("deal ops by epoch: %w", err)
	}

	outState := market2.State{
		Proposals:                     proposalsRoot,
		States:                        statesRoot,
		PendingProposals:              pendingRoot,
		EscrowTable:                   escrowRoot,
		LockedTable:                   lockedRoot,
		NextID:                        inState.NextID,
		DealOpsByEpoch:                dealOpsRoot,
		LastCron:                      inState.LastCron,
		TotalClientLockedCollateral:   inState.TotalClientLockedCollateral,
		TotalProviderLockedCollateral: inState.TotalProviderLockedCollateral,
		TotalClientStorageFee:         inState.TotalClientStorageFee,
	}
	return storeOut.Put(ctx, &outState)
}

func (m *marketMigrator) migrateProposals(_ context.Context, _, _ cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// AMT and both the key and value type unchanged between v0 and v2.
	// Verify that the value type is identical.
	var _ = market0.DealProposal(market2.DealProposal{})
	return root, nil
}

func (m *marketMigrator) migrateStates(_ context.Context, _, _ cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// AMT and both the key and value type unchanged between v0 and v2.
	// Verify that the value type is identical.
	var _ = market0.DealState(market2.DealState{})
	return root, nil
}

func (m *marketMigrator) migratePendingProposals(ctx context.Context, storeIn, storeOut cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// The HAMT has changed, but the value type is identical.
	var _ = market0.DealProposal(market2.DealProposal{})
	return migrateHAMTRaw(ctx, storeIn, storeOut, root)
}

func (m *marketMigrator) migrateBalanceTable(ctx context.Context, storeIn, storeOut cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// The HAMT has changed, but the value type (abi.TokenAmount) is identical.
	return migrateHAMTRaw(ctx, storeIn, storeOut, root)
}

func (m *marketMigrator) migrateDealOps(ctx context.Context, storeIn, storeOut cbor.IpldStore, root cid.Cid) (cid.Cid, error) {
	// The HAMT has changed, at each level, but the final value type (abi.DealID) is identical.
	return migrateHAMTHAMTRaw(ctx, storeIn, storeOut, root)
}
