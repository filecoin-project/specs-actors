package verifreg

import (
	"fmt"

	addr "github.com/filecoin-project/go-address"
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
	cid "github.com/ipfs/go-cid"
)

// DataCap is an integer number of bytes.
// We can introduce policy changes and replace this in the future.
type DataCap = abi.StoragePower
type AddrKey = adt.AddrKey

type State struct {
	// Root key holder multisig.
	// Authorize and remove verifiers.
	RootKey addr.Address

	// Verifiers authorize VerifiedClients.
	// Verifiers delegate their DataCap.
	Verifiers cid.Cid // HAMT[addr.Address]DataCap

	// VerifiedClients can add VerifiedClientData, up to DataCap.
	VerifiedClients cid.Cid // HAMT[addr.Address]DataCap
}

var MinVerifiedDealSize abi.StoragePower = big.NewInt(1 << 20) // PARAM_FINISH

// rootKeyAddress comes from genesis.
func ConstructState(emptyMapCid cid.Cid, rootKeyAddress addr.Address) *State {
	return &State{
		RootKey:         rootKeyAddress,
		Verifiers:       emptyMapCid,
		VerifiedClients: emptyMapCid,
	}
}

func (st *State) mutator(store adt.Store) *stateMutator {
	return &stateMutator{st: st, store: store}
}

type stateMutator struct {
	st    *State
	store adt.Store

	verifiersPermit adt.MutationPermission
	verifiers       *adt.Map

	clientsPermit   adt.MutationPermission
	verifiedClients *adt.Map
}

func (m *stateMutator) build() (*stateMutator, error) {
	if m.verifiersPermit != adt.InvalidPermission {
		verifiers, err := adt.AsMap(m.store, m.st.Verifiers)
		if err != nil {
			return nil, fmt.Errorf("failed to load verifiers: %w", err)
		}
		m.verifiers = verifiers
	}

	if m.clientsPermit != adt.InvalidPermission {
		clients, err := adt.AsMap(m.store, m.st.VerifiedClients)
		if err != nil {
			return nil, fmt.Errorf("failed to load verified clients: %w", err)
		}
		m.verifiedClients = clients
	}

	return m, nil
}

func (m *stateMutator) withVerifiers(permit adt.MutationPermission) *stateMutator {
	m.verifiersPermit = permit
	return m
}

func (m *stateMutator) withVerifiedClients(permit adt.MutationPermission) *stateMutator {
	m.clientsPermit = permit
	return m
}

func (m *stateMutator) commitState() error {
	var err error
	if m.verifiersPermit == adt.WritePermission {
		if m.st.Verifiers, err = m.verifiers.Root(); err != nil {
			return fmt.Errorf("failed to flush Verifiers: %w", err)
		}
	}

	if m.clientsPermit == adt.WritePermission {
		if m.st.VerifiedClients, err = m.verifiedClients.Root(); err != nil {
			return fmt.Errorf("failed to flush Verified Clients: %w", err)
		}
	}

	return nil
}
