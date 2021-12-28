package verifreg

import (
	"bytes"
	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/specs-actors/v7/actors/runtime"
	cid "github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/v7/actors/builtin"
	"github.com/filecoin-project/specs-actors/v7/actors/util/adt"
)

// DataCap is an integer number of bytes.
// We can introduce policy changes and replace this in the future.
type DataCap = abi.StoragePower

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

var MinVerifiedDealSize = abi.NewStoragePower(1 << 20)

// rootKeyAddress comes from genesis.
func ConstructState(store adt.Store, rootKeyAddress addr.Address) (*State, error) {
	emptyMapCid, err := adt.StoreEmptyMap(store, builtin.DefaultHamtBitwidth)
	if err != nil {
		return nil, xerrors.Errorf("failed to create empty map: %w", err)
	}

	return &State{
		RootKey:         rootKeyAddress,
		Verifiers:       emptyMapCid,
		VerifiedClients: emptyMapCid,
	}, nil
}

// A verifier who wants to send/agree to a RemoveDataCapRequest should sign a RemoveDataCapProposal and send the signed proposal to the root key holder.
type RemoveDataCapProposal struct {
	// Verifier is the verifier address that signs the proposal
	// The address can be address.SECP256K1 or address.BLS
	Verifier addr.Address
	// VerifiedClient is the client address to remove the DataCap from
	// The address must be an ID address
	VerifiedClient addr.Address
	// DataCapAmount is the amount of DataCap to be removed from the VerifiedClient address
	DataCapAmount DataCap
}


// A verifier who wants to submit a request should send their RemoveDataCapRequest to the RKH.
type RemoveDataCapRequest struct {
	// Verifier is the verifier address used for VerifierSignature.
	// The address can be address.SECP256K1 or address.BLS
	Verifier           addr.Address
	// VerifierSignature is the Verifier's signature over a RemoveDataCapProposal
	VerifierSignature crypto.Signature
}

func isVerifier(rt runtime.Runtime, st State, address addr.Address) (bool, exitcode.ExitCode, error) {
	var verifiers *adt.Map
	var err error
	if verifiers, err = adt.AsMap(adt.AsStore(rt), st.Verifiers, builtin.DefaultHamtBitwidth); err != nil {
		return false, exitcode.ErrIllegalState, xerrors.Errorf("failed to load verifiers.")
	}

	var isVerifier bool
	if isVerifier, err = verifiers.Get(abi.AddrKey(address), nil); err != nil {
		return false, exitcode.ErrIllegalState, xerrors.Errorf("failed to load verifier %v.", address)
	}
	if !isVerifier {
		rt.Abortf(exitcode.ErrNotFound, "%v is not a verifier", address)
	}

	return true, 0, nil
}

////////////////////////////////////////////////////////////////////////////////
// State utility functions
////////////////////////////////////////////////////////////////////////////////
func removeDataCapRequestIsValid(rt runtime.Runtime, request RemoveDataCapRequest, proposal RemoveDataCapProposal) (bool, exitcode.ExitCode, error) {
	// get the expected RemoveDataCapProposal
	proposal.Verifier = request.Verifier
	buf := bytes.Buffer{}
	err := proposal.MarshalCBOR(&buf)

	if err != nil {
		return false, exitcode.ErrSerialization, xerrors.Errorf("remove datacap request signature validation failed to marshal request: %w", err)
	}

	err = rt.VerifySignature(request.VerifierSignature, request.Verifier, buf.Bytes())
	if err != nil {
		return false, exitcode.SysErrorIllegalArgument, xerrors.Errorf("remove datacap request signature is invalid: %w", err)
	}
	return true, 0, nil
}
