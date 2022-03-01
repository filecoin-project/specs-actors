package verifreg

import (
	"bytes"

	"github.com/filecoin-project/go-address"
	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/specs-actors/v8/actors/runtime"
	cid "github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/v8/actors/builtin"
	"github.com/filecoin-project/specs-actors/v8/actors/util/adt"
)

// DataCap is an integer number of bytes.
// We can introduce policy changes and replace this in the future.
type DataCap = abi.StoragePower

const SignatureDomainSeparation_RemoveDataCap = "fil_removedatacap:"

type RmDcProposalID struct {
	ProposalID uint64
}

type State struct {
	// Root key holder multisig.
	// Authorize and remove verifiers.
	RootKey addr.Address

	// Verifiers authorize VerifiedClients.
	// Verifiers delegate their DataCap.
	Verifiers cid.Cid // HAMT[addr.Address]DataCap

	// VerifiedClients can add VerifiedClientData, up to DataCap.
	VerifiedClients cid.Cid // HAMT[addr.Address]DataCap

	// RemoveDataCapProposalIDs keeps the counters of the datacap removal proposal a verifier has submitted for a
	//specific client. Unique proposal ids ensure that removal proposals cannot be replayed.√
	// AddrPairKey is constructed as <verifier address, client address>, both using ID addresses.
	RemoveDataCapProposalIDs cid.Cid // HAMT[AddrPairKey]RmDcProposalID
}

var MinVerifiedDealSize = abi.NewStoragePower(1 << 20)

// rootKeyAddress comes from genesis.
func ConstructState(store adt.Store, rootKeyAddress addr.Address) (*State, error) {
	emptyMapCid, err := adt.StoreEmptyMap(store, builtin.DefaultHamtBitwidth)
	if err != nil {
		return nil, xerrors.Errorf("failed to create empty map: %w", err)
	}

	return &State{
		RootKey:                  rootKeyAddress,
		Verifiers:                emptyMapCid,
		VerifiedClients:          emptyMapCid,
		RemoveDataCapProposalIDs: emptyMapCid,
	}, nil
}

// A verifier who wants to send/agree to a RemoveDataCapRequest should sign a RemoveDataCapProposal and send the signed proposal to the root key holder.
type RemoveDataCapProposal struct {
	// VerifiedClient is the client address to remove the DataCap from
	// The address must be an ID address
	VerifiedClient addr.Address
	// DataCapAmount is the amount of DataCap to be removed from the VerifiedClient address
	DataCapAmount DataCap
	// RemovalProposalID is the counter of the proposal sent by the Verifier for the VerifiedClient
	RemovalProposalID RmDcProposalID
}

// A verifier who wants to submit a request should send their RemoveDataCapRequest to the RKH.
type RemoveDataCapRequest struct {
	// Verifier is the verifier address used for VerifierSignature.
	// The address can be address.SECP256K1 or address.BLS
	Verifier addr.Address
	// VerifierSignature is the Verifier's signature over a RemoveDataCapProposal
	VerifierSignature crypto.Signature
}

func isVerifier(rt runtime.Runtime, st State, address addr.Address) bool {
	verifiers, err := adt.AsMap(adt.AsStore(rt), st.Verifiers, builtin.DefaultHamtBitwidth)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load verifiers")

	ok, err := verifiers.Get(abi.AddrKey(address), nil)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to load verifier")

	return ok
}

////////////////////////////////////////////////////////////////////////////////
// State utility functions
////////////////////////////////////////////////////////////////////////////////
func removeDataCapRequestIsValidOrAbort(rt runtime.Runtime, request RemoveDataCapRequest, id RmDcProposalID, toRemove DataCap, client address.Address) {
	proposal := RemoveDataCapProposal{
		RemovalProposalID: id,
		DataCapAmount:     toRemove,
		VerifiedClient:    client,
	}
	buf := bytes.Buffer{}
	buf.WriteString(SignatureDomainSeparation_RemoveDataCap)
	if err := proposal.MarshalCBOR(&buf); err != nil {
		rt.Abortf(exitcode.ErrSerialization, "remove datacap request failed to marshal request: %s", err)
	}

	if err := rt.VerifySignature(request.VerifierSignature, request.Verifier, buf.Bytes()); err != nil {
		rt.Abortf(exitcode.ErrIllegalArgument, "remove datacap request signature is invalid: %s", err)
	}
}

func useProposalID(rt runtime.Runtime, proposalIDs *adt.Map, verifier, client address.Address) RmDcProposalID {
	var id RmDcProposalID
	idExists, err := proposalIDs.Get(abi.NewAddrPairKey(verifier, client), &id)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed getting proposal id for verifier,client %s,%s", verifier, client)
	if !idExists { //initialize with 0
		id = RmDcProposalID{ProposalID: 0}
	}
	next := RmDcProposalID{ProposalID: id.ProposalID + 1}
	err = proposalIDs.Put(abi.NewAddrPairKey(verifier, client), &next)
	builtin.RequireNoErr(rt, err, exitcode.ErrIllegalState, "failed to update remove datacap proposal id for verifier,client %s,%s", verifier, client)
	return id
}
