package test

import (
	"bytes"
	"context"
	"testing"

	"github.com/filecoin-project/go-state-types/crypto"

	"github.com/filecoin-project/specs-actors/v8/actors/util/adt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/v8/actors/builtin"
	"github.com/filecoin-project/specs-actors/v8/actors/builtin/verifreg"
	"github.com/filecoin-project/specs-actors/v8/support/ipld"
	"github.com/filecoin-project/specs-actors/v8/support/vm"
)

func TestRemoveDataCapSimpleSuccessfulPath(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	addrs := vm.CreateAccounts(ctx, t, v, 4, big.Mul(big.NewInt(10_000), vm.FIL), 93837778)
	verifier1, verifier2, verifiedClient := addrs[0], addrs[1], addrs[2]
	verifier1IdAddr, _ := v.NormalizeAddress(verifier1)
	verifier2IdAddr, _ := v.NormalizeAddress(verifier2)
	verifiedClientID, _ := v.NormalizeAddress(verifiedClient)
	verifierAllowance := abi.NewStoragePower(2 * (32 << 30))
	allowanceToRemove := big.Div(verifierAllowance, big.NewInt(2))

	// register verifiers and verified client
	addVerifierParams := verifreg.AddVerifierParams{
		Address:   verifier1,
		Allowance: verifierAllowance,
	}
	vm.ApplyOk(t, v, vm.VerifregRoot, builtin.VerifiedRegistryActorAddr, big.Zero(), builtin.MethodsVerifiedRegistry.AddVerifier, &addVerifierParams)

	addVerifierParams = verifreg.AddVerifierParams{
		Address:   verifier2,
		Allowance: verifierAllowance,
	}
	vm.ApplyOk(t, v, vm.VerifregRoot, builtin.VerifiedRegistryActorAddr, big.Zero(), builtin.MethodsVerifiedRegistry.AddVerifier, &addVerifierParams)

	addClientParams := verifreg.AddVerifiedClientParams{
		Address:   verifiedClient,
		Allowance: verifierAllowance,
	}
	vm.ApplyOk(t, v, verifier1, builtin.VerifiedRegistryActorAddr, big.Zero(), builtin.MethodsVerifiedRegistry.AddVerifiedClient, &addClientParams)

	var verifregState verifreg.State
	err := v.GetState(builtin.VerifiedRegistryActorAddr, &verifregState)
	require.NoError(t, err)

	var datacapCur verifreg.DataCap
	verifiedClients, err := adt.AsMap(v.Store(), verifregState.VerifiedClients, builtin.DefaultHamtBitwidth)
	require.NoError(t, err)
	assert.NotNil(t, verifiedClients)
	ok, err := verifiedClients.Get(abi.AddrKey(verifiedClientID), &datacapCur)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, verifierAllowance, datacapCur)

	// remove half the datacap from the verified client
	proposalIds, err := adt.AsMap(v.Store(), verifregState.RemoveDataCapProposalIDs, builtin.DefaultHamtBitwidth)
	require.NoError(t, err)
	var ver1Id, ver2Id verifreg.RmDcProposalID
	ok1, err := proposalIds.Get(abi.NewAddrPairKey(verifier1IdAddr, verifiedClientID), &ver1Id)
	require.NoError(t, err)
	// should not be found, since no removal has ever been proposed
	require.False(t, ok1)
	ok2, err := proposalIds.Get(abi.NewAddrPairKey(verifier2IdAddr, verifiedClientID), &ver2Id)
	require.NoError(t, err)
	require.False(t, ok2)

	ver1Proposal := verifreg.RemoveDataCapProposal{
		RemovalProposalID: verifreg.RmDcProposalID{ProposalID: 0},
		DataCapAmount:     allowanceToRemove,
		VerifiedClient:    verifiedClientID,
	}

	ver2Proposal := verifreg.RemoveDataCapProposal{
		RemovalProposalID: verifreg.RmDcProposalID{ProposalID: 0},
		DataCapAmount:     allowanceToRemove,
		VerifiedClient:    verifiedClientID,
	}

	paramBuf1 := new(bytes.Buffer)
	paramBuf1.WriteString(verifreg.SignatureDomainSeparation_RemoveDataCap)
	err = ver1Proposal.MarshalCBOR(paramBuf1)
	require.NoError(t, err)

	paramBuf2 := new(bytes.Buffer)
	paramBuf2.WriteString(verifreg.SignatureDomainSeparation_RemoveDataCap)
	err = ver2Proposal.MarshalCBOR(paramBuf2)
	require.NoError(t, err)

	removeDatacapParams := verifreg.RemoveDataCapParams{
		VerifiedClientToRemove: verifiedClientID,
		DataCapAmountToRemove:  allowanceToRemove,
		VerifierRequest1: verifreg.RemoveDataCapRequest{
			Verifier: verifier1IdAddr,
			VerifierSignature: crypto.Signature{
				Type: crypto.SigTypeSecp256k1,
				Data: paramBuf1.Bytes(),
			},
		},
		VerifierRequest2: verifreg.RemoveDataCapRequest{
			Verifier: verifier2IdAddr,
			VerifierSignature: crypto.Signature{
				Type: crypto.SigTypeSecp256k1,
				Data: paramBuf2.Bytes(),
			},
		},
	}
	ret := vm.ApplyOk(t, v, vm.VerifregRoot, builtin.VerifiedRegistryActorAddr, big.Zero(), builtin.MethodsVerifiedRegistry.RemoveVerifiedClientDataCap, &removeDatacapParams)
	err = v.GetState(builtin.VerifiedRegistryActorAddr, &verifregState)
	require.NoError(t, err)

	removeRet, ok := ret.(*verifreg.RemoveDataCapReturn)
	require.True(t, ok)
	require.Equal(t, verifiedClientID, removeRet.VerifiedClient)
	require.Equal(t, allowanceToRemove, removeRet.DataCapRemoved)

	verifiedClients, err = adt.AsMap(v.Store(), verifregState.VerifiedClients, builtin.DefaultHamtBitwidth)
	require.NoError(t, err)
	assert.NotNil(t, verifiedClients)
	ok, err = verifiedClients.Get(abi.AddrKey(verifiedClientID), &datacapCur)
	require.NoError(t, err)
	require.True(t, ok)

	assert.Equal(t, allowanceToRemove, datacapCur)

	// do it again, this time the client should get deleted
	proposalIds, err = adt.AsMap(v.Store(), verifregState.RemoveDataCapProposalIDs, builtin.DefaultHamtBitwidth)
	require.NoError(t, err)
	ok1, err = proposalIds.Get(abi.NewAddrPairKey(verifier1IdAddr, verifiedClientID), &ver1Id)
	require.NoError(t, err)
	require.True(t, ok1)
	require.Equal(t, uint64(1), ver1Id.ProposalID)
	ok2, err = proposalIds.Get(abi.NewAddrPairKey(verifier2IdAddr, verifiedClientID), &ver2Id)
	require.NoError(t, err)
	require.True(t, ok2)
	require.Equal(t, uint64(1), ver2Id.ProposalID)

	ver1Proposal = verifreg.RemoveDataCapProposal{
		RemovalProposalID: ver1Id,
		DataCapAmount:     allowanceToRemove,
		VerifiedClient:    verifiedClientID,
	}

	ver2Proposal = verifreg.RemoveDataCapProposal{
		RemovalProposalID: ver2Id,
		DataCapAmount:     allowanceToRemove,
		VerifiedClient:    verifiedClientID,
	}

	paramBuf1 = new(bytes.Buffer)
	paramBuf1.WriteString(verifreg.SignatureDomainSeparation_RemoveDataCap)
	err = ver1Proposal.MarshalCBOR(paramBuf1)
	require.NoError(t, err)

	paramBuf2 = new(bytes.Buffer)
	paramBuf2.WriteString(verifreg.SignatureDomainSeparation_RemoveDataCap)
	err = ver2Proposal.MarshalCBOR(paramBuf2)
	require.NoError(t, err)

	removeDatacapParams = verifreg.RemoveDataCapParams{
		VerifiedClientToRemove: verifiedClientID,
		DataCapAmountToRemove:  allowanceToRemove,
		VerifierRequest1: verifreg.RemoveDataCapRequest{
			Verifier: verifier1IdAddr,
			VerifierSignature: crypto.Signature{
				Type: crypto.SigTypeSecp256k1,
				Data: paramBuf1.Bytes(),
			},
		},
		VerifierRequest2: verifreg.RemoveDataCapRequest{
			Verifier: verifier2IdAddr,
			VerifierSignature: crypto.Signature{
				Type: crypto.SigTypeSecp256k1,
				Data: paramBuf2.Bytes(),
			},
		},
	}
	ret = vm.ApplyOk(t, v, vm.VerifregRoot, builtin.VerifiedRegistryActorAddr, big.Zero(), builtin.MethodsVerifiedRegistry.RemoveVerifiedClientDataCap, &removeDatacapParams)
	err = v.GetState(builtin.VerifiedRegistryActorAddr, &verifregState)
	require.NoError(t, err)

	removeRet, ok = ret.(*verifreg.RemoveDataCapReturn)
	require.True(t, ok)
	require.Equal(t, verifiedClientID, removeRet.VerifiedClient)
	require.Equal(t, allowanceToRemove, removeRet.DataCapRemoved)

	verifiedClients, err = adt.AsMap(v.Store(), verifregState.VerifiedClients, builtin.DefaultHamtBitwidth)
	require.NoError(t, err)
	assert.NotNil(t, verifiedClients)
	ok, err = verifiedClients.Get(abi.AddrKey(verifiedClientID), &datacapCur)
	require.NoError(t, err)
	require.False(t, ok)
}
