package test

import (
	"context"
	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/specs-actors/v7/actors/util/adt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/verifreg"
	"github.com/filecoin-project/specs-actors/v7/support/ipld"
	"github.com/filecoin-project/specs-actors/v7/support/vm"
)

func TestRemoveDataCapSimpleSuccessfulPath(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	addrs := vm.CreateAccounts(ctx, t, v, 4, big.Mul(big.NewInt(10_000), vm.FIL), 93837778)
	verifier1, verifier2, verifiedClient := addrs[0], addrs[1], addrs[2]
	verifier1IdAddr, _ := v.NormalizeAddress(verifier1)
	verifier2IdAddr, _ := v.NormalizeAddress(verifier2)
	verifiedClientID, _ := v.NormalizeAddress(verifiedClient)
	verifierAllowance := abi.NewStoragePower(2* (32 << 30))
	allowanceToRemove := abi.NewStoragePower(32 << 30)

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
	assert.NotNil(t, verifiedClients )
	_, err = verifiedClients.Get(abi.AddrKey(verifiedClientID), &datacapCur)
	require.NoError(t, err)
	assert.Equal(t, verifierAllowance, datacapCur)

	// remove 50 datacap from the verified client
	proposalIds, err := adt.AsMap(v.Store(), verifregState.RemoveDataCapProposalIDs, builtin.DefaultHamtBitwidth)
	require.NoError(t, err)
	var ver1Id, ver2Id verifreg.RmDcProposalID
	_, err = proposalIds.Get(abi.NewAddrPairKey(verifier1IdAddr, verifiedClientID), &ver1Id)
	_, err = proposalIds.Get(abi.NewAddrPairKey(verifier2IdAddr, verifiedClientID), &ver2Id)

	ver1Proposal := verifreg.RemoveDataCapProposal{
		RemovalProposalID: ver1Id,
		DataCapAmount:     allowanceToRemove,
		VerifiedClient:    verifiedClientID,
	}

	ver2Proposal := verifreg.RemoveDataCapProposal{
		RemovalProposalID: ver2Id,
		DataCapAmount:     allowanceToRemove,
		VerifiedClient:    verifiedClientID,
	}
	//sign this thing


	removeDatacapParams := verifreg.RemoveDataCapParams{
		VerifiedClientToRemove: verifiedClientID,
		DataCapAmountToRemove:  allowanceToRemove,
		VerifierRequest1:       verifreg.RemoveDataCapRequest{
			Verifier:          verifier1IdAddr,
			VerifierSignature: sig1,
		},
		VerifierRequest2: verifreg.RemoveDataCapRequest{
			Verifier:          verifier2IdAddr,
			VerifierSignature: sig2,
		},
	}
	vm.ApplyOk(t, v, vm.VerifregRoot, builtin.VerifiedRegistryActorAddr, big.Zero(), builtin.MethodsVerifiedRegistry.AddVerifier, &addVerifierParams)

}