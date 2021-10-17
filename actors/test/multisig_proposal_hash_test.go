package test

import (
	"bytes"
	"context"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin"
	init_ "github.com/filecoin-project/specs-actors/v7/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/v7/actors/builtin/multisig"
	"github.com/filecoin-project/specs-actors/v7/support/ipld"
	"github.com/filecoin-project/specs-actors/v7/support/vm"
	"github.com/minio/blake2b-simd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProposalHash(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t, ipld.NewBlockStoreInMemory())
	addrs := vm.CreateAccounts(ctx, t, v, 3, big.Mul(big.NewInt(10_000), big.NewInt(1e18)), 93837778)

	alice := vm.RequireNormalizeAddress(t, addrs[0], v)
	bob := vm.RequireNormalizeAddress(t, addrs[1], v)

	getSysActBal := func() abi.TokenAmount {
		act, found, err := v.GetActor(builtin.SystemActorAddr)
		require.NoError(t, err)
		require.True(t, found)
		return act.Balance
	}
	sysActStartBal := getSysActBal()

	multisigParams := multisig.ConstructorParams{
		Signers:               addrs,
		NumApprovalsThreshold: 2,
	}

	paramBuf := new(bytes.Buffer)
	err := multisigParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	initParam := init_.ExecParams{
		CodeCID:           builtin.MultisigActorCodeID,
		ConstructorParams: paramBuf.Bytes(),
	}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.InitActorAddr, big.Zero(), builtin.MethodsInit.Exec, &initParam)
	initRet := ret.(*init_.ExecReturn)
	assert.NotNil(t, initRet)
	multisigAddr := initRet.IDAddress

	// fund msig and propose msig sends funds to system actor
	filDelta := big.Mul(big.NewInt(3), builtin.OneNanoFIL)
	proposeSendSystemParams := multisig.ProposeParams{
		To:     builtin.SystemActorAddr,
		Value:  filDelta,
		Method: builtin.MethodSend,
	}
	vm.ApplyOk(t, v, alice, multisigAddr, filDelta, builtin.MethodsMultisig.Propose, &proposeSendSystemParams)

	wrongTx := &multisig.Transaction{
		To:       builtin.SystemActorAddr,
		Value:    big.Sub(filDelta, big.NewInt(1)), // incorrect send amount not consistent with proposal
		Method:   builtin.MethodSend,
		Approved: []addr.Address{alice},
	}
	wrongHash, err := multisig.ComputeProposalHash(wrongTx, blake2b.Sum256)
	require.NoError(t, err)
	badApproveParams := multisig.TxnIDParams{
		ID:           0,
		ProposalHash: wrongHash,
	}
	vm.ApplyCode(t, v, bob, multisigAddr, big.Zero(), builtin.MethodsMultisig.Approve, &badApproveParams, exitcode.ErrIllegalArgument)
	assert.Equal(t, sysActStartBal, getSysActBal())

	correctTx := wrongTx
	correctTx.Value = filDelta
	correctHash, err := multisig.ComputeProposalHash(correctTx, blake2b.Sum256)
	require.NoError(t, err)
	approveParams := multisig.TxnIDParams{
		ID:           0,
		ProposalHash: correctHash,
	}
	vm.ApplyOk(t, v, bob, multisigAddr, big.Zero(), builtin.MethodsMultisig.Approve, &approveParams)

	assert.Equal(t, big.Add(sysActStartBal, filDelta), getSysActBal())
}
