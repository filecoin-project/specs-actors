package test

import (
	"bytes"
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	init_ "github.com/filecoin-project/specs-actors/v2/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/multisig"
	vm "github.com/filecoin-project/specs-actors/v2/support/vm"
)

func TestMultisigDeleteSelf(t *testing.T) {
	ctx := context.Background()
	v := vm.NewVMWithSingletons(ctx, t)
	addrs := vm.CreateAccounts(ctx, t, v, 2, big.Mul(big.NewInt(10_000), big.NewInt(1e18)), 93837778)

	multisigParams := multisig.ConstructorParams{
		Signers:               addrs,
		NumApprovalsThreshold: 1,
	}

	paramBuf := new(bytes.Buffer)
	err := multisigParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	initParam := init_.ExecParams{
		CodeCID:           builtin.MultisigActorCodeID,
		ConstructorParams: paramBuf.Bytes(),
	}
	ret := vm.ApplyOk(t, v, addrs[0], builtin.InitActorAddr, big.Zero(), builtin.MethodsPower.CreateMiner, &initParam)
	initRet := ret.(*init_.ExecReturn)
	assert.NotNil(t, initRet)
	multisigAddr := initRet.IDAddress

	removeParams := multisig.RemoveSignerParams{
		Signer:   addrs[0],
		Decrease: false,
	}

	paramBuf = new(bytes.Buffer)
	err = removeParams.MarshalCBOR(paramBuf)
	require.NoError(t, err)

	proposeRemoveSignerParams := multisig.ProposeParams{
		To:     multisigAddr,
		Value:  big.Zero(),
		Method: builtin.MethodsMultisig.RemoveSigner,
		Params: paramBuf.Bytes(),
	}
	vm.ApplyOk(t, v, addrs[0], multisigAddr, big.Zero(), builtin.MethodsMultisig.Propose, &proposeRemoveSignerParams)
}
