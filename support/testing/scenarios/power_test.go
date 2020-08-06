package scenarios_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/support/testing/scenarios"

	"github.com/stretchr/testify/assert"
)

func TestCreateMiner(t *testing.T) {
	ctx := context.Background()
	vm, addrs := scenarios.NewVMWithSingletons(ctx, t, 1)

	params := power.CreateMinerParams{
		Owner:         addrs[0],
		Worker:        addrs[0],
		SealProofType: abi.RegisteredSealProof_StackedDrg32GiBV1,
		Peer:          abi.PeerID("not reallly a peer id"),
	}
	_, code := vm.ApplyMessage(addrs[0], builtin.StoragePowerActorAddr, big.Zero(), builtin.MethodsPower.CreateMiner, &params)
	assert.Equal(t, exitcode.Ok, code)
}
