package test_test

import (
	"context"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	vm "github.com/filecoin-project/specs-actors/support/vm"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/stretchr/testify/assert"
)

func TestCreateMiner(t *testing.T) {
	ctx := context.Background()
	runtime := vm.NewVMWithSingletons(ctx, t)
	addrs := vm.CreateAccounts(ctx, t, runtime, 1, big.Mul(big.NewInt(10_000), big.NewInt(1e18)))

	params := power.CreateMinerParams{
		Owner:         addrs[0],
		Worker:        addrs[0],
		SealProofType: abi.RegisteredSealProof_StackedDrg32GiBV1,
		Peer:          abi.PeerID("not really a peer id"),
	}
	ret, code := runtime.ApplyMessage(addrs[0], builtin.StoragePowerActorAddr, big.NewInt(1e10), builtin.MethodsPower.CreateMiner, &params)
	assert.Equal(t, exitcode.Ok, code)

	minerAddrs, ok := ret.(*power.CreateMinerReturn)
	require.True(t, ok)

	// all expectations implicitly expected to be Ok
	vm.ExpectInvocation{
		// Original send to storage power actor
		To:     builtin.StoragePowerActorAddr,
		Method: builtin.MethodsPower.CreateMiner,
		Params: vm.ExpectObject(&params),
		Ret:    vm.ExpectObject(ret),
		SubInvocations: []vm.ExpectInvocation{{

			// Storage power requests init actor construct a miner
			To:     builtin.InitActorAddr,
			Method: builtin.MethodsInit.Exec,
			SubInvocations: []vm.ExpectInvocation{{

				// Miner constructor gets params from original call
				To:     minerAddrs.IDAddress,
				Method: builtin.MethodConstructor,
				Params: vm.ExpectObject(&miner.ConstructorParams{
					OwnerAddr:     params.Owner,
					WorkerAddr:    params.Worker,
					SealProofType: params.SealProofType,
					PeerId:        params.Peer,
				}),
				SubInvocations: []vm.ExpectInvocation{{

					// Miner calls back to power actor to enroll its cron event
					To:             builtin.StoragePowerActorAddr,
					Method:         builtin.MethodsPower.EnrollCronEvent,
					SubInvocations: []vm.ExpectInvocation{},
				}},
			}},
		}},
	}.Matches(t, runtime.Invocations()[0])
}
