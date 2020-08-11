package vm_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/account"
	"github.com/filecoin-project/specs-actors/actors/builtin/cron"
	"github.com/filecoin-project/specs-actors/actors/builtin/exported"
	initactor "github.com/filecoin-project/specs-actors/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/actors/builtin/system"
	"github.com/filecoin-project/specs-actors/actors/builtin/verifreg"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/ipld"
	actor_testing "github.com/filecoin-project/specs-actors/support/testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/runtime"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"
)

var FIL = big.NewInt(1e18)

// Creates a new VM and initializes all singleton actors plus a root verifier account.
func NewVMWithSingletons(ctx context.Context, t *testing.T) *VM {
	store := ipld.NewADTStore(ctx)

	lookup := map[cid.Cid]exported.BuiltinActor{}
	for _, ba := range exported.BuiltinActors() {
		lookup[ba.Code()] = ba
	}

	vm := NewVM(ctx, lookup, store)

	emptyMapCID, err := adt.MakeEmptyMap(vm.store).Root()
	require.NoError(t, err)
	emptyArrayCID, err := adt.MakeEmptyArray(vm.store).Root()
	require.NoError(t, err)
	emptyMultimapCID, err := adt.MakeEmptyMultimap(vm.store).Root()
	require.NoError(t, err)

	initializeActor(ctx, t, vm, &system.State{}, builtin.SystemActorCodeID, builtin.SystemActorAddr, big.Zero())

	// Verified registry account needs to be instantiated and initialized in init actor prior to construction
	// of verified registry actor.
	rootVerifier := verifregRootAddresses(t)
	addrTreeRoot := initAddressTree(t, vm, rootVerifier)
	initState := initactor.ConstructState(addrTreeRoot, "scenarios")
	initState.NextID = abi.ActorID(builtin.FirstNonSingletonActorId)
	initializeActor(ctx, t, vm, initState, builtin.InitActorCodeID, builtin.InitActorAddr, big.Zero())

	rewardState := reward.ConstructState(big.Mul(big.NewInt(100), FIL))
	initializeActor(ctx, t, vm, rewardState, builtin.RewardActorCodeID, builtin.RewardActorAddr, big.Max(big.NewInt(14e8), FIL))

	cronState := cron.ConstructState(cron.BuiltInEntries())
	initializeActor(ctx, t, vm, cronState, builtin.CronActorCodeID, builtin.CronActorAddr, big.Zero())

	powerState := power.ConstructState(emptyMapCID, emptyMultimapCID)
	initializeActor(ctx, t, vm, powerState, builtin.StoragePowerActorCodeID, builtin.StoragePowerActorAddr, big.Zero())

	marketState := market.ConstructState(emptyMapCID, emptyArrayCID, emptyMultimapCID)
	initializeActor(ctx, t, vm, marketState, builtin.StorageMarketActorCodeID, builtin.StorageMarketActorAddr, big.Zero())

	vrState := verifreg.ConstructState(emptyMapCID, rootVerifier.idAddr)
	initializeActor(ctx, t, vm, vrState, builtin.VerifiedRegistryActorCodeID, builtin.VerifiedRegistryActorAddr, big.Zero())

	// burnt funds
	initializeActor(ctx, t, vm, &account.State{Address: builtin.BurntFundsActorAddr}, builtin.AccountActorCodeID, builtin.BurntFundsActorAddr, big.Zero())

	// verified root
	rootVerifierState := &account.State{Address: rootVerifier.pubAddr}
	initializeActor(ctx, t, vm, rootVerifierState, builtin.AccountActorCodeID, rootVerifier.idAddr, big.Zero())

	_, err = vm.checkpoint()
	require.NoError(t, err)

	return vm
}

// Creates n account actors in the VM with the given balance
func CreateAccounts(ctx context.Context, t *testing.T, vm *VM, n int, balance abi.TokenAmount) []address.Address {
	var initState initactor.State
	err := vm.GetState(builtin.InitActorAddr, &initState)
	require.NoError(t, err)

	m, err := adt.AsMap(vm.store, initState.AddressMap)
	require.NoError(t, err)

	addrPairs := make([]addrPair, n)
	for i := range addrPairs {
		addr := actor_testing.NewBLSAddr(t, 93837778)
		idAddr, err := address.NewIDAddress(uint64(initState.NextID))
		require.NoError(t, err)
		initState.NextID++

		err = m.Put(adt.AddrKey(addr), cborAddrID(t, idAddr))
		require.NoError(t, err)

		addrPairs[i] = addrPair{
			pubAddr: addr,
			idAddr:  idAddr,
		}
	}
	initState.AddressMap, err = m.Root()
	require.NoError(t, err)

	err = vm.setActorState(ctx, builtin.InitActorAddr, &initState)
	require.NoError(t, err)

	pubAddrs := make([]address.Address, len(addrPairs))
	for i, addrPair := range addrPairs {
		st := &account.State{Address: addrPair.pubAddr}
		initializeActor(ctx, t, vm, st, builtin.AccountActorCodeID, addrPair.idAddr, balance)
		pubAddrs[i] = addrPair.pubAddr
	}
	return pubAddrs
}

func initializeActor(ctx context.Context, t *testing.T, vm *VM, state runtime.CBORMarshaler, code cid.Cid, a address.Address, balance abi.TokenAmount) {
	stateCID, err := vm.store.Put(ctx, state)
	require.NoError(t, err)
	actor := &TestActor{
		Head:    stateCID,
		Code:    code,
		Balance: balance,
	}
	err = vm.setActor(ctx, a, actor)
	require.NoError(t, err)
}

type addrPair struct {
	pubAddr address.Address
	idAddr  address.Address
}

func initAddressTree(t *testing.T, vm *VM, rootVerifier addrPair) cid.Cid {
	m := adt.MakeEmptyMap(vm.store)
	err := m.Put(adt.AddrKey(rootVerifier.pubAddr), cborAddrID(t, rootVerifier.idAddr))
	require.NoError(t, err)

	root, err := m.Root()
	require.NoError(t, err)
	return root
}

func cborAddrID(t *testing.T, idAddr address.Address) *cbg.CborInt {
	id, err := address.IDFromAddress(idAddr)
	require.NoError(t, err)
	cbgId := cbg.CborInt(id)
	return &cbgId
}

func verifregRootAddresses(t *testing.T) addrPair {
	rootVerifierAddr, err := address.NewFromString("t3qfoulel6fy6gn3hjmbhpdpf6fs5aqjb5fkurhtwvgssizq4jey5nw4ptq5up6h7jk7frdvvobv52qzmgjinq")
	require.NoError(t, err)

	rootVerifierID, err := address.NewFromString("t080")
	require.NoError(t, err)
	return addrPair{pubAddr: rootVerifierAddr, idAddr: rootVerifierID}
}
