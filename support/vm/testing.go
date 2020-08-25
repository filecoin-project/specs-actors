package vm_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/account"
	"github.com/filecoin-project/specs-actors/actors/builtin/cron"
	"github.com/filecoin-project/specs-actors/actors/builtin/exported"
	initactor "github.com/filecoin-project/specs-actors/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/actors/builtin/system"
	"github.com/filecoin-project/specs-actors/actors/builtin/verifreg"
	"github.com/filecoin-project/specs-actors/actors/runtime"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/actors/util/smoothing"
	"github.com/filecoin-project/specs-actors/support/ipld"
	actor_testing "github.com/filecoin-project/specs-actors/support/testing"

	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var FIL = big.NewInt(1e18)

//
// Genesis like setup
//

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

	initState := initactor.ConstructState(emptyMapCID, "scenarios")
	initializeActor(ctx, t, vm, initState, builtin.InitActorCodeID, builtin.InitActorAddr, big.Zero())

	rewardState := reward.ConstructState(abi.NewStoragePower(0))
	initializeActor(ctx, t, vm, rewardState, builtin.RewardActorCodeID, builtin.RewardActorAddr, big.Max(big.NewInt(14e8), FIL))

	cronState := cron.ConstructState(cron.BuiltInEntries())
	initializeActor(ctx, t, vm, cronState, builtin.CronActorCodeID, builtin.CronActorAddr, big.Zero())

	powerState := power.ConstructState(emptyMapCID, emptyMultimapCID)
	initializeActor(ctx, t, vm, powerState, builtin.StoragePowerActorCodeID, builtin.StoragePowerActorAddr, big.Zero())

	marketState := market.ConstructState(emptyArrayCID, emptyMapCID, emptyMultimapCID)
	initializeActor(ctx, t, vm, marketState, builtin.StorageMarketActorCodeID, builtin.StorageMarketActorAddr, big.Zero())

	// this will need to be replaced with the address of a multisig actor for the verified registry to be tested accurately
	rootVerifier, err := address.NewIDAddress(80)
	require.NoError(t, err)
	vrState := verifreg.ConstructState(emptyMapCID, rootVerifier)
	initializeActor(ctx, t, vm, vrState, builtin.VerifiedRegistryActorCodeID, builtin.VerifiedRegistryActorAddr, big.Zero())

	// burnt funds
	initializeActor(ctx, t, vm, &account.State{Address: builtin.BurntFundsActorAddr}, builtin.AccountActorCodeID, builtin.BurntFundsActorAddr, big.Zero())

	_, err = vm.checkpoint()
	require.NoError(t, err)

	return vm
}

// Creates n account actors in the VM with the given balance
func CreateAccounts(ctx context.Context, t *testing.T, vm *VM, n int, balance abi.TokenAmount, seed int64) []address.Address {
	var initState initactor.State
	err := vm.GetState(builtin.InitActorAddr, &initState)
	require.NoError(t, err)

	addrPairs := make([]addrPair, n)
	for i := range addrPairs {
		addr := actor_testing.NewBLSAddr(t, seed+int64(i))
		idAddr, err := initState.MapAddressToNewID(vm.store, addr)
		require.NoError(t, err)

		addrPairs[i] = addrPair{
			pubAddr: addr,
			idAddr:  idAddr,
		}
	}
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

//
// Invocation expectations
//

// ExpectInvocation is a pattern for a message invocation within the VM.
// The To and Method fields must be supplied. Exitcode defaults to exitcode.Ok.
// All other field are optional, where a nil or Undef value indicates that any value will match.
// SubInvocations will be matched recursively.
type ExpectInvocation struct {
	To     address.Address
	Method abi.MethodNum

	// optional
	Exitcode       exitcode.ExitCode
	From           address.Address
	Value          *abi.TokenAmount
	Params         *objectExpectation
	Ret            *objectExpectation
	SubInvocations []ExpectInvocation
}

func (ei ExpectInvocation) Matches(t *testing.T, invocations *Invocation) {
	ei.matches(t, "", invocations)
}

func (ei ExpectInvocation) matches(t *testing.T, breadcrumb string, invocation *Invocation) {
	identifier := fmt.Sprintf("%s[%s:%d]", breadcrumb, invocation.Msg.to, invocation.Msg.method)

	// mismatch of to or method probably indicates skipped message or messages out of order. halt.
	require.Equal(t, ei.To, invocation.Msg.to, "%s unexpected 'to' address", identifier)
	require.Equal(t, ei.Method, invocation.Msg.method, "%s unexpected method", identifier)

	// other expectations are optional
	if address.Undef != ei.From {
		assert.Equal(t, ei.From, invocation.Msg.from, "%s unexpected from address", identifier)
	}
	if ei.Value != nil {
		assert.Equal(t, *ei.Value, invocation.Msg.value, "%s unexpected value", identifier)
	}
	if ei.Params != nil {
		assert.True(t, ei.Params.matches(invocation.Msg.params), "%s params aren't equal (%v != %v)", identifier, ei.Params.val, invocation.Msg.params)
	}
	if ei.SubInvocations != nil {
		for i, invk := range invocation.SubInvocations {
			subidentifier := fmt.Sprintf("%s%d:", identifier, i)
			require.True(t, len(ei.SubInvocations) > i, "%s unexpected subinvocation [%s:%d]", subidentifier, invk.Msg.to, invk.Msg.method)
			ei.SubInvocations[i].matches(t, subidentifier, invk)
		}
		missingInvocations := len(ei.SubInvocations) - len(invocation.SubInvocations)
		if missingInvocations > 0 {
			missingIndex := len(invocation.SubInvocations)
			missingExpect := ei.SubInvocations[missingIndex]
			require.Fail(t, "%s%d: expected invocation [%s:%d]", identifier, missingIndex, missingExpect.To, missingExpect.From)
		}
	}

	// expect results
	assert.Equal(t, ei.Exitcode, invocation.Exitcode, "%s unexpected exitcode", identifier)
	if ei.Ret != nil {
		assert.True(t, ei.Ret.matches(invocation.Ret), "%s unexpected return value (%v != %v)", identifier, ei.Ret, invocation.Ret)
	}
}

// helpers to simplify pointer creation
func ExpectAttoFil(amount big.Int) *big.Int                    { return &amount }
func ExpectBytes(b []byte) *objectExpectation                  { return ExpectObject(runtime.CBORBytes(b)) }
func ExpectExitCode(code exitcode.ExitCode) *exitcode.ExitCode { return &code }

func ExpectObject(v runtime.CBORMarshaler) *objectExpectation {
	return &objectExpectation{v}
}

// distinguishes a non-expectation from an expectation of nil
type objectExpectation struct {
	val runtime.CBORMarshaler
}

// match by cbor encoding to avoid inconsistencies in internal representations of effectively equal objects
func (oe objectExpectation) matches(obj interface{}) bool {
	if oe.val == nil || obj == nil {
		return oe.val == nil && obj == nil
	}

	paramBuf1 := new(bytes.Buffer)
	oe.val.MarshalCBOR(paramBuf1) // nolint: errcheck
	marshaller, ok := obj.(runtime.CBORMarshaler)
	if !ok {
		return false
	}
	paramBuf2 := new(bytes.Buffer)
	if marshaller != nil {
		marshaller.MarshalCBOR(paramBuf2) // nolint: errcheck
	}
	return bytes.Equal(paramBuf1.Bytes(), paramBuf2.Bytes())
}

var okExitCode = exitcode.Ok
var ExpectOK = &okExitCode

func ParamsForInvocation(t *testing.T, vm *VM, idxs ...int) interface{} {
	invocations := vm.Invocations()
	var invocation *Invocation
	for _, idx := range idxs {
		require.Greater(t, len(invocations), idx)
		invocation = invocations[idx]
		invocations = invocation.SubInvocations
	}
	require.NotNil(t, invocation)
	return invocation.Msg.params
}

//
// Advancing Time while updating state
//

type advanceDeadlinePredicate func(dlInfo *miner.DeadlineInfo) bool

func MinerDLInfo(t *testing.T, v *VM, minerIDAddr address.Address) *miner.DeadlineInfo {
	var minerState miner.State
	err := v.GetState(minerIDAddr, &minerState)
	require.NoError(t, err)

	return minerState.DeadlineInfo(v.GetEpoch())
}

// AdvanceByDeadline creates a new VM advanced to an epoch specified by the predicate while keeping the
// miner state upu-to-date by running a cron at the end of each deadline period.
func AdvanceByDeadline(t *testing.T, v *VM, minerIDAddr address.Address, predicate advanceDeadlinePredicate) (*VM, *miner.DeadlineInfo) {
	dlInfo := MinerDLInfo(t, v, minerIDAddr)
	var err error
	for predicate(dlInfo) {
		v, err = v.WithEpoch(dlInfo.Last())
		require.NoError(t, err)

		_, code := v.ApplyMessage(builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
		require.Equal(t, exitcode.Ok, code)

		dlInfo = MinerDLInfo(t, v, minerIDAddr)
	}
	return v, dlInfo
}

// Advances by deadline until e is contained within the deadline period represented by the returned deadline info.
// The VM returned will be set to the last deadline close, not at e.
func AdvanceByDeadlineTillEpoch(t *testing.T, v *VM, minerIDAddr address.Address, e abi.ChainEpoch) (*VM, *miner.DeadlineInfo) {
	return AdvanceByDeadline(t, v, minerIDAddr, func(dlInfo *miner.DeadlineInfo) bool {
		return dlInfo.Close <= e
	})
}

// Advances by deadline until the deadline index matches the given index.
// The vm returned will be set to the close epoch of the previous deadline.
func AdvanceByDeadlineTillIndex(t *testing.T, v *VM, minerIDAddr address.Address, i uint64) (*VM, *miner.DeadlineInfo) {
	return AdvanceByDeadline(t, v, minerIDAddr, func(dlInfo *miner.DeadlineInfo) bool {
		return dlInfo.Index != i
	})
}

//
// state abstraction
//

type MinerBalances struct {
	AvailableBalance abi.TokenAmount
	VestingBalance   abi.TokenAmount
	InitialPledge    abi.TokenAmount
	PreCommitDeposit abi.TokenAmount
}

func GetMinerBalances(t *testing.T, vm *VM, minerIdAddr address.Address) MinerBalances {
	var state miner.State
	a, found, err := vm.GetActor(minerIdAddr)
	require.NoError(t, err)
	require.True(t, found)

	err = vm.GetState(minerIdAddr, &state)
	require.NoError(t, err)

	return MinerBalances{
		AvailableBalance: big.Subtract(a.Balance, state.PreCommitDeposits, state.InitialPledge, state.LockedFunds, state.FeeDebt),
		PreCommitDeposit: state.PreCommitDeposits,
		VestingBalance:   state.LockedFunds,
		InitialPledge:    state.InitialPledge,
	}
}

type NetworkStats struct {
	power.State
	TotalRawBytePower         abi.StoragePower
	TotalBytesCommitted       abi.StoragePower
	TotalQualityAdjPower      abi.StoragePower
	TotalQABytesCommitted     abi.StoragePower
	TotalPledgeCollateral     abi.TokenAmount
	ThisEpochRawBytePower     abi.StoragePower
	ThisEpochQualityAdjPower  abi.StoragePower
	ThisEpochPledgeCollateral abi.TokenAmount
	MinerCount                int64
	MinerAboveMinPowerCount   int64
	ThisEpochReward           abi.TokenAmount
	ThisEpochRewardSmoothed   *smoothing.FilterEstimate
	ThisEpochBaselinePower    abi.StoragePower
	TotalMined                abi.TokenAmount
}

func GetNetworkStats(t *testing.T, vm *VM) NetworkStats {
	var powerState power.State
	err := vm.GetState(builtin.StoragePowerActorAddr, &powerState)
	require.NoError(t, err)

	var rewardState reward.State
	err = vm.GetState(builtin.RewardActorAddr, &rewardState)
	require.NoError(t, err)

	return NetworkStats{
		TotalRawBytePower:         powerState.TotalRawBytePower,
		TotalBytesCommitted:       powerState.TotalBytesCommitted,
		TotalQualityAdjPower:      powerState.TotalQualityAdjPower,
		TotalQABytesCommitted:     powerState.TotalQABytesCommitted,
		TotalPledgeCollateral:     powerState.TotalPledgeCollateral,
		ThisEpochRawBytePower:     powerState.ThisEpochRawBytePower,
		ThisEpochQualityAdjPower:  powerState.ThisEpochQualityAdjPower,
		ThisEpochPledgeCollateral: powerState.ThisEpochPledgeCollateral,
		MinerCount:                powerState.MinerCount,
		MinerAboveMinPowerCount:   powerState.MinerAboveMinPowerCount,
		ThisEpochReward:           rewardState.ThisEpochReward,
		ThisEpochRewardSmoothed:   rewardState.ThisEpochRewardSmoothed,
		ThisEpochBaselinePower:    rewardState.ThisEpochBaselinePower,
		TotalMined:                rewardState.TotalMined,
	}
}

//
//  internal stuff
//

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
