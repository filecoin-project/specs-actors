package verifreg_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/verifreg"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
	"github.com/stretchr/testify/require"
)

func TestExports(t *testing.T) {
	mock.CheckActorExports(t, verifreg.Actor{})
}

func TestConstruction(t *testing.T) {
	actor := verifreg.Actor{}
	receiver := tutil.NewIDAddr(t, 100)
	builder := mock.NewBuilder(context.Background(), receiver).
		WithCaller(builtin.SystemActorAddr, builtin.InitActorCodeID)

	rt := builder.Build(t)

	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)

	raddr := tutil.NewIDAddr(t, 101)

	ret := rt.Call(actor.Constructor, &raddr).(*adt.EmptyValue)
	require.Nil(t, ret)
	rt.Verify()

	store := adt.AsStore(rt)

	emptyMap, err := adt.MakeEmptyMap(store).Root()
	require.NoError(t, err)

	var state verifreg.State
	rt.GetState(&state)

	require.Equal(t, emptyMap, state.VerifiedClients)
	require.Equal(t, emptyMap, state.Verifiers)
	require.Equal(t, raddr, state.RootKey)
}

func TestAddVerifier(t *testing.T) {
	root := tutil.NewIDAddr(t, 101)
	va := tutil.NewIDAddr(t, 201)

	t.Run("fails when caller is not the root key", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		verifier := ac.generateVerifier(va)
		param := verifreg.AddVerifierParams{Address: verifier.Address, Allowance: verifreg.DataCap{}}

		rt.ExpectValidateCallerAddr(ac.rootkey)

		rt.SetCaller(tutil.NewIDAddr(t, 501), builtin.VerifiedRegistryActorCodeID)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			rt.Call(ac.AddVerifier, &param)
		})
		rt.Verify()

	})

	t.Run("successfully add a verifier", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		ac.generateAndAddVerifier(rt, va)
	})
}

func TestRemoveVerifier(t *testing.T) {
	root := tutil.NewIDAddr(t, 101)
	va := tutil.NewIDAddr(t, 201)

	t.Run("fails when caller is not the root key", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		// add a verifier
		v := ac.generateAndAddVerifier(rt, va)

		rt.ExpectValidateCallerAddr(ac.rootkey)

		rt.SetCaller(tutil.NewIDAddr(t, 501), builtin.VerifiedRegistryActorCodeID)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			rt.Call(ac.RemoveVerifier, &v.Address)
		})
		rt.Verify()
	})

	t.Run("fails when verifier does not exist", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)

		rt.ExpectValidateCallerAddr(ac.rootkey)

		rt.SetCaller(ac.rootkey, builtin.VerifiedRegistryActorCodeID)
		v := tutil.NewIDAddr(t, 501)
		rt.ExpectAbort(exitcode.ErrIllegalState, func() {
			rt.Call(ac.RemoveVerifier, &v)
		})
		rt.Verify()
	})

	t.Run("successfully remove a verifier", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		ac.generateAndAddVerifier(rt, va)

		ac.removeVerifier(rt, va)
	})
}

func TestAddVerifiedClient(t *testing.T) {
	root := tutil.NewIDAddr(t, 101)
	verifiedClientAddr := tutil.NewIDAddr(t, 201)
	verifiedClientAddr2 := tutil.NewIDAddr(t, 202)
	verifiedClientAddr3 := tutil.NewIDAddr(t, 203)
	verifiedClientAddr4 := tutil.NewIDAddr(t, 204)

	verifierAddr := tutil.NewIDAddr(t, 301)
	verifierAddr2 := tutil.NewIDAddr(t, 302)

	t.Run("successfully add multiple verified clients from different verifiers", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)

		c1 := ac.generateVerifiedClient(verifiedClientAddr)
		c2 := ac.generateVerifiedClient(verifiedClientAddr2)
		c3 := ac.generateVerifiedClient(verifiedClientAddr3)
		c4 := ac.generateVerifiedClient(verifiedClientAddr4)

		// verifier 1 should have enough allowance for both clients
		verifier := ac.generateVerifier(verifierAddr)
		verifier.Allowance = big.Sum(c1.Allowance, c2.Allowance)
		ac.addVerifier(rt, verifier.Address, verifier.Allowance)

		// verifier 2 should have enough allowance for both clients
		verifier2 := ac.generateVerifier(verifierAddr2)
		verifier2.Allowance = big.Sum(c3.Allowance, c4.Allowance)
		ac.addVerifier(rt, verifier2.Address, verifier2.Allowance)

		// add client 1 & 2
		ac.addVerifiedClient(rt, verifier.Address, c1.Address, c1.Allowance)
		ac.addVerifiedClient(rt, verifier.Address, c2.Address, c2.Allowance)

		// add clients 3 & 4
		ac.addVerifiedClient(rt, verifier2.Address, c3.Address, c3.Allowance)
		ac.addVerifiedClient(rt, verifier2.Address, c4.Address, c4.Allowance)

		// all clients should exist and verifiers should have no more allowance left
		require.EqualValues(t, c1.Allowance, ac.getVerifiedClientCap(rt, c1.Address))
		require.EqualValues(t, c2.Allowance, ac.getVerifiedClientCap(rt, c2.Address))
		require.EqualValues(t, c3.Allowance, ac.getVerifiedClientCap(rt, c3.Address))
		require.EqualValues(t, c4.Allowance, ac.getVerifiedClientCap(rt, c4.Address))

		require.EqualValues(t, big.Zero(), ac.getVerifierCap(rt, verifierAddr))
		require.EqualValues(t, big.Zero(), ac.getVerifierCap(rt, verifierAddr2))
	})

	t.Run("verifier successfully adds a verified client and then fails on adding another verified client because of low allowance", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)

		c1 := ac.generateVerifiedClient(verifiedClientAddr)
		c2 := ac.generateVerifiedClient(verifiedClientAddr2)

		// verifier only has enough balance for one client
		verifier := ac.generateVerifier(verifierAddr)
		verifier.Allowance = c1.Allowance
		ac.addVerifier(rt, verifier.Address, verifier.Allowance)

		// add client 1 works
		ac.addVerifiedClient(rt, verifier.Address, c1.Address, c1.Allowance)

		// add client 2 fails
		rt.SetCaller(verifier.Address, builtin.VerifiedRegistryActorCodeID)
		rt.ExpectValidateCallerAny()
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(ac.AddVerifiedClient, c2)
		})
		rt.Verify()

		// one client should exist and verifier should have no more allowance left
		require.EqualValues(t, c1.Allowance, ac.getVerifiedClientCap(rt, c1.Address))
		require.EqualValues(t, big.Zero(), ac.getVerifierCap(rt, verifierAddr))
	})

	t.Run("fails when allowance is less than MinVerifiedDealSize", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		allowance := big.Sub(verifreg.MinVerifiedDealSize, big.NewInt(1))
		p := &verifreg.AddVerifiedClientParams{tutil.NewIDAddr(t, 501), allowance}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(ac.AddVerifiedClient, p)
		})
		rt.Verify()
	})

	t.Run("fails when caller is not a verifier", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		client := ac.generateVerifiedClient(verifiedClientAddr)
		ac.generateAndAddVerifier(rt, verifierAddr)

		nc := tutil.NewIDAddr(t, 209)
		rt.SetCaller(nc, builtin.VerifiedRegistryActorCodeID)
		rt.ExpectValidateCallerAny()

		rt.ExpectAbort(exitcode.ErrNotFound, func() {
			rt.Call(ac.AddVerifiedClient, client)
		})
		rt.Verify()
	})

	t.Run("fails when verifier cap is less than client allowance", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		verifier := ac.generateAndAddVerifier(rt, verifierAddr)

		rt.SetCaller(verifier.Address, builtin.VerifiedRegistryActorCodeID)
		rt.ExpectValidateCallerAny()

		client := ac.generateVerifiedClient(verifiedClientAddr)
		client.Allowance = big.Add(verifier.Allowance, big.NewInt(1))
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(ac.AddVerifiedClient, client)
		})
		rt.Verify()
	})

	t.Run("fails when verified client already exists", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)

		// add verified client with caller 1
		verifier := ac.generateAndAddVerifier(rt, verifierAddr)
		client := ac.generateVerifiedClient(verifiedClientAddr)
		ac.addVerifiedClient(rt, verifier.Address, client.Address, client.Allowance)

		// add verified client with caller 2
		verifier2 := ac.generateAndAddVerifier(rt, verifierAddr)
		rt.SetCaller(verifier2.Address, builtin.VerifiedRegistryActorCodeID)
		rt.ExpectValidateCallerAny()
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(ac.AddVerifiedClient, client)
		})
		rt.Verify()
	})
}

func TestUseBytes(t *testing.T) {
	root := tutil.NewIDAddr(t, 101)
	verifiedClientAddr := tutil.NewIDAddr(t, 201)
	verifiedClientAddr2 := tutil.NewIDAddr(t, 202)
	verifiedClientAddr3 := tutil.NewIDAddr(t, 203)

	verifierAddr := tutil.NewIDAddr(t, 301)

	t.Run("successfully consume deal bytes for deals from different verified clients", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)

		ca1 := big.Mul(verifreg.MinVerifiedDealSize, big.NewInt(3))
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, verifiedClientAddr, ca1)

		ca2 := big.Mul(verifreg.MinVerifiedDealSize, big.NewInt(2))
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, verifiedClientAddr2, ca2)

		ca3 := big.Add(verifreg.MinVerifiedDealSize, big.NewInt(1))
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, verifiedClientAddr3, ca3)

		// client 1 uses bytes
		dSize := verifreg.MinVerifiedDealSize
		ac.useBytes(rt, verifiedClientAddr, dSize)
		// client 2 uses bytes
		ac.useBytes(rt, verifiedClientAddr2, dSize)
		// client 3 uses bytes
		ac.useBytes(rt, verifiedClientAddr3, dSize)

		// verify cap for all three clients
		require.EqualValues(t, big.Sub(ca1, dSize), ac.getVerifiedClientCap(rt, verifiedClientAddr))
		require.EqualValues(t, big.Sub(ca2, dSize), ac.getVerifiedClientCap(rt, verifiedClientAddr2))
		ac.assertVerifiedClientRemoved(rt, verifiedClientAddr3)

		// client 1 adds a deal and it works
		ac.useBytes(rt, verifiedClientAddr, dSize)
		// client 2 adds a deal and it works
		ac.useBytes(rt, verifiedClientAddr2, dSize)
		require.EqualValues(t, big.Subtract(ca1, dSize, dSize), ac.getVerifiedClientCap(rt, verifiedClientAddr))
		ac.assertVerifiedClientRemoved(rt, verifiedClientAddr2)
	})

	t.Run("successfully consume deal bytes for verified client and then fail on next attempt because it does NOT have enough allowance", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		clientAllowance := big.Sum(verifreg.MinVerifiedDealSize, verifreg.MinVerifiedDealSize, big.NewInt(1))

		// add verified client
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, verifiedClientAddr, clientAllowance)

		// use bytes
		dSize1 := verifreg.MinVerifiedDealSize
		ac.useBytes(rt, verifiedClientAddr, dSize1)

		// fails now because client does NOT have enough capacity for second deal
		dSize2 := big.Add(verifreg.MinVerifiedDealSize, big.NewInt(2))
		rt.ExpectValidateCallerAddr(builtin.StorageMarketActorAddr)
		rt.SetCaller(builtin.StorageMarketActorAddr, builtin.StorageMinerActorCodeID)
		param := &verifreg.UseBytesParams{verifiedClientAddr, dSize2}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(ac.UseBytes, param)
		})

		rt.Verify()
	})

	t.Run("successfully consume deal for verified client and then fail on next attempt because it has been removed", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		clientAllowance := big.Sum(verifreg.MinVerifiedDealSize, big.NewInt(1))

		// add verified client
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, verifiedClientAddr, clientAllowance)

		// use bytes
		dSize1 := verifreg.MinVerifiedDealSize
		ac.useBytes(rt, verifiedClientAddr, dSize1)

		// fails now because client has been removed
		dSize2 := verifreg.MinVerifiedDealSize
		rt.ExpectValidateCallerAddr(builtin.StorageMarketActorAddr)
		rt.SetCaller(builtin.StorageMarketActorAddr, builtin.StorageMinerActorCodeID)
		param := &verifreg.UseBytesParams{verifiedClientAddr, dSize2}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(ac.UseBytes, param)
		})

		rt.Verify()
	})

	t.Run("fail if caller is not storage market actor", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		rt.ExpectValidateCallerAddr(builtin.StorageMarketActorAddr)
		rt.SetCaller(builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID)
		param := &verifreg.UseBytesParams{verifiedClientAddr, verifreg.MinVerifiedDealSize}

		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			rt.Call(ac.UseBytes, param)
		})

		rt.Verify()
	})

	t.Run("fail if deal size is less than min verified deal size", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		dSize2 := big.Sub(verifreg.MinVerifiedDealSize, big.NewInt(1))
		rt.ExpectValidateCallerAddr(builtin.StorageMarketActorAddr)
		rt.SetCaller(builtin.StorageMarketActorAddr, builtin.StorageMinerActorCodeID)
		param := &verifreg.UseBytesParams{verifiedClientAddr, dSize2}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(ac.UseBytes, param)
		})

		rt.Verify()
	})

	t.Run("fail if verified client does not exist", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		dSize2 := verifreg.MinVerifiedDealSize
		rt.ExpectValidateCallerAddr(builtin.StorageMarketActorAddr)
		rt.SetCaller(builtin.StorageMarketActorAddr, builtin.StorageMinerActorCodeID)
		param := &verifreg.UseBytesParams{verifiedClientAddr, dSize2}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(ac.UseBytes, param)
		})

		rt.Verify()
	})

	t.Run("fail if deal size is greater than verified client cap", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		clientAllowance := big.Sum(verifreg.MinVerifiedDealSize, big.NewInt(1))

		// add verified client
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, verifiedClientAddr, clientAllowance)

		// use bytes
		dSize := big.Add(clientAllowance, big.NewInt(1))
		rt.ExpectValidateCallerAddr(builtin.StorageMarketActorAddr)
		rt.SetCaller(builtin.StorageMarketActorAddr, builtin.StorageMinerActorCodeID)
		param := &verifreg.UseBytesParams{verifiedClientAddr, dSize}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(ac.UseBytes, param)
		})

		rt.Verify()
	})
}

func TestRestoreBytes(t *testing.T) {
	root := tutil.NewIDAddr(t, 101)
	verifiedClientAddr := tutil.NewIDAddr(t, 201)
	verifiedClientAddr2 := tutil.NewIDAddr(t, 202)
	verifiedClientAddr3 := tutil.NewIDAddr(t, 203)
	verifierAddr := tutil.NewIDAddr(t, 301)

	t.Run("successfully restore deal bytes for different verified clients", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)

		ca1 := big.Mul(verifreg.MinVerifiedDealSize, big.NewInt(3))
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, verifiedClientAddr, ca1)

		ca2 := big.Mul(verifreg.MinVerifiedDealSize, big.NewInt(2))
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, verifiedClientAddr2, ca2)

		ca3 := big.Add(verifreg.MinVerifiedDealSize, big.NewInt(1))
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, verifiedClientAddr3, ca3)

		// client 1 restores bytes
		dSize := verifreg.MinVerifiedDealSize
		ac.restoreBytes(rt, verifiedClientAddr, dSize)
		// client 2 uses bytes
		ac.restoreBytes(rt, verifiedClientAddr2, dSize)
		// client 3 uses bytes
		ac.restoreBytes(rt, verifiedClientAddr3, dSize)

		// verify cap for all three clients
		bal1 := big.Add(ca1, dSize)
		bal2 := big.Add(ca2, dSize)
		bal3 := big.Add(ca3, dSize)
		require.EqualValues(t, bal1, ac.getVerifiedClientCap(rt, verifiedClientAddr))
		require.EqualValues(t, bal2, ac.getVerifiedClientCap(rt, verifiedClientAddr2))
		require.EqualValues(t, bal3, ac.getVerifiedClientCap(rt, verifiedClientAddr3))

		// client1 and client2 use bytes
		ac.useBytes(rt, verifiedClientAddr, dSize)
		ac.useBytes(rt, verifiedClientAddr2, dSize)

		bal1 = big.Sub(bal1, dSize)
		bal2 = big.Sub(bal2, dSize)
		require.EqualValues(t, bal1, ac.getVerifiedClientCap(rt, verifiedClientAddr))
		require.EqualValues(t, bal2, ac.getVerifiedClientCap(rt, verifiedClientAddr2))
		require.EqualValues(t, bal3, ac.getVerifiedClientCap(rt, verifiedClientAddr3))

		// restore bytes for client1, 2 and 3
		ac.restoreBytes(rt, verifiedClientAddr, dSize)
		ac.restoreBytes(rt, verifiedClientAddr2, dSize)
		ac.restoreBytes(rt, verifiedClientAddr3, dSize)

		bal1 = big.Add(bal1, dSize)
		bal2 = big.Add(bal2, dSize)
		bal3 = big.Add(bal3, dSize)
		require.EqualValues(t, bal1, ac.getVerifiedClientCap(rt, verifiedClientAddr))
		require.EqualValues(t, bal2, ac.getVerifiedClientCap(rt, verifiedClientAddr2))
		require.EqualValues(t, bal3, ac.getVerifiedClientCap(rt, verifiedClientAddr3))
	})

	t.Run("successfully restore bytes after using bytes reduces a client's cap", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		clientAllowance := big.Sum(verifreg.MinVerifiedDealSize, verifreg.MinVerifiedDealSize)

		// add verified client -> use bytes
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, verifiedClientAddr, clientAllowance)
		dSize1 := verifreg.MinVerifiedDealSize
		ac.useBytes(rt, verifiedClientAddr, dSize1)

		sz := verifreg.MinVerifiedDealSize
		ac.restoreBytes(rt, verifiedClientAddr, sz)
	})

	t.Run("successfully restore bytes after using bytes removes a client", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		clientAllowance := big.Sum(verifreg.MinVerifiedDealSize, big.NewInt(1))

		// add verified client -> use bytes -> client is removed
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, verifiedClientAddr, clientAllowance)
		dSize1 := verifreg.MinVerifiedDealSize
		ac.useBytes(rt, verifiedClientAddr, dSize1)
		ac.assertVerifiedClientRemoved(rt, verifiedClientAddr)

		sz := verifreg.MinVerifiedDealSize
		ac.restoreBytes(rt, verifiedClientAddr, sz)
	})

	t.Run("fail if caller is not storage market actor", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		rt.ExpectValidateCallerAddr(builtin.StorageMarketActorAddr)
		rt.SetCaller(builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID)
		param := &verifreg.RestoreBytesParams{verifiedClientAddr, verifreg.MinVerifiedDealSize}

		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			rt.Call(ac.RestoreBytes, param)
		})

		rt.Verify()
	})

	t.Run("fail if deal size is less than min verified deal size", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		dSize2 := big.Sub(verifreg.MinVerifiedDealSize, big.NewInt(1))
		rt.ExpectValidateCallerAddr(builtin.StorageMarketActorAddr)
		rt.SetCaller(builtin.StorageMarketActorAddr, builtin.StorageMinerActorCodeID)
		param := &verifreg.RestoreBytesParams{verifiedClientAddr, dSize2}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(ac.RestoreBytes, param)
		})

		rt.Verify()
	})
}

type verifRegActorTestHarness struct {
	rootkey address.Address
	verifreg.Actor
	t testing.TB
}

func basicVerifRegSetup(t *testing.T, root address.Address) (*mock.Runtime, *verifRegActorTestHarness) {
	builder := mock.NewBuilder(context.Background(), builtin.StorageMarketActorAddr).
		WithCaller(builtin.SystemActorAddr, builtin.InitActorCodeID).
		WithActorType(root, builtin.VerifiedRegistryActorCodeID)

	rt := builder.Build(t)

	actor := verifRegActorTestHarness{t: t, rootkey: root}
	actor.constructAndVerify(rt)

	return rt, &actor
}

func (h *verifRegActorTestHarness) constructAndVerify(rt *mock.Runtime) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	ret := rt.Call(h.Constructor, &h.rootkey)
	require.Nil(h.t, ret)
	rt.Verify()
}

func (h *verifRegActorTestHarness) generateVerifier(a address.Address) *verifreg.AddVerifierParams {
	d := big.Add(abi.NewStoragePower(100), verifreg.MinVerifiedDealSize)

	return &verifreg.AddVerifierParams{Address: a, Allowance: d}
}

func (h *verifRegActorTestHarness) generateVerifiedClient(a address.Address) *verifreg.AddVerifiedClientParams {
	allowance := big.Add(verifreg.MinVerifiedDealSize, big.NewInt(99))

	return &verifreg.AddVerifiedClientParams{a, allowance}
}

func (h *verifRegActorTestHarness) generateAndAddVerifier(rt *mock.Runtime, a address.Address) *verifreg.AddVerifierParams {
	v := h.generateVerifier(a)
	h.addVerifier(rt, v.Address, v.Allowance)
	return v
}

func (h *verifRegActorTestHarness) generateAndAddVerifierAndVerifiedClient(rt *mock.Runtime, verifierAddr address.Address, clientAddr address.Address,
	clientAllowance verifreg.DataCap) {

	// add verifier with greater allowance than client
	verifier := h.generateVerifier(verifierAddr)
	verifier.Allowance = big.Add(verifier.Allowance, clientAllowance)
	h.addVerifier(rt, verifier.Address, verifier.Allowance)

	// add client
	client := h.generateVerifiedClient(clientAddr)
	client.Allowance = clientAllowance
	h.addVerifiedClient(rt, verifier.Address, client.Address, client.Allowance)
}

func (h *verifRegActorTestHarness) addVerifiedClient(rt *mock.Runtime, verifier, client address.Address, allowance verifreg.DataCap) {
	rt.SetCaller(verifier, builtin.VerifiedRegistryActorCodeID)
	rt.ExpectValidateCallerAny()

	params := &verifreg.AddVerifiedClientParams{client, allowance}
	rt.Call(h.AddVerifiedClient, params)
	rt.Verify()

	require.EqualValues(h.t, allowance, h.getVerifiedClientCap(rt, client))
}

func (h *verifRegActorTestHarness) addVerifier(rt *mock.Runtime, verifier address.Address, datacap verifreg.DataCap) {
	param := verifreg.AddVerifierParams{Address: verifier, Allowance: datacap}

	rt.ExpectValidateCallerAddr(h.rootkey)

	rt.SetCaller(h.rootkey, builtin.VerifiedRegistryActorCodeID)
	ret := rt.Call(h.AddVerifier, &param)
	rt.Verify()

	require.Nil(h.t, ret)
	require.EqualValues(h.t, datacap, h.getVerifierCap(rt, verifier))
}

func (h *verifRegActorTestHarness) removeVerifier(rt *mock.Runtime, verifier address.Address) {
	rt.ExpectValidateCallerAddr(h.rootkey)

	rt.SetCaller(h.rootkey, builtin.VerifiedRegistryActorCodeID)
	ret := rt.Call(h.RemoveVerifier, &verifier)
	rt.Verify()

	require.Nil(h.t, ret)
	h.assertVerifierRemoved(rt, verifier)
}

func (h *verifRegActorTestHarness) useBytes(rt *mock.Runtime, a address.Address, dealSize verifreg.DataCap) {
	rt.ExpectValidateCallerAddr(builtin.StorageMarketActorAddr)
	rt.SetCaller(builtin.StorageMarketActorAddr, builtin.StorageMinerActorCodeID)

	// client will be removed if it's cap after the usebytes call is less than MinVerifiedDealSize
	var removed bool
	prev := h.getVerifiedClientCap(rt, a)
	newCap := big.Sub(prev, dealSize)
	if newCap.LessThan(verifreg.MinVerifiedDealSize) {
		removed = true
	}

	param := &verifreg.UseBytesParams{a, dealSize}

	ret := rt.Call(h.UseBytes, param)
	rt.Verify()
	require.Nil(h.t, ret)

	// assert client cap now
	if removed {
		h.assertVerifiedClientRemoved(rt, a)
	} else {
		require.EqualValues(h.t, newCap, h.getVerifiedClientCap(rt, a))
	}
}

func (h *verifRegActorTestHarness) restoreBytes(rt *mock.Runtime, a address.Address, dealSize verifreg.DataCap) {
	rt.ExpectValidateCallerAddr(builtin.StorageMarketActorAddr)
	rt.SetCaller(builtin.StorageMarketActorAddr, builtin.StorageMinerActorCodeID)

	// get current cap -> zero if client does NOT exist
	var prev verifreg.DataCap
	var st verifreg.State
	rt.GetState(&st)
	v, err := adt.AsMap(adt.AsStore(rt), st.VerifiedClients)
	require.NoError(h.t, err)
	var dc verifreg.DataCap
	found, err := v.Get(verifreg.AddrKey(a), &dc)
	require.NoError(h.t, err)
	if found {
		prev = dc
	} else {
		prev = big.Zero()
	}

	// call RestoreBytes
	param := &verifreg.RestoreBytesParams{a, dealSize}
	ret := rt.Call(h.RestoreBytes, param)
	rt.Verify()
	require.Nil(h.t, ret)

	// assert client cap now
	require.EqualValues(h.t, big.Add(prev, dealSize), h.getVerifiedClientCap(rt, a))
}

func (h *verifRegActorTestHarness) getVerifierCap(rt *mock.Runtime, a address.Address) verifreg.DataCap {
	var st verifreg.State
	rt.GetState(&st)

	v, err := adt.AsMap(adt.AsStore(rt), st.Verifiers)
	require.NoError(h.t, err)

	var dc verifreg.DataCap
	found, err := v.Get(verifreg.AddrKey(a), &dc)
	require.NoError(h.t, err)
	require.True(h.t, found)
	return dc
}

func (h *verifRegActorTestHarness) getVerifiedClientCap(rt *mock.Runtime, a address.Address) verifreg.DataCap {
	var st verifreg.State
	rt.GetState(&st)

	v, err := adt.AsMap(adt.AsStore(rt), st.VerifiedClients)
	require.NoError(h.t, err)

	var dc verifreg.DataCap
	found, err := v.Get(verifreg.AddrKey(a), &dc)
	require.NoError(h.t, err)
	require.True(h.t, found)
	return dc
}

func (h *verifRegActorTestHarness) assertVerifierRemoved(rt *mock.Runtime, a address.Address) {
	var st verifreg.State
	rt.GetState(&st)

	v, err := adt.AsMap(adt.AsStore(rt), st.Verifiers)
	require.NoError(h.t, err)

	var dc verifreg.DataCap
	found, err := v.Get(verifreg.AddrKey(a), &dc)
	require.NoError(h.t, err)
	require.False(h.t, found)
}

func (h *verifRegActorTestHarness) assertVerifiedClientRemoved(rt *mock.Runtime, a address.Address) {
	var st verifreg.State
	rt.GetState(&st)

	v, err := adt.AsMap(adt.AsStore(rt), st.VerifiedClients)
	require.NoError(h.t, err)

	var dc verifreg.DataCap
	found, err := v.Get(verifreg.AddrKey(a), &dc)
	require.NoError(h.t, err)
	require.False(h.t, found)
}
