package verifreg

import (
	addr "github.com/filecoin-project/go-address"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	builtin "github.com/filecoin-project/specs-actors/actors/builtin"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	. "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type Actor struct{}

func (a Actor) Exports() []interface{} {
	return []interface{}{
		builtin.MethodConstructor: a.Constructor,
		2:                         a.AddVerifier,
		3:                         a.RemoveVerifier,
		4:                         a.AddVerifiedClient,
		5:                         a.UseBytes,
		6:                         a.RestoreBytes,
	}
}

var _ abi.Invokee = Actor{}

////////////////////////////////////////////////////////////////////////////////
// Actor methods
////////////////////////////////////////////////////////////////////////////////

func (a Actor) Constructor(rt vmr.Runtime, _ *adt.EmptyValue) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.SystemActorAddr)

	emptyMap, err := adt.MakeEmptyMap(adt.AsStore(rt))
	if err != nil {
		rt.Abortf(exitcode.ErrIllegalState, "failed to create verified registry state: %v", err)
	}

	st := ConstructState(emptyMap.Root())
	rt.State().Create(st)
	return nil
}

type AddVerifierParams struct {
	Address   addr.Address
	Allowance DataCap
}

func (a Actor) AddVerifier(rt vmr.Runtime, params *AddVerifierParams) *adt.EmptyValue {
	var st State
	rt.State().Readonly(&st)
	rt.ValidateImmediateCallerIs(st.RootKey) // why: do we need a resolve address here?

	rt.State().Transaction(&st, func() interface{} {
		err := st.PutVerifier(adt.AsStore(rt), params.Address, params.Allowance)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to add verifier: %v", err)
		}
	})

	return nil
}

func (a Actor) RemoveVerifier(rt vmr.Runtime, verifierAddr addr.Address) *adt.EmptyValue {
	var st State
	rt.State().Readonly(&st)
	rt.ValidateImmediateCallerIs(st.RootKey) // why: do we need a resolve address here?

	rt.State().Transaction(&st, func() interface{} {
		err := st.DeleteVerifier(adt.AsStore(rt), verifierAddr)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "failed to delete verifier: %v", err)
		}
	})

	return nil
}

type AddVerifiedClientParams struct {
	Address   addr.Address
	Allowance DataCap
}

func (a Actor) AddVerifiedClient(rt vmr.Runtime, params *AddVerifiedClientParams) *adt.EmptyValue {
	if params.Allowance.LessThanEqual(big.Zero()) {
		rt.Abortf(exitcode.ErrIllegalArgument, "Non-positive allowance %d for add verified client %v", params.Allowance, params.Address)
	}

	var st State
	rt.State().Transaction(&st, func() interface{} {
		// Validate caller is one of the verifiers.
		verifierAddr := rt.Message().Caller()
		verifierCap, found, err := st.GetVerifier(adt.AsStore(rt), verifierAddr)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "Failed to get Verifier for %v", err)
		} else if !found {
			rt.Abortf(exitcode.ErrNotFound, "Invalid verifier %v", verifierAddr)
		}

		// Compute new verifier cap and update.
		newVerifierCap := big.Sub(*verifierCap, params.Allowance)
		if newVerifierCap.LessThan(big.Zero()) {
			rt.Abortf(exitcode.ErrIllegalArgument, "Add more DataCap (%d) for VerifiedClient than allocated %d", params.Allowance, verifierCap)
		}

		err = st.PutVerifier(adt.AsStore(rt), verifierAddr, newVerifierCap)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "Failed to update new verifier cap (%d) for %v", newVerifierCap, verifierAddr)
		}

		// Write-once entry and does not get changed for simplicity.
		// If parties neeed more allowance, they can get another VerifiedClient account.
		// This is a one-time, upfront allocation.
		// Returns error if VerifiedClient already exists.
		_, found, err = st.GetVerifiedClient(adt.AsStore(rt), params.Address)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "Failed to load verified client state for %v", params.Address)
		} else if found {
			rt.Abortf(exitcode.ErrIllegalArgument, "Verified client already exists: %v", params.Address)
		}

		err = st.PutVerifiedClient(adt.AsStore(rt), params.Address, params.Allowance)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "Failed to add verified client %v with cap %d", params.Address, params.Allowance)
		}
	})

	return nil
}

type UseBytesParams struct {
	Address  addr.Address // Address of verified client.
	NumBytes DataCap      // Number of bytes to use.
}

func (a Actor) UseBytes(rt vmr.Runtime, params *UseBytesParams) *abi.StoragePower {
	rt.ValidateImmediateCallerIs(builtin.StorageMarketActorAddr)

	if params.NumBytes.LessThanEqual(big.Zero()) {
		rt.Abortf(exitcode.ErrIllegalArgument, "Negative bytes requested in UseBytes: %d", params.NumBytes)
	}

	var st State
	usedBytes := rt.State().Transaction(&st, func() interface{} {
		vcCap, found, err := st.GetVerifiedClient(adt.AsStore(rt), params.Address)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "Failed to get verified client state for %v", params.Address)
		} else if !found {
			rt.Abortf(exitcode.ErrIllegalArgument, "Invalid address for verified client %v", params.Address)
		}
		Assert(vcCap.GreaterThanEqual(big.Zero()))

		bytesToUse := big.Min(vcCap, params.NumBytes)
		newVcCap := big.Sub(vcCap, bytesToUse)
		// TODO: review this
		if newVcCap.LessThanEqual(big.Zero()) {
			// Delete entry if all DataCap is used.
			// Will be restored if the deal did not go through.
			err = st.DeleteVerifiedClient(adt.AsStore(rt), params.Address)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "Failed to delete verified client %v with cap %d when %d bytes are used.", params.Address, vcCap, params.NumBytes)
			}
		} else {
			err = st.PutVerifiedClient(adt.AsStore(rt), params.Address, newVcCap)
			if err != nil {
				rt.Abortf(exitcode.ErrIllegalState, "Failed to update verified client %v when %d bytes are used.", params.Address, params.NumBytes)
			}
		}

		return bytesToUse
	}).(abi.StoragePower)

	return &usedBytes
}

type RestoreBytesParams struct {
	Address  addr.Address
	NumBytes DataCap
}

func (a Actor) RestoreBytes(rt vmr.Runtime, params *RestoreBytesParams) *adt.EmptyValue {
	rt.ValidateImmediateCallerIs(builtin.StorageMarketActorAddr)

	if params.NumBytes.LessThanEqual(big.Zero()) {
		rt.Abortf(exitcode.ErrIllegalArgument, "Negative bytes requested in RestoreBytes: %d", params.NumBytes)
	}

	var st State
	rt.State().Transaction(&st, func() interface{} {
		vcCap, found, err := st.GetVerifiedClient(adt.AsStore(rt), params.Address)
		if err != nil {
			rt.Abortf(exitcode.ErrIllegalState, "Failed to get verified client state for %v", params.Address)
		}

		if !found {
			vcCap = big.Zero()
		}

		newVcCap := big.Add(vcCap, params.NumBytes)
		err = st.PutVerifiedClient(adt.AsStore(rt), params.Address, newVcCap)
	})

	return nil
}
