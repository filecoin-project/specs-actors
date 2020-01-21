package storage_power

import (
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	autil "github.com/filecoin-project/specs-actors/actors/util"
)

type BalanceTableHAMT = autil.BalanceTableHAMT
type SectorStorageWeightDesc = autil.SectorStorageWeightDesc
type SectorTerminationType = autil.SectorTermination

////////////////////////////////////////////////////////////////////////////////
// Boilerplate
//
// This boilerplate should be essentially identical for all actors, and
// conceptually belongs in the runtime/VM. It is only duplicated here as a
// workaround due to the lack of generics support in Go.
////////////////////////////////////////////////////////////////////////////////

type Runtime = vmr.Runtime

var Assert = autil.Assert
var IMPL_FINISH = autil.IMPL_FINISH
var TODO = autil.TODO

func Release(rt Runtime, h vmr.ActorStateHandle, st StoragePowerActorState) {
	checkCID := abi.ActorSubstateCID(rt.IpldPut(&st))
	h.Release(checkCID)
}

func UpdateRelease(rt Runtime, h vmr.ActorStateHandle, st StoragePowerActorState) {
	newCID := abi.ActorSubstateCID(rt.IpldPut(&st))
	h.UpdateRelease(newCID)
}
