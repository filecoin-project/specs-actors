package cron

import (
	addr "github.com/filecoin-project/go-address"

	"github.com/filecoin-project/specs-actors/actors/abi"
)

type CronActorState struct {
	Entries []CronTableEntry
}

type CronTableEntry struct {
	Receiver  addr.Address  // The actor to call (must be an ID-address)
	MethodNum abi.MethodNum // The method number to call (must accept empty parameters)
}

func ConstructState(entries []CronTableEntry) *CronActorState {
	return &CronActorState{Entries: entries}
}
