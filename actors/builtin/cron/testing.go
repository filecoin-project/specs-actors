package cron

import (
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type StateSummary struct {
	EntryCount int
}

// Checks internal invariants of cron state.
func CheckStateInvariants(st *State, store adt.Store) (*StateSummary, *builtin.MessageAccumulator, error) {
	acc := &builtin.MessageAccumulator{}
	for i, e := range st.Entries {
		acc.Require(e.Receiver.Protocol() == address.ID, "entry %d receiver address %v must be ID protocol", i, e.Receiver)
		acc.Require(e.MethodNum > 0, "entry %d has invalid method number %d", i, e.MethodNum)
	}

	return &StateSummary{
		EntryCount: len(st.Entries),
	}, acc, nil
}
