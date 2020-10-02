package account

import (
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type StateSummary struct {
	PubKeyAddr address.Address
}

// Checks internal invariants of account state.
func CheckStateInvariants(st *State, store adt.Store) (*StateSummary, *builtin.MessageAccumulator, error) {
	acc := &builtin.MessageAccumulator{}
	acc.Require(
		st.Address.Protocol() == address.BLS || st.Address.Protocol() == address.SECP256K1,
		"actor address %v must be BLS or SECP256K1 protocol", st.Address)

	return &StateSummary{
		PubKeyAddr: st.Address,
	}, acc, nil
}
