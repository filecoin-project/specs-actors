package account

import (
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
)

type StateSummary struct {
	PubKeyAddr address.Address
}

// Checks internal invariants of account state.
func CheckStateInvariants(st *State, idAddr address.Address) (*StateSummary, *builtin.MessageAccumulator, error) {
	acc := &builtin.MessageAccumulator{}

	id, err := address.IDFromAddress(idAddr)
	if err != nil {
		return nil, acc, err
	}
	if id >= builtin.FirstNonSingletonActorId {
		acc.Require(
			st.Address.Protocol() == address.BLS || st.Address.Protocol() == address.SECP256K1,
			"actor address %v must be BLS or SECP256K1 protocol", st.Address)
	}

	return &StateSummary{
		PubKeyAddr: st.Address,
	}, acc, nil
}
