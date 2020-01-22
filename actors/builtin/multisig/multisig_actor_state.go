package multisig

import (
	"github.com/filecoin-project/go-address"
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/bigint"
	"github.com/ipfs/go-cid"
)

type MultiSigActorState struct {
	AuthorizedParties     []address.Address
	NumApprovalsThreshold int64
	NextTxnID             TxnID

	// Linear unlock
	InitialBalance abi.TokenAmount
	StartEpoch     abi.ChainEpoch
	UnlockDuration abi.ChainEpoch

	PendingTxns cid.Cid
}

func (st *MultiSigActorState) AmountLocked(elapsedEpoch abi.ChainEpoch) abi.TokenAmount {
	if elapsedEpoch >= st.UnlockDuration {
		return abi.NewTokenAmount(0)
	}

	lockedProportion := (st.UnlockDuration - elapsedEpoch) / st.UnlockDuration
	return big.BigMul(st.InitialBalance, big.NewInt(int64(lockedProportion)))
}

func (st *MultiSigActorState) isAuthorizedParty(party address.Address) bool {
	for _, ap := range st.AuthorizedParties {
		if party == ap {
			return true
		}
	}
	return false
}

// return true if MultiSig maintains required locked balance after spending the amount
func (st *MultiSigActorState) _hasAvailable(currBalance abi.TokenAmount, amountToSpend abi.TokenAmount, currEpoch abi.ChainEpoch) bool {
	if amountToSpend.LessThan(big.NewInt(0)) || currBalance.LessThan(amountToSpend) {
		return false
	}

	if big.BigSub(currBalance, amountToSpend).LessThan(st.AmountLocked(currEpoch - st.StartEpoch)) {
		return false
	}

	return true
}
