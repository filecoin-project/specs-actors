package multisig

import (
	address "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type MultiSigActorState struct {
	Signers               []address.Address
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

	unitLocked := big.Div(st.InitialBalance, big.NewInt(int64(st.UnlockDuration)))
	return big.Mul(unitLocked, big.Sub(big.NewInt(int64(st.UnlockDuration)), big.NewInt(int64(elapsedEpoch))))
}

func (st *MultiSigActorState) isSigner(party address.Address) bool {
	for _, ap := range st.Signers {
		if party == ap {
			return true
		}
	}
	return false
}

// return true if MultiSig maintains required locked balance after spending the amount
func (st *MultiSigActorState) _hasAvailable(currBalance abi.TokenAmount, amountToSpend abi.TokenAmount, currEpoch abi.ChainEpoch) bool {
	if amountToSpend.LessThan(big.Zero()) || currBalance.LessThan(amountToSpend) {
		return false
	}

	if big.Sub(currBalance, amountToSpend).LessThan(st.AmountLocked(currEpoch - st.StartEpoch)) {
		return false
	}

	return true
}

func (as *MultiSigActorState) getPendingTransaction(rt vmr.Runtime, txnID TxnID) MultiSigTransaction {
	hm := adt.NewMap(adt.AsStore(rt), as.PendingTxns)

	var out MultiSigTransaction
	err := hm.Get(txnID, &out)
	requireNoStateErr(rt, err, "failed to get transaction %v from pending transaction HAMT", txnID)

	as.PendingTxns = hm.Root()
	return out
}

func (as *MultiSigActorState) putPendingTransaction(rt vmr.Runtime, txnID TxnID, txn MultiSigTransaction) {
	hm := adt.NewMap(adt.AsStore(rt), as.PendingTxns)

	err := hm.Put(txnID, &txn)
	requireNoStateErr(rt, err, "failed to put transaction %v into pending transaction HAMT", txnID)

	as.PendingTxns = hm.Root()
}

func (as *MultiSigActorState) deletePendingTransaction(rt vmr.Runtime, txnID TxnID) {
	hm := adt.NewMap(adt.AsStore(rt), as.PendingTxns)

	err := hm.Delete(txnID)
	requireNoStateErr(rt, err, "failed to remove transaction %v from pending transaction HAMT", txnID)

	as.PendingTxns = hm.Root()
}

func requireNoStateErr(rt vmr.Runtime, err error, msg string, args ...interface{}) {
	if err != nil {
		errMsg := msg + " :" + err.Error()
		rt.Abort(exitcode.ErrIllegalState, errMsg, args...)
	}
}
