package multisig

import (
	address "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	errors "github.com/pkg/errors"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
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

func (as *MultiSigActorState) getPendingTransaction(s adt.Store, txnID TxnID) (MultiSigTransaction, error) {
	hm := adt.NewMap(s, as.PendingTxns)

	var out MultiSigTransaction
	found, err := hm.Get(txnID, &out)
	if err != nil {
		return MultiSigTransaction{}, errors.Wrapf(err, "failed to read transaction")
	}
	if !found {
		return MultiSigTransaction{}, errors.Errorf("failed to find transaction %v in HAMT %s", txnID, as.PendingTxns)
	}

	as.PendingTxns = hm.Root()
	return out, nil
}

func (as *MultiSigActorState) putPendingTransaction(s adt.Store, txnID TxnID, txn MultiSigTransaction) error {
	hm := adt.NewMap(s, as.PendingTxns)

	if err := hm.Put(txnID, &txn); err != nil {
		return errors.Wrapf(err, "failed to write transaction")
	}

	as.PendingTxns = hm.Root()
	return nil
}

func (as *MultiSigActorState) deletePendingTransaction(s adt.Store, txnID TxnID) error {
	hm := adt.NewMap(s, as.PendingTxns)

	if err := hm.Delete(txnID); err != nil {
		return errors.Wrapf(err, "failed to delete transaction")
	}

	as.PendingTxns = hm.Root()
	return nil
}
