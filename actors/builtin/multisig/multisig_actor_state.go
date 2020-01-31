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

// return nil if MultiSig maintains required locked balance after spending the amount, else return an error.
func (st *MultiSigActorState) assertAvailable(currBalance abi.TokenAmount, amountToSpend abi.TokenAmount, currEpoch abi.ChainEpoch) error {
	if amountToSpend.LessThan(big.Zero()) {
		return errors.Errorf("amount to spend %s less than zero", amountToSpend.String())
	}
	if currBalance.LessThan(amountToSpend) {
		return errors.Errorf("current balance %s less than amount to spend %s", currBalance.String(), amountToSpend.String())
	}

	remainingBalance := big.Sub(currBalance, amountToSpend)
	amountLocked := st.AmountLocked(currEpoch - st.StartEpoch)
	if remainingBalance.LessThan(amountLocked) {
		return errors.Errorf("actor balance if spent %s would be less than required locked amount %s", remainingBalance.String(), amountLocked.String())
	}

	return nil
}

func (as *MultiSigActorState) getPendingTransaction(s adt.Store, txnID TxnID) (MultiSigTransaction, error) {
	hm := adt.AsMap(s, as.PendingTxns)

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
	hm := adt.AsMap(s, as.PendingTxns)

	if err := hm.Put(txnID, &txn); err != nil {
		return errors.Wrapf(err, "failed to write transaction")
	}

	as.PendingTxns = hm.Root()
	return nil
}

func (as *MultiSigActorState) deletePendingTransaction(s adt.Store, txnID TxnID) error {
	hm := adt.AsMap(s, as.PendingTxns)

	if err := hm.Delete(txnID); err != nil {
		return errors.Wrapf(err, "failed to delete transaction")
	}

	as.PendingTxns = hm.Root()
	return nil
}
