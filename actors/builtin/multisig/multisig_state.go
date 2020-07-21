package multisig

import (
	address "github.com/filecoin-project/go-address"
	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
	cid "github.com/ipfs/go-cid"
	errors "github.com/pkg/errors"
	"golang.org/x/xerrors"
)

type State struct {
	// Signers may be either public-key or actor ID-addresses. The ID address is canonical, but doesn't exist
	// for a public key that has not yet received a message on chain.
	// If any signer address is a public-key address, it will be resolved to an ID address and persisted
	// in this state when the address is used.
	Signers               []address.Address
	NumApprovalsThreshold uint64
	NextTxnID             TxnID

	// Linear unlock
	InitialBalance abi.TokenAmount
	StartEpoch     abi.ChainEpoch
	UnlockDuration abi.ChainEpoch

	PendingTxns cid.Cid
}

func (st *State) AmountLocked(elapsedEpoch abi.ChainEpoch) abi.TokenAmount {
	if elapsedEpoch >= st.UnlockDuration {
		return abi.NewTokenAmount(0)
	}

	unitLocked := big.Div(st.InitialBalance, big.NewInt(int64(st.UnlockDuration)))
	return big.Mul(unitLocked, big.Sub(big.NewInt(int64(st.UnlockDuration)), big.NewInt(int64(elapsedEpoch))))
}

// return nil if MultiSig maintains required locked balance after spending the amount, else return an error.
func (st *State) assertAvailable(currBalance abi.TokenAmount, amountToSpend abi.TokenAmount, currEpoch abi.ChainEpoch) error {
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

func getPendingTransaction(ptx *adt.Map, txnID TxnID) (Transaction, error) {
	var out Transaction
	found, err := ptx.Get(txnID, &out)
	if err != nil {
		return Transaction{}, errors.Wrapf(err, "failed to read transaction")
	}
	if !found {
		return Transaction{}, xerrors.Errorf("failed to find transaction %v", txnID)
	}

	return out, nil
}
