package market

import (
	"io"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	. "github.com/filecoin-project/specs-actors/v8/actors/util/adt"
	"golang.org/x/xerrors"

	"github.com/ipfs/go-cid"
)

///// Deal proposal array /////

// A specialization of a array to deals.
// It is an error to query for a key that doesn't exist.
type DealArray struct {
	*Array
}

// Interprets a store as balance table with root `r`.
func AsDealProposalArray(s Store, r cid.Cid) (*DealArray, error) {
	a, err := AsArray(s, r, ProposalsAmtBitwidth)
	if err != nil {
		return nil, err
	}
	return &DealArray{a}, nil
}

// Returns the root cid of underlying AMT.
func (t *DealArray) Root() (cid.Cid, error) {
	return t.Array.Root()
}

// Gets the deal for a key. The entry must have been previously initialized.
func (t *DealArray) Get(id abi.DealID) (*DealProposal, bool, error) {
	var value DealProposal
	found, err := t.Array.Get(uint64(id), &value)
	return &value, found, err
}

func (t *DealArray) Set(k abi.DealID, value *DealProposal) error {
	return t.Array.Set(uint64(k), value)
}

func (t *DealArray) Delete(id abi.DealID) error {
	return t.Array.Delete(uint64(id))
}

///// Deal state array /////

// A specialization of a array to deals.
// It is an error to query for a key that doesn't exist.
type DealMetaArray struct {
	*Array
}

type DealState struct {
	SectorStartEpoch abi.ChainEpoch // -1 if not yet included in proven sector
	LastUpdatedEpoch abi.ChainEpoch // -1 if deal state never updated
	SlashEpoch       abi.ChainEpoch // -1 if deal never slashed
}

// Interprets a store as balance table with root `r`.
func AsDealStateArray(s Store, r cid.Cid) (*DealMetaArray, error) {
	dsa, err := AsArray(s, r, StatesAmtBitwidth)
	if err != nil {
		return nil, err
	}

	return &DealMetaArray{dsa}, nil
}

// Returns the root cid of underlying AMT.
func (t *DealMetaArray) Root() (cid.Cid, error) {
	return t.Array.Root()
}

// Gets the deal for a key. The entry must have been previously initialized.
func (t *DealMetaArray) Get(id abi.DealID) (*DealState, bool, error) {
	var value DealState
	found, err := t.Array.Get(uint64(id), &value)
	if err != nil {
		return nil, false, err // The errors from Map carry good information, no need to wrap here.
	}
	if !found {
		return &DealState{
			SectorStartEpoch: epochUndefined,
			LastUpdatedEpoch: epochUndefined,
			SlashEpoch:       epochUndefined,
		}, false, nil
	}
	return &value, true, nil
}

func (t *DealMetaArray) Set(k abi.DealID, value *DealState) error {
	return t.Array.Set(uint64(k), value)
}

func (t *DealMetaArray) Delete(id abi.DealID) error {
	return t.Array.Delete(uint64(id))
}

///// Provider verified claims map /////

// A specialization of a map to provider verified claims.
// It is an error to query for a key that doesn't exist.
type ProviderVerifiedClaims struct {
	*Map
}

type ProviderVerifiedClaim struct {
	// The epoch until which rewards were last processed for the provider.
	LastClaimEpoch abi.ChainEpoch
	// The provider's active verified deal space at the end of LastClaimEpoch.
	VerifiedDealSpace abi.StoragePower
	// Events since LastClaimEpoch, keyed by epoch.
	DeltaQueue cid.Cid // AMT[epoch]big.Int
}

// Interprets a store as a provider verified claims map with root `r`.
func AsProviderVerifiedClaims(s Store, r cid.Cid) (*ProviderVerifiedClaims, error) {
	pvc, err := AsMap(s, r, ProviderVerifiedClaimsHamtBitwidth)
	if err != nil {
		return nil, err
	}
	return &ProviderVerifiedClaims{pvc}, nil
}

func (pvc *ProviderVerifiedClaims) Add(s Store, provider address.Address, epoch abi.ChainEpoch, size abi.StoragePower) error {
	var providerClaim ProviderVerifiedClaim
	if found, err := pvc.Get(abi.AddrKey(provider), &providerClaim); err != nil {
		return err
	} else if !found {
		return xerrors.Errorf("no verified claim entry for %v", provider)
	}
	deltaQueue, err := AsStoragePowerDeltaQueue(s, providerClaim.DeltaQueue)
	if err != nil {
		return err
	}

	if err = deltaQueue.Add(epoch, size); err != nil {
		return err
	}
	if providerClaim.DeltaQueue, err = deltaQueue.Root(); err != nil {
		return err
	}
	if err = pvc.Put(abi.AddrKey(provider), &providerClaim); err != nil {
		return err
	}
	return nil
}

///// Provider event queue array /////

// A provider event queue is an ordered collection of events keyed by epoch, implemented as an AMT with
// array values (supporting multiple events at one epoch).
// An array value will be efficient while the number of events per epoch is small, or written in batch.
type ProviderEventQueue struct {
	*Array
}

//type ProviderEventQueueEntry struct {
//	Events []VerifiedDealEvent
//}
//
//type VerifiedDealEvent struct {
//	EventType     VerifiedDealEventType
//	VerifiedSpace abi.StoragePower
//}
//
//// Interprets a store as a provider event queue with root `r`.
//func AsProviderEventQueue(s Store, r cid.Cid) (*ProviderEventQueue, error) {
//	array, err := AsArray(s, r, ProviderEventQueueAmtBitwidth)
//	if err != nil {
//		return nil, err
//	}
//	return &ProviderEventQueue{array}, nil
//}
//
//func (q *ProviderEventQueue) Enqueue(epoch abi.ChainEpoch, eventType VerifiedDealEventType, size abi.StoragePower) error {
//	var queueEntry ProviderEventQueueEntry
//	_, err := q.Get(uint64(epoch), &queueEntry)
//	if err != nil {
//		return err
//	}
//
//	queueEntry.Events = append(queueEntry.Events, VerifiedDealEvent{
//		EventType:     eventType,
//		VerifiedSpace: size,
//	})
//
//	if err := q.Set(uint64(epoch), &queueEntry); err != nil {
//		return err
//	}
//	return nil
//}

///// Storage power delta queue array /////

type StoragePowerDeltaQueue struct {
	*Array
}

// Interprets a store as a queue with root `r`.
func AsStoragePowerDeltaQueue(s Store, r cid.Cid) (*StoragePowerDeltaQueue, error) {
	array, err := AsArray(s, r, StoragePowerDeltaQueueAmtBitwidth)
	if err != nil {
		return nil, err
	}
	return &StoragePowerDeltaQueue{array}, nil
}

// Adds `size` to the entry at `epoch`. The entry is taken to be zero if absent.
func (q *StoragePowerDeltaQueue) Add(epoch abi.ChainEpoch, size abi.StoragePower) error {
	queueEntry := big.Zero()
	_, err := q.Get(uint64(epoch), &queueEntry)
	if err != nil {
		return err
	}

	queueEntry = big.Add(queueEntry, size)

	if err := q.Set(uint64(epoch), &queueEntry); err != nil {
		return err
	}
	return nil
}

///// Verified reward history array ///

type VerifiedRewardHistory struct {
	*Array
}

type VerifiedRewardHistoryEntry struct {
	TotalVerifiedSpace abi.StoragePower
	TotalReward        abi.TokenAmount
}

func AsVerifiedRewardHistory(s Store, r cid.Cid) (*VerifiedRewardHistory, error) {
	array, err := AsArray(s, r, VerifiedRewardsHistoryAmtBitwidth)
	if err != nil {
		return nil, err
	}
	return &VerifiedRewardHistory{array}, nil
}

func (a *VerifiedRewardHistory) Set(epoch abi.ChainEpoch, totalVerifiedSpace abi.StoragePower, totalReward abi.TokenAmount) error {
	value := VerifiedRewardHistoryEntry{
		TotalVerifiedSpace: totalVerifiedSpace,
		TotalReward:        totalReward,
	}
	return a.Array.Set(uint64(epoch), &value)
}

//func (p ProviderEventQueueEntry) MarshalCBOR(w io.Writer) error {
//	panic("implement me") // FIXME
//}
//
//func (p ProviderEventQueueEntry) UnmarshalCBOR(r io.Reader) error {
//	panic("implement me") // FIXME
//}

func (p ProviderVerifiedClaim) UnmarshalCBOR(r io.Reader) error {
	panic("implement me") // FIXME
}

func (p ProviderVerifiedClaim) MarshalCBOR(w io.Writer) error {
	panic("implement me") // FIXME
}

func (v VerifiedRewardHistoryEntry) MarshalCBOR(w io.Writer) error {
	panic("implement me") // FIXME
}
