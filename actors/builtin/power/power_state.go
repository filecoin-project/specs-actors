package power

import (
	"fmt"
	"reflect"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	errors "github.com/pkg/errors"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	. "github.com/filecoin-project/specs-actors/actors/util"
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
)

type State struct {
	TotalRawBytePower abi.StoragePower
	// TotalBytesCommitted includes claims from miners below min power threshold
	TotalBytesCommitted  abi.StoragePower
	TotalQualityAdjPower abi.StoragePower
	// TotalQABytesCommitted includes claims from miners below min power threshold
	TotalQABytesCommitted abi.StoragePower
	TotalPledgeCollateral abi.TokenAmount
	MinerCount            int64
	// Number of miners having proven the minimum consensus power.
	MinerAboveMinPowerCount int64

	// A queue of events to be triggered by cron, indexed by epoch.
	CronEventQueue cid.Cid // Multimap, (HAMT[ChainEpoch]AMT[CronEvent]

	// First epoch in which a cron task may be stored.
	// Cron will iterate every epoch between this and the current epoch inclusively to find tasks to execute.
	FirstCronEpoch abi.ChainEpoch

	// Claimed power for each miner.
	Claims cid.Cid // Map, HAMT[address]Claim

	ProofValidationBatch *cid.Cid
}

type Claim struct {
	// Sum of raw byte power for a miner's sectors.
	RawBytePower abi.StoragePower

	// Sum of quality adjusted power for a miner's sectors.
	QualityAdjPower abi.StoragePower
}

type CronEvent struct {
	MinerAddr       addr.Address
	CallbackPayload []byte
}

type AddrKey = adt.AddrKey

func ConstructState(emptyMapCid, emptyMMapCid cid.Cid) *State {
	return &State{
		TotalRawBytePower:       abi.NewStoragePower(0),
		TotalBytesCommitted:     abi.NewStoragePower(0),
		TotalQualityAdjPower:    abi.NewStoragePower(0),
		TotalQABytesCommitted:   abi.NewStoragePower(0),
		TotalPledgeCollateral:   abi.NewTokenAmount(0),
		FirstCronEpoch:          0,
		CronEventQueue:          emptyMapCid,
		Claims:                  emptyMapCid,
		MinerCount:              0,
		MinerAboveMinPowerCount: 0,
	}
}

// Parameters may be negative to subtract.
func (st *State) AddToClaim(s adt.Store, miner addr.Address, power abi.StoragePower, qapower abi.StoragePower) error {
	m, err := st.mutator(s).withClaims(adt.WritePermission).build()
	if err != nil {
		return fmt.Errorf("failed to load state: %w", err)
	}

	if err := m.addToClaim(miner, power, qapower); err != nil {
		return fmt.Errorf("faield to add to claim: %w", err)
	}

	if err := m.commitState(); err != nil {
		return fmt.Errorf("failed to flush state: %w", err)
	}
	return nil
}

func (m *stateMutator) addToClaim(miner addr.Address, power abi.StoragePower, qapower abi.StoragePower) error {
	oldClaim, ok, err := m.getClaim(miner)
	if err != nil {
		return fmt.Errorf("failed to get claim: %w", err)
	}
	if !ok {
		return errors.Errorf("no claim for actor %v", miner)
	}

	// TotalBytes always update directly
	m.st.TotalQABytesCommitted = big.Add(m.st.TotalQABytesCommitted, qapower)
	m.st.TotalBytesCommitted = big.Add(m.st.TotalBytesCommitted, power)

	newClaim := Claim{
		RawBytePower:    big.Add(oldClaim.RawBytePower, power),
		QualityAdjPower: big.Add(oldClaim.QualityAdjPower, qapower),
	}

	prevBelow := oldClaim.QualityAdjPower.LessThan(ConsensusMinerMinPower)
	stillBelow := newClaim.QualityAdjPower.LessThan(ConsensusMinerMinPower)

	if prevBelow && !stillBelow {
		// just passed min miner size
		m.st.MinerAboveMinPowerCount++
		m.st.TotalQualityAdjPower = big.Add(m.st.TotalQualityAdjPower, newClaim.QualityAdjPower)
		m.st.TotalRawBytePower = big.Add(m.st.TotalRawBytePower, newClaim.RawBytePower)
	} else if !prevBelow && stillBelow {
		// just went below min miner size
		m.st.MinerAboveMinPowerCount--
		m.st.TotalQualityAdjPower = big.Sub(m.st.TotalQualityAdjPower, oldClaim.QualityAdjPower)
		m.st.TotalRawBytePower = big.Sub(m.st.TotalRawBytePower, oldClaim.RawBytePower)
	} else if !prevBelow && !stillBelow {
		// Was above the threshold, still above
		m.st.TotalQualityAdjPower = big.Add(m.st.TotalQualityAdjPower, qapower)
		m.st.TotalRawBytePower = big.Add(m.st.TotalRawBytePower, power)
	}

	AssertMsg(newClaim.RawBytePower.GreaterThanEqual(big.Zero()), "negative claimed raw byte power: %v", newClaim.RawBytePower)
	AssertMsg(newClaim.QualityAdjPower.GreaterThanEqual(big.Zero()), "negative claimed quality adjusted power: %v", newClaim.QualityAdjPower)
	AssertMsg(m.st.MinerAboveMinPowerCount >= 0, "negative number of miners larger than min: %v", m.st.MinerAboveMinPowerCount)
	return m.setClaim(miner, &newClaim)
}

func (m *stateMutator) getClaim(a addr.Address) (*Claim, bool, error) {
	var out Claim
	found, err := m.claims.Get(AddrKey(a), &out)
	if err != nil {
		return nil, false, errors.Wrapf(err, "failed to get claim for address %v", a)
	}
	if !found {
		return nil, false, nil
	}
	return &out, true, nil
}

func (st *State) addPledgeTotal(amount abi.TokenAmount) {
	st.TotalPledgeCollateral = big.Add(st.TotalPledgeCollateral, amount)
	Assert(st.TotalPledgeCollateral.GreaterThanEqual(big.Zero()))
}

func (m *stateMutator) appendCronEvent(epoch abi.ChainEpoch, event *CronEvent) error {
	// if event is in past, alter FirstCronEpoch so it will be found.
	if epoch < m.st.FirstCronEpoch {
		m.st.FirstCronEpoch = epoch
	}

	if err := m.cronEventQueue.Add(epochKey(epoch), event); err != nil {
		return errors.Wrapf(err, "failed to store cron event at epoch %v for miner %v", epoch, event)
	}

	return nil
}

func (m *stateMutator) loadCronEvents(epoch abi.ChainEpoch) ([]CronEvent, error) {
	var events []CronEvent
	var ev CronEvent
	err := m.cronEventQueue.ForEach(epochKey(epoch), &ev, func(i int64) error {
		events = append(events, ev)
		return nil
	})
	return events, err
}

func (m *stateMutator) clearCronEvents(epoch abi.ChainEpoch) error {
	err := m.cronEventQueue.RemoveAll(epochKey(epoch))
	if err != nil {
		return errors.Wrapf(err, "failed to clear cron events")
	}

	return nil
}

func (m *stateMutator) setClaim(a addr.Address, claim *Claim) error {
	Assert(claim.RawBytePower.GreaterThanEqual(big.Zero()))
	Assert(claim.QualityAdjPower.GreaterThanEqual(big.Zero()))

	if err := m.claims.Put(AddrKey(a), claim); err != nil {
		return errors.Wrapf(err, "failed to put claim with address %s power %v", a, claim)
	}

	return nil
}

// CurrentTotalPower returns current power values accounting for minimum miner
// and minimum power
func CurrentTotalPower(st *State) (abi.StoragePower, abi.StoragePower) {
	if st.MinerAboveMinPowerCount < ConsensusMinerMinMiners {
		return st.TotalBytesCommitted, st.TotalQABytesCommitted
	}
	return st.TotalRawBytePower, st.TotalQualityAdjPower
}

func epochKey(e abi.ChainEpoch) adt.Keyer {
	return adt.IntKey(int64(e))
}

func init() {
	// Check that ChainEpoch is indeed a signed integer to confirm that epochKey is making the right interpretation.
	var e abi.ChainEpoch
	if reflect.TypeOf(e).Kind() != reflect.Int64 {
		panic("incorrect chain epoch encoding")
	}
}

func (st *State) mutator(store adt.Store) *stateMutator {
	return &stateMutator{st: st, store: store}
}

type stateMutator struct {
	st    *State
	store adt.Store

	proofValidPermit     adt.MutationPermission
	proofValidationBatch *adt.Multimap

	claimsPermit adt.MutationPermission
	claims       *adt.Map

	cronEventPermit adt.MutationPermission
	cronEventQueue  *adt.Multimap
}

func (m *stateMutator) withProofValidationBatch(permit adt.MutationPermission) *stateMutator {
	m.proofValidPermit = permit
	return m
}

func (m *stateMutator) withClaims(permit adt.MutationPermission) *stateMutator {
	m.claimsPermit = permit
	return m
}

func (m *stateMutator) withCronEventQueue(permit adt.MutationPermission) *stateMutator {
	m.cronEventPermit = permit
	return m
}

func (m *stateMutator) build() (*stateMutator, error) {
	if m.proofValidPermit != adt.InvalidPermission {
		if m.st.ProofValidationBatch == nil {
			m.proofValidationBatch = adt.MakeEmptyMultimap(m.store)
		} else {
			proofValidationBatch, err := adt.AsMultimap(m.store, *m.st.ProofValidationBatch)
			if err != nil {
				return nil, fmt.Errorf("failed to load ProofValidationBatch: %w", err)
			}
			m.proofValidationBatch = proofValidationBatch
		}
	}

	if m.claimsPermit != adt.InvalidPermission {
		claims, err := adt.AsMap(m.store, m.st.Claims)
		if err != nil {
			return nil, fmt.Errorf("failed to load claims: %w", err)
		}
		m.claims = claims
	}

	if m.cronEventPermit != adt.InvalidPermission {
		cron, err := adt.AsMultimap(m.store, m.st.CronEventQueue)
		if err != nil {
			return nil, fmt.Errorf("failed to load CronEventQueue: %w", err)
		}
		m.cronEventQueue = cron
	}

	return m, nil
}

func (m *stateMutator) commitState() error {
	var err error
	if m.proofValidPermit == adt.WritePermission {
		cid, err := m.proofValidationBatch.Root()
		if err != nil {
			return fmt.Errorf("failed to flush proofValidationBatch: %w", err)
		}
		m.st.ProofValidationBatch = &cid
	}

	if m.claimsPermit == adt.WritePermission {
		if m.st.Claims, err = m.claims.Root(); err != nil {
			return fmt.Errorf("failed to flush claims: %w", err)
		}
	}

	if m.cronEventPermit == adt.WritePermission {
		if m.st.CronEventQueue, err = m.cronEventQueue.Root(); err != nil {
			return fmt.Errorf("failed to flush CronEventQueue: %w", err)
		}
	}

	return nil
}
