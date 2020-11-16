package agent

import (
	"container/heap"
	"crypto/sha256"
	"fmt"
	"github.com/filecoin-project/go-state-types/dline"
	"github.com/pkg/errors"
	"math/rand"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"
)

type MinerAgentConfig struct {
	PrecommitRate   float64                 // average number of PreCommits per epoch
	ProofType       abi.RegisteredSealProof // seal proof type for this miner
	StartingBalance abi.TokenAmount         // initial actor balance for miner actor
	FaultRate       float64                 // rate at which committed sectors go faulty (faults per committed sector per epoch)
	RecoveryRate    float64                 // rate at which faults are recovered (recoveries per fault per epoch)
}

type MinerGenerator struct {
	config            MinerAgentConfig // eventually this should become a set of probabilities to support miner differentiation
	createMinerEvents *RateIterator
	minersCreated     int
	accounts          []address.Address
	rnd               *rand.Rand
}

func NewMinerGenerator(accounts []address.Address, config MinerAgentConfig, createMinerRate float64, rndSeed int64) *MinerGenerator {
	rnd := rand.New(rand.NewSource(rndSeed))
	return &MinerGenerator{
		config:            config,
		createMinerEvents: NewRateIterator(createMinerRate, rnd.Int63()),
		accounts:          accounts,
		rnd:               rnd,
	}
}

func (mg *MinerGenerator) Tick(_ SimState) ([]message, error) {
	var msgs []message
	if mg.minersCreated >= len(mg.accounts) {
		return msgs, nil
	}

	err := mg.createMinerEvents.Tick(func() error {
		if mg.minersCreated < len(mg.accounts) {
			addr := mg.accounts[mg.minersCreated]
			mg.minersCreated++
			msgs = append(msgs, mg.createMiner(addr, mg.config))
		}
		return nil
	})
	return msgs, err
}

func (mg *MinerGenerator) createMiner(owner address.Address, cfg MinerAgentConfig) message {
	return message{
		From:   owner,
		To:     builtin.StoragePowerActorAddr,
		Value:  mg.config.StartingBalance, // miner gets all account funds
		Method: builtin.MethodsPower.CreateMiner,
		Params: &power.CreateMinerParams{
			Owner:         owner,
			Worker:        owner,
			SealProofType: cfg.ProofType,
		},
		ReturnHandler: func(s SimState, msg message, ret cbor.Marshaler) error {
			createMinerRet, ok := ret.(*power.CreateMinerReturn)
			if !ok {
				return errors.Errorf("create miner return has wrong type: %v", ret)
			}

			params := msg.Params.(*power.CreateMinerParams)
			if !ok {
				return errors.Errorf("create miner params has wrong type: %v", msg.Params)
			}

			s.AddAgent(NewMinerAgent(params.Owner, params.Worker, createMinerRet.IDAddress, createMinerRet.RobustAddress, mg.rnd.Int63(), cfg))
			return nil
		},
	}
}

type MinerAgent struct {
	Config        MinerAgentConfig // parameters used to define miner prior to creation
	Owner         address.Address
	Worker        address.Address
	IDAddress     address.Address
	RobustAddress address.Address

	// These slices are used to track counts and for random selections
	// total number of committed sectors (including sectors pending proof validation) that are not faulty and have not expired
	liveSectors []uint64
	// total number of sectors expected to be faulty
	faultySectors []uint64

	// priority queue used to trigger actions at future epochs
	operationSchedule *opQueue
	// which sector belongs to which deadline/partition
	deadlines [miner.WPoStPeriodDeadlines][]partition
	// iterator to time PreCommit events according to rate
	preCommitEvents *RateIterator
	// iterator to time faults events according to rate
	faultEvents *RateIterator
	// iterator to time recoveries according to rate
	recoveryEvents *RateIterator
	// tracks which sector number to use next
	nextSectorNumber abi.SectorNumber
	// random numnber generator provided by sim
	rnd *rand.Rand
}

func NewMinerAgent(owner address.Address, worker address.Address, idAddress address.Address, robustAddress address.Address,
	rndSeed int64, config MinerAgentConfig,
) *MinerAgent {
	rnd := rand.New(rand.NewSource(rndSeed))
	return &MinerAgent{
		Config:        config,
		Owner:         owner,
		Worker:        worker,
		IDAddress:     idAddress,
		RobustAddress: robustAddress,

		operationSchedule: &opQueue{},
		preCommitEvents:   NewRateIterator(config.PrecommitRate, rnd.Int63()),

		// fault rate is the configured fault rate times the number of live sectors or zero.
		faultEvents: NewRateIterator(0.0, rnd.Int63()),
		// recovery rate is the configured recovery rate times the number of faults or zero.
		recoveryEvents: NewRateIterator(0.0, rnd.Int63()),
		rnd:            rnd, // rng for this miner isolated from original source
	}
}

func (ma *MinerAgent) Tick(s SimState) ([]message, error) {
	var messages []message

	// act on scheduled operations
	for _, op := range ma.operationSchedule.PopOpsUntil(s.GetEpoch()) {
		switch o := op.action.(type) {
		case proveCommitAction:
			messages = append(messages, ma.createProveCommit(s.GetEpoch(), o.sectorNumber))
		case registerSectorAction:
			err := ma.registerSector(s, o.sectorNumber)
			if err != nil {
				return nil, err
			}
		case proveDeadlineAction:
			msgs, err := ma.submitPoStForDeadline(s, o.dlIdx)
			if err != nil {
				return nil, err
			}
			messages = append(messages, msgs...)
		case recoverSectorAction:
			msgs, err := ma.delayedRecoveryMessage(o.dlIdx, o.pIdx, o.sectorNumber)
			if err != nil {
				return nil, err
			}
			messages = append(messages, msgs...)
		case syncDeadlineStateAction:
			if err := ma.syncMinerState(s, o.dlIdx); err != nil {
				return nil, err
			}
		}
	}

	// Start PreCommits. PreCommits are triggered with a Poisson distribution at the PreCommit rate.
	// This permits multiple PreCommits per epoch while also allowing multiple epochs to pass
	// between PreCommits. For now always assume we have enough funds for the PreCommit deposit.
	if err := ma.preCommitEvents.Tick(func() error {
		// can't create precommit if in fee debt
		if st, err := ma.getState(s); err != nil {
			return err
		} else if st.FeeDebt.GreaterThan(big.Zero()) {
			return nil
		}

		messages = append(messages, ma.createPreCommit(s.GetEpoch()))
		return nil
	}); err != nil {
		return nil, err
	}

	// Fault sectors.
	// Rate must be multiplied by the number of live sectors
	faultRate := ma.Config.FaultRate * float64(len(ma.liveSectors))
	if err := ma.faultEvents.TickWithRate(faultRate, func() error {
		msgs, err := ma.createFault(s)
		if err != nil {
			return err
		}
		messages = append(messages, msgs...)
		return nil
	}); err != nil {
		return nil, err
	}

	// Recover sectors.
	// Rate must be multiplied by the number of faulty sectors
	recoveryRate := ma.Config.RecoveryRate * float64(len(ma.faultySectors))
	if err := ma.recoveryEvents.TickWithRate(recoveryRate, func() error {
		msgs, err := ma.createRecovery(s)
		if err != nil {
			return err
		}
		messages = append(messages, msgs...)
		return nil
	}); err != nil {
		return nil, err
	}

	return messages, nil
}

// create PreCommit message and activation trigger
func (ma *MinerAgent) createPreCommit(currentEpoch abi.ChainEpoch) message {
	// go ahead and choose when we're going to activate this sector
	sectorActivation := ma.sectorActivation(currentEpoch)
	sectorNumber := ma.nextSectorNumber
	ma.nextSectorNumber++

	// assume PreCommit succeeds and schedule prove commit
	ma.operationSchedule.ScheduleOp(sectorActivation, proveCommitAction{sectorNumber})

	params := miner.PreCommitSectorParams{
		SealProof:     ma.Config.ProofType,
		SectorNumber:  sectorNumber,
		SealedCID:     sectorSealCID(ma.rnd),
		SealRandEpoch: currentEpoch - 1,
		Expiration:    ma.sectorExpiration(currentEpoch),
	}

	return message{
		From:   ma.Worker,
		To:     ma.IDAddress,
		Value:  big.Zero(),
		Method: builtin.MethodsMiner.PreCommitSector,
		Params: &params,
	}
}

// create prove commit message
func (ma *MinerAgent) createProveCommit(epoch abi.ChainEpoch, sectorNumber abi.SectorNumber) message {
	params := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}

	// register an op for next epoch (after batch prove) to schedule a post for the sector
	ma.operationSchedule.ScheduleOp(epoch+1, registerSectorAction{sectorNumber: sectorNumber})

	return message{
		From:   ma.Worker,
		To:     ma.IDAddress,
		Value:  big.Zero(),
		Method: builtin.MethodsMiner.ProveCommitSector,
		Params: &params,
	}
}

// Fault a sector.
// This chooses a sector from live sectors and then either declares the recovery
// or adds it as a fault
func (ma *MinerAgent) createFault(v SimState) ([]message, error) {
	// opt out if no live sectors
	if len(ma.liveSectors) == 0 {
		return nil, nil
	}

	// choose a live sector to go faulty
	var faultNumber uint64
	faultNumber, ma.liveSectors = PopRandom(ma.liveSectors, ma.rnd)
	ma.faultySectors = append(ma.faultySectors, faultNumber)

	faultDlInfo, pIdx, err := ma.dlInfoForSector(v, faultNumber)
	if err != nil {
		return nil, err
	}

	parts := ma.deadlines[faultDlInfo.Index]
	if pIdx >= uint64(len(parts)) {
		return nil, errors.Errorf("sector %d in deadline %d has unregistered partition %d",
			faultNumber, faultDlInfo.Index, pIdx)
	}
	parts[pIdx].faults.Set(faultNumber)

	// If it's too late, skip fault rather than declaring it
	if faultDlInfo.FaultCutoffPassed() {
		parts[pIdx].toBeSkipped.Set(faultNumber)
		return nil, nil
	}

	// for now, just send a message per fault rather than trying to batch them
	faultParams := miner.DeclareFaultsParams{
		Faults: []miner.FaultDeclaration{{
			Deadline:  faultDlInfo.Index,
			Partition: pIdx,
			Sectors:   bitfield.NewFromSet([]uint64{faultNumber}),
		}},
	}

	return []message{{
		From:   ma.Worker,
		To:     ma.IDAddress,
		Value:  big.Zero(),
		Method: builtin.MethodsMiner.DeclareFaults,
		Params: &faultParams,
	}}, nil
}

// Recover a sector.
// This chooses a sector from faulty sectors and then either declare the recovery or schedule one for later
func (ma *MinerAgent) createRecovery(v SimState) ([]message, error) {
	// opt out if no faulty sectors
	if len(ma.faultySectors) == 0 {
		return nil, nil
	}

	// choose a faulty sector to recover
	var recoveryNumber uint64
	recoveryNumber, ma.faultySectors = PopRandom(ma.faultySectors, ma.rnd)

	recoveryDlInfo, pIdx, err := ma.dlInfoForSector(v, recoveryNumber)
	if err != nil {
		return nil, err
	}

	parts := ma.deadlines[recoveryDlInfo.Index]
	if pIdx >= uint64(len(parts)) {
		return nil, errors.Errorf("recovered sector %d in deadline %d has unregistered partition %d",
			recoveryNumber, recoveryDlInfo.Index, pIdx)
	}
	if set, err := parts[pIdx].faults.IsSet(recoveryNumber); err != nil {
		return nil, errors.Errorf("could not check if %d in deadline %d partition %d is faulty",
			recoveryNumber, recoveryDlInfo.Index, pIdx)
	} else if !set {
		return nil, errors.Errorf("recovery %d in deadline %d partition %d was not a fault",
			recoveryNumber, recoveryDlInfo.Index, pIdx)
	}

	// If it's too late, schedule recovery rather than declaring it
	if recoveryDlInfo.FaultCutoffPassed() {
		ma.operationSchedule.ScheduleOp(recoveryDlInfo.Close, recoverSectorAction{
			dlIdx:        recoveryDlInfo.Index,
			pIdx:         pIdx,
			sectorNumber: abi.SectorNumber(recoveryNumber),
		})
		return nil, nil
	}

	return ma.recoveryMessage(recoveryDlInfo.Index, pIdx, abi.SectorNumber(recoveryNumber))
}

// prove sectors in deadline
func (ma *MinerAgent) submitPoStForDeadline(v SimState, dlIdx uint64) ([]message, error) {
	var partitions []miner.PoStPartition
	for pIdx, part := range ma.deadlines[dlIdx] {
		if live, err := bitfield.SubtractBitField(part.sectors, part.faults); err != nil {
			return nil, err
		} else if empty, err := live.IsEmpty(); err != nil {
			return nil, err
		} else if !empty {
			partitions = append(partitions, miner.PoStPartition{
				Index:   uint64(pIdx),
				Skipped: part.toBeSkipped,
			})

			part.toBeSkipped = bitfield.New()
		}
	}

	// submitPoSt only if we have something to prove
	if len(partitions) == 0 {
		return nil, nil
	}

	postProofType, err := ma.Config.ProofType.RegisteredWindowPoStProof()
	if err != nil {
		return nil, err
	}

	params := miner.SubmitWindowedPoStParams{
		Deadline:   dlIdx,
		Partitions: partitions,
		Proofs: []proof.PoStProof{{
			PoStProof:  postProofType,
			ProofBytes: []byte{},
		}},
		ChainCommitEpoch: v.GetEpoch() - 1,
		ChainCommitRand:  []byte("not really random"),
	}

	// schedule post-deadline state synchronization and next PoSt
	if err := ma.scheduleSyncAndNextProof(v, dlIdx); err != nil {
		return nil, err
	}

	return []message{{
		From:   ma.Worker,
		To:     ma.IDAddress,
		Value:  big.Zero(),
		Method: builtin.MethodsMiner.SubmitWindowedPoSt,
		Params: &params,
	}}, nil
}

// looks up sector deadline and partition so we can start adding it to PoSts
func (ma *MinerAgent) registerSector(v SimState, sectorNumber abi.SectorNumber) error {
	var st miner.State
	err := v.GetState(ma.IDAddress, &st)
	if err != nil {
		return err
	}

	// first check for sector
	if found, err := st.HasSectorNo(v.Store(), sectorNumber); err != nil {
		return err
	} else if !found {
		fmt.Printf("failed to register sector %d, did proof verification fail?\n", sectorNumber)
		return nil
	}

	dlIdx, pIdx, err := st.FindSector(v.Store(), sectorNumber)
	if err != nil {
		return err
	}

	if len(ma.deadlines[dlIdx]) == 0 {
		err := ma.scheduleSyncAndNextProof(v, dlIdx)
		if err != nil {
			return err
		}
	}

	ma.liveSectors = append(ma.liveSectors, uint64(sectorNumber))

	// pIdx should be sequential, but add empty partitions just in case
	for pIdx >= uint64(len(ma.deadlines[dlIdx])) {
		ma.deadlines[dlIdx] = append(ma.deadlines[dlIdx], partition{
			sectors:     bitfield.New(),
			toBeSkipped: bitfield.New(),
			faults:      bitfield.New(),
		})
	}
	ma.deadlines[dlIdx][pIdx].sectors.Set(uint64(sectorNumber))
	return nil
}

// schedule a proof within the deadline's bounds
func (ma *MinerAgent) scheduleSyncAndNextProof(v SimState, dlIdx uint64) error {
	var st miner.State
	err := v.GetState(ma.IDAddress, &st)
	if err != nil {
		return err
	}

	// find next proving window for this deadline
	deadlineStart := st.ProvingPeriodStart + abi.ChainEpoch(dlIdx)*miner.WPoStChallengeWindow
	if deadlineStart-miner.WPoStChallengeWindow < v.GetEpoch() {
		deadlineStart += miner.WPoStProvingPeriod
	}
	deadlineClose := deadlineStart + miner.WPoStChallengeWindow

	ma.operationSchedule.ScheduleOp(deadlineClose, syncDeadlineStateAction{dlIdx: dlIdx})

	proveAt := deadlineStart + abi.ChainEpoch(ma.rnd.Int63n(int64(deadlineClose-deadlineStart)))
	ma.operationSchedule.ScheduleOp(proveAt, proveDeadlineAction{dlIdx: dlIdx})

	return nil
}

// ensure recovery hasn't expired since it was scheduled
func (ma *MinerAgent) delayedRecoveryMessage(dlIdx uint64, pIdx uint64, recoveryNumber abi.SectorNumber) ([]message, error) {
	part := ma.deadlines[dlIdx][pIdx]
	if expired, err := part.expired.IsSet(uint64(recoveryNumber)); err != nil {
		return nil, err
	} else if expired {
		// just ignore this recovery if expired
		return nil, nil
	}

	return ma.recoveryMessage(dlIdx, pIdx, recoveryNumber)
}

func (ma *MinerAgent) recoveryMessage(dlIdx uint64, pIdx uint64, recoveryNumber abi.SectorNumber) ([]message, error) {
	// assume this message succeeds
	ma.liveSectors = append(ma.liveSectors, uint64(recoveryNumber))
	part := ma.deadlines[dlIdx][pIdx]
	part.faults.Unset(uint64(recoveryNumber))

	recoverParams := miner.DeclareFaultsRecoveredParams{
		Recoveries: []miner.RecoveryDeclaration{{
			Deadline:  dlIdx,
			Partition: pIdx,
			Sectors:   bitfield.NewFromSet([]uint64{uint64(recoveryNumber)}),
		}},
	}

	return []message{{
		From:   ma.Worker,
		To:     ma.IDAddress,
		Value:  big.Zero(),
		Method: builtin.MethodsMiner.DeclareFaultsRecovered,
		Params: &recoverParams,
	}}, nil
}

// This function updates all sectors in deadline that have newly expired
func (ma *MinerAgent) syncMinerState(s SimState, dlIdx uint64) error {
	st, err := ma.getState(s)
	if err != nil {
		return err

	}

	dl, err := ma.loadDeadlineState(s, st, dlIdx)
	if err != nil {
		return err
	}

	// update sector state for all partitions in deadline
	var allNewExpired []bitfield.BitField
	for pIdx, part := range ma.deadlines[dlIdx] {
		partState, err := dl.LoadPartition(s.Store(), uint64(pIdx))
		if err != nil {
			return err
		}
		newExpired, err := bitfield.IntersectBitField(part.sectors, partState.Terminated)
		if err != nil {
			return err
		}

		if empty, err := newExpired.IsEmpty(); err != nil {
			return err
		} else if !empty {
			err := part.expireSectors(newExpired)
			if err != nil {
				return err
			}
			allNewExpired = append(allNewExpired, newExpired)
		}
	}

	// remove newly expired sectors from miner agent state to prevent choosing them in the future.
	toRemoveBF, err := bitfield.MultiMerge(allNewExpired...)
	if err != nil {
		return err
	}

	toRemove, err := toRemoveBF.AllMap(uint64(ma.nextSectorNumber))
	if err != nil {
		return err
	}

	if len(toRemove) > 0 {
		ma.liveSectors = filterSlice(ma.liveSectors, toRemove)
		ma.faultySectors = filterSlice(ma.faultySectors, toRemove)
	}
	return nil
}

func filterSlice(ns []uint64, toRemove map[uint64]bool) []uint64 {
	var nextLive []uint64
	for _, sn := range ns {
		_, expired := toRemove[sn]
		if !expired {
			nextLive = append(nextLive, sn)
		}
	}
	return nextLive
}

func (ma *MinerAgent) loadDeadlineState(s SimState, st miner.State, dlIdx uint64) (*miner.Deadline, error) {
	dls, err := st.LoadDeadlines(s.Store())
	if err != nil {
		return nil, err
	}

	return dls.LoadDeadline(s.Store(), dlIdx)
}

func (ma *MinerAgent) getState(s SimState) (miner.State, error) {
	var st miner.State
	err := s.GetState(ma.IDAddress, &st)
	if err != nil {
		return miner.State{}, err
	}
	return st, err
}

func (ma *MinerAgent) dlInfoForSector(v SimState, sectorNumber uint64) (*dline.Info, uint64, error) {
	var st miner.State
	err := v.GetState(ma.IDAddress, &st)
	if err != nil {
		return nil, 0, err
	}

	dlIdx, pIdx, err := st.FindSector(v.Store(), abi.SectorNumber(sectorNumber))
	if err != nil {
		return nil, 0, err
	}

	dlInfo := st.DeadlineInfo(v.GetEpoch())
	sectorDLInfo := miner.NewDeadlineInfo(dlInfo.PeriodStart, dlIdx, v.GetEpoch()).NextNotElapsed()
	return sectorDLInfo, pIdx, nil
}

// create a random valid sector expiration
func (ma *MinerAgent) sectorExpiration(currentEpoch abi.ChainEpoch) abi.ChainEpoch {
	// Require sector lifetime meets minimum by assuming activation happens at last epoch permitted for seal proof
	// to meet the constraints imposed in PreCommit.
	minExp := currentEpoch + miner.MaxProveCommitDuration[ma.Config.ProofType] + miner.MinSectorExpiration
	// Require duration of sector from now does not exceed the maximum sector extension. This constraint
	// is also imposed by PreCommit, and along with the first constraint define the bounds for a valid
	// expiration of a new sector.
	maxExp := currentEpoch + miner.MaxSectorExpirationExtension

	// generate a uniformly distributed expiration in the valid range.
	return minExp + abi.ChainEpoch(ma.rnd.Int63n(int64(maxExp-minExp)))
}

// Generate a sector activation over the range of acceptable values.
// The range varies widely from 150 - 3030 epochs after precommit.
// Assume differences in hardware and contention in the miner's sealing queue create a uniform distribution
// over the acceptable range
func (ma *MinerAgent) sectorActivation(preCommitAt abi.ChainEpoch) abi.ChainEpoch {
	minActivation := preCommitAt + miner.PreCommitChallengeDelay + 1
	maxActivation := preCommitAt + miner.MaxProveCommitDuration[ma.Config.ProofType]
	return minActivation + abi.ChainEpoch(ma.rnd.Int63n(int64(maxActivation-minActivation)))
}

// create a random seal CID
func sectorSealCID(rnd *rand.Rand) cid.Cid {
	data := make([]byte, 10)
	_, err := rnd.Read(data)
	if err != nil {
		panic(err)
	}

	sum := sha256.Sum256(data)
	hash, err := mh.Encode(sum[:], miner.SealedCIDPrefix.MhType)
	if err != nil {
		panic(err)
	}
	return cid.NewCidV1(miner.SealedCIDPrefix.Codec, hash)
}

/////////////////////////////////////////////
//
//  Internal data structures
//
/////////////////////////////////////////////

// tracks state relevant to each partition
type partition struct {
	sectors     bitfield.BitField // sector numbers of all sectors that have not expired
	toBeSkipped bitfield.BitField // sector numbers of sectors to be skipped next PoSt
	faults      bitfield.BitField // sector numbers of sectors believed to be faulty
	expired     bitfield.BitField // sector number of sectors believed to have expired
}

func (part *partition) expireSectors(newExpired bitfield.BitField) error {
	var err error
	part.sectors, err = bitfield.SubtractBitField(part.sectors, newExpired)
	if err != nil {
		return err
	}
	part.faults, err = bitfield.SubtractBitField(part.faults, newExpired)
	if err != nil {
		return err
	}
	part.toBeSkipped, err = bitfield.SubtractBitField(part.toBeSkipped, newExpired)
	if err != nil {
		return err
	}
	part.expired, err = bitfield.MergeBitFields(part.expired, newExpired)
	if err != nil {
		return err
	}
	return nil
}

type minerOp struct {
	epoch  abi.ChainEpoch
	action interface{}
}

type proveCommitAction struct {
	sectorNumber abi.SectorNumber
}

type registerSectorAction struct {
	sectorNumber abi.SectorNumber
}

type recoverSectorAction struct {
	dlIdx        uint64
	pIdx         uint64
	sectorNumber abi.SectorNumber
}

type proveDeadlineAction struct {
	dlIdx uint64
}

type syncDeadlineStateAction struct {
	dlIdx uint64
}

/////////////////////////////////////////////
//
//  opQueue priority queue for scheduling
//
/////////////////////////////////////////////

type opQueue struct {
	ops []minerOp
}

var _ heap.Interface = (*opQueue)(nil)

// add an op to schedule
func (o *opQueue) ScheduleOp(epoch abi.ChainEpoch, action interface{}) {
	heap.Push(o, minerOp{
		epoch:  epoch,
		action: action,
	})
}

// get operations for up to and including current epoch
func (o *opQueue) PopOpsUntil(epoch abi.ChainEpoch) []minerOp {
	var ops []minerOp

	for !o.IsEmpty() && o.NextEpoch() <= epoch {
		next := heap.Pop(o).(minerOp)
		ops = append(ops, next)
	}
	return ops
}

func (o *opQueue) NextEpoch() abi.ChainEpoch {
	return o.ops[0].epoch
}

func (o *opQueue) IsEmpty() bool {
	return len(o.ops) == 0
}

func (o *opQueue) Len() int {
	return len(o.ops)
}

func (o *opQueue) Less(i, j int) bool {
	return o.ops[i].epoch < o.ops[j].epoch
}

func (o *opQueue) Swap(i, j int) {
	o.ops[i], o.ops[j] = o.ops[j], o.ops[i]
}

func (o *opQueue) Push(x interface{}) {
	o.ops = append(o.ops, x.(minerOp))
}

func (o *opQueue) Pop() interface{} {
	op := o.ops[len(o.ops)-1]
	o.ops = o.ops[:len(o.ops)-1]
	return op
}
