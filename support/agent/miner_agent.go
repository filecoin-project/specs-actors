package agent

import (
	"container/heap"
	"crypto/sha256"
	"math"
	"math/rand"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/runtime/proof"
)

type MinerAgentConfig struct {
	PrecommitRate   float64                 // average number of precommits per epoch
	ProofType       abi.RegisteredSealProof // seal proof type for this miner
	StartingBalance abi.TokenAmount         // initial actor balance for miner actor
}

type MinerAgent struct {
	Config        MinerAgentConfig // parameters used to define miner prior to creation
	Owner         address.Address
	Worker        address.Address
	IDAddress     address.Address
	RobustAddress address.Address

	// priority queue used to trigger actions at future epochs
	operationSchedule *opQueue
	// which sector belongs to which deadline/partition
	deadlines [miner.WPoStPeriodDeadlines][]bitfield.BitField
	// offset used to maintain precommit rate (as a fraction of an epoch).
	nextPrecommit float64
	// tracks which sector number to use next
	nextSectorNumber abi.SectorNumber
	// random numnber generator provided by sim
	rnd *rand.Rand
}

func NewMinerAgent(owner address.Address, worker address.Address, idAddress address.Address, robustAddress address.Address,
	rnd *rand.Rand, config MinerAgentConfig,
) *MinerAgent {
	return &MinerAgent{
		Config:        config,
		Owner:         owner,
		Worker:        worker,
		IDAddress:     idAddress,
		RobustAddress: robustAddress,

		operationSchedule: &opQueue{},
		nextPrecommit:     1.0 + precommitDelay(config.PrecommitRate, rnd), // next tick + random delay
		rnd:               rnd,
	}
}

func (ma *MinerAgent) Tick(v VMState) ([]Message, error) {
	var messages []Message

	// Start precommits.
	// Precommits are triggered with a Poisson distribution at the precommit rate.
	// This is done by choosing delays and adding precommits until the total delay is greater
	// than one epoch. The next epoch, we decrement the delay and start from there. This
	// permits multiple precommits per epoch while also allowing multiple epochs to pass between
	// precommits.
	// For now always assume we have enough funds for the precommit deposit.
	ma.nextPrecommit -= 1.0
	for ma.nextPrecommit < 1.0 {
		// go ahead and choose when we're going to activate this sector
		sectorActivation := ma.sectorActivation(v.GetEpoch())
		sectorNumber := ma.nextSectorNumber

		messages = append(messages, ma.createPrecommit(v.GetEpoch(), sectorNumber, sectorActivation))

		// assume precommit succeeds and schedule prove commit
		ma.operationSchedule.ScheduleOp(sectorActivation, proveCommitAction{sectorNumber})

		ma.nextPrecommit += precommitDelay(ma.Config.PrecommitRate, ma.rnd)
		ma.nextSectorNumber++
	}

	// act on scheduled operations
	for _, op := range ma.operationSchedule.PopOpsUntil(v.GetEpoch()) {
		switch o := op.action.(type) {
		case proveCommitAction:
			messages = append(messages, ma.createProveCommit(v.GetEpoch(), o.sectorNumber))
		case registerSectorAction:
			err := ma.registerSector(v, o.sectorNumber)
			if err != nil {
				return nil, err
			}
		case proveDeadlineAction:
			msg, err := ma.proveDeadline(v, o.dlIdx)
			if err != nil {
				return nil, err
			}
			messages = append(messages, msg)
		}
	}

	return messages, nil
}

// proove sectors in deadline
func (ma *MinerAgent) proveDeadline(v VMState, dlIdx uint64) (Message, error) {
	var partitions []miner.PoStPartition
	for pIdx, bf := range ma.deadlines[dlIdx] {
		if empty, err := bf.IsEmpty(); err != nil {
			return Message{}, err
		} else if !empty {
			partitions = append(partitions, miner.PoStPartition{
				Index:   uint64(pIdx),
				Skipped: bitfield.New(),
			})
		}
	}

	postProofType, err := ma.Config.ProofType.RegisteredWindowPoStProof()
	if err != nil {
		panic(err)
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

	// assume prove sectors succeeds and schedule its first PoSt (if necessary)
	if err := ma.scheduleNextProof(v, dlIdx); err != nil {
		return Message{}, err
	}

	return Message{
		From:   ma.Worker,
		To:     ma.RobustAddress,
		Value:  big.Zero(),
		Method: builtin.MethodsMiner.SubmitWindowedPoSt,
		Params: &params,
	}, nil
}

// looks up sector deadline and partition so we can start adding it to PoSts
func (ma *MinerAgent) registerSector(v VMState, sectorNumber abi.SectorNumber) error {
	var st miner.State
	err := v.GetState(ma.IDAddress, &st)
	if err != nil {
		return err
	}

	dlIdx, pIdx, err := st.FindSector(v.Store(), sectorNumber)
	if err != nil {
		return err
	}

	if len(ma.deadlines[dlIdx]) == 0 {
		err := ma.scheduleNextProof(v, dlIdx)
		if err != nil {
			return err
		}
	}

	// pIdx should be sequential, but add empty partitions just in case
	for pIdx >= uint64(len(ma.deadlines[dlIdx])) {
		ma.deadlines[dlIdx] = append(ma.deadlines[dlIdx], bitfield.New())
	}
	ma.deadlines[dlIdx][pIdx].Set(uint64(sectorNumber))
	return nil
}

// schedule a proof within the deadline's bounds
func (ma *MinerAgent) scheduleNextProof(v VMState, dlIdx uint64) error {
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
	prooveAt := deadlineStart + abi.ChainEpoch(ma.rnd.Int63n(int64(deadlineClose-deadlineStart)))
	ma.operationSchedule.ScheduleOp(prooveAt, proveDeadlineAction{dlIdx: dlIdx})
	return nil
}

// create prove commit message
func (ma *MinerAgent) createProveCommit(epoch abi.ChainEpoch, sectorNumber abi.SectorNumber) Message {
	params := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}

	// register an op for next epoch (after batch prove) to schedule a post for the sector
	ma.operationSchedule.ScheduleOp(epoch, registerSectorAction{sectorNumber: sectorNumber})

	return Message{
		From:   ma.Worker,
		To:     ma.RobustAddress,
		Value:  big.Zero(),
		Method: builtin.MethodsMiner.ProveCommitSector,
		Params: &params,
	}
}

// create precommit message and activation trigger
func (ma *MinerAgent) createPrecommit(currentEpoch abi.ChainEpoch, sectorNumber abi.SectorNumber, sectorActivation abi.ChainEpoch) Message {
	params := miner.PreCommitSectorParams{
		SealProof:     ma.Config.ProofType,
		SectorNumber:  sectorNumber,
		SealedCID:     sectorSealCID(ma.rnd),
		SealRandEpoch: currentEpoch - 1,
		Expiration:    ma.sectorExpiration(currentEpoch),
	}

	return Message{
		From:   ma.Worker,
		To:     ma.RobustAddress,
		Value:  big.Zero(),
		Method: builtin.MethodsMiner.PreCommitSector,
		Params: &params,
	}
}

// create a random valid sector expiration
func (ma *MinerAgent) sectorExpiration(currentEpoch abi.ChainEpoch) abi.ChainEpoch {
	// these are precommit bounds. Prove commit is more lenient but contains this range.
	minExp := currentEpoch + miner.MaxProveCommitDuration[ma.Config.ProofType] + miner.MinSectorExpiration
	maxExp := currentEpoch + miner.MaxSectorExpirationExtension
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

// compute next precommit according to a poisson distribution
func precommitDelay(rate float64, rnd *rand.Rand) float64 {
	return -math.Log(1-rnd.Float64()) / rate
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

type proveDeadlineAction struct {
	dlIdx uint64
}

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
