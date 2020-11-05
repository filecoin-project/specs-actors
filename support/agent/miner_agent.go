package agent

import (
	"container/heap"
	"crypto/sha256"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"github.com/pkg/errors"
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

type MinerGenerator struct {
	config                 MinerAgentConfig // eventually this should become a set of probabilities to support miner differentiation
	createMinerProbability float32
	minersCreated          int
	accounts               []address.Address
	rnd                    *rand.Rand
}

func NewMinerGenerator(accounts []address.Address, config MinerAgentConfig, createMinerProbability float32, rndSeed int64) *MinerGenerator {
	rnd := rand.New(rand.NewSource(rndSeed))
	return &MinerGenerator{
		config:                 config,
		createMinerProbability: createMinerProbability,
		accounts:               accounts,
		rnd:                    rnd,
	}
}

func (mg *MinerGenerator) Tick(_ SimState) ([]message, error) {
	// add at most 1 miner per epoch.
	var msgs []message
	if mg.minersCreated < len(mg.accounts) && mg.rnd.Float32() < mg.createMinerProbability {
		addr := mg.accounts[mg.minersCreated]
		mg.minersCreated++
		msgs = append(msgs, mg.createMiner(addr, mg.config))
	}
	return msgs, nil
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

	// priority queue used to trigger actions at future epochs
	operationSchedule *opQueue
	// which sector belongs to which deadline/partition
	deadlines [miner.WPoStPeriodDeadlines][]bitfield.BitField
	// offset used to maintain precommit rate (as a fraction of an epoch).
	nextPreCommit float64
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
		nextPreCommit:     1.0 + precommitDelay(config.PrecommitRate, rnd), // next tick + random delay
		rnd:               rnd,                                             // rng for this miner isolated from original source
	}
}

func (ma *MinerAgent) Tick(v SimState) ([]message, error) {
	var messages []message

	// Start PreCommits.
	// PreCommits are triggered with a Poisson distribution at the PreCommit rate.
	// This is done by choosing delays and adding PreCommits until the total delay is greater
	// than one epoch. The next epoch, we decrement the delay and start from there. This
	// permits multiple PreCommits per epoch while also allowing multiple epochs to pass between
	// PreCommits.
	// For now always assume we have enough funds for the PreCommit deposit.
	ma.nextPreCommit -= 1.0
	for ma.nextPreCommit < 1.0 {
		// go ahead and choose when we're going to activate this sector
		sectorActivation := ma.sectorActivation(v.GetEpoch())
		sectorNumber := ma.nextSectorNumber

		messages = append(messages, ma.createPreCommit(v.GetEpoch(), sectorNumber))

		// assume PreCommit succeeds and schedule prove commit
		ma.operationSchedule.ScheduleOp(sectorActivation, proveCommitAction{sectorNumber})

		ma.nextPreCommit += precommitDelay(ma.Config.PrecommitRate, ma.rnd)
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

// prove sectors in deadline
func (ma *MinerAgent) proveDeadline(v SimState, dlIdx uint64) (message, error) {
	var partitions []miner.PoStPartition
	for pIdx, bf := range ma.deadlines[dlIdx] {
		if empty, err := bf.IsEmpty(); err != nil {
			return message{}, err
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
		return message{}, err
	}

	return message{
		From:   ma.Worker,
		To:     ma.IDAddress,
		Value:  big.Zero(),
		Method: builtin.MethodsMiner.SubmitWindowedPoSt,
		Params: &params,
	}, nil
}

// looks up sector deadline and partition so we can start adding it to PoSts
func (ma *MinerAgent) registerSector(v SimState, sectorNumber abi.SectorNumber) error {
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
func (ma *MinerAgent) scheduleNextProof(v SimState, dlIdx uint64) error {
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
func (ma *MinerAgent) createProveCommit(epoch abi.ChainEpoch, sectorNumber abi.SectorNumber) message {
	params := miner.ProveCommitSectorParams{
		SectorNumber: sectorNumber,
	}

	// register an op for next epoch (after batch prove) to schedule a post for the sector
	ma.operationSchedule.ScheduleOp(epoch, registerSectorAction{sectorNumber: sectorNumber})

	return message{
		From:   ma.Worker,
		To:     ma.IDAddress,
		Value:  big.Zero(),
		Method: builtin.MethodsMiner.ProveCommitSector,
		Params: &params,
	}
}

// create PreCommit message and activation trigger
func (ma *MinerAgent) createPreCommit(currentEpoch abi.ChainEpoch, sectorNumber abi.SectorNumber) message {
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
