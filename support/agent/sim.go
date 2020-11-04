package agent

import (
	"context"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/reward"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	"math"
	big2 "math/big"
	"math/rand"
	"strings"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	vm "github.com/filecoin-project/specs-actors/v2/support/vm"
)

// Sim is a simulation framework to exercise actor code in a network-like environment.
// It's goal is to simulate realistic call sequences and interactions to perform invariant analysis
// and test performance assumptions prior to shipping actor code out to implementations.
// The model is that the simulation will "Tick" once per epoch. Within this tick:
// * It will first compute winning tickets from previous state for miners to simulate block mining.
// * It will create any agents it is configured to create and generate messages to create their associated actors.
// * It will call tick on all it agents. This call will return messages that will get added to the simulated "tipset".
// * Messages will be shuffled to simulate network entropy.
// * Messages will be applied and an new VM will be created from the resulting state tree for the next tick.
type Sim struct {
	Config        SimConfig
	Agents        []Agent
	v             *vm.VM
	rnd           *rand.Rand
	statsByMethod map[vm.MethodKey]*vm.CallStats
}

type SimState interface {
	GetEpoch() abi.ChainEpoch
	GetState(addr address.Address, out cbor.Unmarshaler) error
	Store() adt.Store
	AddAgent(a Agent)
	Rnd() int64
}

type Agent interface {
	Tick(v SimState) ([]message, error)
}

type SimConfig struct {
	AccountCount           int
	AccountInitialBalance  abi.TokenAmount
	Seed                   int64
	CreateMinerProbability float32
}

func NewSim(ctx context.Context, t require.TestingT, store adt.Store, config SimConfig) *Sim {
	v := vm.NewCustomStoreVMWithSingletons(ctx, store, t)
	return &Sim{
		Config: config,
		Agents: []Agent{},
		v:      v,
		rnd:    rand.New(rand.NewSource(config.Seed)),
	}
}

func (s *Sim) Tick() error {
	var err error
	var blockMessages []message

	// compute power table before state transition to create block rewards at the end
	powerTable, err := ComputePowerTable(s.v, s.Agents)
	if err != nil {
		return err
	}

	// add all agent messages
	for _, agent := range s.Agents {
		msgs, err := agent.Tick(s)
		if err != nil {
			return err
		}

		blockMessages = append(blockMessages, msgs...)
	}

	// shuffle messages
	s.rnd.Shuffle(len(blockMessages), func(i, j int) {
		blockMessages[i], blockMessages[j] = blockMessages[j], blockMessages[i]
	})

	// run messages
	for _, msg := range blockMessages {
		ret, code := s.v.ApplyMessage(msg.From, msg.To, msg.Value, msg.Method, msg.Params)

		// for now, assume everything should work
		if code != exitcode.Ok {
			return errors.Errorf("exitcode %d: message failed: %v\n%s\n", code, msg, strings.Join(s.v.GetLogs(), "\n"))
		}

		if msg.ReturnHandler != nil {
			if err := msg.ReturnHandler(s, msg, ret); err != nil {
				return err
			}
		}
	}

	// apply block rewards
	for _, miner := range powerTable.minerPower {
		if powerTable.totalQAPower.GreaterThan(big.Zero()) {
			wins := s.WinCount(miner.qaPower, powerTable.totalQAPower)
			err := s.rewardMiner(miner.addr, wins)
			if err != nil {
				return err
			}
		}
	}

	// run cron
	_, code := s.v.ApplyMessage(builtin.SystemActorAddr, builtin.CronActorAddr, big.Zero(), builtin.MethodsCron.EpochTick, nil)
	if code != exitcode.Ok {
		return errors.Errorf("exitcode %d: cron message failed:\n%s\n", code, strings.Join(s.v.GetLogs(), "\n"))
	}

	// store last stats
	s.statsByMethod = s.v.GetCallStats()

	s.v, err = s.v.WithEpoch(s.v.GetEpoch() + 1)
	return err
}

//////////////////////////////////////////////////
//
//  SimState Methods
//
//////////////////////////////////////////////////

func (s *Sim) GetEpoch() abi.ChainEpoch {
	return s.v.GetEpoch()
}

func (s *Sim) GetState(addr address.Address, out cbor.Unmarshaler) error {
	return s.v.GetState(addr, out)
}

func (s *Sim) Store() adt.Store {
	return s.v.Store()
}

func (s *Sim) AddAgent(a Agent) {
	s.Agents = append(s.Agents, a)
}

func (s *Sim) Rnd() int64 {
	return s.rnd.Int63()
}

//////////////////////////////////////////////////
//
//  Misc Methods
//
//////////////////////////////////////////////////

func (s *Sim) GetCallStats() map[vm.MethodKey]*vm.CallStats {
	return s.statsByMethod
}

func (s *Sim) rewardMiner(addr address.Address, wins uint64) error {
	if wins < 1 {
		return nil
	}

	rewardParams := reward.AwardBlockRewardParams{
		Miner:     addr,
		Penalty:   big.Zero(),
		GasReward: big.Zero(),
		WinCount:  int64(wins),
	}
	_, code := s.v.ApplyMessage(builtin.SystemActorAddr, builtin.RewardActorAddr, big.Zero(), builtin.MethodsReward.AwardBlockReward, &rewardParams)
	if code != exitcode.Ok {
		return errors.Errorf("exitcode %d: reward message failed:\n%s\n", code, strings.Join(s.v.GetLogs(), "\n"))
	}
	return nil
}

func ComputePowerTable(v *vm.VM, agents []Agent) (powerTable, error) {
	pt := powerTable{}

	var rwst reward.State
	if err := v.GetState(builtin.RewardActorAddr, &rwst); err != nil {
		return powerTable{}, err
	}
	pt.blockReward = rwst.ThisEpochReward

	var st power.State
	if err := v.GetState(builtin.StoragePowerActorAddr, &st); err != nil {
		return powerTable{}, err
	}
	pt.totalQAPower = st.TotalQualityAdjPower

	for _, agent := range agents {
		if miner, ok := agent.(*MinerAgent); ok {
			if claim, found, err := st.GetClaim(v.Store(), miner.IDAddress); err != nil {
				return pt, err
			} else if found {
				if sufficient, err := st.MinerNominalPowerMeetsConsensusMinimum(v.Store(), miner.IDAddress); err != nil {
					return pt, err
				} else if sufficient {
					pt.minerPower = append(pt.minerPower, minerPowerTable{miner.IDAddress, claim.QualityAdjPower})
				}
			}
		}
	}
	return pt, nil
}

// This is the Filecoin algorithm for winning a ticket within a block with the tickets replaced
// with random numbers. It lets miners win according to a Poisson distribution with rate
// proportional to the miner's fraction of network power.
func (s *Sim) WinCount(minerPower abi.StoragePower, totalPower abi.StoragePower) uint64 {
	E := big2.NewRat(5, 1)
	lambdaR := new(big2.Rat)
	lambdaR.SetFrac(minerPower.Int, totalPower.Int)
	lambdaR.Mul(lambdaR, E)
	lambda, _ := lambdaR.Float64()

	h := s.rnd.Float64()
	rhs := 1 - poissonPMF(lambda, 0)

	winCount := uint64(0)
	for rhs > h {
		winCount++
		rhs -= poissonPMF(lambda, winCount)
	}
	return winCount
}

func poissonPMF(lambda float64, k uint64) float64 {
	fk := float64(k)
	return (math.Exp(-lambda) * math.Pow(lambda, fk)) / fact(fk)
}

func fact(k float64) float64 {
	fact := 1.0
	for i := 2.0; i <= k; i += 1.0 {
		fact *= i
	}
	return fact
}

func (s *Sim) GetVM() *vm.VM {
	return s.v
}

//////////////////////////////////////////////
//
//  Internal Types
//
//////////////////////////////////////////////

type returnHandler func(v SimState, msg message, ret cbor.Marshaler) error

type message struct {
	From          address.Address
	To            address.Address
	Value         abi.TokenAmount
	Method        abi.MethodNum
	Params        interface{}
	ReturnHandler returnHandler
}

type minerPowerTable struct {
	addr    address.Address
	qaPower abi.StoragePower
}

type powerTable struct {
	blockReward  abi.TokenAmount
	totalQAPower abi.StoragePower
	minerPower   []minerPowerTable
}
