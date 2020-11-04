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

type Sim struct {
	Config        SimConfig
	Accounts      []address.Address
	Miners        []*MinerAgent
	v             *vm.VM
	rnd           *rand.Rand
	statsByMethod map[vm.MethodKey]*vm.CallStats
}

type VMState interface {
	GetEpoch() abi.ChainEpoch
	GetState(addr address.Address, out cbor.Unmarshaler) error
	Store() adt.Store
}

type SimConfig struct {
	AccountCount           int
	AccountInitialBalance  abi.TokenAmount
	Seed                   int64
	CreateMinerProbability float32
}

type ReturnHandler func(v VMState, msg Message, ret cbor.Marshaler) error

type Message struct {
	From          address.Address
	To            address.Address
	Value         abi.TokenAmount
	Method        abi.MethodNum
	Params        interface{}
	ReturnHandler ReturnHandler
}

type MinerPowerTable struct {
	addr    address.Address
	qaPower abi.StoragePower
}

type PowerTable struct {
	blockReward  abi.TokenAmount
	totalQAPower abi.StoragePower
	minerPower   []MinerPowerTable
}

func NewSim(ctx context.Context, t require.TestingT, store adt.Store, config SimConfig) *Sim {
	v := vm.NewCustomStoreVMWithSingletons(ctx, store, t)
	return &Sim{
		Config:   config,
		Accounts: vm.CreateAccounts(ctx, t, v, config.AccountCount, config.AccountInitialBalance, config.Seed),
		Miners:   []*MinerAgent{},
		v:        v,
		rnd:      rand.New(rand.NewSource(config.Seed)),
	}
}

func (s *Sim) Tick() error {
	var err error
	var blockMessages []Message

	// compute power table before state transition to create block rewards at the end
	powerTable, err := ComputePowerTable(s.v, s.Miners)
	if err != nil {
		return err
	}

	// add all miner messages
	for _, miner := range s.Miners {
		msgs, err := miner.Tick(s.v)
		if err != nil {
			return err
		}

		blockMessages = append(blockMessages, msgs...)
	}

	// add at most 1 miner per epoch.
	if len(s.Miners) < len(s.Accounts) && s.rnd.Float32() < s.Config.CreateMinerProbability {
		addr := s.Accounts[len(s.Miners)]
		blockMessages = append(blockMessages, s.createMiner(addr, MinerAgentConfig{
			PrecommitRate:   2.5,
			ProofType:       abi.RegisteredSealProof_StackedDrg32GiBV1,
			StartingBalance: s.Config.AccountInitialBalance, // miner gets all account funds
		}))
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
			if err := msg.ReturnHandler(s.v, msg, ret); err != nil {
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

func ComputePowerTable(v *vm.VM, miners []*MinerAgent) (PowerTable, error) {
	pt := PowerTable{}

	var rwst reward.State
	if err := v.GetState(builtin.RewardActorAddr, &rwst); err != nil {
		return PowerTable{}, err
	}
	pt.blockReward = rwst.ThisEpochReward

	var st power.State
	if err := v.GetState(builtin.StoragePowerActorAddr, &st); err != nil {
		return PowerTable{}, err
	}
	pt.totalQAPower = st.TotalQualityAdjPower

	for _, miner := range miners {
		if claim, found, err := st.GetClaim(v.Store(), miner.IDAddress); err != nil {
			return pt, err
		} else if found {
			if sufficient, err := st.MinerNominalPowerMeetsConsensusMinimum(v.Store(), miner.IDAddress); err != nil {
				return pt, err
			} else if sufficient {
				pt.minerPower = append(pt.minerPower, MinerPowerTable{miner.IDAddress, claim.QualityAdjPower})
			}
		}
	}
	return pt, nil
}

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

func (s *Sim) createMiner(owner address.Address, cfg MinerAgentConfig) Message {
	return Message{
		From:   owner,
		To:     builtin.StoragePowerActorAddr,
		Value:  s.Config.AccountInitialBalance, // miner gets all account funds
		Method: builtin.MethodsPower.CreateMiner,
		Params: &power.CreateMinerParams{
			Owner:         owner,
			Worker:        owner,
			SealProofType: cfg.ProofType,
		},
		ReturnHandler: func(_ VMState, msg Message, ret cbor.Marshaler) error {
			createMinerRet, ok := ret.(*power.CreateMinerReturn)
			if !ok {
				return errors.Errorf("create miner return has wrong type: %v", ret)
			}

			params := msg.Params.(*power.CreateMinerParams)
			if !ok {
				return errors.Errorf("create miner params has wrong type: %v", msg.Params)
			}

			miner := NewMinerAgent(params.Owner, params.Worker, createMinerRet.IDAddress, createMinerRet.RobustAddress, s.rnd, cfg)
			s.Miners = append(s.Miners, miner)
			return nil
		},
	}
}
