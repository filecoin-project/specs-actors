package states

import (
	"bytes"
	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/account"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/cron"
	init_ "github.com/filecoin-project/specs-actors/v2/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/multisig"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/paych"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/verifreg"
)

// Within this code, Go errors are not expected, but are often converted to messages so that execution
// can continue to find more errors rather than fail with no insight.
// Only errors thar are particularly troublesome to recover from should propagate as Go errors.
func CheckStateInvariants(tree *Tree, expectedBalanceTotal abi.TokenAmount, priorEpoch abi.ChainEpoch) (*builtin.MessageAccumulator, error) {
	acc := &builtin.MessageAccumulator{}
	totalFIl := big.Zero()
	var initSummary *init_.StateSummary
	var cronSummary *cron.StateSummary
	var verifregSummary *verifreg.StateSummary
	var marketSummary *market.StateSummary
	var accountSummaries []*account.StateSummary
	var powerSummary *power.StateSummary
	var paychSummaries []*paych.StateSummary
	var multisigSummaries []*multisig.StateSummary
	minerSummaries := make(map[addr.Address]*miner.StateSummary)

	if err := tree.ForEach(func(key addr.Address, actor *Actor) error {
		acc := acc.WithPrefix("%v ", key) // Intentional shadow
		if key.Protocol() != addr.ID {
			acc.Addf("unexpected address protocol in state tree root: %v", key)
		}
		totalFIl = big.Add(totalFIl, actor.Balance)

		switch actor.Code {
		case builtin.SystemActorCodeID:

		case builtin.InitActorCodeID:
			var st init_.State
			if err := tree.Store.Get(tree.Store.Context(), actor.Head, &st); err != nil {
				return err
			}
			if summary, msgs, err := init_.CheckStateInvariants(&st, tree.Store); err != nil {
				return err
			} else {
				acc.WithPrefix("init: ").AddAll(msgs)
				initSummary = summary
			}
		case builtin.CronActorCodeID:
			var st cron.State
			if err := tree.Store.Get(tree.Store.Context(), actor.Head, &st); err != nil {
				return err
			}
			if summary, msgs, err := cron.CheckStateInvariants(&st, tree.Store); err != nil {
				return err
			} else {
				acc.WithPrefix("cron: ").AddAll(msgs)
				cronSummary = summary
			}

		case builtin.AccountActorCodeID:
			var st account.State
			if err := tree.Store.Get(tree.Store.Context(), actor.Head, &st); err != nil {
				return err
			}
			if summary, msgs, err := account.CheckStateInvariants(&st, key); err != nil {
				return err
			} else {
				acc.WithPrefix("account: ").AddAll(msgs)
				accountSummaries = append(accountSummaries, summary)
			}
		case builtin.StoragePowerActorCodeID:
			var st power.State
			if err := tree.Store.Get(tree.Store.Context(), actor.Head, &st); err != nil {
				return err
			}
			if summary, msgs, err := power.CheckStateInvariants(&st, tree.Store); err != nil {
				return err
			} else {
				acc.WithPrefix("power: ").AddAll(msgs)
				powerSummary = summary
			}
		case builtin.StorageMinerActorCodeID:
			var st miner.State
			if err := tree.Store.Get(tree.Store.Context(), actor.Head, &st); err != nil {
				return err
			}
			summary, msgs := miner.CheckStateInvariants(&st, tree.Store, actor.Balance)
			acc.WithPrefix("miner: ").AddAll(msgs)
			minerSummaries[key] = summary
		case builtin.StorageMarketActorCodeID:
			var st market.State
			if err := tree.Store.Get(tree.Store.Context(), actor.Head, &st); err != nil {
				return err
			}
			if summary, msgs, err := market.CheckStateInvariants(&st, tree.Store, actor.Balance, priorEpoch); err != nil {
				return err
			} else {
				acc.WithPrefix("market: ").AddAll(msgs)
				marketSummary = summary
			}

		case builtin.PaymentChannelActorCodeID:
			var st paych.State
			if err := tree.Store.Get(tree.Store.Context(), actor.Head, &st); err != nil {
				return err
			}
			if summary, msgs, err := paych.CheckStateInvariants(&st, tree.Store, actor.Balance); err != nil {
				return err
			} else {
				acc.WithPrefix("paych: ").AddAll(msgs)
				paychSummaries = append(paychSummaries, summary)
			}

		case builtin.MultisigActorCodeID:
			var st multisig.State
			if err := tree.Store.Get(tree.Store.Context(), actor.Head, &st); err != nil {
				return err
			}
			if summary, msgs, err := multisig.CheckStateInvariants(&st, tree.Store); err != nil {
				return err
			} else {
				acc.WithPrefix("multisig: ").AddAll(msgs)
				multisigSummaries = append(multisigSummaries, summary)
			}

		case builtin.RewardActorCodeID:

		case builtin.VerifiedRegistryActorCodeID:
			var st verifreg.State
			if err := tree.Store.Get(tree.Store.Context(), actor.Head, &st); err != nil {
				return err
			}
			if summary, msgs, err := verifreg.CheckStateInvariants(&st, tree.Store); err != nil {
				return err
			} else {
				acc.WithPrefix("verifreg: ").AddAll(msgs)
				verifregSummary = summary
			}
		default:
			return xerrors.Errorf("unexpected actor code CID %v for address %v", actor.Code, key)

		}
		return nil
	}); err != nil {
		return nil, err
	}

	//
	// Perform cross-actor checks from state summaries here.
	//

	CheckMinersAgainstPower(acc, minerSummaries, powerSummary)

	_ = initSummary
	_ = verifregSummary
	_ = cronSummary
	_ = marketSummary

	if !totalFIl.Equals(expectedBalanceTotal) {
		acc.Addf("total token balance is %v, expected %v", totalFIl, expectedBalanceTotal)
	}

	return acc, nil
}

func CheckMinersAgainstPower(acc *builtin.MessageAccumulator, minerSummaries map[addr.Address]*miner.StateSummary, powerSummary *power.StateSummary) {
	for addr, minerSummary := range minerSummaries { // nolint:nomaprange

		// check claim
		claim, ok := powerSummary.Claims[addr]
		acc.Require(ok, "miner %v has no power claim", addr)
		if ok {
			claimPower := miner.NewPowerPair(claim.RawBytePower, claim.QualityAdjPower)
			acc.Require(minerSummary.ActivePower.Equals(claimPower),
				"miner %v computed active power %v does not match claim %v", addr, minerSummary.ActivePower, claimPower)
			acc.Require(minerSummary.SealProofType == claim.SealProofType,
				"miner seal proof type %d does not match claim proof type %d", minerSummary.SealProofType, claim.SealProofType)
		}

		// check crons
		crons, ok := powerSummary.Crons[addr]
		if !ok {
			continue
		}

		var payload miner.CronEventPayload
		var provingPeriodCron *power.MinerCronEvent
		for _, event := range crons {
			err := payload.UnmarshalCBOR(bytes.NewReader(event.Payload))
			acc.Require(err == nil, "miner %v registered cron at epoch %d with wrong or corrupt payload",
				addr, event.Epoch)

			if payload.EventType == miner.CronEventProvingDeadline {
				if provingPeriodCron != nil {
					acc.Require(false, "miner %v has duplicate proving period crons at epoch %d and %d",
						addr, provingPeriodCron.Epoch, event.Epoch)
				}
				provingPeriodCron = &event
			}
		}

		acc.Require(provingPeriodCron != nil, "miner %v has no proving period cron", addr)
	}
}
