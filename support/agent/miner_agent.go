package agent

import (
	"errors"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin/power"
	vm "github.com/filecoin-project/specs-actors/v2/support/vm"
)

type MinerAgent struct {
	owner           address.Address
	worker          address.Address
	proofType       abi.RegisteredSealProof
	startingBalance abi.TokenAmount
	idAddress       address.Address
	robustAddress   address.Address
	actions         []interface{}
	created         bool
}

func (ma *MinerAgent) Tick(v *vm.VM) error {
	// create miner if it has not yet been created
	if !ma.created {
		ret, code := v.ApplyMessage(ma.owner, builtin.StoragePowerActorAddr, ma.startingBalance, builtin.MethodsPower.CreateMiner, &power.CreateMinerParams{
			Owner:         ma.owner,
			Worker:        ma.worker,
			SealProofType: ma.proofType,
			Peer:          abi.PeerID("not really a peer id"),
		})
		if code != exitcode.Ok {
			return errors.New("Could not create miner")
		}

		if cmr, ok := ret.(*power.CreateMinerReturn); !ok {
			return errors.New("Invalid return from power.CreateMiner")
		} else {
			ma.idAddress = cmr.IDAddress
			ma.robustAddress = cmr.RobustAddress
		}
	}

	return nil
}
