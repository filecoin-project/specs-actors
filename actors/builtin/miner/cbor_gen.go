// Code generated by github.com/whyrusleeping/cbor-gen. DO NOT EDIT.

package miner

import (
	"fmt"
	"io"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	peer "github.com/libp2p/go-libp2p-core/peer"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"
)

var _ = xerrors.Errorf

func (t *State) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{134}); err != nil {
		return err
	}

	// t.PreCommittedSectors (cid.Cid) (struct)

	if err := cbg.WriteCid(w, t.PreCommittedSectors); err != nil {
		return xerrors.Errorf("failed to write cid field t.PreCommittedSectors: %w", err)
	}

	// t.Sectors (cid.Cid) (struct)

	if err := cbg.WriteCid(w, t.Sectors); err != nil {
		return xerrors.Errorf("failed to write cid field t.Sectors: %w", err)
	}

	// t.FaultSet (abi.BitField) (struct)
	if err := t.FaultSet.MarshalCBOR(w); err != nil {
		return err
	}

	// t.ProvingSet (cid.Cid) (struct)

	if err := cbg.WriteCid(w, t.ProvingSet); err != nil {
		return xerrors.Errorf("failed to write cid field t.ProvingSet: %w", err)
	}

	// t.Info (miner.MinerInfo) (struct)
	if err := t.Info.MarshalCBOR(w); err != nil {
		return err
	}

	// t.PoStState (miner.PoStState) (struct)
	if err := t.PoStState.MarshalCBOR(w); err != nil {
		return err
	}
	return nil
}

func (t *State) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 6 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.PreCommittedSectors (cid.Cid) (struct)

	{

		c, err := cbg.ReadCid(br)
		if err != nil {
			return xerrors.Errorf("failed to read cid field t.PreCommittedSectors: %w", err)
		}

		t.PreCommittedSectors = c

	}
	// t.Sectors (cid.Cid) (struct)

	{

		c, err := cbg.ReadCid(br)
		if err != nil {
			return xerrors.Errorf("failed to read cid field t.Sectors: %w", err)
		}

		t.Sectors = c

	}
	// t.FaultSet (abi.BitField) (struct)

	{

		if err := t.FaultSet.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	// t.ProvingSet (cid.Cid) (struct)

	{

		c, err := cbg.ReadCid(br)
		if err != nil {
			return xerrors.Errorf("failed to read cid field t.ProvingSet: %w", err)
		}

		t.ProvingSet = c

	}
	// t.Info (miner.MinerInfo) (struct)

	{

		if err := t.Info.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	// t.PoStState (miner.PoStState) (struct)

	{

		if err := t.PoStState.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	return nil
}

func (t *MinerInfo) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{133}); err != nil {
		return err
	}

	// t.Owner (address.Address) (struct)
	if err := t.Owner.MarshalCBOR(w); err != nil {
		return err
	}

	// t.Worker (address.Address) (struct)
	if err := t.Worker.MarshalCBOR(w); err != nil {
		return err
	}

	// t.PendingWorkerKey (miner.WorkerKeyChange) (struct)
	if err := t.PendingWorkerKey.MarshalCBOR(w); err != nil {
		return err
	}

	// t.PeerId (peer.ID) (string)
	if len(t.PeerId) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.PeerId was too long")
	}

	if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajTextString, uint64(len(t.PeerId)))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(t.PeerId)); err != nil {
		return err
	}

	// t.SectorSize (abi.SectorSize) (uint64)
	if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.SectorSize))); err != nil {
		return err
	}
	return nil
}

func (t *MinerInfo) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 5 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.Owner (address.Address) (struct)

	{

		if err := t.Owner.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	// t.Worker (address.Address) (struct)

	{

		if err := t.Worker.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	// t.PendingWorkerKey (miner.WorkerKeyChange) (struct)

	{

		pb, err := br.PeekByte()
		if err != nil {
			return err
		}
		if pb == cbg.CborNull[0] {
			var nbuf [1]byte
			if _, err := br.Read(nbuf[:]); err != nil {
				return err
			}
		} else {
			t.PendingWorkerKey = new(WorkerKeyChange)
			if err := t.PendingWorkerKey.UnmarshalCBOR(br); err != nil {
				return err
			}
		}

	}
	// t.PeerId (peer.ID) (string)

	{
		sval, err := cbg.ReadString(br)
		if err != nil {
			return err
		}

		t.PeerId = peer.ID(sval)
	}
	// t.SectorSize (abi.SectorSize) (uint64)

	maj, extra, err = cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajUnsignedInt {
		return fmt.Errorf("wrong type for uint64 field")
	}
	t.SectorSize = abi.SectorSize(extra)
	return nil
}

func (t *PoStState) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{130}); err != nil {
		return err
	}

	// t.ProvingPeriodStart (abi.ChainEpoch) (int64)
	if t.ProvingPeriodStart >= 0 {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.ProvingPeriodStart))); err != nil {
			return err
		}
	} else {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajNegativeInt, uint64(-t.ProvingPeriodStart)-1)); err != nil {
			return err
		}
	}

	// t.NumConsecutiveFailures (int64) (int64)
	if t.NumConsecutiveFailures >= 0 {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.NumConsecutiveFailures))); err != nil {
			return err
		}
	} else {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajNegativeInt, uint64(-t.NumConsecutiveFailures)-1)); err != nil {
			return err
		}
	}
	return nil
}

func (t *PoStState) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 2 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.ProvingPeriodStart (abi.ChainEpoch) (int64)
	{
		maj, extra, err := cbg.CborReadHeader(br)
		var extraI int64
		if err != nil {
			return err
		}
		switch maj {
		case cbg.MajUnsignedInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 positive overflow")
			}
		case cbg.MajNegativeInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 negative oveflow")
			}
			extraI = -1 - extraI
		default:
			return fmt.Errorf("wrong type for int64 field: %d", maj)
		}

		t.ProvingPeriodStart = abi.ChainEpoch(extraI)
	}
	// t.NumConsecutiveFailures (int64) (int64)
	{
		maj, extra, err := cbg.CborReadHeader(br)
		var extraI int64
		if err != nil {
			return err
		}
		switch maj {
		case cbg.MajUnsignedInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 positive overflow")
			}
		case cbg.MajNegativeInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 negative oveflow")
			}
			extraI = -1 - extraI
		default:
			return fmt.Errorf("wrong type for int64 field: %d", maj)
		}

		t.NumConsecutiveFailures = int64(extraI)
	}
	return nil
}

func (t *SectorPreCommitOnChainInfo) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{131}); err != nil {
		return err
	}

	// t.Info (miner.SectorPreCommitInfo) (struct)
	if err := t.Info.MarshalCBOR(w); err != nil {
		return err
	}

	// t.PreCommitDeposit (big.Int) (struct)
	if err := t.PreCommitDeposit.MarshalCBOR(w); err != nil {
		return err
	}

	// t.PreCommitEpoch (abi.ChainEpoch) (int64)
	if t.PreCommitEpoch >= 0 {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.PreCommitEpoch))); err != nil {
			return err
		}
	} else {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajNegativeInt, uint64(-t.PreCommitEpoch)-1)); err != nil {
			return err
		}
	}
	return nil
}

func (t *SectorPreCommitOnChainInfo) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 3 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.Info (miner.SectorPreCommitInfo) (struct)

	{

		if err := t.Info.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	// t.PreCommitDeposit (big.Int) (struct)

	{

		if err := t.PreCommitDeposit.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	// t.PreCommitEpoch (abi.ChainEpoch) (int64)
	{
		maj, extra, err := cbg.CborReadHeader(br)
		var extraI int64
		if err != nil {
			return err
		}
		switch maj {
		case cbg.MajUnsignedInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 positive overflow")
			}
		case cbg.MajNegativeInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 negative oveflow")
			}
			extraI = -1 - extraI
		default:
			return fmt.Errorf("wrong type for int64 field: %d", maj)
		}

		t.PreCommitEpoch = abi.ChainEpoch(extraI)
	}
	return nil
}

func (t *SectorPreCommitInfo) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{133}); err != nil {
		return err
	}

	// t.SectorNumber (abi.SectorNumber) (uint64)
	if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.SectorNumber))); err != nil {
		return err
	}

	// t.SealedCID (cid.Cid) (struct)

	if err := cbg.WriteCid(w, t.SealedCID); err != nil {
		return xerrors.Errorf("failed to write cid field t.SealedCID: %w", err)
	}

	// t.SealEpoch (abi.ChainEpoch) (int64)
	if t.SealEpoch >= 0 {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.SealEpoch))); err != nil {
			return err
		}
	} else {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajNegativeInt, uint64(-t.SealEpoch)-1)); err != nil {
			return err
		}
	}

	// t.DealIDs ([]abi.DealID) (slice)
	if len(t.DealIDs) > cbg.MaxLength {
		return xerrors.Errorf("Slice value in field t.DealIDs was too long")
	}

	if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajArray, uint64(len(t.DealIDs)))); err != nil {
		return err
	}
	for _, v := range t.DealIDs {
		if err := cbg.CborWriteHeader(w, cbg.MajUnsignedInt, uint64(v)); err != nil {
			return err
		}
	}

	// t.Expiration (abi.ChainEpoch) (int64)
	if t.Expiration >= 0 {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.Expiration))); err != nil {
			return err
		}
	} else {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajNegativeInt, uint64(-t.Expiration)-1)); err != nil {
			return err
		}
	}
	return nil
}

func (t *SectorPreCommitInfo) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 5 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.SectorNumber (abi.SectorNumber) (uint64)

	maj, extra, err = cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajUnsignedInt {
		return fmt.Errorf("wrong type for uint64 field")
	}
	t.SectorNumber = abi.SectorNumber(extra)
	// t.SealedCID (cid.Cid) (struct)

	{

		c, err := cbg.ReadCid(br)
		if err != nil {
			return xerrors.Errorf("failed to read cid field t.SealedCID: %w", err)
		}

		t.SealedCID = c

	}
	// t.SealEpoch (abi.ChainEpoch) (int64)
	{
		maj, extra, err := cbg.CborReadHeader(br)
		var extraI int64
		if err != nil {
			return err
		}
		switch maj {
		case cbg.MajUnsignedInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 positive overflow")
			}
		case cbg.MajNegativeInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 negative oveflow")
			}
			extraI = -1 - extraI
		default:
			return fmt.Errorf("wrong type for int64 field: %d", maj)
		}

		t.SealEpoch = abi.ChainEpoch(extraI)
	}
	// t.DealIDs ([]abi.DealID) (slice)

	maj, extra, err = cbg.CborReadHeader(br)
	if err != nil {
		return err
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("t.DealIDs: array too large (%d)", extra)
	}

	if maj != cbg.MajArray {
		return fmt.Errorf("expected cbor array")
	}
	if extra > 0 {
		t.DealIDs = make([]abi.DealID, extra)
	}
	for i := 0; i < int(extra); i++ {

		maj, val, err := cbg.CborReadHeader(br)
		if err != nil {
			return xerrors.Errorf("failed to read uint64 for t.DealIDs slice: %w", err)
		}

		if maj != cbg.MajUnsignedInt {
			return xerrors.Errorf("value read for array t.DealIDs was not a uint, instead got %d", maj)
		}

		t.DealIDs[i] = abi.DealID(val)
	}

	// t.Expiration (abi.ChainEpoch) (int64)
	{
		maj, extra, err := cbg.CborReadHeader(br)
		var extraI int64
		if err != nil {
			return err
		}
		switch maj {
		case cbg.MajUnsignedInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 positive overflow")
			}
		case cbg.MajNegativeInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 negative oveflow")
			}
			extraI = -1 - extraI
		default:
			return fmt.Errorf("wrong type for int64 field: %d", maj)
		}

		t.Expiration = abi.ChainEpoch(extraI)
	}
	return nil
}

func (t *SectorOnChainInfo) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{134}); err != nil {
		return err
	}

	// t.Info (miner.SectorPreCommitInfo) (struct)
	if err := t.Info.MarshalCBOR(w); err != nil {
		return err
	}

	// t.ActivationEpoch (abi.ChainEpoch) (int64)
	if t.ActivationEpoch >= 0 {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.ActivationEpoch))); err != nil {
			return err
		}
	} else {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajNegativeInt, uint64(-t.ActivationEpoch)-1)); err != nil {
			return err
		}
	}

	// t.DealWeight (big.Int) (struct)
	if err := t.DealWeight.MarshalCBOR(w); err != nil {
		return err
	}

	// t.PledgeRequirement (big.Int) (struct)
	if err := t.PledgeRequirement.MarshalCBOR(w); err != nil {
		return err
	}

	// t.DeclaredFaultEpoch (abi.ChainEpoch) (int64)
	if t.DeclaredFaultEpoch >= 0 {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.DeclaredFaultEpoch))); err != nil {
			return err
		}
	} else {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajNegativeInt, uint64(-t.DeclaredFaultEpoch)-1)); err != nil {
			return err
		}
	}

	// t.DeclaredFaultDuration (abi.ChainEpoch) (int64)
	if t.DeclaredFaultDuration >= 0 {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.DeclaredFaultDuration))); err != nil {
			return err
		}
	} else {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajNegativeInt, uint64(-t.DeclaredFaultDuration)-1)); err != nil {
			return err
		}
	}
	return nil
}

func (t *SectorOnChainInfo) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 6 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.Info (miner.SectorPreCommitInfo) (struct)

	{

		if err := t.Info.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	// t.ActivationEpoch (abi.ChainEpoch) (int64)
	{
		maj, extra, err := cbg.CborReadHeader(br)
		var extraI int64
		if err != nil {
			return err
		}
		switch maj {
		case cbg.MajUnsignedInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 positive overflow")
			}
		case cbg.MajNegativeInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 negative oveflow")
			}
			extraI = -1 - extraI
		default:
			return fmt.Errorf("wrong type for int64 field: %d", maj)
		}

		t.ActivationEpoch = abi.ChainEpoch(extraI)
	}
	// t.DealWeight (big.Int) (struct)

	{

		if err := t.DealWeight.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	// t.PledgeRequirement (big.Int) (struct)

	{

		if err := t.PledgeRequirement.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	// t.DeclaredFaultEpoch (abi.ChainEpoch) (int64)
	{
		maj, extra, err := cbg.CborReadHeader(br)
		var extraI int64
		if err != nil {
			return err
		}
		switch maj {
		case cbg.MajUnsignedInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 positive overflow")
			}
		case cbg.MajNegativeInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 negative oveflow")
			}
			extraI = -1 - extraI
		default:
			return fmt.Errorf("wrong type for int64 field: %d", maj)
		}

		t.DeclaredFaultEpoch = abi.ChainEpoch(extraI)
	}
	// t.DeclaredFaultDuration (abi.ChainEpoch) (int64)
	{
		maj, extra, err := cbg.CborReadHeader(br)
		var extraI int64
		if err != nil {
			return err
		}
		switch maj {
		case cbg.MajUnsignedInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 positive overflow")
			}
		case cbg.MajNegativeInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 negative oveflow")
			}
			extraI = -1 - extraI
		default:
			return fmt.Errorf("wrong type for int64 field: %d", maj)
		}

		t.DeclaredFaultDuration = abi.ChainEpoch(extraI)
	}
	return nil
}

func (t *WorkerKeyChange) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{130}); err != nil {
		return err
	}

	// t.NewWorker (address.Address) (struct)
	if err := t.NewWorker.MarshalCBOR(w); err != nil {
		return err
	}

	// t.EffectiveAt (abi.ChainEpoch) (int64)
	if t.EffectiveAt >= 0 {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.EffectiveAt))); err != nil {
			return err
		}
	} else {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajNegativeInt, uint64(-t.EffectiveAt)-1)); err != nil {
			return err
		}
	}
	return nil
}

func (t *WorkerKeyChange) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 2 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.NewWorker (address.Address) (struct)

	{

		if err := t.NewWorker.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	// t.EffectiveAt (abi.ChainEpoch) (int64)
	{
		maj, extra, err := cbg.CborReadHeader(br)
		var extraI int64
		if err != nil {
			return err
		}
		switch maj {
		case cbg.MajUnsignedInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 positive overflow")
			}
		case cbg.MajNegativeInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 negative oveflow")
			}
			extraI = -1 - extraI
		default:
			return fmt.Errorf("wrong type for int64 field: %d", maj)
		}

		t.EffectiveAt = abi.ChainEpoch(extraI)
	}
	return nil
}

func (t *TerminateSectorsParams) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{129}); err != nil {
		return err
	}

	// t.Sectors (abi.BitField) (struct)
	if err := t.Sectors.MarshalCBOR(w); err != nil {
		return err
	}
	return nil
}

func (t *TerminateSectorsParams) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 1 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.Sectors (abi.BitField) (struct)

	{

		pb, err := br.PeekByte()
		if err != nil {
			return err
		}
		if pb == cbg.CborNull[0] {
			var nbuf [1]byte
			if _, err := br.Read(nbuf[:]); err != nil {
				return err
			}
		} else {
			t.Sectors = new(abi.BitField)
			if err := t.Sectors.UnmarshalCBOR(br); err != nil {
				return err
			}
		}

	}
	return nil
}

func (t *ProveCommitSectorParams) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{130}); err != nil {
		return err
	}

	// t.SectorNumber (abi.SectorNumber) (uint64)
	if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.SectorNumber))); err != nil {
		return err
	}

	// t.Proof (abi.SealProof) (struct)
	if err := t.Proof.MarshalCBOR(w); err != nil {
		return err
	}
	return nil
}

func (t *ProveCommitSectorParams) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 2 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.SectorNumber (abi.SectorNumber) (uint64)

	maj, extra, err = cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajUnsignedInt {
		return fmt.Errorf("wrong type for uint64 field")
	}
	t.SectorNumber = abi.SectorNumber(extra)
	// t.Proof (abi.SealProof) (struct)

	{

		if err := t.Proof.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	return nil
}

func (t *OnDeferredCronEventParams) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{129}); err != nil {
		return err
	}

	// t.CallbackPayload ([]uint8) (slice)
	if len(t.CallbackPayload) > cbg.ByteArrayMaxLen {
		return xerrors.Errorf("Byte array in field t.CallbackPayload was too long")
	}

	if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajByteString, uint64(len(t.CallbackPayload)))); err != nil {
		return err
	}
	if _, err := w.Write(t.CallbackPayload); err != nil {
		return err
	}
	return nil
}

func (t *OnDeferredCronEventParams) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 1 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.CallbackPayload ([]uint8) (slice)

	maj, extra, err = cbg.CborReadHeader(br)
	if err != nil {
		return err
	}

	if extra > cbg.ByteArrayMaxLen {
		return fmt.Errorf("t.CallbackPayload: byte array too large (%d)", extra)
	}
	if maj != cbg.MajByteString {
		return fmt.Errorf("expected byte array")
	}
	t.CallbackPayload = make([]byte, extra)
	if _, err := io.ReadFull(br, t.CallbackPayload); err != nil {
		return err
	}
	return nil
}

func (t *ChangeWorkerAddressParams) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{129}); err != nil {
		return err
	}

	// t.NewWorker (address.Address) (struct)
	if err := t.NewWorker.MarshalCBOR(w); err != nil {
		return err
	}
	return nil
}

func (t *ChangeWorkerAddressParams) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 1 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.NewWorker (address.Address) (struct)

	{

		if err := t.NewWorker.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	return nil
}

func (t *ExtendSectorExpirationParams) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{130}); err != nil {
		return err
	}

	// t.SectorNumber (abi.SectorNumber) (uint64)
	if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.SectorNumber))); err != nil {
		return err
	}

	// t.NewExpiration (abi.ChainEpoch) (int64)
	if t.NewExpiration >= 0 {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.NewExpiration))); err != nil {
			return err
		}
	} else {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajNegativeInt, uint64(-t.NewExpiration)-1)); err != nil {
			return err
		}
	}
	return nil
}

func (t *ExtendSectorExpirationParams) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 2 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.SectorNumber (abi.SectorNumber) (uint64)

	maj, extra, err = cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajUnsignedInt {
		return fmt.Errorf("wrong type for uint64 field")
	}
	t.SectorNumber = abi.SectorNumber(extra)
	// t.NewExpiration (abi.ChainEpoch) (int64)
	{
		maj, extra, err := cbg.CborReadHeader(br)
		var extraI int64
		if err != nil {
			return err
		}
		switch maj {
		case cbg.MajUnsignedInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 positive overflow")
			}
		case cbg.MajNegativeInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 negative oveflow")
			}
			extraI = -1 - extraI
		default:
			return fmt.Errorf("wrong type for int64 field: %d", maj)
		}

		t.NewExpiration = abi.ChainEpoch(extraI)
	}
	return nil
}

func (t *DeclareTemporaryFaultsParams) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{130}); err != nil {
		return err
	}

	// t.SectorNumbers (abi.BitField) (struct)
	if err := t.SectorNumbers.MarshalCBOR(w); err != nil {
		return err
	}

	// t.Duration (abi.ChainEpoch) (int64)
	if t.Duration >= 0 {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.Duration))); err != nil {
			return err
		}
	} else {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajNegativeInt, uint64(-t.Duration)-1)); err != nil {
			return err
		}
	}
	return nil
}

func (t *DeclareTemporaryFaultsParams) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 2 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.SectorNumbers (abi.BitField) (struct)

	{

		if err := t.SectorNumbers.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	// t.Duration (abi.ChainEpoch) (int64)
	{
		maj, extra, err := cbg.CborReadHeader(br)
		var extraI int64
		if err != nil {
			return err
		}
		switch maj {
		case cbg.MajUnsignedInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 positive overflow")
			}
		case cbg.MajNegativeInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 negative oveflow")
			}
			extraI = -1 - extraI
		default:
			return fmt.Errorf("wrong type for int64 field: %d", maj)
		}

		t.Duration = abi.ChainEpoch(extraI)
	}
	return nil
}

func (t *GetControlAddressesReturn) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{130}); err != nil {
		return err
	}

	// t.Owner (address.Address) (struct)
	if err := t.Owner.MarshalCBOR(w); err != nil {
		return err
	}

	// t.Worker (address.Address) (struct)
	if err := t.Worker.MarshalCBOR(w); err != nil {
		return err
	}
	return nil
}

func (t *GetControlAddressesReturn) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 2 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.Owner (address.Address) (struct)

	{

		if err := t.Owner.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	// t.Worker (address.Address) (struct)

	{

		if err := t.Worker.UnmarshalCBOR(br); err != nil {
			return err
		}

	}
	return nil
}

func (t *CronEventPayload) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write([]byte{130}); err != nil {
		return err
	}

	// t.EventType (miner.CronEventType) (int64)
	if t.EventType >= 0 {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajUnsignedInt, uint64(t.EventType))); err != nil {
			return err
		}
	} else {
		if _, err := w.Write(cbg.CborEncodeMajorType(cbg.MajNegativeInt, uint64(-t.EventType)-1)); err != nil {
			return err
		}
	}

	// t.Sectors (abi.BitField) (struct)
	if err := t.Sectors.MarshalCBOR(w); err != nil {
		return err
	}
	return nil
}

func (t *CronEventPayload) UnmarshalCBOR(r io.Reader) error {
	br := cbg.GetPeeker(r)

	maj, extra, err := cbg.CborReadHeader(br)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 2 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.EventType (miner.CronEventType) (int64)
	{
		maj, extra, err := cbg.CborReadHeader(br)
		var extraI int64
		if err != nil {
			return err
		}
		switch maj {
		case cbg.MajUnsignedInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 positive overflow")
			}
		case cbg.MajNegativeInt:
			extraI = int64(extra)
			if extraI < 0 {
				return fmt.Errorf("int64 negative oveflow")
			}
			extraI = -1 - extraI
		default:
			return fmt.Errorf("wrong type for int64 field: %d", maj)
		}

		t.EventType = CronEventType(extraI)
	}
	// t.Sectors (abi.BitField) (struct)

	{

		pb, err := br.PeekByte()
		if err != nil {
			return err
		}
		if pb == cbg.CborNull[0] {
			var nbuf [1]byte
			if _, err := br.Read(nbuf[:]); err != nil {
				return err
			}
		} else {
			t.Sectors = new(abi.BitField)
			if err := t.Sectors.UnmarshalCBOR(br); err != nil {
				return err
			}
		}

	}
	return nil
}
