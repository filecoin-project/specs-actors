// Code generated by github.com/whyrusleeping/cbor-gen. DO NOT EDIT.

package power

import (
	"fmt"
	"io"

	abi "github.com/filecoin-project/go-state-types/abi"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"
)

var _ = xerrors.Errorf

var lengthBufState = []byte{143}

func (t *State) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write(lengthBufState); err != nil {
		return err
	}

	scratch := make([]byte, 9)

	// t.TotalRawBytePower (big.Int) (struct)
	if err := t.TotalRawBytePower.MarshalCBOR(w); err != nil {
		return err
	}

	// t.TotalBytesCommitted (big.Int) (struct)
	if err := t.TotalBytesCommitted.MarshalCBOR(w); err != nil {
		return err
	}

	// t.TotalQualityAdjPower (big.Int) (struct)
	if err := t.TotalQualityAdjPower.MarshalCBOR(w); err != nil {
		return err
	}

	// t.TotalQABytesCommitted (big.Int) (struct)
	if err := t.TotalQABytesCommitted.MarshalCBOR(w); err != nil {
		return err
	}

	// t.TotalPledgeCollateral (big.Int) (struct)
	if err := t.TotalPledgeCollateral.MarshalCBOR(w); err != nil {
		return err
	}

	// t.ThisEpochRawBytePower (big.Int) (struct)
	if err := t.ThisEpochRawBytePower.MarshalCBOR(w); err != nil {
		return err
	}

	// t.ThisEpochQualityAdjPower (big.Int) (struct)
	if err := t.ThisEpochQualityAdjPower.MarshalCBOR(w); err != nil {
		return err
	}

	// t.ThisEpochPledgeCollateral (big.Int) (struct)
	if err := t.ThisEpochPledgeCollateral.MarshalCBOR(w); err != nil {
		return err
	}

	// t.ThisEpochQAPowerSmoothed (smoothing.FilterEstimate) (struct)
	if err := t.ThisEpochQAPowerSmoothed.MarshalCBOR(w); err != nil {
		return err
	}

	// t.MinerCount (int64) (int64)
	if t.MinerCount >= 0 {
		if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajUnsignedInt, uint64(t.MinerCount)); err != nil {
			return err
		}
	} else {
		if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajNegativeInt, uint64(-t.MinerCount-1)); err != nil {
			return err
		}
	}

	// t.MinerAboveMinPowerCount (int64) (int64)
	if t.MinerAboveMinPowerCount >= 0 {
		if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajUnsignedInt, uint64(t.MinerAboveMinPowerCount)); err != nil {
			return err
		}
	} else {
		if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajNegativeInt, uint64(-t.MinerAboveMinPowerCount-1)); err != nil {
			return err
		}
	}

	// t.CronEventQueue (cid.Cid) (struct)

	if err := cbg.WriteCidBuf(scratch, w, t.CronEventQueue); err != nil {
		return xerrors.Errorf("failed to write cid field t.CronEventQueue: %w", err)
	}

	// t.FirstCronEpoch (abi.ChainEpoch) (int64)
	if t.FirstCronEpoch >= 0 {
		if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajUnsignedInt, uint64(t.FirstCronEpoch)); err != nil {
			return err
		}
	} else {
		if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajNegativeInt, uint64(-t.FirstCronEpoch-1)); err != nil {
			return err
		}
	}

	// t.Claims (cid.Cid) (struct)

	if err := cbg.WriteCidBuf(scratch, w, t.Claims); err != nil {
		return xerrors.Errorf("failed to write cid field t.Claims: %w", err)
	}

	// t.ProofValidationBatch (cid.Cid) (struct)

	if t.ProofValidationBatch == nil {
		if _, err := w.Write(cbg.CborNull); err != nil {
			return err
		}
	} else {
		if err := cbg.WriteCidBuf(scratch, w, *t.ProofValidationBatch); err != nil {
			return xerrors.Errorf("failed to write cid field t.ProofValidationBatch: %w", err)
		}
	}

	return nil
}

func (t *State) UnmarshalCBOR(r io.Reader) error {
	*t = State{}

	br := cbg.GetPeeker(r)
	scratch := make([]byte, 8)

	maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 15 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.TotalRawBytePower (big.Int) (struct)

	{

		if err := t.TotalRawBytePower.UnmarshalCBOR(br); err != nil {
			return xerrors.Errorf("unmarshaling t.TotalRawBytePower: %w", err)
		}

	}
	// t.TotalBytesCommitted (big.Int) (struct)

	{

		if err := t.TotalBytesCommitted.UnmarshalCBOR(br); err != nil {
			return xerrors.Errorf("unmarshaling t.TotalBytesCommitted: %w", err)
		}

	}
	// t.TotalQualityAdjPower (big.Int) (struct)

	{

		if err := t.TotalQualityAdjPower.UnmarshalCBOR(br); err != nil {
			return xerrors.Errorf("unmarshaling t.TotalQualityAdjPower: %w", err)
		}

	}
	// t.TotalQABytesCommitted (big.Int) (struct)

	{

		if err := t.TotalQABytesCommitted.UnmarshalCBOR(br); err != nil {
			return xerrors.Errorf("unmarshaling t.TotalQABytesCommitted: %w", err)
		}

	}
	// t.TotalPledgeCollateral (big.Int) (struct)

	{

		if err := t.TotalPledgeCollateral.UnmarshalCBOR(br); err != nil {
			return xerrors.Errorf("unmarshaling t.TotalPledgeCollateral: %w", err)
		}

	}
	// t.ThisEpochRawBytePower (big.Int) (struct)

	{

		if err := t.ThisEpochRawBytePower.UnmarshalCBOR(br); err != nil {
			return xerrors.Errorf("unmarshaling t.ThisEpochRawBytePower: %w", err)
		}

	}
	// t.ThisEpochQualityAdjPower (big.Int) (struct)

	{

		if err := t.ThisEpochQualityAdjPower.UnmarshalCBOR(br); err != nil {
			return xerrors.Errorf("unmarshaling t.ThisEpochQualityAdjPower: %w", err)
		}

	}
	// t.ThisEpochPledgeCollateral (big.Int) (struct)

	{

		if err := t.ThisEpochPledgeCollateral.UnmarshalCBOR(br); err != nil {
			return xerrors.Errorf("unmarshaling t.ThisEpochPledgeCollateral: %w", err)
		}

	}
	// t.ThisEpochQAPowerSmoothed (smoothing.FilterEstimate) (struct)

	{

		if err := t.ThisEpochQAPowerSmoothed.UnmarshalCBOR(br); err != nil {
			return xerrors.Errorf("unmarshaling t.ThisEpochQAPowerSmoothed: %w", err)
		}

	}
	// t.MinerCount (int64) (int64)
	{
		maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
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

		t.MinerCount = int64(extraI)
	}
	// t.MinerAboveMinPowerCount (int64) (int64)
	{
		maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
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

		t.MinerAboveMinPowerCount = int64(extraI)
	}
	// t.CronEventQueue (cid.Cid) (struct)

	{

		c, err := cbg.ReadCid(br)
		if err != nil {
			return xerrors.Errorf("failed to read cid field t.CronEventQueue: %w", err)
		}

		t.CronEventQueue = c

	}
	// t.FirstCronEpoch (abi.ChainEpoch) (int64)
	{
		maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
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

		t.FirstCronEpoch = abi.ChainEpoch(extraI)
	}
	// t.Claims (cid.Cid) (struct)

	{

		c, err := cbg.ReadCid(br)
		if err != nil {
			return xerrors.Errorf("failed to read cid field t.Claims: %w", err)
		}

		t.Claims = c

	}
	// t.ProofValidationBatch (cid.Cid) (struct)

	{

		b, err := br.ReadByte()
		if err != nil {
			return err
		}
		if b != cbg.CborNull[0] {
			if err := br.UnreadByte(); err != nil {
				return err
			}

			c, err := cbg.ReadCid(br)
			if err != nil {
				return xerrors.Errorf("failed to read cid field t.ProofValidationBatch: %w", err)
			}

			t.ProofValidationBatch = &c
		}

	}
	return nil
}

var lengthBufClaim = []byte{131}

func (t *Claim) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write(lengthBufClaim); err != nil {
		return err
	}

	scratch := make([]byte, 9)

	// t.WindowPoStProofType (abi.RegisteredPoStProof) (int64)
	if t.WindowPoStProofType >= 0 {
		if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajUnsignedInt, uint64(t.WindowPoStProofType)); err != nil {
			return err
		}
	} else {
		if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajNegativeInt, uint64(-t.WindowPoStProofType-1)); err != nil {
			return err
		}
	}

	// t.RawBytePower (big.Int) (struct)
	if err := t.RawBytePower.MarshalCBOR(w); err != nil {
		return err
	}

	// t.QualityAdjPower (big.Int) (struct)
	if err := t.QualityAdjPower.MarshalCBOR(w); err != nil {
		return err
	}
	return nil
}

func (t *Claim) UnmarshalCBOR(r io.Reader) error {
	*t = Claim{}

	br := cbg.GetPeeker(r)
	scratch := make([]byte, 8)

	maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 3 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.WindowPoStProofType (abi.RegisteredPoStProof) (int64)
	{
		maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
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

		t.WindowPoStProofType = abi.RegisteredPoStProof(extraI)
	}
	// t.RawBytePower (big.Int) (struct)

	{

		if err := t.RawBytePower.UnmarshalCBOR(br); err != nil {
			return xerrors.Errorf("unmarshaling t.RawBytePower: %w", err)
		}

	}
	// t.QualityAdjPower (big.Int) (struct)

	{

		if err := t.QualityAdjPower.UnmarshalCBOR(br); err != nil {
			return xerrors.Errorf("unmarshaling t.QualityAdjPower: %w", err)
		}

	}
	return nil
}

var lengthBufCronEvent = []byte{130}

func (t *CronEvent) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write(lengthBufCronEvent); err != nil {
		return err
	}

	scratch := make([]byte, 9)

	// t.MinerAddr (address.Address) (struct)
	if err := t.MinerAddr.MarshalCBOR(w); err != nil {
		return err
	}

	// t.CallbackPayload ([]uint8) (slice)
	if len(t.CallbackPayload) > cbg.ByteArrayMaxLen {
		return xerrors.Errorf("Byte array in field t.CallbackPayload was too long")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajByteString, uint64(len(t.CallbackPayload))); err != nil {
		return err
	}

	if _, err := w.Write(t.CallbackPayload[:]); err != nil {
		return err
	}
	return nil
}

func (t *CronEvent) UnmarshalCBOR(r io.Reader) error {
	*t = CronEvent{}

	br := cbg.GetPeeker(r)
	scratch := make([]byte, 8)

	maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 2 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.MinerAddr (address.Address) (struct)

	{

		if err := t.MinerAddr.UnmarshalCBOR(br); err != nil {
			return xerrors.Errorf("unmarshaling t.MinerAddr: %w", err)
		}

	}
	// t.CallbackPayload ([]uint8) (slice)

	maj, extra, err = cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}

	if extra > cbg.ByteArrayMaxLen {
		return fmt.Errorf("t.CallbackPayload: byte array too large (%d)", extra)
	}
	if maj != cbg.MajByteString {
		return fmt.Errorf("expected byte array")
	}

	if extra > 0 {
		t.CallbackPayload = make([]uint8, extra)
	}

	if _, err := io.ReadFull(br, t.CallbackPayload[:]); err != nil {
		return err
	}
	return nil
}

var lengthBufCurrentTotalPowerReturn = []byte{132}

func (t *CurrentTotalPowerReturn) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	if _, err := w.Write(lengthBufCurrentTotalPowerReturn); err != nil {
		return err
	}

	// t.RawBytePower (big.Int) (struct)
	if err := t.RawBytePower.MarshalCBOR(w); err != nil {
		return err
	}

	// t.QualityAdjPower (big.Int) (struct)
	if err := t.QualityAdjPower.MarshalCBOR(w); err != nil {
		return err
	}

	// t.PledgeCollateral (big.Int) (struct)
	if err := t.PledgeCollateral.MarshalCBOR(w); err != nil {
		return err
	}

	// t.QualityAdjPowerSmoothed (smoothing.FilterEstimate) (struct)
	if err := t.QualityAdjPowerSmoothed.MarshalCBOR(w); err != nil {
		return err
	}
	return nil
}

func (t *CurrentTotalPowerReturn) UnmarshalCBOR(r io.Reader) error {
	*t = CurrentTotalPowerReturn{}

	br := cbg.GetPeeker(r)
	scratch := make([]byte, 8)

	maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 4 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.RawBytePower (big.Int) (struct)

	{

		if err := t.RawBytePower.UnmarshalCBOR(br); err != nil {
			return xerrors.Errorf("unmarshaling t.RawBytePower: %w", err)
		}

	}
	// t.QualityAdjPower (big.Int) (struct)

	{

		if err := t.QualityAdjPower.UnmarshalCBOR(br); err != nil {
			return xerrors.Errorf("unmarshaling t.QualityAdjPower: %w", err)
		}

	}
	// t.PledgeCollateral (big.Int) (struct)

	{

		if err := t.PledgeCollateral.UnmarshalCBOR(br); err != nil {
			return xerrors.Errorf("unmarshaling t.PledgeCollateral: %w", err)
		}

	}
	// t.QualityAdjPowerSmoothed (smoothing.FilterEstimate) (struct)

	{

		if err := t.QualityAdjPowerSmoothed.UnmarshalCBOR(br); err != nil {
			return xerrors.Errorf("unmarshaling t.QualityAdjPowerSmoothed: %w", err)
		}

	}
	return nil
}
