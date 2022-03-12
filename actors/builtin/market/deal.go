package market

import (
	"bytes"
	"fmt"
	"io"

	addr "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	acrypto "github.com/filecoin-project/go-state-types/crypto"
	market0 "github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
)

//var PieceCIDPrefix = cid.Prefix{
//	Version:  1,
//	Codec:    cid.FilCommitmentUnsealed,
//	MhType:   mh.SHA2_256_TRUNC254_PADDED,
//	MhLength: 32,
//}
var PieceCIDPrefix = market0.PieceCIDPrefix

// The DealLabel is a "kinded union" -- can either be a String or a byte slice, but not both
// TODO: What represents the empty label and how do we marshall it?
type DealLabel struct {
	labelString string
	labelBytes  []byte
}

var EmptyDealLabel = DealLabel{}

func NewDealLabelFromString(s string) (DealLabel, error) {
	if len(s) > DealMaxLabelSize {
		return EmptyDealLabel, xerrors.Errorf("provided string is too large to be a label (%d), max length (%d)", len(s), DealMaxLabelSize)
	}
	// TODO: Check UTF-8
	return DealLabel{
		labelString: s,
	}, nil
}

func NewDealLabelFromBytes(b []byte) (DealLabel, error) {
	if len(b) > DealMaxLabelSize {
		return EmptyDealLabel, xerrors.Errorf("provided bytes are too large to be a label (%d), max length (%d)", len(b), DealMaxLabelSize)
	}
	// TODO: nilcheck? See note about emptiness.
	return DealLabel{
		labelBytes: b,
	}, nil
}

func (label DealLabel) IsStringSet() bool {
	return label.labelString == ""
}

func (label DealLabel) IsBytesSet() bool {
	return len(label.labelBytes) != 0
}

func (label DealLabel) ToString() (string, error) {
	if label.IsBytesSet() {
		return "", xerrors.Errorf("label has bytes set")
	}

	return label.labelString, nil
}

func (label DealLabel) ToBytes() ([]byte, error) {
	if label.IsStringSet() {
		return nil, xerrors.Errorf("label has string set")
	}

	return label.labelBytes, nil
}

func (label DealLabel) Length() int {
	if label.IsStringSet() {
		return len(label.labelString)
	}

	return len(label.labelBytes)
}
func (l DealLabel) Equals(o DealLabel) bool {
	return l.labelString == o.labelString && bytes.Equal(l.labelBytes, o.labelBytes)
}

func (label *DealLabel) MarshalCBOR(w io.Writer) error {
	// TODO: Whait if nil?
	if label.IsStringSet() && label.IsBytesSet() {
		return fmt.Errorf("dealLabel cannot have both string and bytes set")
	}

	scratch := make([]byte, 9)

	if label.IsBytesSet() {
		if len(label.labelBytes) > cbg.ByteArrayMaxLen {
			return xerrors.Errorf("labelBytes is too long to marshal (%d), max allowed (%d)", len(label.labelBytes), cbg.ByteArrayMaxLen)
		}

		if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajByteString, uint64(len(label.labelBytes))); err != nil {
			return err
		}

		if _, err := w.Write(label.labelBytes[:]); err != nil {
			return err
		}
	} else {
		// "Empty" labels (empty strings and nil bytes) get marshaled as empty strings
		if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, uint64(len(label.labelString))); err != nil {
			return err
		}
		if _, err := io.WriteString(w, label.labelString); err != nil {
			return err
		}
	}

	return nil
}

func (label *DealLabel) UnmarshalCBOR(br io.Reader) error {
	// TODO: Whait if nil?

	scratch := make([]byte, 8)

	maj, length, err := cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}

	if maj == cbg.MajTextString {
		label.labelBytes = nil
		if length > cbg.MaxLength {
			return fmt.Errorf("label string was too long (%d), max allowed (%d)", length, cbg.MaxLength)
		}

		buf := make([]byte, length)
		_, err = io.ReadAtLeast(br, buf, int(length))
		if err != nil {
			return err
		}

		// TODO: Check UTF-8 here too
		label.labelString = string(buf)
	} else if maj == cbg.MajByteString {
		label.labelString = ""
		if length > cbg.ByteArrayMaxLen {
			return fmt.Errorf("label bytes was too long (%d), max allowed (%d)", length, cbg.ByteArrayMaxLen)
		}

		if length > 0 {
			label.labelBytes = make([]uint8, length)
		}

		if _, err := io.ReadFull(br, label.labelBytes[:]); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("unexpected major tag (%d) when unmarshaling DealLabel: only textString (%d) or byteString (%d) expected", maj, cbg.MajTextString, cbg.MajByteString)
	}

	return nil
}

// Note: Deal Collateral is only released and returned to clients and miners
// when the storage deal stops counting towards power. In the current iteration,
// it will be released when the sector containing the storage deals expires,
// even though some storage deals can expire earlier than the sector does.
// Collaterals are denominated in PerEpoch to incur a cost for self dealing or
// minimal deals that last for a long time.
// Note: ClientCollateralPerEpoch may not be needed and removed pending future confirmation.
// There will be a Minimum value for both client and provider deal collateral.
type DealProposal struct {
	PieceCID     cid.Cid `checked:"true"` // Checked in validateDeal, CommP
	PieceSize    abi.PaddedPieceSize
	VerifiedDeal bool
	Client       addr.Address
	Provider     addr.Address

	// Label is an arbitrary client chosen label to apply to the deal
	Label DealLabel

	// Nominal start epoch. Deal payment is linear between StartEpoch and EndEpoch,
	// with total amount StoragePricePerEpoch * (EndEpoch - StartEpoch).
	// Storage deal must appear in a sealed (proven) sector no later than StartEpoch,
	// otherwise it is invalid.
	StartEpoch           abi.ChainEpoch
	EndEpoch             abi.ChainEpoch
	StoragePricePerEpoch abi.TokenAmount

	ProviderCollateral abi.TokenAmount
	ClientCollateral   abi.TokenAmount
}

// ClientDealProposal is a DealProposal signed by a client
type ClientDealProposal struct {
	Proposal        DealProposal
	ClientSignature acrypto.Signature
}

func (p *DealProposal) Duration() abi.ChainEpoch {
	return p.EndEpoch - p.StartEpoch
}

func (p *DealProposal) TotalStorageFee() abi.TokenAmount {
	return big.Mul(p.StoragePricePerEpoch, big.NewInt(int64(p.Duration())))
}

func (p *DealProposal) ClientBalanceRequirement() abi.TokenAmount {
	return big.Add(p.ClientCollateral, p.TotalStorageFee())
}

func (p *DealProposal) ProviderBalanceRequirement() abi.TokenAmount {
	return p.ProviderCollateral
}

func (p *DealProposal) Cid() (cid.Cid, error) {
	buf := new(bytes.Buffer)
	if err := p.MarshalCBOR(buf); err != nil {
		return cid.Undef, err
	}
	return abi.CidBuilder.Sum(buf.Bytes())
}
