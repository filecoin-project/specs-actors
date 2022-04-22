package market

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"

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

// The DealLabel is a kinded union of string or byte slice.
// It serializes to a CBOR string or CBOR byte string depending on which form it takes.
// The zero value is serialized as an empty CBOR string (maj type 3).
type DealLabel struct {
	bs        []byte
	notString bool
}

// Zero value of DealLabel is canonical EmptyDealLabel
var EmptyDealLabel = DealLabel{}

func NewLabelFromString(s string) (DealLabel, error) {
	if len(s) > DealMaxLabelSize {
		return EmptyDealLabel, xerrors.Errorf("provided string is too large to be a label (%d), max length (%d)", len(s), DealMaxLabelSize)
	}
	if !utf8.ValidString(s) {
		return EmptyDealLabel, xerrors.Errorf("provided string is invalid utf8")
	}
	return DealLabel{
		bs:        []byte(s),
		notString: false,
	}, nil
}

func NewLabelFromBytes(b []byte) (DealLabel, error) {
	if len(b) > DealMaxLabelSize {
		return EmptyDealLabel, xerrors.Errorf("provided bytes are too large to be a label (%d), max length (%d)", len(b), DealMaxLabelSize)
	}

	return DealLabel{
		bs:        b,
		notString: true,
	}, nil
}

func (label DealLabel) IsString() bool {
	return !label.notString
}

func (label DealLabel) IsBytes() bool {
	return label.notString
}

func (label DealLabel) ToString() (string, error) {
	if !label.IsString() {
		return "", xerrors.Errorf("label is not string")
	}

	return string(label.bs), nil
}

func (label DealLabel) ToBytes() ([]byte, error) {
	if !label.IsBytes() {
		return nil, xerrors.Errorf("label is not bytes")
	}
	return label.bs, nil
}

func (label DealLabel) Length() int {
	return len(label.bs)
}

func (l DealLabel) Equals(o DealLabel) bool {
	return bytes.Equal(l.bs, o.bs) && l.notString == o.notString
}

func (label *DealLabel) MarshalCBOR(w io.Writer) error {
	scratch := make([]byte, 9)

	// nil *DealLabel counts as EmptyLabel
	// on chain structures should never have a pointer to a DealLabel but the case is included for completeness
	if label == nil {
		if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajTextString, 0); err != nil {
			return err
		}
		_, err := io.WriteString(w, string(""))
		return err
	}
	if len(label.bs) > cbg.ByteArrayMaxLen {
		return xerrors.Errorf("label is too long to marshal (%d), max allowed (%d)", len(label.bs), cbg.ByteArrayMaxLen)
	}

	majorType := byte(cbg.MajByteString)
	if label.IsString() {
		majorType = cbg.MajTextString
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, majorType, uint64(len(label.bs))); err != nil {
		return err
	}
	_, err := w.Write(label.bs)
	return err
}

func (label *DealLabel) UnmarshalCBOR(br io.Reader) error {
	if label == nil {
		return xerrors.Errorf("cannot unmarshal into nil pointer")
	}

	// reset fields
	label.bs = nil

	scratch := make([]byte, 8)

	maj, length, err := cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}
	if maj != cbg.MajTextString && maj != cbg.MajByteString {
		return fmt.Errorf("unexpected major tag (%d) when unmarshaling DealLabel: only textString (%d) or byteString (%d) expected", maj, cbg.MajTextString, cbg.MajByteString)
	}
	if length > cbg.ByteArrayMaxLen {
		return fmt.Errorf("label was too long (%d), max allowed (%d)", length, cbg.ByteArrayMaxLen)
	}
	buf := make([]byte, length)
	_, err = io.ReadAtLeast(br, buf, int(length))
	if err != nil {
		return err
	}
	label.bs = buf
	label.notString = maj != cbg.MajTextString
	if !label.notString && !utf8.ValidString(string(buf)) {
		return fmt.Errorf("label string not valid utf8")
	}

	return nil
}

func (label *DealLabel) MarshalJSON() ([]byte, error) {
	str, err := label.ToString()
	if err != nil {
		return nil, xerrors.Errorf("can only marshal strings: %w", err)
	}

	return json.Marshal(str)
}

func (label *DealLabel) UnmarshalJSON(b []byte) error {
	var str string
	if err := json.Unmarshal(b, &str); err != nil {
		return xerrors.Errorf("failed to unmarshal string: %w", err)
	}

	newLabel, err := NewLabelFromString(str)
	if err != nil {
		return xerrors.Errorf("failed to create label from string: %w", err)
	}

	*label = newLabel
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
