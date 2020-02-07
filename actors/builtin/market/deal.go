package market

import (
	"bytes"
	"context"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	acrypto "github.com/filecoin-project/specs-actors/actors/crypto"
)

// Note: Deal Collateral is only released and returned to clients and miners
// when the storage deal stops counting towards power. In the current iteration,
// it will be released when the sector containing the storage deals expires,
// even though some storage deals can expire earlier than the sector does.
// Collaterals are denominated in PerEpoch to incur a cost for self dealing or
// minimal deals that last for a long time.
// Note: ClientCollateralPerEpoch may not be needed and removed pending future confirmation.
// There will be a Minimum value for both client and provider deal collateral.
type DealProposal struct {
	PieceCID        cid.Cid // CommP
	PieceSize       abi.PaddedPieceSize
	Client          addr.Address
	Provider        addr.Address
	ClientSignature *acrypto.Signature

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

func (p *DealProposal) Sign(ctx context.Context, sign acrypto.SignFunc) error {
	if p.ClientSignature != nil {
		return xerrors.New("signature already present in StorageDealProposal")
	}
	var buf bytes.Buffer
	if err := p.MarshalCBOR(&buf); err != nil {
		return err
	}
	sig, err := sign(ctx, buf.Bytes())
	if err != nil {
		return err
	}
	p.ClientSignature = sig
	return nil
}

type DealState struct {
	SectorStartEpoch abi.ChainEpoch // -1 if not yet included in proven sector
	LastUpdatedEpoch abi.ChainEpoch // -1 if deal state never updated
	SlashEpoch       abi.ChainEpoch // -1 if deal never slashed
}
