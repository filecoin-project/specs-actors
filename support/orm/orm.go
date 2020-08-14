package orm

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi"
	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"

	models "github.com/filecoin-project/specs-actors/support/orm/models"
)

func CreateSchema(db *pg.DB) error {
	models := []interface{}{
		(*models.Actor)(nil),
	}

	for _, model := range models {
		if err := db.Model(model).CreateTable(&orm.CreateTableOptions{
			IfNotExists: true,
		}); err != nil {
			return err
		}
	}
	return nil
}

type txCtxKey struct{}

// TxFromContext returns the Tx stored in a context, or nil if there isn't one.
func TxFromContext(ctx context.Context) *pg.Tx {
	tx, ok := ctx.Value(txCtxKey{}).(*pg.Tx)
	if !ok {
		return nil
	}
	return tx
}

// NewTxContext returns a new context with the given Client attached.
func NewTxContext(parent context.Context, tx *pg.Tx) context.Context {
	return context.WithValue(parent, txCtxKey{}, tx)
}

type cidKey struct{}

func CIDFromContext(ctx context.Context) cid.Cid {
	c, ok := ctx.Value(cidKey{}).(cid.Cid)
	if !ok {
		return cid.Undef
	}
	return c
}

func NewCIDContext(parent context.Context, c cid.Cid) context.Context {
	return context.WithValue(parent, cidKey{}, c)
}

func AbiPeerIDAsString(p abi.PeerID) string {
	pid, err := peer.IDFromBytes(p)
	if err != nil {
		return ""
	}
	return pid.String()
}

func SectorSizeString(s abi.RegisteredSealProof) string {
	size, err := s.SectorSize()
	if err != nil {
		return ""
	}
	return size.ShortString()
}

type addrKey struct{}

func NewAddressContext(parent context.Context, addr address.Address) context.Context {
	return context.WithValue(parent, addrKey{}, addr)
}

func AddressFromContext(ctx context.Context) string {
	a, ok := ctx.Value(addrKey{}).(address.Address)
	if !ok {
		fmt.Println("failed to get address in context")
		return "FAILURE"
	}
	return a.String()
}
