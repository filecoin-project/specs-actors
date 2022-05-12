package ipld

import (
	"context"

	ipld2 "github.com/filecoin-project/specs-actors/v2/support/ipld"

	ipldcbor "github.com/ipfs/go-ipld-cbor"

	"github.com/filecoin-project/specs-actors/v3/actors/util/adt"
)

// Creates a new, empty, unsynchronized IPLD store in memory.
// This store is appropriate for most kinds of testing.
func NewADTStore(ctx context.Context) adt.Store {
	return adt.WrapBlockStore(ctx, ipld2.NewBlockStoreInMemory())
}

func NewBlockStoreInMemory() *ipld2.BlockStoreInMemory {
	return ipld2.NewBlockStoreInMemory()
}

func NewMetricsBlockStore(underlying ipldcbor.IpldBlockstore) *ipld2.MetricsBlockStore {
	return ipld2.NewMetricsBlockStore(underlying)
}
