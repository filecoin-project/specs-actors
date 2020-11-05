package ipld

import (
	"context"
	"fmt"
	"sync"

	block "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
)

type BlockStoreInMemory struct {
	data map[cid.Cid]block.Block
}

func NewBlockStoreInMemory() *BlockStoreInMemory {
	return &BlockStoreInMemory{make(map[cid.Cid]block.Block)}
}

func (mb *BlockStoreInMemory) Get(c cid.Cid) (block.Block, error) {
	d, ok := mb.data[c]
	if ok {
		return d, nil
	}
	return nil, fmt.Errorf("not found")
}

func (mb *BlockStoreInMemory) Put(b block.Block) error {
	mb.data[b.Cid()] = b
	return nil
}

// Creates a new, empty IPLD store in memory.
func NewADTStore(ctx context.Context) adt.Store {
	return adt.WrapStore(ctx, cbor.NewCborStore(NewBlockStoreInMemory()))

}

type SyncBlockStoreInMemory struct {
	bs *BlockStoreInMemory
	mu sync.Mutex
}

func NewSyncBlockStoreInMemory() *SyncBlockStoreInMemory {
	return &SyncBlockStoreInMemory{
		bs: NewBlockStoreInMemory(),
	}
}

func (ss *SyncBlockStoreInMemory) Get(c cid.Cid) (block.Block, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.bs.Get(c)
}

func (ss *SyncBlockStoreInMemory) Put(b block.Block) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.bs.Put(b)
}

// Creates a new, threadsafe, empty IPLD store in memory
func NewSyncADTStore(ctx context.Context) adt.Store {
	return adt.WrapStore(ctx, cbor.NewCborStore(NewSyncBlockStoreInMemory()))
}

type MetricsStore struct {
	bs     cbor.IpldBlockstore
	Writes uint64
	Reads  uint64
}

func NewMetricsStore(underlying cbor.IpldBlockstore) *MetricsStore {
	return &MetricsStore{bs: underlying}
}

func (ms *MetricsStore) Get(c cid.Cid) (block.Block, error) {
	ms.Reads++
	return ms.bs.Get(c)
}

func (ms *MetricsStore) Put(b block.Block) error {
	ms.Writes++
	return ms.bs.Put(b)
}

func (ms *MetricsStore) ReadCount() uint64 {
	return ms.Reads
}

func (ms *MetricsStore) WriteCount() uint64 {
	return ms.Writes
}
