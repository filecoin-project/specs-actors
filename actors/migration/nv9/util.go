package nv9

import (
	"bytes"
	"context"
	"sync"

	amt2 "github.com/filecoin-project/go-amt-ipld/v2"
	amt3 "github.com/filecoin-project/go-amt-ipld/v3"
	hamt2 "github.com/filecoin-project/go-hamt-ipld/v2"
	hamt3 "github.com/filecoin-project/go-hamt-ipld/v3"
	adt2 "github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"

	adt3 "github.com/filecoin-project/specs-actors/v3/actors/util/adt"
)

// Migrates a HAMT from v2 to v3 without re-encoding keys or values. The new hamt
// configuration is specified by newOpts.
func migrateHAMTRaw(ctx context.Context, store cbor.IpldStore, root cid.Cid, newBitwidth int) (cid.Cid, error) {
	inRootNode, err := hamt2.LoadNode(ctx, store, root, adt2.HamtOptions...)
	if err != nil {
		return cid.Undef, err
	}

	newOpts := append(adt3.DefaultHamtOptions, hamt3.UseTreeBitWidth(newBitwidth))
	outRootNode := hamt3.NewNode(store, newOpts...)

	if err = inRootNode.ForEach(ctx, func(k string, val interface{}) error {
		return outRootNode.SetRaw(ctx, k, val.(*cbg.Deferred).Raw)
	}); err != nil {
		return cid.Undef, err
	}

	err = outRootNode.Flush(ctx)
	if err != nil {
		return cid.Undef, err
	}
	return store.Put(ctx, outRootNode)
}

// Migrates an AMT from v2 to v3 without re-encoding values. The new amt configuration is specified by newOpts
func migrateAMTRaw(ctx context.Context, store cbor.IpldStore, root cid.Cid, newBitwidth int) (cid.Cid, error) {
	inRootNode, err := amt2.LoadAMT(ctx, store, root)
	if err != nil {
		return cid.Undef, err
	}

	newOpts := append(adt3.DefaultAmtOptions, amt3.UseTreeBitWidth(newBitwidth))
	outRootNode, err := amt3.NewAMT(store, newOpts...)
	if err != nil {
		return cid.Undef, err
	}

	if err = inRootNode.ForEach(ctx, func(k uint64, d *cbg.Deferred) error {
		return outRootNode.Set(ctx, k, d)
	}); err != nil {
		return cid.Undef, err
	}

	return outRootNode.Flush(ctx)
}

// Migrates a HAMT of HAMTs from v2 to v3 without re-encoding leaf keys or values.
// The new outer hamt configurion is newOptsOuter, the new inner hamt configuration
// is newOptsInner.
func migrateHAMTHAMTRaw(ctx context.Context, store cbor.IpldStore, root cid.Cid, newOuterBitwidth, newInnerBitwidth int) (cid.Cid, error) {
	inRootNodeOuter, err := hamt2.LoadNode(ctx, store, root)
	if err != nil {
		return cid.Undef, err
	}

	newOptsOuter := append(adt3.DefaultHamtOptions, hamt3.UseTreeBitWidth(newOuterBitwidth))
	outRootNodeOuter := hamt3.NewNode(store, newOptsOuter...)

	if err = inRootNodeOuter.ForEach(ctx, func(k string, val interface{}) error {
		var inInner cbg.CborCid
		if err := inInner.UnmarshalCBOR(bytes.NewReader(val.(*cbg.Deferred).Raw)); err != nil {
			return err
		}
		outInner, err := migrateHAMTRaw(ctx, store, cid.Cid(inInner), newInnerBitwidth)
		if err != nil {
			return err
		}
		c := cbg.CborCid(outInner)
		return outRootNodeOuter.Set(ctx, k, &c)
	}); err != nil {
		return cid.Undef, err
	}

	if err := outRootNodeOuter.Flush(ctx); err != nil {
		return cid.Undef, err
	}
	return store.Put(ctx, outRootNodeOuter)
}

// Migrates a HAMT of AMTs from v2 to v3 without re-encoding values.
// The new outer hamt configuraTIon is newOptsOuter, the new inner amt configuration
// is newOptsInner
func migrateHAMTAMTRaw(ctx context.Context, store cbor.IpldStore, root cid.Cid, newOuterBitwidth, newInnerBitwidth int) (cid.Cid, error) {
	inRootNodeOuter, err := hamt2.LoadNode(ctx, store, root)
	if err != nil {
		return cid.Undef, err
	}
	newOptsOuter := append(adt3.DefaultHamtOptions, hamt3.UseTreeBitWidth(newOuterBitwidth))
	outRootNodeOuter := hamt3.NewNode(store, newOptsOuter...)

	if err = inRootNodeOuter.ForEach(ctx, func(k string, val interface{}) error {
		var inInner cbg.CborCid
		if err := inInner.UnmarshalCBOR(bytes.NewReader(val.(*cbg.Deferred).Raw)); err != nil {
			return err
		}
		outInner, err := migrateAMTRaw(ctx, store, cid.Cid(inInner), newInnerBitwidth)
		if err != nil {
			return err
		}
		c := cbg.CborCid(outInner)
		return outRootNodeOuter.Set(ctx, k, &c)
	}); err != nil {
		return cid.Undef, err
	}
	return store.Put(ctx, outRootNodeOuter)
}

// Guards an unsychronized store with a mutex.
// Useful for migrating the store created by a scenario VM (which is unsynchronized).
type SyncStore struct {
	store cbor.IpldStore
	mu sync.Mutex
}

func (ss *SyncStore) Get(ctx context.Context, c cid.Cid, out interface{}) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.store.Get(ctx, c, out)
}

func (ss *SyncStore) Put(ctx context.Context, v interface{}) (cid.Cid, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.store.Put(ctx, v)
}
