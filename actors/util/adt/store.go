package adt

import (
	"context"
	"encoding/binary"

	addr "github.com/filecoin-project/go-address"
	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	vmr "github.com/filecoin-project/specs-actors/actors/runtime"
	exitcode "github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
)

// Store defines an interface required to back the ADTs in this package.
type Store interface {
	Context() context.Context
	cbor.IpldStore
}

// Keyer defines an interface required to put values in mapping.
type Keyer interface {
	Key() string
}

// AsStore allows Runtime to satisfy the adt.Store interface.
func AsStore(rt vmr.Runtime) Store {
	return rtStore{rt}
}

var _ Store = &rtStore{}

type rtStore struct {
	vmr.Runtime
}

func (r rtStore) Context() context.Context {
	return r.Runtime.Context()
}

func (r rtStore) Get(ctx context.Context, c cid.Cid, out interface{}) error {
	if !r.IpldGet(c, out.(vmr.CBORUnmarshaler)) {
		r.Abort(exitcode.ErrNotFound, "not found")
	}
	return nil
}

func (r rtStore) Put(ctx context.Context, v interface{}) (cid.Cid, error) {
	return r.IpldPut(v.(vmr.CBORMarshaler)), nil
}

// Adapts an address as a mapping key.
type AddrKey addr.Address

func (k AddrKey) Key() string {
	return string(addr.Address(k).Bytes())
}

// Adapts an epoch as a mapping key.
type EpochKey abi.ChainEpoch

func (k EpochKey) Key() string {
	var buf []byte
	n := binary.PutVarint(buf, int64(k))
	return string(buf[:n])
}
