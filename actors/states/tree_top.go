package states

import (
	"context"

	address "github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/specs-actors/v2/actors/builtin"
	init_ "github.com/filecoin-project/specs-actors/v2/actors/builtin/init"
	"github.com/filecoin-project/specs-actors/v2/actors/util/adt"
	cid "github.com/ipfs/go-cid"
	xerrors "golang.org/x/xerrors"
)

var ErrActorNotFound = xerrors.Errorf("Actor not found")

type Actor struct {
	Code    cid.Cid
	Head    cid.Cid
	Nonce   uint64
	Balance big.Int
}

// A specialization of a map of addresses to actor heads
type TreeTop struct {
	m *adt.Map
	s adt.Store
}

func AsTreeTop(s adt.Store, r cid.Cid) (*TreeTop, error) {
	m, err := adt.AsMap(s, r)
	if err != nil {
		return nil, err
	}
	return &TreeTop{
		m: m,
		s: s,
	}, nil
}

func (t *TreeTop) GetActor(ctx context.Context, addr address.Address) (*Actor, error) {
	iaddr, err := t.LookupID(ctx, addr)
	if err != nil {
		return nil, err
	}
	var actor Actor
	found, err := t.m.Get(abi.AddrKey(iaddr), &Actor{})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrActorNotFound
	}

	return &actor, nil
}

func (t *TreeTop) SetActor(ctx context.Context, addr address.Address, actor *Actor) error {
	iaddr, err := t.LookupID(ctx, addr)
	if err != nil {
		return err
	}
	return t.m.Put(abi.AddrKey(iaddr), actor)
}

func (t *TreeTop) Root() (cid.Cid, error) {
	return t.m.Root()
}

func (t *TreeTop) LookupID(ctx context.Context, addr address.Address) (address.Address, error) {
	if addr.Protocol() == address.ID {
		return addr, nil
	}
	act, err := t.GetActor(ctx, builtin.InitActorAddr)
	if err != nil {
		return address.Undef, xerrors.Errorf("getting init actor: %w", err)
	}

	var ias init_.State
	if err := t.s.Get(ctx, act.Head, &ias); err != nil {
		return address.Undef, xerrors.Errorf("loading init actor state: %w", err)
	}

	a, found, err := ias.ResolveAddress(t.s, addr)
	if err == nil && !found {
		err = ErrActorNotFound
	}
	if err != nil {
		return address.Undef, xerrors.Errorf("resolve address %s: %w", addr, err)
	}

	return a, nil
}

func (t *TreeTop) ForEach(ctx context.Context, fn func(addr address.Address, actor *Actor) error) error {
	var val Actor
	return t.m.ForEach(&val, func(key string) error {
		addr, err := address.NewFromString(key)
		if err != nil {
			return err
		}
		return fn(addr, &val)
	})

}
