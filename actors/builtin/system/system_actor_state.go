package system

import (
	"context"

	cid "github.com/ipfs/go-cid"
	xerrors "golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/v8/actors/builtin/manifest"
	"github.com/filecoin-project/specs-actors/v8/actors/util/adt"
)

type State struct {
	BuiltinActors cid.Cid // ManifestData
}

func ConstructState(store adt.Store) (*State, error) {
	empty, err := store.Put(context.TODO(), &manifest.ManifestData{})
	if err != nil {
		return nil, xerrors.Errorf("failed to create empty manifest: %w", err)
	}

	return &State{BuiltinActors: empty}, nil
}
