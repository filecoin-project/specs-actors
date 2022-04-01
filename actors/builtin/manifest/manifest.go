package manifest

import (
	"context"

	adt8 "github.com/filecoin-project/specs-actors/v8/actors/util/adt"

	"github.com/ipfs/go-cid"
)

type Manifest struct {
	Version uint64 // this is really u32, but cbor-gen can't deal with it
	Data    cid.Cid

	entries map[string]cid.Cid
}

type ManifestEntry struct {
	Name string
	Code cid.Cid
}

type ManifestData struct {
	Entries []ManifestEntry
}

func (m *Manifest) Load(ctx context.Context, store adt8.Store) error {
	data := ManifestData{}
	if err := store.Get(ctx, m.Data, &data); err != nil {
		return err
	}

	m.entries = make(map[string]cid.Cid)
	for _, e := range data.Entries {
		m.entries[e.Name] = e.Code
	}

	return nil
}

func (m *Manifest) Get(name string) (cid.Cid, bool) {
	c, ok := m.entries[name]
	return c, ok
}
