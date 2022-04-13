package manifest

import (
	"context"
	"fmt"
	"io"

	adt8 "github.com/filecoin-project/specs-actors/v8/actors/util/adt"

	"github.com/ipfs/go-cid"

	cbg "github.com/whyrusleeping/cbor-gen"
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
	if m.Version != 1 {
		return fmt.Errorf("unknown manifest version %d", m.Version)
	}

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

// this is a flat tuple, so we need to write these by hand
func (d *ManifestData) UnmarshalCBOR(r io.Reader) error {
	*d = ManifestData{}

	br := cbg.GetPeeker(r)
	scratch := make([]byte, 8)

	maj, extra, err := cbg.CborReadHeaderBuf(br, scratch)
	if err != nil {
		return err
	}
	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("too many manifest entries")
	}

	entries := int(extra)
	d.Entries = make([]ManifestEntry, 0, entries)

	for i := 0; i < entries; i++ {
		entry := ManifestEntry{}
		if err := entry.UnmarshalCBOR(r); err != nil {
			return fmt.Errorf("error unmarsnalling manifest entry: %w", err)
		}

		d.Entries = append(d.Entries, entry)
	}

	return nil
}

func (d *ManifestData) MarshalCBOR(w io.Writer) error {
	if d == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	scratch := make([]byte, 9)

	if len(d.Entries) > cbg.MaxLength {
		return fmt.Errorf("too many manifest entries")
	}

	if err := cbg.WriteMajorTypeHeaderBuf(scratch, w, cbg.MajArray, uint64(len(d.Entries))); err != nil {
		return err
	}

	for _, v := range d.Entries {
		if err := v.MarshalCBOR(w); err != nil {
			return err
		}
	}

	return nil
}
