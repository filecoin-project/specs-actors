package adt

import (
	"bytes"
	"io"

	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
)

const InlineLimit = 64

type AutoInline struct {
	link   cid.Cid
	inline cbg.Deferred
}

func (i *AutoInline) Load(store Store, out cbg.CBORUnmarshaler) error {
	if i.link.Defined() {
		return store.Get(store.Context(), i.link, out)
	}
	return out.UnmarshalCBOR(bytes.NewReader(i.inline.Raw))
}

func (i *AutoInline) Store(store Store, val cbg.CBORMarshaler) error {
	// Encode
	var buf bytes.Buffer
	if err := val.MarshalCBOR(&buf); err != nil {
		return err
	}
	encoded := cbg.Deferred{Raw: buf.Bytes()}

	// If it's small enough, inline.
	if len(encoded.Raw) <= InlineLimit {
		i.link = cid.Undef
		i.inline = encoded
		return nil
	}

	// If it's too large, store it.
	c, err := store.Put(store.Context(), &encoded)
	if err != nil {
		return err
	}

	i.inline = cbg.Deferred{}
	i.link = c
	return nil
}

func (i *AutoInline) UnmarshalCBOR(br io.Reader) error {
	*i = AutoInline{}

	var deferred cbg.Deferred
	if err := deferred.UnmarshalCBOR(br); err != nil {
		return err
	}
	cidReader := bytes.NewReader(deferred.Raw)
	maj, extra, err := cbg.CborReadHeader(cidReader)
	if err != nil {
		return err
	}
	// Is it a CID?
	if maj == cbg.MajTag && extra == 42 {
		_, _ = cidReader.Seek(0, io.SeekStart)
		c, err := cbg.ReadCid(cidReader)
		if err != nil {
			return err
		}
		i.link = c
		return nil
	}

	// no.
	i.inline = deferred
	return nil
}

func (i *AutoInline) MarshalCBOR(w io.Writer) error {
	if i.link.Defined() {
		return cbg.WriteCid(w, i.link)
	}
	return i.inline.MarshalCBOR(w)
}
