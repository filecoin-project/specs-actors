package runtime

import (
	"fmt"
	"io"
)

type EmptyReturn struct{}

var _ CBORMarshaler = (*EmptyReturn)(nil)
var _ CBORUnmarshaler = (*EmptyReturn)(nil)

// 0x80 is empty list (major type 4 with zero length)
// 0xa0 is empty map (major type 5 with zero length)
// This is encoded with empty-list since we use tuple-encoding for everything.
const emptyListEncoded = 0x80

func (EmptyReturn) MarshalCBOR(w io.Writer) error {
	_, err := w.Write([]byte{emptyListEncoded})
	return err
}

func (EmptyReturn) UnmarshalCBOR(r io.Reader) error {
	buf := make([]byte, 1)
	_, err := r.Read(buf)
	if err != nil {
		return err
	}
	if buf[0] != emptyListEncoded {
		return fmt.Errorf("invalid empty return %x", buf[0])
	}
	return nil
}
