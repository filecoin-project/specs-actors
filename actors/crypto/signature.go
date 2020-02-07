package crypto

import (
	"context"
)

type SigType int64

const (
	SigTypeUnknown = SigType(-1)

	SigTypeSecp256k1 = SigType(iota)
	SigTypeBLS
)

type Signature struct {
	Type SigType
	Data []byte
}

type SignFunc = func(context.Context, []byte) (*Signature, error)

type Signable interface {
	Sign(context.Context, SignFunc) error
}
