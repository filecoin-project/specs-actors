package builtin

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"

	rtt "github.com/filecoin-project/go-state-types/rt"

	cbg "github.com/whyrusleeping/cbor-gen"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/cbor"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/v7/actors/runtime"
	"github.com/ipfs/go-cid"
)

type stateMock struct{}

func (s *stateMock) MarshalCBOR(w io.Writer) error {
	if s == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	return nil
}

func (s *stateMock) UnmarshalCBOR(r io.Reader) error {
	*s = stateMock{}

	return nil
}

type actorMock struct {
}

func (a actorMock) Exports() []interface{} {
	return []interface{}{
		builtin.MethodConstructor: a.Constructor,
	}
}

func (a actorMock) Code() cid.Cid {
	return builtin.SystemActorCodeID
}

func (a actorMock) IsSingleton() bool {
	return true
}

func (a actorMock) State() cbor.Er { return new(stateMock) }

func (a actorMock) Constructor(rt runtime.Runtime) *abi.EmptyValue {
	rt.Log(GetActorLogLevel(a, rtt.DEBUG), "Constructor func")

	return nil
}

func TestActorLogLevel(t *testing.T) {
	actorMock := actorMock{}

	t.Run("log with default", func(t *testing.T) {
		assert.Equal(t, rtt.DEBUG, GetActorLogLevel(actorMock, rtt.DEBUG))
		assert.Equal(t, rtt.INFO, GetActorLogLevel(actorMock, rtt.INFO))
		assert.Equal(t, rtt.WARN, GetActorLogLevel(actorMock, rtt.WARN))
		assert.Equal(t, rtt.ERROR, GetActorLogLevel(actorMock, rtt.ERROR))
	})

	t.Run("set log level with default debug", func(t *testing.T) {
		SetActorsLogLevel(rtt.DEBUG, actorMock)
		assert.Equal(t, rtt.DEBUG, GetActorLogLevel(actorMock, rtt.DEBUG))

		SetActorsLogLevel(rtt.INFO, actorMock)
		assert.Equal(t, rtt.INFO, GetActorLogLevel(actorMock, rtt.DEBUG))

		SetActorsLogLevel(rtt.WARN, actorMock)
		assert.Equal(t, rtt.WARN, GetActorLogLevel(actorMock, rtt.DEBUG))

		SetActorsLogLevel(rtt.ERROR, actorMock)
		assert.Equal(t, rtt.ERROR, GetActorLogLevel(actorMock, rtt.DEBUG))
	})

	t.Run("set log level with default info", func(t *testing.T) {
		SetActorsLogLevel(rtt.DEBUG, actorMock)
		assert.Equal(t, rtt.DEBUG, GetActorLogLevel(actorMock, rtt.INFO))

		SetActorsLogLevel(rtt.INFO, actorMock)
		assert.Equal(t, rtt.INFO, GetActorLogLevel(actorMock, rtt.INFO))

		SetActorsLogLevel(rtt.WARN, actorMock)
		assert.Equal(t, rtt.WARN, GetActorLogLevel(actorMock, rtt.INFO))

		SetActorsLogLevel(rtt.ERROR, actorMock)
		assert.Equal(t, rtt.ERROR, GetActorLogLevel(actorMock, rtt.INFO))
	})

	t.Run("set log level with default warn", func(t *testing.T) {
		SetActorsLogLevel(rtt.DEBUG, actorMock)
		assert.Equal(t, rtt.DEBUG, GetActorLogLevel(actorMock, rtt.WARN))

		SetActorsLogLevel(rtt.INFO, actorMock)
		assert.Equal(t, rtt.INFO, GetActorLogLevel(actorMock, rtt.WARN))

		SetActorsLogLevel(rtt.WARN, actorMock)
		assert.Equal(t, rtt.WARN, GetActorLogLevel(actorMock, rtt.WARN))

		SetActorsLogLevel(rtt.ERROR, actorMock)
		assert.Equal(t, rtt.ERROR, GetActorLogLevel(actorMock, rtt.WARN))
	})

	t.Run("set log level with default error", func(t *testing.T) {
		SetActorsLogLevel(rtt.DEBUG, actorMock)
		assert.Equal(t, rtt.DEBUG, GetActorLogLevel(actorMock, rtt.ERROR))

		SetActorsLogLevel(rtt.INFO, actorMock)
		assert.Equal(t, rtt.INFO, GetActorLogLevel(actorMock, rtt.ERROR))

		SetActorsLogLevel(rtt.WARN, actorMock)
		assert.Equal(t, rtt.WARN, GetActorLogLevel(actorMock, rtt.ERROR))

		SetActorsLogLevel(rtt.ERROR, actorMock)
		assert.Equal(t, rtt.ERROR, GetActorLogLevel(actorMock, rtt.ERROR))
	})
}
