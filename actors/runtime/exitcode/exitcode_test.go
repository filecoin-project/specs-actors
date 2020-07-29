package exitcode_test

import (
	"errors"
	"testing"

	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/stretchr/testify/assert"
	"golang.org/x/xerrors"
)

func TestWithContext(t *testing.T) {
	baseErr := errors.New("base error")
	codedErr := exitcode.ErrForbidden.Wrapf("coded: %w", baseErr)
	wrappedErr := xerrors.Errorf("wrapper: %w", codedErr)
	shadowedErr := exitcode.ErrIllegalState.Wrapf("shadow: %w", codedErr)

	// Test default.
	assert.Equal(t, exitcode.Ok, exitcode.Unwrap(baseErr, exitcode.Ok))
	assert.Equal(t, exitcode.ErrIllegalState, exitcode.Unwrap(baseErr, exitcode.ErrIllegalState))
	assert.True(t, errors.Is(baseErr, baseErr))

	// Test coded.
	assert.Equal(t, exitcode.ErrForbidden, exitcode.Unwrap(codedErr, exitcode.Ok))
	assert.True(t, errors.Is(wrappedErr, codedErr))
	assert.False(t, errors.Is(codedErr, wrappedErr))
	assert.False(t, errors.Is(wrappedErr, exitcode.Ok))

	// Test wrapped
	assert.Equal(t, exitcode.ErrForbidden, exitcode.Unwrap(wrappedErr, exitcode.Ok))
	assert.True(t, errors.Is(wrappedErr, codedErr))
	assert.True(t, errors.Is(wrappedErr, wrappedErr))
	assert.False(t, errors.Is(wrappedErr, exitcode.Ok))

	// Test shadowed
	assert.Equal(t, exitcode.ErrIllegalState, exitcode.Unwrap(shadowedErr, exitcode.Ok))
	assert.True(t, errors.Is(shadowedErr, exitcode.ErrIllegalState))
	assert.False(t, errors.Is(shadowedErr, exitcode.ErrForbidden))
}
