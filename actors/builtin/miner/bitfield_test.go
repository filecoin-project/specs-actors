package miner_test

import (
	"testing"

	"github.com/filecoin-project/go-bitfield"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertBitfieldsEqual(t *testing.T, expected *bitfield.BitField, actual *bitfield.BitField) {
	const maxDiff = 100

	missing, err := bitfield.SubtractBitField(expected, actual)
	require.NoError(t, err)
	unexpected, err := bitfield.SubtractBitField(actual, expected)
	require.NoError(t, err)

	missingSet, err := missing.All(maxDiff)
	require.NoError(t, err, "more than %d missing bits expected", maxDiff)
	assert.Empty(t, missingSet, "expected missing bits")

	unexpectedSet, err := unexpected.All(maxDiff)
	require.NoError(t, err, "more than %d unexpected bits", maxDiff)
	assert.Empty(t, unexpectedSet, "unexpected bits set")
}

func assertBitfieldEquals(t *testing.T, actual *bitfield.BitField, expected ...uint64) {
	assertBitfieldsEqual(t, actual, bf(expected...))
}

func assertBitfieldEmpty(t *testing.T, bf *bitfield.BitField) {
	empty, err := bf.IsEmpty()
	require.NoError(t, err)
	assert.True(t, empty)
}
