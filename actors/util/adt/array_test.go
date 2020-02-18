package adt_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
)

func TestArrayNotFound(t *testing.T) {
	rt := mock.NewBuilder(context.Background(), address.Undef).Build(t)
	store := adt.AsStore(rt)
	arr, err := adt.MakeEmptyArray(store)
	require.NoError(t, err)

	found, err := arr.Get(7, adt.EmptyValue{})
	require.NoError(t, err)
	require.False(t, found)
}
