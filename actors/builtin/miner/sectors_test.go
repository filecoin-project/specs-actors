package miner_test

import (
	"testing"

	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/stretchr/testify/require"
)

func sectorsArr(t *testing.T, store adt.Store, sectors []*miner.SectorOnChainInfo) miner.Sectors {
	sectorArr := miner.Sectors{adt.MakeEmptyArray(store)}
	require.NoError(t, sectorArr.Store(sectors...))
	return sectorArr
}
