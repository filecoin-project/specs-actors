package miner_test

import (
	"testing"

	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	"github.com/stretchr/testify/require"
)

func sectorsArr(t *testing.T, rt *mock.Runtime, sectors []*miner.SectorOnChainInfo) miner.Sectors {
	sectorArr := miner.Sectors{adt.MakeEmptyArray(adt.AsStore(rt))}
	require.NoError(t, sectorArr.Store(sectors...))
	return sectorArr
}
