package miner

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/specs-actors/actors/abi/big"
)

func TestExpirations(t *testing.T) {
	sectors := []*SectorOnChainInfo{{
		Expiration:         10,
		SectorNumber:       1,
		DealWeight:         big.Zero(),
		VerifiedDealWeight: big.Zero(),
		InitialPledge:      big.Zero(),
	}, {
		Expiration:         10,
		SectorNumber:       2,
		DealWeight:         big.Zero(),
		VerifiedDealWeight: big.Zero(),
		InitialPledge:      big.Zero(),
	}, {
		Expiration:         12,
		SectorNumber:       3,
		DealWeight:         big.Zero(),
		VerifiedDealWeight: big.Zero(),
		InitialPledge:      big.Zero(),
	}, {
		Expiration:         10,
		SectorNumber:       4,
		DealWeight:         big.Zero(),
		VerifiedDealWeight: big.Zero(),
		InitialPledge:      big.Zero(),
	}}
	result := groupSectorsByExpiration(2048, sectors)
	expected := []sectorEpochSet{{
		epoch:   0,
		sectors: []uint64{1, 2, 4},
	}, {
		epoch:   2,
		sectors: []uint64{3},
	}}
	require.Equal(t, expected, result)
}

func TestExpirationsEmpty(t *testing.T) {
	sectors := []*SectorOnChainInfo{}
	result := groupSectorsByExpiration(2048, sectors)
	expected := []sectorEpochSet{}
	require.Equal(t, expected, result)
}
