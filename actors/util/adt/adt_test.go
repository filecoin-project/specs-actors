package adt_test

import (
	adt "github.com/filecoin-project/specs-actors/actors/util/adt"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAddrKey(t *testing.T) {
	id_address_1 := tutil.NewIDAddr(t, 101)
	id_address_2 := tutil.NewIDAddr(t, 102)
	actor_address_1 := tutil.NewActorAddr(t, "actor1")
	actor_address_2 := tutil.NewActorAddr(t, "222")

	t.Run("address to key string conversion", func(t *testing.T) {
		assert.Equal(t, "t0101", adt.AddrKey(id_address_1).Key())
		assert.Equal(t, "t0102", adt.AddrKey(id_address_2).Key())
		assert.Equal(t, "t2lc7e7v3vudem3gxnqzhhhk5rqzdf737bv3bjjoq", adt.AddrKey(actor_address_1).Key())
		assert.Equal(t, "t2vlilfgfj32v3xnt7qbpwnktirtoytlpv3u7jiuy", adt.AddrKey(actor_address_2).Key())
	})
}
