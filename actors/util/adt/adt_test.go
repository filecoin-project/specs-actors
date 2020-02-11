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
		assert.Equal(t, "\x00e", adt.AddrKey(id_address_1).Key())
		assert.Equal(t, "\x00f", adt.AddrKey(id_address_2).Key())
		assert.Equal(t, "\x02X\xbeO\xd7u\xa0\xc8͚\xed\x86Ns\xab\xb1\x86F_\xef\xe1", adt.AddrKey(actor_address_1).Key())
		assert.Equal(t, "\x02\xaaв\x98\xa9ޫ\xbb\xb6\u007f\x80_f\xaah\x8c݉\xad\xf5", adt.AddrKey(actor_address_2).Key())
	})
}
