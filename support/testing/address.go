package testing

import (
	"math/rand"
	"testing"

	addr "github.com/filecoin-project/go-address"
	"github.com/stretchr/testify/require"
)

func NewIDAddr(t *testing.T, id uint64) addr.Address {
	address, err := addr.NewIDAddress(id)
	if err != nil {
		t.Fatal(err)
	}
	return address
}

func NewSECP256K1Addr(t *testing.T, pubkey string) addr.Address {
	// the pubkey of a secp256k1 address is hashed for consistent length.
	address, err := addr.NewSecp256k1Address([]byte(pubkey))
	if err != nil {
		t.Fatal(err)
	}
	return address
}

func NewBLSAddr(t *testing.T, seed int64) addr.Address {
	// the pubkey of a bls address is not hashed and must be the correct length.
	buf := make([]byte, 48)
	r := rand.New(rand.NewSource(seed))
	r.Read(buf)

	address, err := addr.NewBLSAddress(buf)
	if err != nil {
		t.Fatal(err)
	}
	return address
}

func NewActorAddr(t *testing.T, data string) addr.Address {
	address, err := addr.NewActorAddress([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	return address
}

func NewActorAddr(t *testing.T, data []byte) addr.Address {
	address, err := addr.NewActorAddress(data)
	require.NoError(t, err)
	return address
}
