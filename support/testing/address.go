package testing

import (
	addr "github.com/filecoin-project/go-address"
	"github.com/stretchr/testify/require"
	"math/rand"
)

func NewIDAddr(t require.TestingT, id uint64) addr.Address {
	address, err := addr.NewIDAddress(id)
	require.NoError(t, err)
	return address
}

func NewSECP256K1Addr(t require.TestingT, pubkey string) addr.Address {
	// the pubkey of a secp256k1 address is hashed for consistent length.
	address, err := addr.NewSecp256k1Address([]byte(pubkey))
	require.NoError(t, err)
	return address
}

func NewBLSAddr(t require.TestingT, seed int64) addr.Address {
	// the pubkey of a bls address is not hashed and must be the correct length.
	buf := make([]byte, 48)
	r := rand.New(rand.NewSource(seed))
	r.Read(buf)

	address, err := addr.NewBLSAddress(buf)
	require.NoError(t, err)
	return address
}

func NewActorAddr(t require.TestingT, data string) addr.Address {
	address, err := addr.NewActorAddress([]byte(data))
	require.NoError(t, err)
	return address
}
