package crypto

import (
	"bytes"
	"encoding/binary"

	addr "github.com/filecoin-project/go-address"

	abi "github.com/filecoin-project/specs-actors/actors/abi"
	big "github.com/filecoin-project/specs-actors/actors/abi/big"
	autil "github.com/filecoin-project/specs-actors/actors/util"
)

// Specifies a domain for randomness generation.
type DomainSeparationTag int

const (
	DomainSeparationTag_TicketDrawing DomainSeparationTag = 1 + iota
	DomainSeparationTag_TicketProduction
	DomainSeparationTag_ElectionPoStChallengeSeed
	DomainSeparationTag_WindowedPoStChallengeSeed
)

// Derive a random byte string from a domain separation tag and the appropriate values
func DeriveRandWithMinerAddr(tag DomainSeparationTag, tix abi.RandomnessSeed, minerAddr addr.Address) abi.Randomness {
	var addrBuf bytes.Buffer
	err := minerAddr.MarshalCBOR(&addrBuf)
	autil.AssertNoError(err)

	return _deriveRandInternal(tag, tix, -1, addrBuf.Bytes())
}

func DeriveRandWithEpoch(tag DomainSeparationTag, tix abi.RandomnessSeed, epoch int64) abi.Randomness {
	return _deriveRandInternal(tag, tix, -1, BigEndianBytesFromInt(epoch))
}

func _deriveRandInternal(tag DomainSeparationTag, randSeed abi.RandomnessSeed, index int, s []byte) abi.Randomness {
	buffer := []byte{}
	buffer = append(buffer, BigEndianBytesFromInt(int64(tag))...)
	buffer = append(buffer, BigEndianBytesFromInt(int64(index))...)
	buffer = append(buffer, randSeed...)
	buffer = append(buffer, s...)
	return SHA256(buffer)
}

// Computes an unpredictable integer less than limit from inputs seed and nonce.
func RandomInt(seed abi.Randomness, nonce int64, limit int64) int64 {
	nonceBytes := BigEndianBytesFromInt(nonce)
	input := append(seed, nonceBytes...)
	ranHash := SHA256(input)
	hashInt := big.PositiveFromUnsignedBytes(ranHash)

	num := big.Mod(hashInt, big.NewInt(limit))
	return num.Int64()
}

// Returns an 8-byte slice of the big-endian bytes of an integer.
func BigEndianBytesFromInt(x int64) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 8))
	err := binary.Write(buf, binary.BigEndian, x)
	autil.AssertNoError(err)
	return buf.Bytes()
}

func SHA256(data []byte) []byte {
	autil.TODO()
	return []byte{}
}

func IntFromBigEndianBytes(data []byte) int {
	ret := big.Zero()
	ret.SetBytes(data)
	return int(ret.Int64())
}
