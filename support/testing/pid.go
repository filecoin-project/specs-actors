package testing

import "github.com/filecoin-project/specs-actors/v1/actors/abi"

func MakePID(input string) abi.PeerID {
	return abi.PeerID([]byte(input))
}
