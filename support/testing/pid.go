package testing

import "github.com/filecoin-project/specs-actors/actors/abi"

func MakePID(input string) abi.PeerID {
	return abi.PeerID([]byte(input))
}
