package testing

import (
	"testing"

	addr "github.com/filecoin-project/go-address"
)

func NewIDAddr(t *testing.T, id uint64) addr.Address {
	address, err := addr.NewIDAddress(id)
	if err != nil {
		t.Fatal(err)
	}
	return address
}
