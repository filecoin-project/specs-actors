package miner_test

import (
	"testing"

	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-actors/support/mock"
)

func TestExports(t *testing.T) {
	mock.CheckActorExports(t, miner.Actor{})
}
