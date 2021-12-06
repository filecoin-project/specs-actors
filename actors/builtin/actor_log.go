package builtin

import (
	"sync"

	"github.com/ipfs/go-cid"

	rtt "github.com/filecoin-project/go-state-types/rt"
	"github.com/filecoin-project/specs-actors/v7/actors/runtime"
)

type ActorLog struct {
	sync.RWMutex
	Actors map[cid.Cid]rtt.LogLevel
}

var actorLogSingle *ActorLog

func init() {
	actorLogSingle = &ActorLog{Actors: make(map[cid.Cid]rtt.LogLevel, 0)}
}

func SetActorsLogLevel(logLevel rtt.LogLevel, actors ...runtime.VMActor) {
	actorLogSingle.Lock()
	defer actorLogSingle.Unlock()

	for _, actor := range actors {
		actorLogSingle.Actors[actor.Code()] = logLevel
	}
}

func GetActorLogLevel(actor runtime.VMActor, defValue rtt.LogLevel) rtt.LogLevel {
	actorLogSingle.RLock()
	defer actorLogSingle.RUnlock()

	actorLogLevel, ok := actorLogSingle.Actors[actor.Code()]
	if ok {
		return actorLogLevel
	}

	return defValue
}
