package builtin

import (
	"github.com/filecoin-project/specs-actors/actors/abi"
)

// Set of actor code types that can represent external signing parties.
var CallerTypesSignable = []abi.ActorCodeID{AccountActorCodeID, MultisigActorCodeID}