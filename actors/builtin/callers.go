package builtin

import "github.com/ipfs/go-cid"

// Set of actor code types that can represent external signing parties.
var CallerTypesSignable = []cid.Cid{AccountActorCodeID, MultisigActorCodeID}
