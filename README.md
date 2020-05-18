# Actors

Actors are Filecoin’s notion of smart contracts. 
They are not true smart contracts—with bytecode running on a VM—but instead implemented in Go. 
It is expected that other implementations will match the behaviour of the Go actors exactly. 
An ABI describes how inputs and outputs to the VM are encoded. 
Future work will replace this implementation with a "real" VM.

The [Actor](https://github.com/filecoin-project/lotus/blob/master/chain/types/actor.go) struct is the base implementation of actors, with fields common to all of them.

- `Code` is a CID identifying the actor code, but since these actors are implemented in Go, is actually some fixed bytes acting as an identifier. 
This identifier selects the kind of actor implementation when a message is sent to its address.
- `Head` is the CID  of this actor instance’s state.
- `Nonce` is a counter of #messages received *from* that actor. 
It is only set for account actors (the actors from which external parties send messages); messages between actors in the VM don’t use the nonce.
- `Balance` is FIL balance the actor controls. 

Some actors are singletons (e.g. the storage market) but others have multiple instances (e.g. storage miners). 
A storage miner actor exists for each miner in the Filesystem network. 
Their structs share the same code CID so they have the same behavior, but have distinct head state CIDs and balance. 
Each actor instance exists at an address in the state tree. An address is the hash of the actor’s public key.

The [account](./actors/builtin/account/account_actor.go) actor doesn’t have any special behavior or state other than a balance. 
Everyone who wants to send messages (transactions) has an account actor, and it is from this actor’s address that they send messages.

Every storage miner has an instance of a [miner](./actors/builtin/miner/miner_actor.go) actor. 
The miner actor plays a role in the storage protocol, for example it pledges space and collateral for storage, posts proofs of storage, etc. 
A miner actor’s state is located in the state tree at its address; the value found there is an Actor structure. 
The head CID in the actor structure points to that miner’s state instance (encoded).

Other built-in actors include the [payment broker](./actors/builtin/paych/paych_actor.go), 
which provides a mechanism for off-chain payments via payment channels, 
and the [storage market](actors/builtin/market/market_actor.go), 
which starts miners and tracks total storage (aka "power"). 
These are both singletons.

Actors declare a list of exported methods with ABI types. 
Method implementations typically load the state tree, perform some query or mutation, then return a value or an error. 
