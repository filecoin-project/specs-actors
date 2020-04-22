# Filecoin actors
[![CircleCI](https://circleci.com/gh/filecoin-project/specs-actors.svg?style=svg)](https://circleci.com/gh/filecoin-project/specs-actors)
[![codecov](https://codecov.io/gh/filecoin-project/specs-actors/branch/master/graph/badge.svg)](https://codecov.io/gh/filecoin-project/specs-actors)

This repo is the specification of the Filecoin builtin actors, in the form of executable code.

This is a companion to the rest of the [Filecoin Specification](https://github.com/filecoin-project/specs), 
but also directly usable by Go implementations of Filecoin.

## Versioning

Releases of this repo follow semantic versioning rules interpreted for distributed state machines.
- A major version change indicates a backwards-incompatible change in the state machine evaluation. 
  This means that the same sequence of messages would produce different states at two different major versions.
  In a blockchain, this would usually require a coordinated network upgrade or "hard fork".
- A minor version change indicates a change in exported software interfaces, while retaining
  compatible state evaluation. A minor version change will usually require some changes to the host software, 
  but different machiens running different minor versions of the same _major_ version should continue to 
  agree about state evaluation. Note that this differs from semantic versioning strictly defined 
  (which would require a major version change for this too).
- A patch version change indicates a backward compatible fix or improvement that doesn't change state evaluation
  semantics or exported interfaces.

## License
This repository is dual-licensed under Apache 2.0 and MIT terms.

Copyright 2019-2020. Protocol Labs, Inc.
