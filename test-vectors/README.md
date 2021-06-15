# specs-actors test-vector generation

This directory is used to store filecoin test-vectors auto genearted from scenario tests.  

## Overview

The structure of the generated files is designed to help with debugging inconsistencies. Vectors live in a directory structure that identifies the name of the test that generated them. Filenames are of the format `<sha256 hash of json data>-<sending actor addr>-<receiving actor addr>-<actor name>-<executed method number>.json`. All test-vectors are of the "message" class and cover the execution of a single message.

The `determinism` directory is used to hold vectors for checking that multiple runs of the same message results in deterministic output. The `tools` directory contains a tool for taking a collision resistant hash of all vectors to catch non-determinism. The checked-in `determinism-check` file is used to track what this hash should be. This file needs to be updated when merging breaking changes to specs-actors.

The `conformance` directory is used to hold vectors for conformance tests with other implementations.

## Generation workflows

Three make directives exist for working with these test-vectors.

### `make determinism-gen`

This runs scenario tests to create a test-vector corpus underneath test-vectors/determinism and creates a digest of the corpus in test-vectors/determinism-check by applying the digest tool to the determinism directory

### `make determinism-check`

This removes any existing content in test-vectors/determinism, runs scenario tests to create a test-vector corpus underneath test-vectors/determinism and regenerates the digest to make sure that state transitions in scenario tests match the recorded run.  If digests do not match it returns a failing exitcode.  This now runs on CI.

### `make conformance-gen`

This runs scenario tests and generates a subset of test-vectors from test state transitions that can serve as valid conformance tests across implementations. It is a subset of the determinism corpus currently because the test-vector format cannot yet handle faking crypto syscalls. The corpus is generated underneath test-vectors/conformance