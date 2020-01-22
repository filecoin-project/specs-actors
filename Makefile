GO_BIN ?= go
GOLINT ?= golangci-lint

all: build lint test tidy
.PHONY: all

build:
	$(GO_BIN) build ./...
.PHONY: build

test:
	$(GO_BIN) test ./...
.PHONY: test

tidy:
	$(GO_BIN) mod tidy
.PHONY:

lint:
	$(GOLINT) run ./...
.PHONY: lint

gen:
	find ./ -type f -name 'cbor_gen.go' -exec rm {} +
	$(GO_BIN) run ./gen/gen.go
.PHONY: gen

