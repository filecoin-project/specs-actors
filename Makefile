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

test-coverage:
	$(GO_BIN) test -coverprofile=coverage.out ./...
.PHONY: test-coverage

tidy:
	$(GO_BIN) mod tidy
.PHONY: tidy

lint: .nomaprange.so
	$(GOLINT) run ./...
.PHONY: lint

.nomaprange.so:
	$(eval TMP=$(shell mktemp -d))
	(cd $(TMP); go mod init a; go build -buildmode=plugin -o .nomaprange.so github.com/Kubuxu/go-no-map-range/plugin)
	cp $(TMP)/.nomaprange.so .nomaprange.so
	rm -rf $(TMP)

gen:
	$(GO_BIN) run ./gen/gen.go
.PHONY: gen
