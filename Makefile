GO_BIN ?= go
# relative path ../../ is included in path because current working directory of tests is directory of test files
# and all test vector generation comes from a call to go test ./actors/test
TEST_VECTOR_PATH = ../../test-vectors
all: build lint test tidy determinism-check
.PHONY: all

build:
	$(GO_BIN) build ./...
.PHONY: build

test:
	$(GO_BIN) test ./...
	$(GO_BIN) test -race ./actors/migration/nv15/test
.PHONY: test

test-migration:
.PHONY: test-migration
	$(GO_BIN) test -race ./actors/migration/nv15/test

test-coverage:
	$(GO_BIN) test -coverprofile=coverage.out ./...
.PHONY: test-coverage

tidy:
	$(GO_BIN) mod tidy
.PHONY: tidy

gen:
	$(GO_BIN) run ./gen/gen.go
.PHONY: gen

determinism-check: 
	rm -rf test-vectors/determinism
	
	SPECS_ACTORS_DETERMINISM="$(TEST_VECTOR_PATH)/determinism" $(GO_BIN) test ./actors/test -count=1
	$(GO_BIN) build ./test-vectors/tools/digest

	if [ "`./digest ./test-vectors/determinism`" != "`cat ./test-vectors/determinism-check`" ]; then \
		echo "test-vectors don't match expected";\
		exit 1;\
	fi

determinism-gen: 
	rm -rf test-vectors/determinism
	SPECS_ACTORS_DETERMINISM="$(TEST_VECTOR_PATH)/determinism" $(GO_BIN) test ./actors/test -count=1
	$(GO_BIN) build ./test-vectors/tools/digest
	./digest ./test-vectors/determinism > ./test-vectors/determinism-check

conformance-gen: 
	rm -rf test-vectors/conformance
	SPECS_ACTORS_CONFORMANCE="$(TEST_VECTOR_PATH)/conformance" $(GO_BIN) test ./actors/test -count=1
	tar -zcf test-vectors/conformance.tar.gz test-vectors/conformance

# tools
toolspath:=support/tools

$(toolspath)/bin/golangci-lint: $(toolspath)/go.mod
	@mkdir -p $(dir $@)
	(cd $(toolspath); go build -tags tools -o $(@:$(toolspath)/%=%) github.com/golangci/golangci-lint/cmd/golangci-lint)


$(toolspath)/bin/no-map-range.so: $(toolspath)/go.mod
	@mkdir -p $(dir $@)
	(cd $(toolspath); go build -tags tools -buildmode=plugin -o $(@:$(toolspath)/%=%) github.com/Kubuxu/go-no-map-range/plugin)

lint: $(toolspath)/bin/golangci-lint $(toolspath)/bin/no-map-range.so
	$(toolspath)/bin/golangci-lint run ./...
.PHONY: lint
