NPROC := $(shell getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)
J ?= $(shell n=$(NPROC); h=$$((n / 2)); [ $$h -lt 2 ] && h=2; echo $$h)
export GOMAXPROCS ?= $(J)

.PHONY: build test lint conformance bench demo clean

build:
	go build -o bin/ ./cmd/...

test:
	go test -race -count=1 ./...

lint:
	test -z "$$(gofmt -l .)"
	go vet ./...
	golangci-lint run ./...

# Differential test against a real Prometheus (needs `prometheus` on PATH).
conformance:
	for s in 1 2 3 4 5 6 7 8 9 10; do SEED=$$s OUT=bench/results/conformance/seed$$s.json ./scripts/conformance.sh || exit 1; done

bench:
	./scripts/bench.sh

demo: build
	./scripts/demo.sh

clean:
	rm -rf bin
