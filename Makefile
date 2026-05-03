.PHONY: help test test-race bench bench-cpu cover lint vet fmt tidy clean all

# Default target — show help.
help:
	@echo "kahora — make targets:"
	@echo ""
	@echo "  make test       Run unit tests"
	@echo "  make test-race  Run unit tests with race detector"
	@echo "  make bench      Run benchmarks (3s per bench)"
	@echo "  make bench-cpu  Run parallel benchmarks across 1,2,4,8,16 cores"
	@echo "  make cover      Run tests with coverage report"
	@echo "  make lint       Run golangci-lint"
	@echo "  make vet        Run go vet"
	@echo "  make fmt        Format code with gofmt"
	@echo "  make tidy       Run go mod tidy"
	@echo "  make clean      Remove build artefacts"
	@echo "  make all        fmt, vet, test-race, bench"

test:
	go test -v ./...

test-race:
	go test -race -v ./...

bench:
	go test -bench=. -benchmem -benchtime=3s -run=^$$ ./...

bench-cpu:
	go test -bench=Parallel -benchmem -cpu=1,2,4,8,16 -run=^$$ ./...

cover:
	go test -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out
	@echo ""
	@echo "Open HTML report:  go tool cover -html=coverage.out"

lint:
	@which golangci-lint > /dev/null || (echo "install golangci-lint: https://golangci-lint.run/usage/install/" && exit 1)
	golangci-lint run ./...

vet:
	go vet ./...

fmt:
	gofmt -s -w .

tidy:
	go mod tidy

clean:
	rm -f coverage.out

all: fmt vet test-race bench