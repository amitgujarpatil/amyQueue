# AmyQueue Makefile

.PHONY: all build build-controller build-broker build-cli clean test test-race test-integration coverage proto run-controller run-broker help

# Variables
BINARY_DIR=bin
CONTROLLER_BINARY=$(BINARY_DIR)/controller
BROKER_BINARY=$(BINARY_DIR)/broker
CLI_BINARY=$(BINARY_DIR)/cli

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Proto parameters
PROTO_DIR=proto
PROTO_OUT=src/pkg/protocol

# Build flags
LDFLAGS=-ldflags "-X main.Version=$(shell git describe --tags --always --dirty) -X main.BuildTime=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)"

all: proto build test

## build: Build all binaries
build: build-controller build-broker build-cli

## build-controller: Build controller binary
build-controller:
	@echo "Building controller..."
	@mkdir -p $(BINARY_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(CONTROLLER_BINARY) ./src/cmd/controller

## build-broker: Build broker binary
build-broker:
	@echo "Building broker..."
	@mkdir -p $(BINARY_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BROKER_BINARY) ./src/cmd/broker

## build-cli: Build CLI binary
build-cli:
	@echo "Building CLI..."
	@mkdir -p $(BINARY_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(CLI_BINARY) ./src/cmd/cli

## proto: Generate protobuf code
proto:
	@echo "Generating protobuf code..."
	@mkdir -p $(PROTO_OUT)
	protoc --go_out=$(PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/*.proto

## clean: Clean build artifacts
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BINARY_DIR)
	rm -rf data/
	rm -rf logs/
	rm -rf tmp/

## test: Run unit tests
test:
	@echo "Running tests..."
	$(GOTEST) -v -short ./...

## test-race: Run tests with race detector
test-race:
	@echo "Running tests with race detector..."
	$(GOTEST) -v -race -short ./...

## test-integration: Run integration tests
test-integration:
	@echo "Running integration tests..."
	$(GOTEST) -v -tags=integration ./test/integration/...

## test-e2e: Run end-to-end tests
test-e2e:
	@echo "Running end-to-end tests..."
	$(GOTEST) -v -tags=e2e ./test/e2e/...

## coverage: Generate test coverage report
coverage:
	@echo "Generating coverage report..."
	$(GOTEST) -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

## fmt: Format code
fmt:
	@echo "Formatting code..."
	$(GOCMD) fmt ./...

## vet: Run go vet
vet:
	@echo "Running go vet..."
	$(GOCMD) vet ./...

## lint: Run golangci-lint
lint:
	@echo "Running golangci-lint..."
	golangci-lint run

## deps: Download dependencies
deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy

## run-controller: Run controller locally
run-controller: build-controller
	@echo "Starting controller..."
	./$(CONTROLLER_BINARY) --config configs/controller-1.yaml

## run-broker: Run broker locally
run-broker: build-broker
	@echo "Starting broker..."
	./$(BROKER_BINARY) --config configs/broker-1.yaml

## docker-build: Build Docker images
docker-build:
	@echo "Building Docker images..."
	docker build -t amyqueue/controller:latest -f docker/Dockerfile.controller .
	docker build -t amyqueue/broker:latest -f docker/Dockerfile.broker .

## docker-compose-up: Start cluster with docker-compose
docker-compose-up:
	@echo "Starting cluster with docker-compose..."
	docker-compose up -d

## docker-compose-down: Stop cluster
docker-compose-down:
	@echo "Stopping cluster..."
	docker-compose down

## install-tools: Install development tools
install-tools:
	@echo "Installing tools..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

## help: Show this help message
help:
	@echo "AmyQueue - Makefile commands:"
	@echo ""
	@grep -E '^## ' Makefile | sed 's/## /  /'
	@echo ""

.DEFAULT_GOAL := help
