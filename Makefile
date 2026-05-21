# Variables
BINARY_NAME="koudmain-worker"
MAIN_PATH="./cmd/koudmain"

# Commands
all:	build

test:
	go test ./... -v

test-cover:
	go test ./... -cover

build:
	go build -o $(BINARY_NAME) $(MAIN_PATH)

run:
	go run $(MAIN_PATH)

lint:
	golangci-lint run

fmt:
	golangci-lint fmt

clean:
	go clean
	rm -rf $(BINARY_NAME)

re: clean all

.PHONY: all test test-cover build run lint fmt clean re
