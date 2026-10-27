BIN    := bin/hookrelay
CONFIG ?= config.example.json

.PHONY: all build test run vet fmt docker clean

all: vet test build

build:
	CGO_ENABLED=0 go build -trimpath -o $(BIN) ./cmd/hookrelay

test:
	go test -race -count=1 ./...

vet:
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)
	go vet ./...

# Example: HOOKRELAY_SECRET_PAYMENTS=whsec_demo HOOKRELAY_ADMIN_TOKEN=admin make run
run:
	go run ./cmd/hookrelay -config $(CONFIG)

fmt:
	gofmt -w .

docker:
	docker build -t hookrelay:dev .

clean:
	rm -rf bin data
