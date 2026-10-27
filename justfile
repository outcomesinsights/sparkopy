# Run the full test suite (matches CI)
test:
    uv run ruff check src/ tests/
    uv run ruff format --check src/ tests/
    uv run pytest tests/ -v

# Build the Go sparkopy as one static linux/amd64 binary at bin/sparkopy
build:
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o bin/sparkopy ./cmd/sparkopy

# Run the Go tests, go vet and the gofmt check
go-test:
    go vet ./...
    test -z "$(gofmt -l cmd internal)" || { gofmt -l cmd internal; exit 1; }
    go test ./...

# Regenerate the Go gRPC stubs from the Spark v3.5.1 protobuf files in proto/
proto:
    #!/usr/bin/env zsh
    set -euo pipefail
    bin=$(mktemp -d)
    GOBIN=$bin go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
    GOBIN=$bin go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.1
    module=github.com/outcomesinsights/sparkopy
    maps=()
    for f in proto/spark/connect/*.proto; do
      maps+=("M${f#proto/}=$module/internal/connectpb;connectpb")
    done
    opts=${(j:,:)maps}
    rm -f internal/connectpb/*.pb.go(N)
    protoc -I proto \
      --plugin=protoc-gen-go=$bin/protoc-gen-go \
      --plugin=protoc-gen-go-grpc=$bin/protoc-gen-go-grpc \
      --go_out=. --go_opt=module=$module,$opts \
      --go-grpc_out=. --go-grpc_opt=module=$module,$opts \
      proto/spark/connect/*.proto
    rm -rf $bin
