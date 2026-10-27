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

# Export every table of DATABASE from a real Spark Connect server and compare
# each file with Spark's schema and row count. Local only: it needs
# SPARKOPY_TEST_SPARK_URI (such as sc://titan.jsaw.io:15002) and skips without it.
parity database="export_f5a5e28f_1c98_45a1_b111_41aa272976cb":
    #!/usr/bin/env zsh
    set -o pipefail
    if [[ -z ${SPARKOPY_TEST_SPARK_URI:-} ]]; then
      echo "parity: SPARKOPY_TEST_SPARK_URI is not set, so the parity check is skipped"
      exit 0
    fi
    mkdir -p claude_stuff
    log=claude_stuff/parity-$(date +%Y%m%d-%H%M%S).log
    SPARKOPY_PARITY_DATABASE={{database}} go test -count=1 -timeout 60m -run '^TestParity$' -v ./cmd/sparkopy 2>&1 | tee $log
    rc=${pipestatus[1]}
    echo "parity log: $log"
    exit $rc

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
