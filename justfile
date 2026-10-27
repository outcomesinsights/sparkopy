# Install the pinned tools (mise.toml) and the locked Python environment, then arm
# the tracked hooks in .githooks (gator's githooks standard). Each clone, host and
# worktree runs this once. bd bootstrap, never bd init: in a clone that already has
# .beads, init re-points the hooks at .beads/hooks and commits.
setup:
    mise install
    uv sync --locked
    @if [ -d .beads ] && [ ! -d .beads/embeddeddolt ] && command -v bd >/dev/null 2>&1; then bd bootstrap; fi
    @if [ -d .githooks ]; then git config core.hooksPath .githooks; fi

# Format Go and Python in place, and apply ruff's safe autofixes
fmt:
    gofmt -w cmd internal
    uv run --locked ruff format src/ tests/
    uv run --locked ruff check --fix src/ tests/

# Fail if fmt would change anything; rewrites nothing
fmt-check:
    test -z "$(gofmt -l cmd internal)" || { gofmt -l cmd internal; exit 1; }
    uv run --locked ruff format --check src/ tests/

# Every non-rewriting check: go vet, ruff, the workflow linters and the commit subjects.
# cog starts after 27abdf5 ("bd init: ...", not a conventional subject, kept as history).
# After the first release tag, switch to gator's tagged form:
#   tag="$(git describe --tags --abbrev=0 --match 'v*')" && cog check --ignore-merge-commits "$tag..HEAD"
lint:
    go vet ./...
    uv run --locked ruff check src/ tests/
    actionlint
    zizmor --offline --config .github/zizmor.yml .
    cog check --ignore-merge-commits 27abdf539336898bd269afe20f211dc12cdfe1d7..HEAD

# Run the Go and the Python tests
test: go-test py-test

# Run the Go tests
go-test:
    go test ./...

# Run the Python tests (until the Python code is removed)
py-test:
    uv run --locked pytest tests/ -v

# The full local equivalent of CI
ci: fmt-check lint test

# The checks the commit hook runs
pre-commit: fmt-check lint

# Build the Go sparkopy as one static linux binary at dist/sparkopy
build arch="amd64":
    CGO_ENABLED=0 GOOS=linux GOARCH={{arch}} go build -trimpath -ldflags='-s -w' -o dist/sparkopy ./cmd/sparkopy

# Build static linux/amd64 and linux/arm64 binaries and SHA256SUMS in dist/release
release-assets:
    rm -rf dist/release
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o dist/release/sparkopy-linux-amd64 ./cmd/sparkopy
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags='-s -w' -o dist/release/sparkopy-linux-arm64 ./cmd/sparkopy
    cd dist/release && sha256sum sparkopy-linux-amd64 sparkopy-linux-arm64 > SHA256SUMS

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
