# Install the pinned tools (mise.toml) and the locked Python environment, then arm
# the tracked hooks in .githooks (gator's githooks standard). Each clone, host and
# worktree runs this once. bd bootstrap, never bd init: in a clone that already has
# .beads, init re-points the hooks at .beads/hooks and commits.
setup:
    mise install
    uv sync --locked
    @if [ -d .beads ] && [ ! -d .beads/embeddeddolt ] && command -v bd >/dev/null 2>&1; then bd bootstrap; fi
    @if [ -d .githooks ]; then git config core.hooksPath .githooks; fi

# Where cargo writes its builds (CARGO_TARGET_DIR, else target/)
target_dir := env("CARGO_TARGET_DIR", "target")

# Format Rust and Python in place, and apply ruff's safe autofixes
fmt:
    cargo fmt
    uv run --locked ruff format src/ tests/
    uv run --locked ruff check --fix src/ tests/

# Fail if fmt would change anything; rewrites nothing
fmt-check:
    cargo fmt --check
    uv run --locked ruff format --check src/ tests/

# Every non-rewriting check: clippy, ruff, the workflow linters and the commit subjects.
# cog starts after 27abdf5 ("bd init: ...", not a conventional subject, kept as history).
# After the first release tag, switch to gator's tagged form:
#   tag="$(git describe --tags --abbrev=0 --match 'v*')" && cog check --ignore-merge-commits "$tag..HEAD"
lint:
    cargo clippy --locked --all-targets -- -D warnings
    uv run --locked ruff check src/ tests/
    actionlint
    zizmor --offline --config .github/zizmor.yml .
    cog check --ignore-merge-commits 27abdf539336898bd269afe20f211dc12cdfe1d7..HEAD

# Run the Rust and the Python tests
test: rust-test py-test

# Run the Rust tests (the parity test skips itself without SPARKOPY_TEST_SPARK_URI)
rust-test:
    cargo test --locked

# Run the Python tests (until the Python code is removed)
py-test:
    uv run --locked pytest tests/ -v

# The full local equivalent of CI
ci: fmt-check lint test

# The checks the commit hook runs
pre-commit: fmt-check lint

# Build sparkopy as one static linux/amd64 (musl) binary at dist/sparkopy
build:
    cargo build --release --locked --target x86_64-unknown-linux-musl
    mkdir -p dist
    cp {{target_dir}}/x86_64-unknown-linux-musl/release/sparkopy dist/sparkopy

# Build static linux/amd64 and linux/arm64 (musl) binaries and SHA256SUMS in dist/release.
# cargo-zigbuild links with zig, so one x86_64 host builds both. mise.toml pins zig and
# cargo-zigbuild.
release-assets:
    rm -rf dist/release
    mkdir -p dist/release
    cargo zigbuild --release --locked --target x86_64-unknown-linux-musl --target aarch64-unknown-linux-musl
    cp {{target_dir}}/x86_64-unknown-linux-musl/release/sparkopy dist/release/sparkopy-linux-amd64
    cp {{target_dir}}/aarch64-unknown-linux-musl/release/sparkopy dist/release/sparkopy-linux-arm64
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
    SPARKOPY_PARITY_DATABASE={{database}} cargo test --release --locked -- --exact parity_tests::parity --nocapture 2>&1 | tee $log
    rc=${pipestatus[1]}
    echo "parity log: $log"
    exit $rc
