# sparkopy

Pronounced "Spar-Copy".

sparkopy reads one table from a Spark Connect server and writes it to one local Parquet file.

- It streams the Arrow batches that Spark sends straight to the Parquet file, so it never holds the whole table in memory.
- It keeps Spark's column types exactly. It converts no type.
- It is one static Linux binary of about 3.4 MB. It needs no Python, no Java and no Spark installation.
- It writes to `PATH.tmp` and renames that file only after success. A failed run leaves an existing output file unchanged.

sparkopy supports only Spark Connect (`sc://` URIs). It uses the Spark 3.5.1 protocol.

## Usage

```bash
sparkopy --spark-uri sc://HOST:15002 --database DB --table TABLE --output PATH
```

Example:

```bash
sparkopy --spark-uri sc://spark.example.com:15002 --database my_database --table events --output /tmp/events.parquet
```

The URI has the form `sc://HOST[:PORT]`. The default port is 15002. Progress and errors go to standard error. The exit status is 0 for success, 1 for a failed export and 2 for a usage error.

## Install

Download the static binary for your architecture from the [GitHub releases](https://github.com/outcomesinsights/sparkopy/releases). Each release has these assets:

- `sparkopy-linux-amd64`
- `sparkopy-linux-arm64`
- `SHA256SUMS`

```bash
curl -fsSLO https://github.com/outcomesinsights/sparkopy/releases/download/v0.1.0/sparkopy-linux-amd64
curl -fsSLO https://github.com/outcomesinsights/sparkopy/releases/download/v0.1.0/SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS
install -m 0755 sparkopy-linux-amd64 /usr/local/bin/sparkopy
```

To build the binary from source, run `just build`. It writes `dist/sparkopy` (static, linux/amd64).

## Development

The tools are pinned in `mise.toml` and the Rust version in `rust-toolchain.toml`. The `just` recipes are:

| Recipe                | What it does                                                                                                                                                                           |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `just setup`          | Installs the pinned tools and arms the git hooks in `.githooks`. Run it once in each clone.                                                                                            |
| `just fmt`            | Formats the Rust code.                                                                                                                                                                 |
| `just lint`           | Runs clippy, actionlint, zizmor and the conventional-commit check.                                                                                                                     |
| `just test`           | Runs the Rust tests against an in-process fake Spark Connect server.                                                                                                                   |
| `just ci`             | Runs the full local equivalent of CI: the format check, `lint` and `test`.                                                                                                             |
| `just build`          | Builds the static linux/amd64 binary at `dist/sparkopy`.                                                                                                                               |
| `just release-assets` | Builds the static linux/amd64 and linux/arm64 binaries and `SHA256SUMS` in `dist/release`.                                                                                             |
| `just parity`         | Exports every table of a database from a real Spark Connect server and checks each file against Spark's schema and row count. It needs `SPARKOPY_TEST_SPARK_URI` and skips without it. |

A pushed `v*` tag builds the release binaries and attaches them to a GitHub release.

## License

MIT. See `LICENSE`.
