# Changelog

## [0.1.0] - 2026-10-27

### Breaking changes

- Remove the Python sparkopy and the Databricks option ([fc3a112](https://github.com/outcomesinsights/sparkopy/commit/fc3a112560bcb5a55c0e35f2e44b6648c318c1b6)): sparkopy is no longer a Python package, and the --databricks-profile option is gone. Install the release binary and use --spark-uri.

### Added

- Rewrite the sparkopy command in Rust, replacing Go ([2366e3c](https://github.com/outcomesinsights/sparkopy/commit/2366e3ca423f48c17437c02721fd4bead3d98ee5))
- **go:** Stream a Spark Connect table to one Parquet file ([99eacb9](https://github.com/outcomesinsights/sparkopy/commit/99eacb902f7a4af9e2ef09e88ab567eedf364b71))
- **beads:** Sync via the Dolt remote; stop tracking the JSONL ([8e18c0d](https://github.com/outcomesinsights/sparkopy/commit/8e18c0d052a9960f9c2b8b19234d5d6268ab0f06))
- Stream large tables to avoid OOM kills ([64f0dcb](https://github.com/outcomesinsights/sparkopy/commit/64f0dcbf496a304913a3f98e6acdd31de92b0995))
- Add tests and CI workflow ([2880e0b](https://github.com/outcomesinsights/sparkopy/commit/2880e0b95f6125bedb91f323d2a63fdb36147861))

### Changed

- **deps:** Bump cryptography to >=48 and idna to >=3.15 (security) ([375a42e](https://github.com/outcomesinsights/sparkopy/commit/375a42e79bd619136aad2d5fedaabb73e815863a))

### Fixed

- **deps:** Bump urllib3 2.6.3 -> 2.7.0 ([5d3f1d4](https://github.com/outcomesinsights/sparkopy/commit/5d3f1d49caae53dd46b0bbb3f9d1c8278f21db53))
- Allow dependabot auto-merge for git SHA ref bumps ([9940b46](https://github.com/outcomesinsights/sparkopy/commit/9940b4607948fae68365472b68a7789ab8f06838))
- Update project URLs to actual repository ([37d5e84](https://github.com/outcomesinsights/sparkopy/commit/37d5e84eeb57c0eb1c1d4422c5837dcf2113ffc3))

[0.1.0]: https://github.com/outcomesinsights/sparkopy/releases/tag/v0.1.0
