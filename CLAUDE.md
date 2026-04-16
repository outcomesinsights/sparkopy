# sparkopy

> CLI tool that downloads tables from Spark/Databricks and exports them as local Parquet files.

## Status

- **Active**
- Last meaningful work: 2024-05

## Tech Stack

- Language: Python
- Framework: Click (CLI)
- Key dependencies: click, databricks-connect, pandas, pyarrow, pyspark

## Purpose

Exports tables from Spark or Databricks databases to local Parquet files. Supports both Spark Connect (via URI) and Databricks Connect (via profile authentication). Useful for copying data from remote Spark/Databricks environments to local storage for analysis.

## Key Entry Points

- `sparkopy` CLI command - Main entry point defined in `src/sparkopy/__init__.py`
- `cli()` - Click command that handles argument parsing
- `dump_table()` - Core function that reads table and writes Parquet

## Commands

```bash
# Export from Spark Connect
sparkopy --spark-uri "sc://hostname:15002" --database mydb --table mytable --output /tmp/output.parquet

# Export from Databricks Connect
sparkopy --databricks-profile DEFAULT --database catalog.schema --table mytable --output /tmp/output.parquet

# Build/install
uv sync
uv pip install -e .

# Run via Makefile (downloads multiple tables)
make all
```

## Relationships

- **Depends on**: Remote Spark/Databricks cluster
- **Feeds into**: None

## Domain Concepts

- **Spark Connect**: Protocol for connecting to remote Spark clusters (sc:// URI scheme)
- **Databricks Connect**: SDK for connecting to Databricks workspaces via authenticated profiles
