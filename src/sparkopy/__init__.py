import sys
import tempfile
from pathlib import Path

import click
import pyarrow as pa
import pyarrow.parquet as pq
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession

LARGE_TABLE_THRESHOLD = 1_000_000


def dump_table(spark, database_name, table_name, output):
    df = spark.read.table(f"{database_name}.{table_name}")

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")

    total_rows = df.count()
    print(f"Exporting {total_rows:,} rows from {database_name}.{table_name}", file=sys.stderr)

    if total_rows > LARGE_TABLE_THRESHOLD:
        dump_table_streaming(df, output, total_rows)
    else:
        dump_table_simple(df, output)

    print(f"Successfully exported to {output}", file=sys.stderr)


def dump_table_simple(df, output):
    df_p = df.toPandas()
    if "metrics" in df_p.attrs:
        del df_p.attrs["metrics"]
    df_p.to_parquet(output)


def _unify_schema(existing, incoming):
    """Return a schema that can hold data from both schemas.

    For decimal types, picks the wider precision/scale. For other type
    mismatches, falls back to string."""
    fields = []
    for ef, nf in zip(existing, incoming, strict=True):
        if ef.type == nf.type:
            fields.append(ef)
        elif pa.types.is_decimal(ef.type) and pa.types.is_decimal(nf.type):
            precision = max(ef.type.precision, nf.type.precision)
            scale = max(ef.type.scale, nf.type.scale)
            fields.append(pa.field(ef.name, pa.decimal128(precision, scale)))
        else:
            fields.append(pa.field(ef.name, pa.string()))
    return pa.schema(fields)


def _stream_arrow_batches(df):
    """Yield Arrow RecordBatches from a Spark Connect DataFrame by tapping
    into the internal gRPC streaming iterator.

    This avoids materializing the entire result set in memory."""
    plan = df._plan.to_proto(df._session.client)
    req = df._session.client._execute_plan_request_with_metadata()
    req.plan.CopyFrom(plan)

    for response in df._session.client._execute_and_fetch_as_iterator(req, {}):
        if isinstance(response, pa.RecordBatch):
            yield response


def dump_table_streaming(df, output, total_rows):
    """Export large tables by streaming Arrow RecordBatches from Spark Connect
    directly to parquet files on disk, then merging with a unified schema."""
    batch_files = []
    rows_written = 0
    unified_schema = None

    with tempfile.TemporaryDirectory() as tmpdir:
        for batch in _stream_arrow_batches(df):
            if batch.num_rows == 0:
                continue

            batch_table = pa.Table.from_batches([batch])

            if unified_schema is None:
                unified_schema = batch_table.schema
            else:
                unified_schema = _unify_schema(unified_schema, batch_table.schema)

            batch_path = str(Path(tmpdir) / f"batch_{len(batch_files)}.parquet")
            pq.write_table(batch_table, batch_path)
            batch_files.append(batch_path)
            rows_written += batch.num_rows

            print(
                f"  Streamed {rows_written:,} / {total_rows:,} rows "
                f"({rows_written * 100 // total_rows}%)",
                file=sys.stderr,
            )

        if rows_written == 0:
            raise RuntimeError(f"No rows exported — expected {total_rows:,}")

        # Merge batch files into a single output with unified schema
        with pq.ParquetWriter(output, unified_schema) as writer:
            for bf in batch_files:
                batch_table = pq.read_table(bf).cast(unified_schema)
                writer.write_table(batch_table)

    print(f"  Merged {len(batch_files)} batches into {output}", file=sys.stderr)


def get_spark(spark_uri, databricks_profile):
    if databricks_profile is not None:
        return get_databricks_session(databricks_profile)
    else:
        return get_spark_session(spark_uri)


def get_databricks_session(databricks_profile):
    return DatabricksSession.builder.profile(databricks_profile).getOrCreate()


def get_spark_session(spark_uri):
    return SparkSession.builder.remote(spark_uri).getOrCreate()


@click.command()
@click.option("--spark-uri", help="URL for Spark Connect, e.g. sc://hostname:15002")
@click.option("--databricks-profile", help="name of authenticated databricks profile, e.g. DEFAULT")
@click.option(
    "--database",
    required=True,
    help="database to either dump completely, or search for the specified table",
)
@click.option("--table", required=True, help="table to dump")
@click.option("--output", required=True, help="file path for Parquet file")
def cli(spark_uri, databricks_profile, database, table, output):
    dump_table(get_spark(spark_uri, databricks_profile), database, table, output)
