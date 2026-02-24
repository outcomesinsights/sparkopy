import click
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


def dump_table(spark, database_name, table_name, output):
    df = spark.read.table(f"{database_name}.{table_name}")

    # Important: When using Spark Connect, we need to collect data locally
    # since df.write would write to the Spark server's filesystem, not local

    # Enable Arrow optimization for faster and more memory-efficient conversion
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
    spark.conf.set("spark.driver.maxResultSize", "4g")

    # Check table size
    total_rows = df.count()
    print(f"Exporting {total_rows:,} rows from {database_name}.{table_name}")

    # For very large tables (>5M rows), consider batching
    if total_rows > 5_000_000:
        print("Large table detected. Consider using --batch option if memory errors occur.")

    # Convert to pandas and write as single local file
    df_p = df.toPandas()

    # Remove metrics attribute that can't be serialized
    if "metrics" in df_p.attrs:
        del df_p.attrs["metrics"]

    # Write single parquet file to local filesystem
    df_p.to_parquet(output)
    print(f"Successfully exported to {output}")


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
