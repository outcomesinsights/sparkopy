from unittest.mock import MagicMock, patch, PropertyMock
import pandas as pd
from click.testing import CliRunner

from sparkopy import cli, dump_table, get_spark


def _mock_spark_df(row_count=100):
    """Create a mock Spark DataFrame that returns a pandas DataFrame from toPandas()."""
    df = MagicMock()
    df.count.return_value = row_count
    pdf = pd.DataFrame({"id": range(row_count), "value": [f"v{i}" for i in range(row_count)]})
    df.toPandas.return_value = pdf
    return df


def _mock_spark_session(row_count=100):
    """Create a mock SparkSession whose read.table() returns a mock DataFrame."""
    spark = MagicMock()
    spark.read.table.return_value = _mock_spark_df(row_count)
    return spark


# --- get_spark routing ---

@patch("sparkopy.get_databricks_session")
@patch("sparkopy.get_spark_session")
def test_get_spark_with_databricks_profile(mock_spark_sess, mock_db_sess):
    get_spark(spark_uri=None, databricks_profile="DEFAULT")
    mock_db_sess.assert_called_once_with("DEFAULT")
    mock_spark_sess.assert_not_called()


@patch("sparkopy.get_databricks_session")
@patch("sparkopy.get_spark_session")
def test_get_spark_with_spark_uri(mock_spark_sess, mock_db_sess):
    get_spark(spark_uri="sc://localhost:15002", databricks_profile=None)
    mock_spark_sess.assert_called_once_with("sc://localhost:15002")
    mock_db_sess.assert_not_called()


@patch("sparkopy.get_databricks_session")
@patch("sparkopy.get_spark_session")
def test_get_spark_databricks_takes_priority(mock_spark_sess, mock_db_sess):
    """When both are provided, databricks_profile wins."""
    get_spark(spark_uri="sc://localhost:15002", databricks_profile="DEFAULT")
    mock_db_sess.assert_called_once_with("DEFAULT")
    mock_spark_sess.assert_not_called()


# --- dump_table ---

def test_dump_table_writes_parquet(tmp_path):
    spark = _mock_spark_session(row_count=10)
    output = str(tmp_path / "out.parquet")

    dump_table(spark, "mydb", "mytable", output)

    spark.read.table.assert_called_once_with("mydb.mytable")
    result = pd.read_parquet(output)
    assert len(result) == 10
    assert list(result.columns) == ["id", "value"]


def test_dump_table_large_table_warning(tmp_path, capsys):
    spark = _mock_spark_session(row_count=6_000_000)
    output = str(tmp_path / "out.parquet")

    dump_table(spark, "db", "big_table", output)

    captured = capsys.readouterr()
    assert "6,000,000 rows" in captured.out
    assert "Large table detected" in captured.out


def test_dump_table_no_large_warning_for_small_table(tmp_path, capsys):
    spark = _mock_spark_session(row_count=100)
    output = str(tmp_path / "out.parquet")

    dump_table(spark, "db", "small_table", output)

    captured = capsys.readouterr()
    assert "Large table detected" not in captured.out


def test_dump_table_cleans_metrics_attr(tmp_path):
    spark = _mock_spark_session(row_count=5)
    # Inject a metrics attr into the pandas DataFrame returned by toPandas()
    pdf = spark.read.table.return_value.toPandas.return_value
    pdf.attrs["metrics"] = {"some": "data"}
    output = str(tmp_path / "out.parquet")

    dump_table(spark, "db", "tbl", output)

    # The file should be written successfully (metrics stripped before write)
    result = pd.read_parquet(output)
    assert len(result) == 5


# --- CLI ---

@patch("sparkopy.get_spark")
def test_cli_requires_options(mock_get_spark):
    runner = CliRunner()
    result = runner.invoke(cli, [])
    assert result.exit_code != 0
    assert "Missing option" in result.output or "required" in result.output.lower()


@patch("sparkopy.dump_table")
@patch("sparkopy.get_spark")
def test_cli_passes_args_to_dump_table(mock_get_spark, mock_dump_table):
    mock_spark = MagicMock()
    mock_get_spark.return_value = mock_spark
    runner = CliRunner()

    result = runner.invoke(cli, [
        "--spark-uri", "sc://host:15002",
        "--database", "mydb",
        "--table", "mytable",
        "--output", "/tmp/out.parquet",
    ])

    assert result.exit_code == 0
    mock_get_spark.assert_called_once_with("sc://host:15002", None)
    mock_dump_table.assert_called_once_with(mock_spark, "mydb", "mytable", "/tmp/out.parquet")


@patch("sparkopy.dump_table")
@patch("sparkopy.get_spark")
def test_cli_databricks_profile(mock_get_spark, mock_dump_table):
    mock_spark = MagicMock()
    mock_get_spark.return_value = mock_spark
    runner = CliRunner()

    result = runner.invoke(cli, [
        "--databricks-profile", "DEFAULT",
        "--database", "catalog.schema",
        "--table", "tbl",
        "--output", "/tmp/out.parquet",
    ])

    assert result.exit_code == 0
    mock_get_spark.assert_called_once_with(None, "DEFAULT")
