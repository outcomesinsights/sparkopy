from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow.parquet as pq
from click.testing import CliRunner

from sparkopy import cli, dump_table, get_databricks_session, get_spark, get_spark_session


def _make_mock_spark(rows=100, attrs=None):
    """Build a mock Spark session with a fake table that converts to a real pandas DataFrame."""
    pdf = pd.DataFrame({"a": range(rows), "b": [f"val_{i}" for i in range(rows)]})
    if attrs:
        pdf.attrs.update(attrs)

    mock_df = MagicMock()
    mock_df.count.return_value = rows
    mock_df.toPandas.return_value = pdf

    spark = MagicMock()
    spark.read.table.return_value = mock_df
    return spark


# ---------------------------------------------------------------------------
# dump_table
# ---------------------------------------------------------------------------
class TestDumpTable:
    def test_writes_parquet(self, tmp_path):
        out = tmp_path / "out.parquet"
        spark = _make_mock_spark(rows=5)
        dump_table(spark, "db", "tbl", str(out))

        table = pq.read_table(str(out))
        assert table.num_rows == 5
        assert set(table.column_names) == {"a", "b"}

    def test_reads_correct_table(self, tmp_path):
        out = tmp_path / "out.parquet"
        spark = _make_mock_spark()
        dump_table(spark, "catalog.schema", "my_table", str(out))

        spark.read.table.assert_called_once_with("catalog.schema.my_table")

    def test_sets_arrow_config(self, tmp_path):
        out = tmp_path / "out.parquet"
        spark = _make_mock_spark()
        dump_table(spark, "db", "tbl", str(out))

        calls = {c.args[0]: c.args[1] for c in spark.conf.set.call_args_list}
        assert calls["spark.sql.execution.arrow.pyspark.enabled"] == "true"
        assert calls["spark.sql.execution.arrow.maxRecordsPerBatch"] == "10000"
        assert calls["spark.driver.maxResultSize"] == "4g"

    def test_prints_row_count(self, tmp_path, capsys):
        out = tmp_path / "out.parquet"
        spark = _make_mock_spark(rows=1234)
        dump_table(spark, "db", "tbl", str(out))

        captured = capsys.readouterr().out
        assert "1,234 rows" in captured

    def test_large_table_warning(self, tmp_path, capsys):
        out = tmp_path / "out.parquet"
        spark = _make_mock_spark(rows=6_000_000)
        dump_table(spark, "db", "tbl", str(out))

        captured = capsys.readouterr().out
        assert "Large table detected" in captured

    def test_no_warning_under_threshold(self, tmp_path, capsys):
        out = tmp_path / "out.parquet"
        spark = _make_mock_spark(rows=100)
        dump_table(spark, "db", "tbl", str(out))

        captured = capsys.readouterr().out
        assert "Large table detected" not in captured

    def test_strips_metrics_attr(self, tmp_path):
        out = tmp_path / "out.parquet"
        spark = _make_mock_spark(rows=3, attrs={"metrics": {"some": "data"}})
        dump_table(spark, "db", "tbl", str(out))

        table = pq.read_table(str(out))
        assert table.num_rows == 3

    def test_success_message(self, tmp_path, capsys):
        out = tmp_path / "out.parquet"
        spark = _make_mock_spark()
        dump_table(spark, "db", "tbl", str(out))

        captured = capsys.readouterr().out
        assert f"Successfully exported to {out}" in captured


# ---------------------------------------------------------------------------
# get_spark
# ---------------------------------------------------------------------------
class TestGetSpark:
    @patch("sparkopy.get_databricks_session")
    def test_databricks_when_profile_provided(self, mock_db):
        mock_db.return_value = "db_session"
        result = get_spark("sc://host:15002", "DEFAULT")
        assert result == "db_session"
        mock_db.assert_called_once_with("DEFAULT")

    @patch("sparkopy.get_spark_session")
    def test_spark_when_no_profile(self, mock_sp):
        mock_sp.return_value = "spark_session"
        result = get_spark("sc://host:15002", None)
        assert result == "spark_session"
        mock_sp.assert_called_once_with("sc://host:15002")

    @patch("sparkopy.get_databricks_session")
    @patch("sparkopy.get_spark_session")
    def test_databricks_takes_precedence(self, mock_sp, mock_db):
        mock_db.return_value = "db_session"
        result = get_spark("sc://host:15002", "DEFAULT")
        assert result == "db_session"
        mock_sp.assert_not_called()


# ---------------------------------------------------------------------------
# get_databricks_session
# ---------------------------------------------------------------------------
class TestGetDatabricksSession:
    @patch("sparkopy.DatabricksSession")
    def test_builder_chain(self, mock_cls):
        mock_builder = MagicMock()
        mock_cls.builder = mock_builder
        mock_builder.profile.return_value = mock_builder
        mock_builder.getOrCreate.return_value = "session"

        result = get_databricks_session("MY_PROFILE")
        assert result == "session"
        mock_builder.profile.assert_called_once_with("MY_PROFILE")
        mock_builder.getOrCreate.assert_called_once()


# ---------------------------------------------------------------------------
# get_spark_session
# ---------------------------------------------------------------------------
class TestGetSparkSession:
    @patch("sparkopy.SparkSession")
    def test_builder_chain(self, mock_cls):
        mock_builder = MagicMock()
        mock_cls.builder = mock_builder
        mock_builder.remote.return_value = mock_builder
        mock_builder.getOrCreate.return_value = "session"

        result = get_spark_session("sc://myhost:15002")
        assert result == "session"
        mock_builder.remote.assert_called_once_with("sc://myhost:15002")
        mock_builder.getOrCreate.assert_called_once()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
class TestCli:
    @patch("sparkopy.get_spark")
    @patch("sparkopy.dump_table")
    def test_spark_uri_invocation(self, mock_dump, mock_get_spark):
        mock_get_spark.return_value = "spark"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--spark-uri",
                "sc://host:15002",
                "--database",
                "db",
                "--table",
                "tbl",
                "--output",
                "/tmp/o.parquet",
            ],
        )
        assert result.exit_code == 0
        mock_get_spark.assert_called_once_with("sc://host:15002", None)
        mock_dump.assert_called_once_with("spark", "db", "tbl", "/tmp/o.parquet")

    @patch("sparkopy.get_spark")
    @patch("sparkopy.dump_table")
    def test_databricks_profile_invocation(self, mock_dump, mock_get_spark):
        mock_get_spark.return_value = "spark"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--databricks-profile",
                "DEFAULT",
                "--database",
                "db",
                "--table",
                "tbl",
                "--output",
                "/tmp/o.parquet",
            ],
        )
        assert result.exit_code == 0
        mock_get_spark.assert_called_once_with(None, "DEFAULT")

    def test_missing_database(self):
        runner = CliRunner()
        result = runner.invoke(
            cli, ["--spark-uri", "sc://host:15002", "--table", "tbl", "--output", "/tmp/o.parquet"]
        )
        assert result.exit_code != 0

    def test_missing_table(self):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["--spark-uri", "sc://host:15002", "--database", "db", "--output", "/tmp/o.parquet"],
        )
        assert result.exit_code != 0

    def test_missing_output(self):
        runner = CliRunner()
        result = runner.invoke(
            cli, ["--spark-uri", "sc://host:15002", "--database", "db", "--table", "tbl"]
        )
        assert result.exit_code != 0

    def test_help_output(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "--spark-uri" in result.output
        assert "--databricks-profile" in result.output
        assert "--database" in result.output
