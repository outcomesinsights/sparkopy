from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from sparkopy import (
    LARGE_TABLE_THRESHOLD,
    _unify_schema,
    cli,
    dump_table,
    dump_table_simple,
    dump_table_streaming,
    get_databricks_session,
    get_spark,
    get_spark_session,
)


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


def _make_streaming_mock_df(total_rows, batch_size=1000):
    """Build a mock DataFrame whose _plan/_session produces Arrow RecordBatches
    via the streaming iterator."""
    schema = pa.schema([
        pa.field("a", pa.int64()),
        pa.field("b", pa.string()),
    ])

    batches = []
    remaining = total_rows
    while remaining > 0:
        n = min(batch_size, remaining)
        batch = pa.record_batch(
            [pa.array(range(n)), pa.array([f"val_{i}" for i in range(n)])],
            schema=schema,
        )
        batches.append(batch)
        remaining -= n

    mock_df = MagicMock()
    mock_plan = MagicMock()
    mock_client = MagicMock()

    mock_df._plan = mock_plan
    mock_df._session.client = mock_client
    mock_client._execute_and_fetch_as_iterator.return_value = iter(batches)

    return mock_df


# ---------------------------------------------------------------------------
# dump_table_simple (small tables)
# ---------------------------------------------------------------------------
class TestDumpTableSimple:
    def test_writes_parquet(self, tmp_path):
        out = tmp_path / "out.parquet"
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        mock_df = MagicMock()
        mock_df.toPandas.return_value = pdf
        dump_table_simple(mock_df, str(out))

        table = pq.read_table(str(out))
        assert table.num_rows == 3
        assert set(table.column_names) == {"a", "b"}

    def test_strips_metrics_attr(self, tmp_path):
        out = tmp_path / "out.parquet"
        pdf = pd.DataFrame({"a": [1]})
        pdf.attrs["metrics"] = {"some": "data"}
        mock_df = MagicMock()
        mock_df.toPandas.return_value = pdf
        dump_table_simple(mock_df, str(out))

        table = pq.read_table(str(out))
        assert table.num_rows == 1


# ---------------------------------------------------------------------------
# dump_table_streaming (large tables)
# ---------------------------------------------------------------------------
class TestDumpTableStreaming:
    def test_writes_all_rows(self, tmp_path):
        out = tmp_path / "out.parquet"
        mock_df = _make_streaming_mock_df(2500, batch_size=1000)

        dump_table_streaming(mock_df, str(out), 2500)

        table = pq.read_table(str(out))
        assert table.num_rows == 2500
        assert set(table.column_names) == {"a", "b"}

    def test_raises_on_zero_rows(self, tmp_path):
        import pytest

        out = tmp_path / "out.parquet"
        mock_df = _make_streaming_mock_df(0)

        with pytest.raises(RuntimeError, match="No rows exported"):
            dump_table_streaming(mock_df, str(out), 100)

    def test_progress_output(self, tmp_path, capsys):
        out = tmp_path / "out.parquet"
        mock_df = _make_streaming_mock_df(2000, batch_size=1000)

        dump_table_streaming(mock_df, str(out), 2000)

        captured = capsys.readouterr().err
        assert "1,000" in captured
        assert "2,000" in captured

    def test_single_batch(self, tmp_path):
        out = tmp_path / "out.parquet"
        mock_df = _make_streaming_mock_df(500, batch_size=500)

        dump_table_streaming(mock_df, str(out), 500)

        table = pq.read_table(str(out))
        assert table.num_rows == 500

    def test_handles_schema_variations(self, tmp_path):
        """Batches with different decimal precisions should be unified."""
        from decimal import Decimal

        out = tmp_path / "out.parquet"

        schema1 = pa.schema([pa.field("x", pa.decimal128(9, 3))])
        schema2 = pa.schema([pa.field("x", pa.decimal128(12, 3))])

        batch1 = pa.record_batch([pa.array([Decimal("1.500")], type=pa.decimal128(9, 3))], schema=schema1)
        batch2 = pa.record_batch([pa.array([Decimal("2.500")], type=pa.decimal128(12, 3))], schema=schema2)

        mock_df = MagicMock()
        mock_df._session.client._execute_and_fetch_as_iterator.return_value = iter([batch1, batch2])

        dump_table_streaming(mock_df, str(out), 2)

        table = pq.read_table(str(out))
        assert table.num_rows == 2
        assert table.schema.field("x").type == pa.decimal128(12, 3)


# ---------------------------------------------------------------------------
# _unify_schema
# ---------------------------------------------------------------------------
class TestUnifySchema:
    def test_same_schemas(self):
        s = pa.schema([pa.field("a", pa.int64())])
        assert _unify_schema(s, s) == s

    def test_decimal_precision_widening(self):
        s1 = pa.schema([pa.field("x", pa.decimal128(9, 3))])
        s2 = pa.schema([pa.field("x", pa.decimal128(12, 3))])
        result = _unify_schema(s1, s2)
        assert result.field("x").type == pa.decimal128(12, 3)

    def test_incompatible_types_fallback_to_string(self):
        s1 = pa.schema([pa.field("x", pa.int64())])
        s2 = pa.schema([pa.field("x", pa.string())])
        result = _unify_schema(s1, s2)
        assert result.field("x").type == pa.string()


# ---------------------------------------------------------------------------
# dump_table (routing logic)
# ---------------------------------------------------------------------------
class TestDumpTable:
    def test_small_table_uses_simple_path(self, tmp_path):
        out = tmp_path / "out.parquet"
        spark = _make_mock_spark(rows=5)
        dump_table(spark, "db", "tbl", str(out))

        spark.read.table.return_value.toPandas.assert_called_once()

    @patch("sparkopy.dump_table_streaming")
    def test_large_table_uses_streaming_path(self, mock_streaming, tmp_path):
        out = tmp_path / "out.parquet"
        spark = _make_mock_spark(rows=LARGE_TABLE_THRESHOLD + 1)

        dump_table(spark, "db", "tbl", str(out))

        mock_streaming.assert_called_once()

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

    def test_prints_row_count_to_stderr(self, tmp_path, capsys):
        out = tmp_path / "out.parquet"
        spark = _make_mock_spark(rows=1234)
        dump_table(spark, "db", "tbl", str(out))

        captured = capsys.readouterr().err
        assert "1,234 rows" in captured

    def test_success_message_to_stderr(self, tmp_path, capsys):
        out = tmp_path / "out.parquet"
        spark = _make_mock_spark()
        dump_table(spark, "db", "tbl", str(out))

        captured = capsys.readouterr().err
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
