//! The parity check runs against a real Spark Connect server. It runs only when
//! SPARKOPY_TEST_SPARK_URI is set (see `just parity`), so CI skips it. It only
//! reads: SHOW TABLES, SELECT count(*), the schema of each table, and the export
//! itself.

use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use std::time::Instant;

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Schema, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::EXIT_OK;
use crate::export::{
    Client, connect, export_plan, named_table, plan_request, spark_target, user_context,
};
use crate::pb;
use crate::pb::data_type::Kind;

/// Runs a small query and returns its record batches. It holds the whole
/// result in memory, so use it only for results of a few rows.
async fn spark_sql(client: &mut Client, query: &str) -> Vec<RecordBatch> {
    let root = pb::Relation {
        rel_type: Some(pb::relation::RelType::Sql(pb::Sql {
            query: query.into(),
            ..Default::default()
        })),
        ..Default::default()
    };
    let mut stream = client
        .execute_plan(plan_request(root))
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"))
        .into_inner();
    let mut batches = Vec::new();
    while let Some(response) = stream
        .message()
        .await
        .unwrap_or_else(|e| panic!("{query}: {e}"))
    {
        if let Some(pb::execute_plan_response::ResponseType::ArrowBatch(b)) = response.response_type
        {
            let reader = StreamReader::try_new(Cursor::new(b.data), None).unwrap();
            batches.extend(reader.map(Result::unwrap));
        }
    }
    batches
}

/// The tables (not the temporary views) in `database`.
async fn spark_tables(client: &mut Client, database: &str) -> Vec<String> {
    let mut tables = Vec::new();
    for b in spark_sql(client, &format!("SHOW TABLES IN `{database}`")).await {
        let column = |name: &str| {
            b.column_by_name(name).unwrap_or_else(|| {
                panic!(
                    "SHOW TABLES returned an unexpected schema: {:?}",
                    b.schema()
                )
            })
        };
        let names = column("tableName")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let temporary = column("isTemporary")
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for r in 0..b.num_rows() {
            if !temporary.value(r) {
                tables.push(names.value(r).to_string());
            }
        }
    }
    tables
}

/// Spark's own row count of `database.table`.
async fn spark_count(client: &mut Client, database: &str, table: &str) -> i64 {
    let batches = spark_sql(
        client,
        &format!("SELECT count(*) FROM `{database}`.`{table}`"),
    )
    .await;
    assert!(
        batches.len() == 1 && batches[0].num_rows() == 1,
        "count(*) of {database}.{table} did not return one row"
    );
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

/// Spark's own schema of `database.table`, from AnalyzePlan.
async fn spark_schema(
    client: &mut Client,
    database: &str,
    table: &str,
) -> Vec<pb::data_type::StructField> {
    use pb::{analyze_plan_request as req, analyze_plan_response as resp};
    let request = pb::AnalyzePlanRequest {
        session_id: uuid::Uuid::new_v4().to_string(),
        user_context: Some(user_context()),
        analyze: Some(req::Analyze::Schema(req::Schema {
            plan: Some(pb::Plan {
                op_type: Some(pb::plan::OpType::Root(named_table(database, table))),
            }),
        })),
        ..Default::default()
    };
    let response = client
        .analyze_plan(request)
        .await
        .unwrap_or_else(|e| panic!("AnalyzePlan(Schema) of {database}.{table}: {e}"))
        .into_inner();
    match response.result {
        Some(resp::Result::Schema(resp::Schema {
            schema: Some(pb::DataType {
                kind: Some(Kind::Struct(s)),
            }),
        })) => s.fields,
        other => {
            panic!("AnalyzePlan(Schema) of {database}.{table} returned no struct type: {other:?}")
        }
    }
}

/// Whether `got` is the Arrow type that Spark 3.5 uses for the Spark type
/// `want` (Spark's ArrowUtils.toArrowType), and a description of that type.
/// This is a check, not a conversion: sparkopy never maps types.
fn arrow_type_ok(want: &pb::DataType, got: &DataType) -> (bool, String) {
    let expected = match want.kind.as_ref() {
        Some(Kind::Boolean(_)) => DataType::Boolean,
        Some(Kind::Byte(_)) => DataType::Int8,
        Some(Kind::Short(_)) => DataType::Int16,
        Some(Kind::Integer(_)) => DataType::Int32,
        Some(Kind::Long(_)) => DataType::Int64,
        Some(Kind::Float(_)) => DataType::Float32,
        Some(Kind::Double(_)) => DataType::Float64,
        Some(Kind::String(_) | Kind::Char(_) | Kind::VarChar(_)) => DataType::Utf8,
        Some(Kind::Binary(_)) => DataType::Binary,
        Some(Kind::Date(_)) => DataType::Date32,
        Some(Kind::Null(_)) => DataType::Null,
        Some(Kind::Decimal(d)) => DataType::Decimal128(
            u8::try_from(d.precision.unwrap_or(10)).unwrap(),
            i8::try_from(d.scale.unwrap_or(0)).unwrap(),
        ),
        Some(Kind::TimestampNtz(_)) => DataType::Timestamp(TimeUnit::Microsecond, None),
        Some(Kind::Timestamp(_)) => {
            let ok = matches!(got, DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) if !tz.is_empty());
            return (ok, "timestamp[us, tz=<session time zone>]".into());
        }
        other => {
            return (
                false,
                format!("Spark type {other:?}, which this check does not know"),
            );
        }
    };
    (&expected == got, format!("{expected}"))
}

/// The row count and the Arrow schema of a Parquet file.
fn parquet_file(path: &Path) -> (i64, Schema) {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let rows = builder.metadata().file_metadata().num_rows();
    (rows, builder.schema().as_ref().clone())
}

/// Checks the file's Arrow schema against Spark's schema. It returns one line
/// for each column that differs.
fn compare_schema(spark: &[pb::data_type::StructField], got: &Schema) -> Vec<String> {
    if spark.len() != got.fields().len() {
        return vec![format!(
            "file has {} columns, Spark has {}",
            got.fields().len(),
            spark.len()
        )];
    }
    let mut errors = Vec::new();
    for (i, (sf, f)) in spark.iter().zip(got.fields()).enumerate() {
        let (ok, want) = arrow_type_ok(sf.data_type.as_ref().unwrap(), f.data_type());
        if f.name() != &sf.name || !ok || f.is_nullable() != sf.nullable {
            errors.push(format!(
                "column {i}: file has {} {} (nullable={}), Spark has {} {want} (nullable={})",
                f.name(),
                f.data_type(),
                f.is_nullable(),
                sf.name,
                sf.nullable
            ));
        }
    }
    errors
}

#[tokio::test(flavor = "multi_thread")]
async fn parity() {
    let Ok(uri) = std::env::var("SPARKOPY_TEST_SPARK_URI") else {
        eprintln!("parity: SPARKOPY_TEST_SPARK_URI is not set, so the parity check is skipped");
        return;
    };
    let database =
        std::env::var("SPARKOPY_PARITY_DATABASE").expect("SPARKOPY_PARITY_DATABASE is not set");
    let target = spark_target(&uri).unwrap();
    let mut client = connect(&target).unwrap();

    let tables = spark_tables(&mut client, &database).await;
    assert!(!tables.is_empty(), "database {database} has no tables");
    eprintln!(
        "database {database} has {} tables: {tables:?}",
        tables.len()
    );
    let dir = std::env::temp_dir().join(format!("sparkopy-parity-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let mut failures = Vec::new();

    for table in &tables {
        let out = dir.join(format!("{table}.parquet"));
        let mut stderr = Vec::new();
        let start = Instant::now();
        let args = [
            "sparkopy",
            "--spark-uri",
            &uri,
            "--database",
            &database,
            "--table",
            table,
            "--output",
            out.to_str().unwrap(),
        ];
        let code = crate::run(args, &mut stderr, std::future::pending()).await;
        let elapsed = start.elapsed();
        if code != EXIT_OK {
            failures.push(format!(
                "{table}: sparkopy exit {code}:\n{}",
                String::from_utf8_lossy(&stderr)
            ));
            continue;
        }
        let (rows, schema) = parquet_file(&out);
        let want = spark_count(&mut client, &database, table).await;
        let mut errors = Vec::new();
        if rows != want {
            errors.push(format!("file has {rows} rows, Spark count(*) is {want}"));
        }
        errors.extend(compare_schema(
            &spark_schema(&mut client, &database, table).await,
            &schema,
        ));
        let verdict = if errors.is_empty() { "PASS" } else { "FAIL" };
        eprintln!(
            "{verdict} {table}: {rows} rows (Spark: {want}), {} columns, {elapsed:.3?}",
            schema.fields().len()
        );
        failures.extend(errors.into_iter().map(|e| format!("{table}: {e}")));
        let _ = std::fs::remove_file(&out);
    }

    // A result with no rows, from the real protocol: Limit 0 on a table. It
    // checks that Spark sends an Arrow batch that carries the schema.
    let table = &tables[0];
    let out = dir.join("empty.parquet");
    let root = pb::Relation {
        rel_type: Some(pb::relation::RelType::Limit(Box::new(pb::Limit {
            input: Some(Box::new(named_table(&database, table))),
            limit: 0,
        }))),
        ..Default::default()
    };
    let mut stderr = Vec::new();
    match export_plan(
        client.clone(),
        &target,
        plan_request(root),
        out.to_str().unwrap(),
        &mut stderr,
    )
    .await
    {
        Err(e) => failures.push(format!(
            "Limit 0 on {table}: {e:#}\n{}",
            String::from_utf8_lossy(&stderr)
        )),
        Ok(()) => {
            let (rows, schema) = parquet_file(&out);
            let mut errors =
                compare_schema(&spark_schema(&mut client, &database, table).await, &schema);
            if rows != 0 {
                errors.push(format!("file has {rows} rows, want 0"));
            }
            let verdict = if errors.is_empty() { "PASS" } else { "FAIL" };
            eprintln!(
                "{verdict} Limit 0 on {table}: {rows} rows, {} columns",
                schema.fields().len()
            );
            failures.extend(
                errors
                    .into_iter()
                    .map(|e| format!("Limit 0 on {table}: {e}")),
            );
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        failures.is_empty(),
        "parity failures:\n{}",
        failures.join("\n")
    );
}
