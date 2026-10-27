//! Tests of the whole command against an in-process fake Spark Connect server.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow_array::{
    Array, Date32Array, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::codegen::BoxStream;
use tonic::{Code, Request, Response, Status};

use crate::export::spark_target;
use crate::pb;
use crate::pb::spark_connect_service_server::{SparkConnectService, SparkConnectServiceServer};
use crate::{EXIT_ERROR, EXIT_OK, EXIT_USAGE};

/// An in-process Spark Connect server. ExecutePlan sends the prepared
/// responses in order, then ends the stream with `error`, or cleanly without one.
#[derive(Clone, Default)]
struct FakeSpark {
    responses: Vec<pb::ExecutePlanResponse>,
    error: Option<(Code, String)>,
    requests: Arc<Mutex<Vec<pb::ExecutePlanRequest>>>,
}

type ResponseStream = BoxStream<pb::ExecutePlanResponse>;

#[tonic::async_trait]
impl SparkConnectService for FakeSpark {
    async fn execute_plan(
        &self,
        request: Request<pb::ExecutePlanRequest>,
    ) -> Result<Response<ResponseStream>, Status> {
        self.requests.lock().unwrap().push(request.into_inner());
        let mut items: Vec<Result<pb::ExecutePlanResponse, Status>> =
            self.responses.iter().cloned().map(Ok).collect();
        if let Some((code, message)) = &self.error {
            items.push(Err(Status::new(*code, message.clone())));
        }
        Ok(Response::new(Box::pin(tokio_stream::iter(items))))
    }
}

/// Starts `fake` on a free local port and returns its sc:// URI.
async fn start_fake(fake: FakeSpark) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(SparkConnectServiceServer::new(fake))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    format!("sc://{addr}")
}

/// Encodes record batches as one Arrow IPC stream, as Spark does for one batch.
fn ipc_bytes(schema: &Schema, batches: &[RecordBatch]) -> Vec<u8> {
    let mut writer = StreamWriter::try_new(Vec::new(), schema).unwrap();
    for b in batches {
        writer.write(b).unwrap();
    }
    writer.into_inner().unwrap()
}

/// Wraps record batches in one ExecutePlanResponse with an ArrowBatch.
fn batch_response(schema: &Schema, batches: &[RecordBatch]) -> pb::ExecutePlanResponse {
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    pb::ExecutePlanResponse {
        response_type: Some(pb::execute_plan_response::ResponseType::ArrowBatch(
            pb::execute_plan_response::ArrowBatch {
                row_count: rows as i64,
                data: ipc_bytes(schema, batches),
            },
        )),
        ..Default::default()
    }
}

fn result_complete() -> pb::ExecutePlanResponse {
    pb::ExecutePlanResponse {
        response_type: Some(pb::execute_plan_response::ResponseType::ResultComplete(
            pb::execute_plan_response::ResultComplete {},
        )),
        ..Default::default()
    }
}

/// A run of the command: its exit status, its stderr and its output path.
struct Outcome {
    code: u8,
    stderr: String,
    out: PathBuf,
    _dir: TempDir,
}

/// A directory that is removed when it is dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("sparkopy-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Runs sparkopy with `args` and returns the exit status and stderr.
async fn run_args(args: &[&str]) -> (u8, String) {
    let mut stderr = Vec::new();
    let mut argv = vec!["sparkopy"];
    argv.extend_from_slice(args);
    let code = crate::run(argv, &mut stderr, std::future::pending()).await;
    let stderr = String::from_utf8(stderr).unwrap();
    eprintln!("exit {code}, stderr:\n{stderr}");
    (code, stderr)
}

/// Runs sparkopy against `uri`, writing to out.parquet in a new temporary directory.
async fn run_command(uri: &str) -> Outcome {
    run_command_in(uri, TempDir::new()).await
}

async fn run_command_in(uri: &str, dir: TempDir) -> Outcome {
    let out = dir.0.join("out.parquet");
    let (code, stderr) = run_args(&[
        "--spark-uri",
        uri,
        "--database",
        "db",
        "--table",
        "tbl",
        "--output",
        out.to_str().unwrap(),
    ])
    .await;
    Outcome {
        code,
        stderr,
        out,
        _dir: dir,
    }
}

/// Reads the whole Parquet file at `path`: its Arrow schema and its record batches.
fn read_parquet(path: &Path) -> (SchemaRef, Vec<RecordBatch>) {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let schema = builder.schema().clone();
    let batches = builder.build().unwrap().map(Result::unwrap).collect();
    (schema, batches)
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// Checks that `got` has the names, types and nullability of `want`. It
/// ignores field metadata, which the Parquet reader adds.
fn assert_same_fields(got: &Schema, want: &Schema) {
    assert_eq!(
        got.fields().len(),
        want.fields().len(),
        "schema has {} fields, want {}:\n got {got:?}\nwant {want:?}",
        got.fields().len(),
        want.fields().len()
    );
    for (g, w) in got.fields().iter().zip(want.fields()) {
        assert!(
            g.name() == w.name()
                && g.data_type() == w.data_type()
                && g.is_nullable() == w.is_nullable(),
            "field is {g:?}, want {w:?}"
        );
    }
}

/// Checks that a failed run left neither the file nor its temporary file.
fn assert_no_output(out: &Path) {
    for p in [out.to_path_buf(), tmp_path(out)] {
        assert!(!p.exists(), "{} exists after a failed run", p.display());
    }
}

fn tmp_path(out: &Path) -> PathBuf {
    PathBuf::from(format!("{}.tmp", out.display()))
}

/// The column types that the Python sparkopy changed.
fn fidelity_schema() -> Schema {
    Schema::new(vec![
        Field::new("null_text", DataType::Utf8, true),
        Field::new("null_date", DataType::Date32, true),
        Field::new("int_with_nulls", DataType::Int32, true),
        Field::new("amount", DataType::Decimal128(24, 4), true),
    ])
}

/// 12345678901234567890.1234 at scale 4.
const BIG_UNSCALED: i128 = 123_456_789_012_345_678_901_234;

fn fidelity_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(fidelity_schema()),
        vec![
            Arc::new(StringArray::from(vec![None::<&str>; 3])),
            Arc::new(Date32Array::from(vec![None; 3])),
            Arc::new(Int32Array::from(vec![Some(7), None, Some(-3)])),
            Arc::new(
                Decimal128Array::from(vec![Some(BIG_UNSCALED), Some(15000), None])
                    .with_precision_and_scale(24, 4)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}

fn int_schema() -> Schema {
    Schema::new(vec![Field::new("n", DataType::Int64, false)])
}

fn int_batch(from: i64, to: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(int_schema()),
        vec![Arc::new(Int64Array::from_iter_values(from..to))],
    )
    .unwrap()
}

#[tokio::test]
async fn type_fidelity() {
    let schema = fidelity_schema();
    let fake = FakeSpark {
        responses: vec![
            batch_response(&schema, &[fidelity_batch()]),
            result_complete(),
        ],
        ..Default::default()
    };
    let run = run_command(&start_fake(fake).await).await;
    assert_eq!(run.code, EXIT_OK);
    let (got, batches) = read_parquet(&run.out);
    assert_same_fields(&got, &schema);
    assert_eq!(total_rows(&batches), 3);
    let b = &batches[0];

    let ints = b.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
    assert!(
        ints.value(0) == 7 && ints.is_null(1) && ints.value(2) == -3,
        "int_with_nulls = {ints:?}, want [7, null, -3]"
    );
    let decs = b
        .column(3)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(decs.value_as_string(0), "12345678901234567890.1234");
    assert_eq!(decs.value(1), 15000, "1.5000 at scale 4");
    assert!(
        decs.is_null(2),
        "amount[2] = {}, want null",
        decs.value_as_string(2)
    );
    for c in 0..2 {
        assert_eq!(
            b.column(c).null_count(),
            3,
            "column {} has {} nulls, want 3",
            got.field(c).name(),
            b.column(c).null_count()
        );
    }
}

#[tokio::test]
async fn request_reads_named_table() {
    let fake = FakeSpark {
        responses: vec![batch_response(&fidelity_schema(), &[fidelity_batch()])],
        ..Default::default()
    };
    let requests = fake.requests.clone();
    let run = run_command(&start_fake(fake).await).await;
    assert_eq!(run.code, EXIT_OK);
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 1, "ExecutePlan requests");
    let request = &requests[0];
    let identifier = match &request.plan.as_ref().unwrap().op_type {
        Some(pb::plan::OpType::Root(pb::Relation {
            rel_type: Some(pb::relation::RelType::Read(read)),
            ..
        })) => match &read.read_type {
            Some(pb::read::ReadType::NamedTable(t)) => t.unparsed_identifier.clone(),
            other => panic!("read type is {other:?}, want a named table"),
        },
        other => panic!("plan is {other:?}, want a root Read relation"),
    };
    assert_eq!(identifier, "db.tbl");
    assert!(!request.session_id.is_empty(), "session_id is empty");
    assert!(
        request.request_options.is_empty(),
        "request options = {:?}, want none (no reattachable execution)",
        request.request_options
    );
}

#[tokio::test]
async fn several_batches_give_one_file_in_order() {
    // Three ArrowBatch messages, and one message that holds two record batches.
    let schema = int_schema();
    let fake = FakeSpark {
        responses: vec![
            batch_response(&schema, &[int_batch(0, 1000)]),
            batch_response(&schema, &[int_batch(1000, 2500)]),
            batch_response(&schema, &[int_batch(2500, 2600), int_batch(2600, 4000)]),
            result_complete(),
        ],
        ..Default::default()
    };
    let run = run_command(&start_fake(fake).await).await;
    assert_eq!(run.code, EXIT_OK);
    let (_, batches) = read_parquet(&run.out);
    assert_eq!(total_rows(&batches), 4000);
    let mut want = 0i64;
    for b in &batches {
        for v in b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
        {
            assert_eq!(*v, want, "row {want} holds {v}: rows are out of order");
            want += 1;
        }
    }
}

#[tokio::test]
async fn stream_end_without_result_complete() {
    // Spark 3.5 ends a stream that is not reattachable without ResultComplete.
    let fake = FakeSpark {
        responses: vec![batch_response(&int_schema(), &[int_batch(0, 10)])],
        ..Default::default()
    };
    let run = run_command(&start_fake(fake).await).await;
    assert_eq!(run.code, EXIT_OK);
    assert_eq!(total_rows(&read_parquet(&run.out).1), 10);
}

#[tokio::test]
async fn empty_result_gives_zero_row_file_with_schema() {
    // For a result with no rows, Spark 3.5.1 sends one Arrow batch with 0 rows
    // that carries the schema (SparkConnectPlanExecution, createEmptyArrowBatch).
    // Test it with a 0-row record batch and with an IPC stream that holds only
    // the schema.
    let schema = fidelity_schema();
    let empty = RecordBatch::new_empty(Arc::new(schema.clone()));
    for (name, response) in [
        ("zero-row batch", batch_response(&schema, &[empty])),
        ("schema only", batch_response(&schema, &[])),
    ] {
        eprintln!("case: {name}");
        let fake = FakeSpark {
            responses: vec![response, result_complete()],
            ..Default::default()
        };
        let run = run_command(&start_fake(fake).await).await;
        assert_eq!(run.code, EXIT_OK, "{name}");
        let (got, batches) = read_parquet(&run.out);
        assert_eq!(total_rows(&batches), 0, "{name}");
        assert_same_fields(&got, &schema);
    }
}

#[tokio::test]
async fn no_arrow_batch_is_an_error() {
    // Without an Arrow batch there is no Arrow schema, and sparkopy does not
    // guess one from Spark's own type names.
    let fake = FakeSpark {
        responses: vec![result_complete()],
        ..Default::default()
    };
    let run = run_command(&start_fake(fake).await).await;
    assert_eq!(run.code, EXIT_ERROR);
    assert!(
        run.stderr.contains("no Arrow batch"),
        "stderr: {}",
        run.stderr
    );
    assert_no_output(&run.out);
}

fn failing_fake() -> FakeSpark {
    let schema = int_schema();
    FakeSpark {
        responses: vec![
            batch_response(&schema, &[int_batch(0, 100)]),
            batch_response(&schema, &[int_batch(100, 200)]),
        ],
        error: Some((
            Code::Internal,
            "[TABLE_OR_VIEW_NOT_FOUND] boom from Spark".into(),
        )),
        ..Default::default()
    }
}

#[tokio::test]
async fn server_error_mid_stream() {
    let run = run_command(&start_fake(failing_fake()).await).await;
    assert_eq!(run.code, EXIT_ERROR);
    assert!(
        run.stderr.contains("boom from Spark"),
        "stderr lacks the Spark error text: {}",
        run.stderr
    );
    assert_no_output(&run.out);
}

#[tokio::test]
async fn failed_run_leaves_existing_output_unchanged() {
    let dir = TempDir::new();
    let out = dir.0.join("out.parquet");
    std::fs::write(&out, "the previous export").unwrap();
    let run = run_command_in(&start_fake(failing_fake()).await, dir).await;
    assert_eq!(run.code, EXIT_ERROR);
    assert_eq!(
        std::fs::read_to_string(&run.out).unwrap(),
        "the previous export"
    );
    assert!(!tmp_path(&run.out).exists(), "the .tmp file is left");
}

#[tokio::test]
async fn interrupted_run_removes_the_tmp_file() {
    let dir = TempDir::new();
    let out = dir.0.join("out.parquet");
    // A server that accepts the connection but never answers.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("sc://{}", listener.local_addr().unwrap());
    let mut stderr = Vec::new();
    let interrupt = async {
        while !tmp_path(&out).exists() {
            tokio::task::yield_now().await;
        }
    };
    let args = [
        "sparkopy",
        "--spark-uri",
        &uri,
        "--database",
        "db",
        "--table",
        "tbl",
        "--output",
        out.to_str().unwrap(),
    ];
    let code = crate::run(args, &mut stderr, interrupt).await;
    let stderr = String::from_utf8(stderr).unwrap();
    assert_eq!(code, EXIT_ERROR, "stderr: {stderr}");
    assert!(stderr.contains("interrupted"), "stderr: {stderr}");
    assert_no_output(&out);
    drop(listener);
}

#[tokio::test]
async fn unreachable_server() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener); // Nothing listens on addr now.
    let run = run_command(&format!("sc://{addr}")).await;
    assert_eq!(run.code, EXIT_ERROR);
    assert!(
        run.stderr.contains("cannot reach the Spark Connect server"),
        "stderr does not explain the error: {}",
        run.stderr
    );
    assert!(
        run.stderr.to_lowercase().contains("connection refused"),
        "stderr does not name the cause: {}",
        run.stderr
    );
    assert_no_output(&run.out);
}

#[tokio::test]
async fn missing_required_option() {
    let full = [
        ("--spark-uri", "sc://localhost:15002"),
        ("--database", "db"),
        ("--table", "tbl"),
        ("--output", "out.parquet"),
    ];
    for (missing, _) in full {
        for empty in [false, true] {
            let mut args = Vec::new();
            for (k, v) in full {
                if k != missing {
                    args.extend([k, v]);
                } else if empty {
                    // An empty value counts as missing, as the Go version did.
                    args.extend([k, ""]);
                }
            }
            let (code, stderr) = run_args(&args).await;
            assert_eq!(code, EXIT_USAGE, "{missing} (empty: {empty})");
            assert!(
                stderr.contains(missing),
                "stderr does not name {missing}: {stderr}"
            );
            // clap prints the usage line for a missing option, not for an empty value.
            assert!(
                empty || stderr.contains("Usage: sparkopy --spark-uri"),
                "stderr has no usage line: {stderr}"
            );
        }
    }
}

#[tokio::test]
async fn unexpected_argument_is_a_usage_error() {
    let (code, _) = run_args(&[
        "--spark-uri",
        "sc://localhost",
        "--database",
        "db",
        "--table",
        "tbl",
        "--output",
        "out.parquet",
        "extra",
    ])
    .await;
    assert_eq!(code, EXIT_USAGE);
}

#[test]
fn spark_target_parses_uris() {
    for (uri, want) in [
        ("sc://titan.jsaw.io:15002", "titan.jsaw.io:15002"),
        ("sc://titan.jsaw.io", "titan.jsaw.io:15002"),
        ("sc://titan.jsaw.io:15002/", "titan.jsaw.io:15002"),
        ("sc://10.0.0.1:9999", "10.0.0.1:9999"),
        ("sc://[::1]:9999", "[::1]:9999"),
        ("sc://[::1]", "[::1]:15002"),
        ("sc://[2001:db8::7]:15002/", "[2001:db8::7]:15002"),
    ] {
        match spark_target(uri) {
            Ok(got) => assert_eq!(got, want, "spark_target({uri:?})"),
            Err(e) => panic!("spark_target({uri:?}) failed: {e}"),
        }
    }
    for uri in [
        "http://host:15002",
        "sc://",
        "sc:///",
        "host:15002",
        "sc://host:15002/;token=x",
        "sc://host:15002/path",
        "sc://host:15002?x=1",
        "sc://user@host:15002",
        "sc://host:",
        "sc://host:port",
        "sc://host:99999",
        "sc://:15002",
        "sc://::1:15002",
        "sc://[::1",
        "sc://[::1]x",
        "sc://[not-ipv6]:15002",
    ] {
        if let Ok(got) = spark_target(uri) {
            panic!("spark_target({uri:?}) = {got:?}, want an error");
        }
    }
}
