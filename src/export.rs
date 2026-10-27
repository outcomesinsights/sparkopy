//! Reads one Spark Connect result and streams it into one Parquet file.

use std::fs::File;
use std::io::{Cursor, Write};
use std::net::Ipv6Addr;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use arrow_ipc::reader::StreamReader;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

use crate::Options;
use crate::pb;
use crate::pb::execute_plan_response::{ArrowBatch, ResponseType};
use crate::pb::spark_connect_service_client::SparkConnectServiceClient;

/// The Spark Connect port when the URI names none.
const DEFAULT_PORT: u16 = 15002;
/// The largest gRPC message that sparkopy accepts. Spark sends Arrow batches of
/// about 4 MiB, and the gRPC default limit is exactly 4 MiB, so the limit must be
/// higher. The PySpark client uses the same value.
const MAX_MESSAGE_BYTES: usize = 128 * 1024 * 1024;
/// The number of rows in each Parquet row group. A row group is held in memory,
/// encoded and compressed, until it is complete.
const MAX_ROW_GROUP_ROWS: usize = 1024 * 1024;
/// The number of rows between two progress messages.
const PROGRESS_EVERY: i64 = 1_000_000;

/// The Spark Connect client type.
pub(crate) type Client = SparkConnectServiceClient<Channel>;

/// Converts a Spark Connect URI (`sc://HOST[:PORT]`) to a gRPC target
/// (`HOST:PORT`). An IPv6 host is written in brackets: `sc://[::1]:15002`.
/// URI parameters (`sc://HOST/;token=...`), paths, queries and user names are
/// rejected.
pub(crate) fn spark_target(uri: &str) -> Result<String> {
    let invalid = |why: &str| anyhow!("invalid --spark-uri {uri:?}: {why}");
    let rest = uri
        .strip_prefix("sc://")
        .ok_or_else(|| invalid("the scheme must be sc://"))?;
    let authority = rest.strip_suffix('/').unwrap_or(rest);
    if authority.contains(['/', '?', '#', '@', ';']) {
        return Err(invalid("only sc://HOST[:PORT] is supported"));
    }
    let (host, port) = if let Some(bracketed) = authority.strip_prefix('[') {
        let (addr, after) = bracketed
            .split_once(']')
            .ok_or_else(|| invalid("an IPv6 host has no closing ]"))?;
        addr.parse::<Ipv6Addr>()
            .map_err(|_| invalid("the host in brackets is not an IPv6 address"))?;
        let port = match after {
            "" => None,
            p => Some(
                p.strip_prefix(':')
                    .ok_or_else(|| invalid("only sc://HOST[:PORT] is supported"))?,
            ),
        };
        (&authority[..addr.len() + 2], port)
    } else {
        match authority.split_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (authority, None),
        }
    };
    if host.is_empty() {
        return Err(invalid("no host"));
    }
    let port = match port {
        None => DEFAULT_PORT,
        Some(p) => p
            .parse::<u16>()
            .map_err(|_| invalid("the port must be a number from 0 to 65535"))?,
    };
    Ok(format!("{host}:{port}"))
}

/// Creates a gRPC client for a Spark Connect server at `target` (`HOST:PORT`).
/// It does not connect: the first call connects.
pub(crate) fn connect(target: &str) -> Result<Client> {
    let channel = Endpoint::from_shared(format!("http://{target}"))
        .with_context(|| format!("connect to {target}"))?
        .connect_lazy();
    Ok(SparkConnectServiceClient::new(channel).max_decoding_message_size(MAX_MESSAGE_BYTES))
}

/// The relation that reads `database.table`.
pub(crate) fn named_table(database: &str, table: &str) -> pb::Relation {
    use pb::{read::ReadType, relation::RelType};
    let read = pb::Read {
        read_type: Some(ReadType::NamedTable(pb::read::NamedTable {
            unparsed_identifier: format!("{database}.{table}"),
            ..Default::default()
        })),
        ..Default::default()
    };
    pb::Relation {
        rel_type: Some(RelType::Read(read)),
        ..Default::default()
    }
}

/// An ExecutePlan request for `root` in a new session. The execution is not
/// reattachable: sparkopy reads the stream once.
pub(crate) fn plan_request(root: pb::Relation) -> pb::ExecutePlanRequest {
    pb::ExecutePlanRequest {
        session_id: uuid::Uuid::new_v4().to_string(),
        user_context: Some(user_context()),
        client_type: Some("sparkopy".into()),
        plan: Some(pb::Plan {
            op_type: Some(pb::plan::OpType::Root(root)),
        }),
        ..Default::default()
    }
}

/// Names the local user to Spark.
pub(crate) fn user_context() -> pb::UserContext {
    let user_id = std::env::var("USER")
        .ok()
        .filter(|u| !u.is_empty())
        .unwrap_or_else(|| "sparkopy".into());
    pb::UserContext {
        user_id,
        ..Default::default()
    }
}

/// Turns a gRPC status into an error that carries Spark's message. When the
/// server cannot be reached, the message also names the cause, such as
/// "Connection refused", from the status's source chain.
fn spark_error(status: Status, target: &str) -> anyhow::Error {
    if status.code() != Code::Unavailable {
        return anyhow!(
            "Spark Connect error ({:?}): {}",
            status.code(),
            status.message()
        );
    }
    let mut parts = vec![status.message().to_string()];
    let mut source = std::error::Error::source(&status);
    while let Some(e) = source {
        let message = e.to_string();
        if !parts.iter().any(|p| p.contains(&message)) {
            parts.push(message);
        }
        source = e.source();
    }
    anyhow!(
        "cannot reach the Spark Connect server at {target}: {}",
        parts.join(": ")
    )
}

/// Reads `o.database`.`o.table` and writes it to `o.output`.
pub(crate) async fn export(o: &Options, stderr: &mut dyn Write) -> Result<()> {
    let target = spark_target(&o.spark_uri)?;
    let client = connect(&target)?;
    let _ = writeln!(
        stderr,
        "sparkopy: reading {}.{} from {}",
        o.database, o.table, o.spark_uri
    );
    let request = plan_request(named_table(&o.database, &o.table));
    export_plan(client, &target, request, &o.output, stderr).await
}

/// Removes a file when it is dropped: at an error, and when the export is
/// interrupted. After the rename, nothing is left at the path to remove.
struct RemoveOnDrop(PathBuf);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Executes `request` and writes its result to `output`. It writes to
/// `output.tmp` and renames that file only after success, so a failed run
/// leaves an existing `output` unchanged and no temporary file.
pub(crate) async fn export_plan(
    mut client: Client,
    target: &str,
    request: pb::ExecutePlanRequest,
    output: &str,
    stderr: &mut dyn Write,
) -> Result<()> {
    let tmp = format!("{output}.tmp");
    let file = File::create(&tmp).with_context(|| format!("create {tmp}"))?;
    let _remove_tmp = RemoveOnDrop(PathBuf::from(&tmp));
    let mut sink = Sink::new(file);
    let start = Instant::now();

    let mut stream = client
        .execute_plan(request)
        .await
        .map_err(|s| spark_error(s, target))?
        .into_inner();
    let mut batches = 0u64;
    let mut next_progress = PROGRESS_EVERY;
    // Stop at ResultComplete or at the end of the stream. Spark 3.5 ends a
    // stream that is not reattachable without ResultComplete.
    while let Some(response) = stream.message().await.map_err(|s| spark_error(s, target))? {
        match response.response_type {
            Some(ResponseType::ResultComplete(_)) => break,
            Some(ResponseType::ArrowBatch(batch)) => {
                batches += 1;
                sink.write_batch(&batch)
                    .with_context(|| format!("Arrow batch {batches}"))?;
                while sink.rows >= next_progress {
                    let _ = writeln!(stderr, "sparkopy: {} rows written", sink.rows);
                    next_progress += PROGRESS_EVERY;
                }
            }
            _ => {}
        }
    }
    if batches == 0 {
        // Spark 3.5 sends one empty Arrow batch that carries the schema when a
        // result has no rows. A stream with no batch at all gives no Arrow
        // schema, and sparkopy does not guess one.
        bail!("Spark sent no Arrow batch, so the schema of the result is unknown");
    }
    let rows = sink.rows;
    sink.close()
        .with_context(|| format!("finish Parquet file {tmp}"))?;
    std::fs::rename(&tmp, output).with_context(|| format!("rename {tmp} to {output}"))?;
    let _ = writeln!(
        stderr,
        "sparkopy: wrote {rows} rows to {output} in {:.3?}",
        start.elapsed()
    );
    Ok(())
}

/// Writes Arrow record batches to one Parquet file. The Parquet writer is
/// created from the schema of the first Arrow batch, and Spark's Arrow schema is
/// kept exactly: no type is converted.
struct Sink {
    file: Option<File>,
    writer: Option<ArrowWriter<File>>,
    rows: i64,
}

impl Sink {
    fn new(file: File) -> Self {
        Self {
            file: Some(file),
            writer: None,
            rows: 0,
        }
    }

    /// Decodes one Arrow IPC stream from Spark and writes its record batches.
    fn write_batch(&mut self, batch: &ArrowBatch) -> Result<()> {
        let reader = StreamReader::try_new(Cursor::new(&batch.data), None)
            .context("decode Arrow IPC data")?;
        let writer = match &mut self.writer {
            Some(writer) => writer,
            None => {
                let props = WriterProperties::builder()
                    .set_compression(Compression::SNAPPY)
                    .set_max_row_group_row_count(Some(MAX_ROW_GROUP_ROWS))
                    .build();
                let file = self
                    .file
                    .take()
                    .expect("the file is open until the writer exists");
                let writer = ArrowWriter::try_new(file, reader.schema(), Some(props))
                    .with_context(|| {
                        format!("create Parquet writer for schema {:?}", reader.schema())
                    })?;
                self.writer.insert(writer)
            }
        };
        let mut rows = 0i64;
        for record_batch in reader {
            let record_batch = record_batch.context("decode Arrow IPC data")?;
            writer.write(&record_batch).context("write Parquet")?;
            rows +=
                i64::try_from(record_batch.num_rows()).expect("a batch has fewer than 2^63 rows");
        }
        if rows != batch.row_count {
            bail!(
                "Spark said the batch has {} rows, but it decoded to {rows} rows",
                batch.row_count
            );
        }
        self.rows += rows;
        Ok(())
    }

    /// Finishes the Parquet file.
    fn close(mut self) -> Result<()> {
        let writer = self
            .writer
            .take()
            .ok_or_else(|| anyhow!("no Parquet writer was created"))?;
        writer.close()?;
        Ok(())
    }
}
