package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "github.com/outcomesinsights/sparkopy/internal/connectpb"
)

const (
	// defaultPort is the Spark Connect port when the URI names none.
	defaultPort = "15002"
	// maxMessageBytes is the largest gRPC message that sparkopy accepts.
	// Spark sends Arrow batches of about 4 MiB, and the gRPC default limit
	// is exactly 4 MiB, so the limit must be higher. The PySpark client
	// uses the same value.
	maxMessageBytes = 128 * 1024 * 1024
	// maxRowGroupRows is the number of rows in each Parquet row group.
	// A row group is held in memory, compressed, until it is complete.
	maxRowGroupRows = 1024 * 1024
	// progressEvery is the number of rows between two progress messages.
	progressEvery = 1_000_000
)

// sparkTarget converts a Spark Connect URI (sc://HOST[:PORT]) to a gRPC
// target (HOST:PORT).
func sparkTarget(uri string) (string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", fmt.Errorf("invalid --spark-uri %q: %w", uri, err)
	}
	if u.Scheme != "sc" {
		return "", fmt.Errorf("invalid --spark-uri %q: the scheme must be sc://", uri)
	}
	if u.Hostname() == "" {
		return "", fmt.Errorf("invalid --spark-uri %q: no host", uri)
	}
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.User != nil {
		return "", fmt.Errorf("invalid --spark-uri %q: only sc://HOST[:PORT] is supported", uri)
	}
	port := u.Port()
	if port == "" {
		port = defaultPort
	}
	return net.JoinHostPort(u.Hostname(), port), nil
}

// readTableRequest builds the ExecutePlan request that reads DB.TABLE.
// The execution is not reattachable: sparkopy reads the stream once.
func readTableRequest(database, table string) *pb.ExecutePlanRequest {
	return planRequest(namedTable(database, table))
}

// namedTable is the relation that reads DB.TABLE.
func namedTable(database, table string) *pb.Relation {
	return &pb.Relation{RelType: &pb.Relation_Read{Read: &pb.Read{
		ReadType: &pb.Read_NamedTable_{NamedTable: &pb.Read_NamedTable{
			UnparsedIdentifier: database + "." + table,
		}},
	}}}
}

// planRequest builds an ExecutePlan request for root in a new session.
func planRequest(root *pb.Relation) *pb.ExecutePlanRequest {
	return &pb.ExecutePlanRequest{
		SessionId:   uuid.NewString(),
		UserContext: userContext(),
		ClientType:  proto.String("sparkopy"),
		Plan:        &pb.Plan{OpType: &pb.Plan_Root{Root: root}},
	}
}

// userContext names the local user to Spark.
func userContext() *pb.UserContext {
	userID := os.Getenv("USER")
	if userID == "" {
		userID = "sparkopy"
	}
	return &pb.UserContext{UserId: userID}
}

// sparkError turns a gRPC error into an error that carries Spark's message.
func sparkError(err error, target string) error {
	s, ok := status.FromError(err)
	if !ok {
		return err
	}
	if s.Code() == codes.Unavailable {
		return fmt.Errorf("cannot reach the Spark Connect server at %s: %s", target, s.Message())
	}
	return fmt.Errorf("Spark Connect error (%s): %s", s.Code(), s.Message())
}

// dial creates a gRPC client for a Spark Connect server at target (HOST:PORT).
// It does not connect: the first call connects.
func dial(target string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessageBytes)),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", target, err)
	}
	return conn, nil
}

// export reads o.database.o.table and writes it to o.output.
func export(ctx context.Context, o options, stderr io.Writer) error {
	target, err := sparkTarget(o.sparkURI)
	if err != nil {
		return err
	}
	conn, err := dial(target)
	if err != nil {
		return err
	}
	defer conn.Close()
	fmt.Fprintf(stderr, "sparkopy: reading %s.%s from %s\n", o.database, o.table, o.sparkURI)
	return exportPlan(ctx, conn, target, readTableRequest(o.database, o.table), o.output, stderr)
}

// exportPlan executes req and writes its result to output.
// It writes to output + ".tmp" and renames that file only after success,
// so a failed run leaves no partial file at output.
func exportPlan(ctx context.Context, conn *grpc.ClientConn, target string, req *pb.ExecutePlanRequest, output string, stderr io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tmp := output + ".tmp"
	sink, err := newParquetSink(tmp)
	if err != nil {
		return err
	}
	start := time.Now()
	client := pb.NewSparkConnectServiceClient(conn)
	stream, err := client.ExecutePlan(ctx, req)
	if err != nil {
		err = sparkError(err, target)
	} else {
		err = receive(stream, sink, target, stderr)
	}
	if err != nil {
		sink.abort()
		return err
	}
	if err := sink.close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, output); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename %s to %s: %w", tmp, output, err)
	}
	fmt.Fprintf(stderr, "sparkopy: wrote %d rows to %s in %s\n",
		sink.rows, output, time.Since(start).Round(time.Millisecond))
	return nil
}

// receive reads every response of the ExecutePlan stream and writes each
// Arrow batch to sink as it arrives. It stops at ResultComplete or at the
// end of the stream.
func receive(stream grpc.ServerStreamingClient[pb.ExecutePlanResponse], sink *parquetSink, target string, stderr io.Writer) error {
	batches := 0
	nextProgress := int64(progressEvery)
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return sparkError(err, target)
		}
		if resp.GetResultComplete() != nil {
			break
		}
		batch := resp.GetArrowBatch()
		if batch == nil {
			continue
		}
		batches++
		if err := sink.writeBatch(batch); err != nil {
			return fmt.Errorf("Arrow batch %d: %w", batches, err)
		}
		for sink.rows >= nextProgress {
			fmt.Fprintf(stderr, "sparkopy: %d rows written\n", sink.rows)
			nextProgress += progressEvery
		}
	}
	if batches == 0 {
		// Spark 3.5 sends one empty Arrow batch that carries the schema
		// when a result has no rows. A stream with no batch at all gives
		// no Arrow schema, and sparkopy does not guess one.
		return errors.New("Spark sent no Arrow batch, so the schema of the result is unknown")
	}
	return nil
}

// parquetSink writes Arrow record batches to one Parquet file. The Parquet
// writer is created from the schema of the first Arrow batch.
type parquetSink struct {
	path string
	file *os.File
	fw   *pqarrow.FileWriter
	mem  memory.Allocator
	rows int64
}

func newParquetSink(path string) (*parquetSink, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create %s: %w", path, err)
	}
	return &parquetSink{path: path, file: f, mem: memory.DefaultAllocator}, nil
}

func (s *parquetSink) open(schema *arrow.Schema) error {
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithMaxRowGroupLength(maxRowGroupRows),
		parquet.WithAllocator(s.mem),
	)
	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
		pqarrow.WithAllocator(s.mem),
	)
	fw, err := pqarrow.NewFileWriter(schema, s.file, props, arrowProps)
	if err != nil {
		return fmt.Errorf("create Parquet writer for schema %s: %w", schema, err)
	}
	s.fw = fw
	return nil
}

// writeBatch decodes one Arrow IPC stream from Spark and writes its records.
func (s *parquetSink) writeBatch(batch *pb.ExecutePlanResponse_ArrowBatch) error {
	rdr, err := ipc.NewReader(bytes.NewReader(batch.GetData()), ipc.WithAllocator(s.mem))
	if err != nil {
		return fmt.Errorf("decode Arrow IPC data: %w", err)
	}
	defer rdr.Release()
	if s.fw == nil {
		if err := s.open(rdr.Schema()); err != nil {
			return err
		}
	}
	var rows int64
	for rdr.Next() {
		rec := rdr.RecordBatch()
		if err := s.fw.WriteBuffered(rec); err != nil {
			return fmt.Errorf("write Parquet: %w", err)
		}
		rows += rec.NumRows()
	}
	if err := rdr.Err(); err != nil {
		return fmt.Errorf("decode Arrow IPC data: %w", err)
	}
	if rows != batch.GetRowCount() {
		return fmt.Errorf("Spark said the batch has %d rows, but it decoded to %d rows",
			batch.GetRowCount(), rows)
	}
	s.rows += rows
	return nil
}

// close finishes the Parquet file. The Parquet writer also closes the file.
func (s *parquetSink) close() error {
	if s.fw == nil {
		s.file.Close()
		os.Remove(s.path)
		return errors.New("no Parquet writer was created")
	}
	if err := s.fw.Close(); err != nil {
		os.Remove(s.path)
		return fmt.Errorf("finish Parquet file %s: %w", s.path, err)
	}
	return nil
}

// abort closes and deletes the partial file.
func (s *parquetSink) abort() {
	if s.fw != nil {
		s.fw.Close()
	}
	s.file.Close()
	os.Remove(s.path)
}
