package main

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/outcomesinsights/sparkopy/internal/connectpb"
)

// fakeSpark is an in-process Spark Connect server. ExecutePlan sends the
// prepared responses in order, then returns err (nil ends the stream).
type fakeSpark struct {
	pb.UnimplementedSparkConnectServiceServer
	responses []*pb.ExecutePlanResponse
	err       error
	requests  []*pb.ExecutePlanRequest
}

func (f *fakeSpark) ExecutePlan(req *pb.ExecutePlanRequest, stream grpc.ServerStreamingServer[pb.ExecutePlanResponse]) error {
	f.requests = append(f.requests, req)
	for _, r := range f.responses {
		if err := stream.Send(r); err != nil {
			return err
		}
	}
	return f.err
}

// startFake starts fake on a free local port and returns its sc:// URI.
func startFake(t *testing.T, fake *fakeSpark) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterSparkConnectServiceServer(srv, fake)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return "sc://" + lis.Addr().String()
}

// ipcBytes encodes records as one Arrow IPC stream, as Spark does for one batch.
func ipcBytes(t *testing.T, schema *arrow.Schema, recs ...arrow.RecordBatch) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	for _, r := range recs {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// batchResponse wraps records in one ExecutePlanResponse with an ArrowBatch.
func batchResponse(t *testing.T, schema *arrow.Schema, recs ...arrow.RecordBatch) *pb.ExecutePlanResponse {
	t.Helper()
	var rows int64
	for _, r := range recs {
		rows += r.NumRows()
	}
	return &pb.ExecutePlanResponse{ResponseType: &pb.ExecutePlanResponse_ArrowBatch_{
		ArrowBatch: &pb.ExecutePlanResponse_ArrowBatch{RowCount: rows, Data: ipcBytes(t, schema, recs...)},
	}}
}

func resultComplete() *pb.ExecutePlanResponse {
	return &pb.ExecutePlanResponse{ResponseType: &pb.ExecutePlanResponse_ResultComplete_{
		ResultComplete: &pb.ExecutePlanResponse_ResultComplete{},
	}}
}

// runCommand runs sparkopy against uri and returns the exit status, stderr and the output path.
func runCommand(t *testing.T, uri string) (int, string, string) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out.parquet")
	var stderr bytes.Buffer
	code := run(context.Background(), []string{
		"--spark-uri", uri, "--database", "db", "--table", "tbl", "--output", out,
	}, &stderr)
	t.Logf("exit %d, stderr:\n%s", code, stderr.String())
	return code, stderr.String(), out
}

// readParquet reads the whole Parquet file at path.
func readParquet(t *testing.T, path string) arrow.Table {
	t.Helper()
	rdr, err := file.OpenParquetFile(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Close()
	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := fr.ReadTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tbl.Release)
	return tbl
}

// assertSameFields checks that got has the names, types and nullability of
// want. It ignores field metadata, which the Parquet reader adds.
func assertSameFields(t *testing.T, got, want *arrow.Schema) {
	t.Helper()
	if got.NumFields() != want.NumFields() {
		t.Fatalf("schema has %d fields, want %d:\n got %s\nwant %s", got.NumFields(), want.NumFields(), got, want)
	}
	for i, w := range want.Fields() {
		g := got.Field(i)
		if g.Name != w.Name || !arrow.TypeEqual(g.Type, w.Type) || g.Nullable != w.Nullable {
			t.Errorf("field %d is %s, want %s", i, g, w)
		}
	}
}

// assertNoOutput checks that a failed run left neither the file nor its temporary file.
func assertNoOutput(t *testing.T, out string) {
	t.Helper()
	for _, p := range []string{out, out + ".tmp"} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s exists after a failed run (stat error: %v)", p, err)
		}
	}
}

// fidelitySchema has the column types that the Python sparkopy changed.
var fidelitySchema = arrow.NewSchema([]arrow.Field{
	{Name: "null_text", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "null_date", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
	{Name: "int_with_nulls", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "amount", Type: &arrow.Decimal128Type{Precision: 24, Scale: 4}, Nullable: true},
}, nil)

func fidelityRecord(t *testing.T) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(memory.DefaultAllocator, fidelitySchema)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).AppendNulls(3)
	b.Field(1).(*array.Date32Builder).AppendNulls(3)
	b.Field(2).(*array.Int32Builder).AppendValues([]int32{7, 0, -3}, []bool{true, false, true})
	big, err := decimal128.FromString("12345678901234567890.1234", 24, 4)
	if err != nil {
		t.Fatal(err)
	}
	b.Field(3).(*array.Decimal128Builder).AppendValues(
		[]decimal128.Num{big, decimal128.FromI64(15000), decimal128.FromI64(0)},
		[]bool{true, true, false})
	rec := b.NewRecordBatch()
	t.Cleanup(rec.Release)
	return rec
}

func TestTypeFidelity(t *testing.T) {
	rec := fidelityRecord(t)
	fake := &fakeSpark{responses: []*pb.ExecutePlanResponse{batchResponse(t, fidelitySchema, rec), resultComplete()}}
	code, _, out := runCommand(t, startFake(t, fake))
	if code != exitOK {
		t.Fatalf("exit %d, want 0", code)
	}
	tbl := readParquet(t, out)
	assertSameFields(t, tbl.Schema(), fidelitySchema)
	if tbl.NumRows() != 3 {
		t.Fatalf("got %d rows, want 3", tbl.NumRows())
	}
	ints := tbl.Column(2).Data().Chunk(0).(*array.Int32)
	if ints.Value(0) != 7 || !ints.IsNull(1) || ints.Value(2) != -3 {
		t.Errorf("int_with_nulls = %v, want [7 (null) -3]", ints)
	}
	decs := tbl.Column(3).Data().Chunk(0).(*array.Decimal128)
	if got := decs.ValueStr(0); got != "12345678901234567890.1234" {
		t.Errorf("amount[0] = %s, want 12345678901234567890.1234", got)
	}
	if got := decs.Value(1); got != decimal128.FromI64(15000) {
		t.Errorf("amount[1] unscaled = %v, want 15000 (1.5000 at scale 4)", got)
	}
	if !decs.IsNull(2) {
		t.Errorf("amount[2] = %s, want null", decs.ValueStr(2))
	}
	for c := 0; c < 2; c++ {
		if n := tbl.Column(c).NullN(); n != 3 {
			t.Errorf("column %s has %d nulls, want 3", tbl.Schema().Field(c).Name, n)
		}
	}
}

func TestRequestReadsNamedTable(t *testing.T) {
	fake := &fakeSpark{responses: []*pb.ExecutePlanResponse{batchResponse(t, fidelitySchema, fidelityRecord(t))}}
	if code, _, _ := runCommand(t, startFake(t, fake)); code != exitOK {
		t.Fatalf("exit %d, want 0", code)
	}
	if len(fake.requests) != 1 {
		t.Fatalf("got %d ExecutePlan requests, want 1", len(fake.requests))
	}
	req := fake.requests[0]
	got := req.GetPlan().GetRoot().GetRead().GetNamedTable().GetUnparsedIdentifier()
	if got != "db.tbl" {
		t.Errorf("unparsed_identifier = %q, want db.tbl", got)
	}
	if req.GetSessionId() == "" {
		t.Error("session_id is empty")
	}
	if len(req.GetRequestOptions()) != 0 {
		t.Errorf("request options = %v, want none (no reattachable execution)", req.GetRequestOptions())
	}
}

var intSchema = arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64}}, nil)

func intRecord(t *testing.T, from, to int64) arrow.RecordBatch {
	t.Helper()
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	for i := from; i < to; i++ {
		b.Append(i)
	}
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecordBatch(intSchema, []arrow.Array{arr}, to-from)
	t.Cleanup(rec.Release)
	return rec
}

func TestSeveralBatchesGiveOneFileInOrder(t *testing.T) {
	// Three ArrowBatch messages, and one message that holds two record batches.
	fake := &fakeSpark{responses: []*pb.ExecutePlanResponse{
		batchResponse(t, intSchema, intRecord(t, 0, 1000)),
		batchResponse(t, intSchema, intRecord(t, 1000, 2500)),
		batchResponse(t, intSchema, intRecord(t, 2500, 2600), intRecord(t, 2600, 4000)),
		resultComplete(),
	}}
	code, _, out := runCommand(t, startFake(t, fake))
	if code != exitOK {
		t.Fatalf("exit %d, want 0", code)
	}
	tbl := readParquet(t, out)
	if tbl.NumRows() != 4000 {
		t.Fatalf("got %d rows, want 4000", tbl.NumRows())
	}
	want := int64(0)
	for _, chunk := range tbl.Column(0).Data().Chunks() {
		for _, v := range chunk.(*array.Int64).Int64Values() {
			if v != want {
				t.Fatalf("row %d holds %d: rows are out of order", want, v)
			}
			want++
		}
	}
}

func TestStreamEndWithoutResultComplete(t *testing.T) {
	// Spark 3.5 ends a non-reattachable stream without ResultComplete.
	fake := &fakeSpark{responses: []*pb.ExecutePlanResponse{batchResponse(t, intSchema, intRecord(t, 0, 10))}}
	code, _, out := runCommand(t, startFake(t, fake))
	if code != exitOK {
		t.Fatalf("exit %d, want 0", code)
	}
	if n := readParquet(t, out).NumRows(); n != 10 {
		t.Fatalf("got %d rows, want 10", n)
	}
}

func TestEmptyResultGivesZeroRowFileWithSchema(t *testing.T) {
	// For a result with no rows, Spark 3.5.1 sends one Arrow batch with 0
	// rows that carries the schema (SparkConnectPlanExecution,
	// createEmptyArrowBatch). Test it with a 0-row record batch and with an
	// IPC stream that holds only the schema.
	b := array.NewRecordBuilder(memory.DefaultAllocator, fidelitySchema)
	empty := b.NewRecordBatch()
	b.Release()
	defer empty.Release()
	cases := map[string]*pb.ExecutePlanResponse{
		"zero-row batch": batchResponse(t, fidelitySchema, empty),
		"schema only":    batchResponse(t, fidelitySchema),
	}
	for name, resp := range cases {
		t.Run(name, func(t *testing.T) {
			fake := &fakeSpark{responses: []*pb.ExecutePlanResponse{resp, resultComplete()}}
			code, _, out := runCommand(t, startFake(t, fake))
			if code != exitOK {
				t.Fatalf("exit %d, want 0", code)
			}
			tbl := readParquet(t, out)
			if tbl.NumRows() != 0 {
				t.Errorf("got %d rows, want 0", tbl.NumRows())
			}
			assertSameFields(t, tbl.Schema(), fidelitySchema)
		})
	}
}

func TestNoArrowBatchIsAnError(t *testing.T) {
	// Without an Arrow batch there is no Arrow schema, and sparkopy does not
	// guess one from Spark's own type names.
	fake := &fakeSpark{responses: []*pb.ExecutePlanResponse{resultComplete()}}
	code, stderr, out := runCommand(t, startFake(t, fake))
	if code != exitError {
		t.Fatalf("exit %d, want %d", code, exitError)
	}
	if !strings.Contains(stderr, "no Arrow batch") {
		t.Errorf("stderr does not explain the error: %q", stderr)
	}
	assertNoOutput(t, out)
}

func TestServerErrorMidStream(t *testing.T) {
	fake := &fakeSpark{
		responses: []*pb.ExecutePlanResponse{
			batchResponse(t, intSchema, intRecord(t, 0, 100)),
			batchResponse(t, intSchema, intRecord(t, 100, 200)),
		},
		err: status.Error(codes.Internal, "[TABLE_OR_VIEW_NOT_FOUND] boom from Spark"),
	}
	code, stderr, out := runCommand(t, startFake(t, fake))
	if code != exitError {
		t.Fatalf("exit %d, want %d", code, exitError)
	}
	if !strings.Contains(stderr, "boom from Spark") {
		t.Errorf("stderr lacks the Spark error text: %q", stderr)
	}
	assertNoOutput(t, out)
}

func TestUnreachableServer(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close() // Nothing listens on addr now.
	code, stderr, out := runCommand(t, "sc://"+addr)
	if code != exitError {
		t.Fatalf("exit %d, want %d", code, exitError)
	}
	if !strings.Contains(stderr, "cannot reach the Spark Connect server") {
		t.Errorf("stderr does not explain the error: %q", stderr)
	}
	assertNoOutput(t, out)
}

func TestMissingRequiredOption(t *testing.T) {
	full := map[string]string{
		"--spark-uri": "sc://localhost:15002", "--database": "db", "--table": "tbl", "--output": "out.parquet",
	}
	for missing := range full {
		t.Run(missing, func(t *testing.T) {
			var args []string
			for k, v := range full {
				if k != missing {
					args = append(args, k, v)
				}
			}
			var stderr bytes.Buffer
			code := run(context.Background(), args, &stderr)
			if code != exitUsage {
				t.Fatalf("exit %d, want %d", code, exitUsage)
			}
			if !strings.Contains(stderr.String(), "missing required options: ["+missing+"]") {
				t.Errorf("stderr does not name %s: %q", missing, stderr.String())
			}
			if !strings.Contains(stderr.String(), "usage: sparkopy") {
				t.Errorf("stderr has no usage line: %q", stderr.String())
			}
		})
	}
}

func TestSparkTarget(t *testing.T) {
	good := map[string]string{
		"sc://titan.jsaw.io:15002":  "titan.jsaw.io:15002",
		"sc://titan.jsaw.io":        "titan.jsaw.io:15002",
		"sc://titan.jsaw.io:15002/": "titan.jsaw.io:15002",
		"sc://[::1]:9999":           "[::1]:9999",
	}
	for uri, want := range good {
		if got, err := sparkTarget(uri); err != nil || got != want {
			t.Errorf("sparkTarget(%q) = %q, %v; want %q", uri, got, err, want)
		}
	}
	for _, uri := range []string{"http://host:15002", "sc://", "host:15002", "sc://host:15002/;token=x"} {
		if got, err := sparkTarget(uri); err == nil {
			t.Errorf("sparkTarget(%q) = %q, want an error", uri, got)
		}
	}
}
