package main

// The parity check runs against a real Spark Connect server. It runs only
// when SPARKOPY_TEST_SPARK_URI is set (see `just parity`), so CI skips it.
// It only reads: SHOW TABLES, SELECT count(*), the schema of each table, and
// the export itself.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/uuid"

	pb "github.com/outcomesinsights/sparkopy/internal/connectpb"
)

// sparkSQL runs a small query and returns its records. It holds the whole
// result in memory, so use it only for results of a few rows.
func sparkSQL(t *testing.T, client pb.SparkConnectServiceClient, query string) []arrow.RecordBatch {
	t.Helper()
	req := planRequest(&pb.Relation{RelType: &pb.Relation_Sql{Sql: &pb.SQL{Query: query}}})
	stream, err := client.ExecutePlan(context.Background(), req)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	var recs []arrow.RecordBatch
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		batch := resp.GetArrowBatch()
		if batch == nil {
			continue
		}
		rdr, err := ipc.NewReader(bytes.NewReader(batch.GetData()))
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		for rdr.Next() {
			rec := rdr.RecordBatch()
			rec.Retain()
			t.Cleanup(rec.Release)
			recs = append(recs, rec)
		}
		if err := rdr.Err(); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		rdr.Release()
	}
	return recs
}

// sparkTables lists the tables (not the temporary views) in database.
func sparkTables(t *testing.T, client pb.SparkConnectServiceClient, database string) []string {
	t.Helper()
	var tables []string
	for _, rec := range sparkSQL(t, client, fmt.Sprintf("SHOW TABLES IN `%s`", database)) {
		var name *array.String
		var temp *array.Boolean
		for i, f := range rec.Schema().Fields() {
			switch f.Name {
			case "tableName":
				name = rec.Column(i).(*array.String)
			case "isTemporary":
				temp = rec.Column(i).(*array.Boolean)
			}
		}
		if name == nil || temp == nil {
			t.Fatalf("SHOW TABLES returned an unexpected schema: %s", rec.Schema())
		}
		for r := 0; r < int(rec.NumRows()); r++ {
			if !temp.Value(r) {
				tables = append(tables, name.Value(r))
			}
		}
	}
	return tables
}

// sparkCount is Spark's own row count of database.table.
func sparkCount(t *testing.T, client pb.SparkConnectServiceClient, database, table string) int64 {
	t.Helper()
	recs := sparkSQL(t, client, fmt.Sprintf("SELECT count(*) FROM `%s`.`%s`", database, table))
	if len(recs) != 1 || recs[0].NumRows() != 1 {
		t.Fatalf("count(*) of %s.%s did not return one row", database, table)
	}
	return recs[0].Column(0).(*array.Int64).Value(0)
}

// sparkSchema is Spark's own schema of database.table, from AnalyzePlan.
func sparkSchema(t *testing.T, client pb.SparkConnectServiceClient, database, table string) []*pb.DataType_StructField {
	t.Helper()
	resp, err := client.AnalyzePlan(context.Background(), &pb.AnalyzePlanRequest{
		SessionId:   uuid.NewString(),
		UserContext: userContext(),
		Analyze: &pb.AnalyzePlanRequest_Schema_{Schema: &pb.AnalyzePlanRequest_Schema{
			Plan: &pb.Plan{OpType: &pb.Plan_Root{Root: namedTable(database, table)}},
		}},
	})
	if err != nil {
		t.Fatalf("AnalyzePlan(Schema) of %s.%s: %v", database, table, err)
	}
	st := resp.GetSchema().GetSchema().GetStruct()
	if st == nil {
		t.Fatalf("AnalyzePlan(Schema) of %s.%s returned no struct type", database, table)
	}
	return st.GetFields()
}

// arrowTypeOK reports whether got is the Arrow type that Spark 3.5 uses for
// the Spark type want (Spark's ArrowUtils.toArrowType). This is a check, not
// a conversion: sparkopy never maps types.
func arrowTypeOK(want *pb.DataType, got arrow.DataType) (bool, string) {
	var exp arrow.DataType
	switch k := want.GetKind().(type) {
	case *pb.DataType_Boolean_:
		exp = arrow.FixedWidthTypes.Boolean
	case *pb.DataType_Byte_:
		exp = arrow.PrimitiveTypes.Int8
	case *pb.DataType_Short_:
		exp = arrow.PrimitiveTypes.Int16
	case *pb.DataType_Integer_:
		exp = arrow.PrimitiveTypes.Int32
	case *pb.DataType_Long_:
		exp = arrow.PrimitiveTypes.Int64
	case *pb.DataType_Float_:
		exp = arrow.PrimitiveTypes.Float32
	case *pb.DataType_Double_:
		exp = arrow.PrimitiveTypes.Float64
	case *pb.DataType_String_, *pb.DataType_Char_, *pb.DataType_VarChar_:
		exp = arrow.BinaryTypes.String
	case *pb.DataType_Binary_:
		exp = arrow.BinaryTypes.Binary
	case *pb.DataType_Date_:
		exp = arrow.FixedWidthTypes.Date32
	case *pb.DataType_Null:
		exp = arrow.Null
	case *pb.DataType_Decimal_:
		exp = &arrow.Decimal128Type{Precision: k.Decimal.GetPrecision(), Scale: k.Decimal.GetScale()}
	case *pb.DataType_TimestampNtz:
		exp = &arrow.TimestampType{Unit: arrow.Microsecond}
	case *pb.DataType_Timestamp_:
		ts, ok := got.(*arrow.TimestampType)
		return ok && ts.Unit == arrow.Microsecond && ts.TimeZone != "", "timestamp[us, tz=<session time zone>]"
	default:
		return false, fmt.Sprintf("Spark type %T, which this check does not know", k)
	}
	return arrow.TypeEqual(exp, got), exp.String()
}

// parquetFile returns the row count and the Arrow schema of a Parquet file.
func parquetFile(t *testing.T, path string) (int64, *arrow.Schema) {
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
	schema, err := fr.Schema()
	if err != nil {
		t.Fatal(err)
	}
	return rdr.NumRows(), schema
}

// compareSchema checks the file's Arrow schema against Spark's schema.
func compareSchema(t *testing.T, spark []*pb.DataType_StructField, got *arrow.Schema) {
	t.Helper()
	if len(spark) != got.NumFields() {
		t.Fatalf("file has %d columns, Spark has %d", got.NumFields(), len(spark))
	}
	for i, sf := range spark {
		f := got.Field(i)
		ok, want := arrowTypeOK(sf.GetDataType(), f.Type)
		if f.Name != sf.GetName() || !ok || f.Nullable != sf.GetNullable() {
			t.Errorf("column %d: file has %s (nullable=%v), Spark has %s %s (nullable=%v)",
				i, f, f.Nullable, sf.GetName(), want, sf.GetNullable())
		}
	}
}

func TestParity(t *testing.T) {
	uri := os.Getenv("SPARKOPY_TEST_SPARK_URI")
	if uri == "" {
		t.Skip("SPARKOPY_TEST_SPARK_URI is not set")
	}
	database := os.Getenv("SPARKOPY_PARITY_DATABASE")
	if database == "" {
		t.Fatal("SPARKOPY_PARITY_DATABASE is not set")
	}
	target, err := sparkTarget(uri)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := dial(target)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewSparkConnectServiceClient(conn)

	tables := sparkTables(t, client, database)
	if len(tables) == 0 {
		t.Fatalf("database %s has no tables", database)
	}
	t.Logf("database %s has %d tables: %v", database, len(tables), tables)
	dir := t.TempDir()

	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			out := filepath.Join(dir, table+".parquet")
			var stderr bytes.Buffer
			start := time.Now()
			code := run(context.Background(), []string{
				"--spark-uri", uri, "--database", database, "--table", table, "--output", out,
			}, &stderr)
			elapsed := time.Since(start)
			if code != exitOK {
				t.Fatalf("sparkopy exit %d:\n%s", code, stderr.String())
			}
			rows, schema := parquetFile(t, out)
			want := sparkCount(t, client, database, table)
			if rows != want {
				t.Errorf("file has %d rows, Spark count(*) is %d", rows, want)
			}
			compareSchema(t, sparkSchema(t, client, database, table), schema)
			t.Logf("%s: %d rows (Spark: %d), %d columns, %s", table, rows, want, schema.NumFields(),
				elapsed.Round(time.Millisecond))
			os.Remove(out)
		})
	}

	// A result with no rows, from the real protocol: Limit 0 on a table.
	// It checks that Spark sends an Arrow batch that carries the schema.
	t.Run("empty result", func(t *testing.T) {
		table := tables[0]
		out := filepath.Join(dir, "empty.parquet")
		req := planRequest(&pb.Relation{RelType: &pb.Relation_Limit{Limit: &pb.Limit{
			Input: namedTable(database, table), Limit: 0,
		}}})
		var stderr bytes.Buffer
		if err := exportPlan(context.Background(), conn, target, req, out, &stderr); err != nil {
			t.Fatalf("Limit 0 on %s: %v\n%s", table, err, stderr.String())
		}
		rows, schema := parquetFile(t, out)
		if rows != 0 {
			t.Errorf("file has %d rows, want 0", rows)
		}
		compareSchema(t, sparkSchema(t, client, database, table), schema)
		t.Logf("Limit 0 on %s: 0 rows, %d columns", table, schema.NumFields())
	})
}
