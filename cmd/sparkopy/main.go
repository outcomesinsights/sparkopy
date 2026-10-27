// Command sparkopy reads one table through Spark Connect and writes it to one
// local Parquet file. It streams: each Arrow batch that Spark sends is written
// to the file as it arrives, so the whole table is never held in memory.
//
// Usage:
//
//	sparkopy --spark-uri sc://HOST:15002 --database DB --table TABLE --output PATH
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

// Exit statuses.
const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stderr)
	stop()
	os.Exit(code)
}

// options holds the parsed command line.
type options struct {
	sparkURI string
	database string
	table    string
	output   string
}

// parseArgs reads the command line. Every error it returns is a usage error.
func parseArgs(args []string, stderr io.Writer) (options, error) {
	var o options
	fs := flag.NewFlagSet("sparkopy", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&o.sparkURI, "spark-uri", "", "Spark Connect URI, such as sc://HOST:15002 (required)")
	fs.StringVar(&o.database, "database", "", "database that holds the table (required)")
	fs.StringVar(&o.table, "table", "", "table to export (required)")
	fs.StringVar(&o.output, "output", "", "path of the Parquet file to write (required)")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: sparkopy --spark-uri sc://HOST:15002 --database DB --table TABLE --output PATH")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() > 0 {
		fs.Usage()
		return o, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	var missing []string
	for _, f := range []struct{ name, value string }{
		{"--spark-uri", o.sparkURI},
		{"--database", o.database},
		{"--table", o.table},
		{"--output", o.output},
	} {
		if f.value == "" {
			missing = append(missing, f.name)
		}
	}
	if len(missing) > 0 {
		fs.Usage()
		return o, fmt.Errorf("missing required options: %v", missing)
	}
	return o, nil
}

// run is the whole command. It returns the exit status.
func run(ctx context.Context, args []string, stderr io.Writer) int {
	o, err := parseArgs(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		fmt.Fprintf(stderr, "sparkopy: %v\n", err)
		return exitUsage
	}
	if err := export(ctx, o, stderr); err != nil {
		fmt.Fprintf(stderr, "sparkopy: export of %s.%s failed: %v\n", o.database, o.table, err)
		return exitError
	}
	return exitOK
}
