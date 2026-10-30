// Command csvproc is the CLI entrypoint for the concurrent CSV processor.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"csvproc/internal/processor"
	"csvproc/internal/progress"
	"csvproc/internal/report"
)

type multiStringFlag []string

func (m *multiStringFlag) String() string { return strings.Join(*m, ",") }
func (m *multiStringFlag) Set(val string) error {
	for _, part := range strings.Split(val, ",") {
		p := strings.TrimSpace(part)
		if p != "" {
			*m = append(*m, p)
		}
	}
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runCLI(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func runCLI(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("csvproc", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		inputDir         string
		fileFlags        multiStringFlag
		workers          int
		batchSize        int
		maxReaders       int
		maxErrors        int
		progressInterval time.Duration
		outputPath       string
	)

	fs.StringVar(&inputDir, "input", "", "Directory containing *.csv files (or path to a single CSV file)")
	fs.Var(&fileFlags, "file", "CSV file path(s), repeatable or comma-separated")
	fs.IntVar(&workers, "workers", runtime.NumCPU(), "Number of concurrent worker goroutines")
	fs.IntVar(&batchSize, "batch-size", processor.DefaultBatchSize, "Number of rows per channel batch")
	fs.IntVar(&maxReaders, "max-readers", processor.DefaultMaxReaders, "Maximum concurrently open CSV files")
	fs.IntVar(&maxErrors, "max-errors", processor.DefaultMaxErrorSamples, "Maximum row error samples retained in memory")
	fs.DurationVar(&progressInterval, "progress-interval", 500*time.Millisecond, "Progress ticker interval (0 to disable)")
	fs.StringVar(&outputPath, "output", "report.json", "Output path for JSON report (empty to skip file write)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	files, err := resolveInputFiles(inputDir, fileFlags)
	if err != nil {
		return err
	}

	start := time.Now()
	tracker := progress.NewTracker()
	stopReporter := tracker.StartReporter(ctx, stderr, progressInterval)

	res, runErr := processor.Run(ctx, processor.Config{
		Files:      files,
		Workers:    workers,
		BatchSize:  batchSize,
		MaxReaders: maxReaders,
		MaxErrors:  maxErrors,
		Progress:   tracker.Hooks(),
	})
	stopReporter()

	// If configuration itself was invalid before processing started, return immediately.
	if runErr != nil && !errors.Is(runErr, context.Canceled) && !errors.Is(runErr, context.DeadlineExceeded) {
		return runErr
	}

	elapsed := time.Since(start)
	doc := report.Build(res, elapsed)
	if outputPath != "" {
		if err := report.WriteJSON(outputPath, doc); err != nil {
			return fmt.Errorf("write report: %w", err)
		}
	}
	if err := report.PrintSummary(stdout, doc); err != nil {
		return fmt.Errorf("print summary: %w", err)
	}
	return runErr
}

func resolveInputFiles(input string, explicit []string) ([]string, error) {
	seen := make(map[string]struct{})
	var out []string
	add := func(p string) {
		clean := filepath.Clean(p)
		if _, ok := seen[clean]; !ok {
			seen[clean] = struct{}{}
			out = append(out, clean)
		}
	}

	if strings.TrimSpace(input) != "" {
		info, err := os.Stat(input)
		if err != nil {
			return nil, fmt.Errorf("stat -input %q: %w", input, err)
		}
		if !info.IsDir() {
			add(input)
		} else {
			entries, err := os.ReadDir(input)
			if err != nil {
				return nil, fmt.Errorf("read dir %q: %w", input, err)
			}
			var dirFiles []string
			for _, e := range entries {
				if !e.IsDir() && strings.EqualFold(filepath.Ext(e.Name()), ".csv") {
					dirFiles = append(dirFiles, filepath.Join(input, e.Name()))
				}
			}
			sort.Strings(dirFiles)
			for _, f := range dirFiles {
				add(f)
			}
		}
	}

	for _, f := range explicit {
		add(f)
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("%w: specify -input <dir|file> or -file <path>", processor.ErrInvalidConfig)
	}
	return out, nil
}
