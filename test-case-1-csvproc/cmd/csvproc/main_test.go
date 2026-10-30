package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"csvproc/internal/processor"
	"csvproc/internal/report"
)

func TestRunCLIDirectoryAndExplicitFiles(t *testing.T) {
	outFile := filepath.Join(t.TempDir(), "out.json")
	var stdout, stderr bytes.Buffer

	err := runCLI(context.Background(), []string{
		"-input", filepath.Join("..", "..", "testdata"),
		"-workers", "2",
		"-batch-size", "2",
		"-max-readers", "2",
		"-progress-interval", "5ms",
		"-output", outFile,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("runCLI failed: %v", err)
	}

	if !strings.Contains(stdout.String(), "=== CSV Processing Summary [COMPLETE] ===") {
		t.Fatalf("unexpected stdout:\n%s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "[done]") {
		t.Fatalf("expected [done] in stderr:\n%s", stderr.String())
	}

	raw, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("read output JSON: %v", err)
	}
	var doc report.Document
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal output JSON: %v", err)
	}
	if doc.Summary.FilesTotal != 7 || doc.Summary.FilesSucceeded != 6 || doc.Summary.FilesFailed != 1 {
		t.Fatalf("unexpected summary in report.json: %+v", doc.Summary)
	}
}

func TestRunCLISingleFileAndRepeatedFileFlag(t *testing.T) {
	outFile := filepath.Join(t.TempDir(), "single.json")
	var stdout, stderr bytes.Buffer

	u := filepath.Join("..", "..", "testdata", "users.csv")
	tx := filepath.Join("..", "..", "testdata", "transactions.csv")

	err := runCLI(context.Background(), []string{
		"-input", u,
		"-file", tx + "," + u, // duplicate 'u' should be deduplicated automatically
		"-progress-interval", "0",
		"-output", outFile,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("runCLI failed: %v", err)
	}

	raw, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	var doc report.Document
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Summary.FilesTotal != 2 || doc.Summary.RowsProcessed != 6450 {
		t.Fatalf("unexpected summary: %+v", doc.Summary)
	}
}

func TestRunCLICancelledStillWritesPartialReport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel to simulate immediate SIGINT

	outFile := filepath.Join(t.TempDir(), "partial.json")
	var stdout, stderr bytes.Buffer

	err := runCLI(ctx, []string{
		"-file", filepath.Join("..", "..", "testdata", "users.csv"),
		"-progress-interval", "0",
		"-output", outFile,
	}, &stdout, &stderr)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	raw, readErr := os.ReadFile(outFile)
	if readErr != nil {
		t.Fatalf("expected partial report file to be written, got: %v", readErr)
	}
	var doc report.Document
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if !doc.Summary.Partial {
		t.Fatalf("expected Partial=true in partial report: %+v", doc.Summary)
	}
}

func TestRunCLIValidationErrors(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := runCLI(context.Background(), []string{}, &stdout, &stderr); !errors.Is(err, processor.ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig when no files given, got: %v", err)
	}
	if err := runCLI(context.Background(), []string{"-input", "/nonexistent/path/xyz"}, &stdout, &stderr); err == nil {
		t.Fatal("expected error for nonexistent -input directory")
	}
}
