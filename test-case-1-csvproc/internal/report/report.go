// Package report formats pipeline results as structured JSON files and
// concise human-readable terminal summaries.
package report

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"csvproc/internal/processor"
)

// Summary aggregates top-level metrics across all processed CSV files.
type Summary struct {
	Partial        bool    `json:"partial"`
	FilesTotal     int     `json:"files_total"`
	FilesSucceeded int     `json:"files_succeeded"`
	FilesFailed    int     `json:"files_failed"`
	RowsRead       int64   `json:"rows_read"`
	RowsProcessed  int64   `json:"rows_processed"`
	RowsFailed     int64   `json:"rows_failed"`
	TotalRowErrors int64   `json:"total_row_errors"`
	Duration       string  `json:"duration"`
	DurationMs     int64   `json:"duration_ms"`
	RowsPerSec     float64 `json:"rows_per_sec"`
}

// Document is the root structure serialized to report.json.
type Document struct {
	Summary     Summary               `json:"summary"`
	Files       []processor.FileStats `json:"files"`
	ErrorSample []processor.RowError  `json:"error_sample,omitempty"`
}

// Build constructs a report Document from a processor.Result and elapsed duration.
func Build(res processor.Result, elapsed time.Duration) Document {
	var sum Summary
	sum.Partial = res.Partial
	sum.FilesTotal = len(res.Files)
	sum.TotalRowErrors = res.TotalErrors
	sum.Duration = elapsed.Round(time.Millisecond).String()
	sum.DurationMs = elapsed.Milliseconds()

	for _, f := range res.Files {
		sum.RowsRead += f.RowsRead
		sum.RowsProcessed += f.RowsProcessed
		sum.RowsFailed += f.RowsFailed
		if f.Err != nil || f.ErrorMsg != "" {
			sum.FilesFailed++
		} else {
			sum.FilesSucceeded++
		}
	}

	if secs := elapsed.Seconds(); secs > 0 {
		sum.RowsPerSec = float64(sum.RowsProcessed+sum.RowsFailed) / secs
	}

	return Document{
		Summary:     sum,
		Files:       res.Files,
		ErrorSample: res.Errors,
	}
}

// WriteJSON serializes doc as indented JSON and writes it atomically (via a
// temporary file rename in the same directory) to path.
func WriteJSON(path string, doc Document) error {
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report json: %w", err)
	}
	data = append(data, '\n')

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create report dir: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".report-*.json.tmp")
	if err != nil {
		return fmt.Errorf("create temp report: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp report: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp report: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("rename report to %s: %w", path, err)
	}
	cleanup = false
	return nil
}

// PrintSummary writes a clean human-readable summary of doc to w.
func PrintSummary(w io.Writer, doc Document) error {
	s := doc.Summary
	status := "COMPLETE"
	if s.Partial {
		status = "PARTIAL (cancelled)"
	}

	_, err := fmt.Fprintf(
		w,
		"=== CSV Processing Summary [%s] ===\n"+
			"Files     : %d total (%d succeeded, %d failed)\n"+
			"Rows      : %d read (%d processed, %d failed)\n"+
			"Throughput: %.0f rows/sec (duration: %s)\n",
		status,
		s.FilesTotal, s.FilesSucceeded, s.FilesFailed,
		s.RowsRead, s.RowsProcessed, s.RowsFailed,
		s.RowsPerSec, s.Duration,
	)
	if err != nil {
		return err
	}

	for _, f := range doc.Files {
		if f.ErrorMsg != "" {
			if _, err := fmt.Fprintf(w, "  - [FAIL] %s: %s (read=%d ok=%d err=%d)\n",
				f.Path, f.ErrorMsg, f.RowsRead, f.RowsProcessed, f.RowsFailed); err != nil {
				return err
			}
			continue
		}
		if _, err := fmt.Fprintf(w, "  - [OK]   %s: %d cols, read=%d ok=%d err=%d\n",
			f.Path, len(f.Columns), f.RowsRead, f.RowsProcessed, f.RowsFailed); err != nil {
			return err
		}
	}
	return nil
}
