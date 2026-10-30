package report

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"csvproc/internal/processor"
)

func TestBuildWriteJSONAndPrintSummary(t *testing.T) {
	res := processor.Result{
		Partial:     false,
		TotalErrors: 2,
		Errors: []processor.RowError{
			processor.NewRowError("a.csv", 3, processor.ErrColumnCount),
		},
		Files: []processor.FileStats{
			{
				Path:          "a.csv",
				RowsRead:      10,
				RowsProcessed: 9,
				RowsFailed:    1,
				Columns: []processor.ColumnStats{
					{Name: "id", NonEmpty: 9, NumericCount: 9, Min: 1, Max: 9, IntCount: 9, IntSum: 45},
				},
			},
			{
				Path:     "bad.csv",
				Err:      processor.ErrEmptyFile,
				ErrorMsg: processor.ErrEmptyFile.Error(),
			},
		},
	}

	doc := Build(res, 500*time.Millisecond)
	if doc.Summary.FilesTotal != 2 || doc.Summary.FilesSucceeded != 1 || doc.Summary.FilesFailed != 1 {
		t.Fatalf("unexpected file counts in summary: %+v", doc.Summary)
	}
	if doc.Summary.RowsRead != 10 || doc.Summary.RowsProcessed != 9 || doc.Summary.RowsFailed != 1 {
		t.Fatalf("unexpected row counts in summary: %+v", doc.Summary)
	}
	if doc.Summary.RowsPerSec != 20 { // (9+1) / 0.5s
		t.Fatalf("RowsPerSec = %v, want 20", doc.Summary.RowsPerSec)
	}

	// Verify WriteJSON writes valid JSON atomically
	outPath := filepath.Join(t.TempDir(), "sub", "report.json")
	if err := WriteJSON(outPath, doc); err != nil {
		t.Fatalf("WriteJSON failed: %v", err)
	}
	raw, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	var decoded Document
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if decoded.Summary.RowsProcessed != 9 || len(decoded.Files) != 2 {
		t.Fatalf("decoded JSON mismatch: %+v", decoded)
	}

	// Verify PrintSummary output for complete and partial runs
	var buf bytes.Buffer
	if err := PrintSummary(&buf, doc); err != nil {
		t.Fatalf("PrintSummary failed: %v", err)
	}
	outStr := buf.String()
	for _, want := range []string{"[COMPLETE]", "2 total (1 succeeded, 1 failed)", "[OK]   a.csv", "[FAIL] bad.csv"} {
		if !strings.Contains(outStr, want) {
			t.Fatalf("PrintSummary missing %q in:\n%s", want, outStr)
		}
	}

	doc.Summary.Partial = true
	buf.Reset()
	if err := PrintSummary(&buf, doc); err != nil {
		t.Fatalf("PrintSummary partial failed: %v", err)
	}
	if !strings.Contains(buf.String(), "PARTIAL (cancelled)") {
		t.Fatalf("expected PARTIAL status in:\n%s", buf.String())
	}
}
