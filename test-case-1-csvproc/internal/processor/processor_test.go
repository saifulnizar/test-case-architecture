package processor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func fixturePath(name string) string {
	return filepath.Join("..", "..", "testdata", name)
}

func TestColumnStatsObserveAndMerge(t *testing.T) {
	var c ColumnStats
	for _, v := range []string{"10", " 25 ", "", "abc", "NaN", "Inf", "-5.5", "20"} {
		c.Observe(v)
	}
	if c.Empty != 1 || c.NonEmpty != 7 {
		t.Fatalf("empty=%d nonEmpty=%d, want 1/7", c.Empty, c.NonEmpty)
	}
	if c.NumericCount != 4 || c.Min != -5.5 || c.Max != 25 {
		t.Fatalf("numeric=%d min=%v max=%v, want 4/-5.5/25", c.NumericCount, c.Min, c.Max)
	}
	if c.IntCount != 3 || c.IntSum != 55 || c.IntSumOverflow {
		t.Fatalf("intCount=%d intSum=%d overflow=%v, want 3/55/false", c.IntCount, c.IntSum, c.IntSumOverflow)
	}

	// Overflow sticky check
	var over ColumnStats
	over.Observe(fmt.Sprintf("%d", int64(math.MaxInt64)))
	over.Observe("10")
	if !over.IntSumOverflow {
		t.Fatal("expected IntSumOverflow=true on MaxInt64 + 10")
	}
	c.merge(&over)
	if !c.IntSumOverflow {
		t.Fatal("expected IntSumOverflow to propagate on merge")
	}
}

func TestFileStatsProcessRowValidation(t *testing.T) {
	norm := NormalizeHeader([]string{"\ufeff id ", " score "})
	if norm[0] != "id" || norm[1] != "score" {
		t.Fatalf("NormalizeHeader = %v", norm)
	}

	st := newFileStats(&FileMeta{Path: "test.csv", Header: norm})
	if err := st.ProcessRow(Row{Line: 2, Fields: []string{"1", "100"}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := st.ProcessRow(Row{Line: 3, Fields: []string{"2"}}); !errors.Is(err, ErrColumnCount) {
		t.Fatalf("err = %v, want ErrColumnCount", err)
	}
	if err := st.ProcessRow(Row{Line: 4, Fields: []string{" ", "\t"}}); !errors.Is(err, ErrEmptyRow) {
		t.Fatalf("err = %v, want ErrEmptyRow", err)
	}
	if st.RowsProcessed != 1 {
		t.Fatalf("RowsProcessed = %d, want 1", st.RowsProcessed)
	}
}

func TestErrorCollectorBoundedConcurrency(t *testing.T) {
	const limit = 15
	const goroutines = 10
	const perGoroutine = 100

	c := NewErrorCollector(limit)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				c.Add(NewRowError("a.csv", i+2, ErrColumnCount))
			}
		}(g)
	}
	wg.Wait()

	if c.Total() != goroutines*perGoroutine {
		t.Fatalf("Total() = %d, want %d", c.Total(), goroutines*perGoroutine)
	}
	samples := c.Samples()
	if len(samples) != limit {
		t.Fatalf("len(Samples()) = %d, want %d", len(samples), limit)
	}
	if !errors.Is(samples[0], ErrColumnCount) || !strings.Contains(samples[0].Error(), "a.csv:") {
		t.Fatalf("unexpected sample error format: %v", samples[0])
	}
}

func TestRunMixedSchemasAndMalformedFiles(t *testing.T) {
	var totalFiles, completedFiles, totalBytes, readBytes, readRows, procRows, failRows atomic.Int64

	files := []string{
		fixturePath("users.csv"),        // 1250 valid rows
		fixturePath("transactions.csv"), // 5200 valid rows
		fixturePath("inventory.csv"),    // 2800 valid rows
		fixturePath("telemetry.csv"),    // 3600 valid rows
		fixturePath("malformed.csv"),    // 1500 rows: 1470 valid, 30 failed
		fixturePath("header_only.csv"),  // 0 data rows
		fixturePath("empty.csv"),        // 0 bytes -> ErrEmptyFile
		fixturePath("does_not_exist.csv"),
	}

	res, err := Run(context.Background(), Config{
		Files:      files,
		Workers:    4,
		BatchSize:  200,
		MaxReaders: 3,
		Progress: ProgressHooks{
			OnFilesTotal:    func(n int64) { totalFiles.Store(n) },
			OnBytesTotal:    func(n int64) { totalBytes.Store(n) },
			OnBytesRead:     func(d int64) { readBytes.Add(d) },
			OnFileCompleted: func() { completedFiles.Add(1) },
			OnRowsRead:      func(d int64) { readRows.Add(d) },
			OnRowsProcessed: func(d int64) { procRows.Add(d) },
			OnRowsFailed:    func(d int64) { failRows.Add(d) },
		},
	})
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if res.Partial {
		t.Fatal("expected Partial=false for completed run")
	}

	// 1. users.csv: 1250 valid rows, columns: id, name, age, city (every 10th age is empty -> 125 empty, 1125 non-empty)
	u := res.Files[0]
	if u.Err != nil || u.RowsRead != 1250 || u.RowsProcessed != 1250 || u.RowsFailed != 0 {
		t.Fatalf("users.csv stats: %+v", u)
	}
	ageCol := u.Columns[2]
	if ageCol.Name != "age" || ageCol.NonEmpty != 1125 || ageCol.Empty != 125 {
		t.Fatalf("users.csv age column: %+v", ageCol)
	}

	// 2. transactions.csv: 5200 valid rows
	tx := res.Files[1]
	if tx.Err != nil || tx.RowsRead != 5200 || tx.RowsProcessed != 5200 || tx.RowsFailed != 0 {
		t.Fatalf("transactions.csv stats: %+v", tx)
	}

	// 3. inventory.csv: 2800 valid rows
	inv := res.Files[2]
	if inv.Err != nil || inv.RowsRead != 2800 || inv.RowsProcessed != 2800 || inv.RowsFailed != 0 {
		t.Fatalf("inventory.csv stats: %+v", inv)
	}

	// 4. telemetry.csv: 3600 valid rows
	tel := res.Files[3]
	if tel.Err != nil || tel.RowsRead != 3600 || tel.RowsProcessed != 3600 || tel.RowsFailed != 0 {
		t.Fatalf("telemetry.csv stats: %+v", tel)
	}

	// 5. malformed.csv: 1500 rows read -> 1470 valid, 30 failed
	m := res.Files[4]
	if m.Err != nil || m.RowsRead != 1500 || m.RowsProcessed != 1470 || m.RowsFailed != 30 {
		t.Fatalf("malformed.csv stats: %+v", m)
	}

	// 6. header_only.csv: 0 rows read, no error
	h := res.Files[5]
	if h.Err != nil || h.RowsRead != 0 || len(h.Columns) != 3 {
		t.Fatalf("header_only.csv stats: %+v", h)
	}

	// 7. empty.csv: file-level error ErrEmptyFile
	e := res.Files[6]
	if !errors.Is(e.Err, ErrEmptyFile) {
		t.Fatalf("empty.csv err = %v, want ErrEmptyFile", e.Err)
	}

	// 8. does_not_exist.csv: file-level error os.ErrNotExist
	missing := res.Files[7]
	if !errors.Is(missing.Err, os.ErrNotExist) {
		t.Fatalf("missing.csv err = %v, want os.ErrNotExist", missing.Err)
	}

	// Progress hooks invariant check
	if totalFiles.Load() != 8 || completedFiles.Load() != 6 {
		t.Fatalf("files total=%d completed=%d, want 8/6", totalFiles.Load(), completedFiles.Load())
	}
	if totalBytes.Load() <= 0 || readBytes.Load() != totalBytes.Load() {
		t.Fatalf("bytes read=%d total=%d, want equal > 0", readBytes.Load(), totalBytes.Load())
	}
	// Total rows across all files: 1250 + 5200 + 2800 + 3600 + 1500 = 14350
	// Processed: 1250 + 5200 + 2800 + 3600 + 1470 = 14320, Failed: 30
	if readRows.Load() != 14350 || procRows.Load() != 14320 || failRows.Load() != 30 {
		t.Fatalf("rows read=%d proc=%d fail=%d, want 14350/14320/30", readRows.Load(), procRows.Load(), failRows.Load())
	}
	if res.TotalErrors != 30 || len(res.Errors) != 30 {
		t.Fatalf("errors total=%d samples=%d, want 30", res.TotalErrors, len(res.Errors))
	}
}

func TestRunDeterminismAcrossWorkersReadersAndBatches(t *testing.T) {
	dir := t.TempDir()
	var files []string
	for fIdx := 0; fIdx < 5; fIdx++ {
		var sb strings.Builder
		sb.WriteString("id,metric_a,metric_b,category\n")
		for r := 0; r < 600; r++ {
			switch {
			case r%23 == 0:
				sb.WriteString("bad_column_count\n")
			case r%41 == 0:
				sb.WriteString(" , , , \n")
			default:
				fmt.Fprintf(&sb, "%d,%d,%d.5,cat-%d\n", r, (r*7)%100, r%50, r%5)
			}
		}
		p := filepath.Join(dir, fmt.Sprintf("f_%d.csv", fIdx))
		if err := os.WriteFile(p, []byte(sb.String()), 0o600); err != nil {
			t.Fatal(err)
		}
		files = append(files, p)
	}

	base, err := Run(context.Background(), Config{
		Files:      files,
		Workers:    1,
		MaxReaders: 1,
		BatchSize:  500,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, workers := range []int{1, 4, 16} {
		for _, readers := range []int{1, 3, 5} {
			for _, batchSize := range []int{1, 13, 500} {
				name := fmt.Sprintf("w=%d_r=%d_b=%d", workers, readers, batchSize)
				t.Run(name, func(t *testing.T) {
					got, err := Run(context.Background(), Config{
						Files:      files,
						Workers:    workers,
						MaxReaders: readers,
						BatchSize:  batchSize,
					})
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(got.Files, base.Files) {
						t.Fatalf("Files mismatch:\ngot:  %+v\nwant: %+v", got.Files, base.Files)
					}
					if got.TotalErrors != base.TotalErrors {
						t.Fatalf("TotalErrors = %d, want %d", got.TotalErrors, base.TotalErrors)
					}
				})
			}
		}
	}
}

func TestRunMaxReadersSemaphoreCap(t *testing.T) {
	dir := t.TempDir()
	var files []string
	for i := 0; i < 10; i++ {
		p := filepath.Join(dir, fmt.Sprintf("f%d.csv", i))
		if err := os.WriteFile(p, []byte("a,b\n1,2\n3,4\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		files = append(files, p)
	}

	for _, maxReaders := range []int{1, 2, 4} {
		t.Run(fmt.Sprintf("maxReaders=%d", maxReaders), func(t *testing.T) {
			var active, peak atomic.Int32
			gate := make(chan struct{})
			var once sync.Once

			_, err := Run(context.Background(), Config{
				Files:      files,
				Workers:    4,
				MaxReaders: maxReaders,
				testReaderStart: func(int) {
					cur := active.Add(1)
					for {
						p := peak.Load()
						if cur <= p || peak.CompareAndSwap(p, cur) {
							break
						}
					}
					if int(cur) >= maxReaders {
						once.Do(func() { close(gate) })
					}
					<-gate
				},
				testReaderEnd: func(int) {
					active.Add(-1)
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := int(peak.Load()); got != maxReaders {
				t.Fatalf("peak active readers = %d, want %d", got, maxReaders)
			}
		})
	}
}

func TestRunCancellationAndNoGoroutineLeak(t *testing.T) {
	t.Run("cancel mid-run via row hook", func(t *testing.T) {
		dir := t.TempDir()
		var sb strings.Builder
		sb.WriteString("a,b\n")
		for i := 0; i < 20000; i++ {
			sb.WriteString("1,2\n")
		}
		var files []string
		for i := 0; i < 4; i++ {
			p := filepath.Join(dir, fmt.Sprintf("big_%d.csv", i))
			if err := os.WriteFile(p, []byte(sb.String()), 0o600); err != nil {
				t.Fatal(err)
			}
			files = append(files, p)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		res, err := Run(ctx, Config{
			Files:      files,
			Workers:    2,
			MaxReaders: 2,
			BatchSize:  10,
			testRowHook: func(string, Row) {
				cancel()
			},
		})
		if !errors.Is(err, context.Canceled) || !res.Partial {
			t.Fatalf("err=%v partial=%v, want context.Canceled and Partial=true", err, res.Partial)
		}
		goleak.VerifyNone(t)
	})

	t.Run("cancel while waiting for reader semaphore slot", func(t *testing.T) {
		dir := t.TempDir()
		var files []string
		for i := 0; i < 4; i++ {
			p := filepath.Join(dir, fmt.Sprintf("f_%d.csv", i))
			if err := os.WriteFile(p, []byte("a,b\n1,2\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			files = append(files, p)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		started := make(chan struct{})
		release := make(chan struct{})

		done := make(chan struct{})
		var res Result
		var runErr error
		go func() {
			defer close(done)
			res, runErr = Run(ctx, Config{
				Files:      files,
				Workers:    2,
				MaxReaders: 1,
				testReaderStart: func(idx int) {
					if idx == 0 {
						close(started)
						<-release
					}
				},
			})
		}()

		<-started
		cancel()
		close(release)

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Run hung after cancel while waiting for reader slot")
		}
		if !errors.Is(runErr, context.Canceled) || !res.Partial {
			t.Fatalf("runErr=%v partial=%v", runErr, res.Partial)
		}
		goleak.VerifyNone(t)
	})
}

func TestWorkerPanicRecoveryIsolatesBadRow(t *testing.T) {
	res, err := Run(context.Background(), Config{
		Files:     []string{fixturePath("users.csv")},
		Workers:   2,
		BatchSize: 200,
		testRowHook: func(_ string, row Row) {
			if row.Line == 3 {
				panic("simulated unexpected panic on line 3")
			}
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	f := res.Files[0]
	if f.RowsRead != 1250 || f.RowsProcessed != 1249 || f.RowsFailed != 1 {
		t.Fatalf("file stats = %+v, want read=1250 processed=1249 failed=1", f)
	}
	if len(res.Errors) != 1 || !errors.Is(res.Errors[0], ErrWorkerPanic) {
		t.Fatalf("errors = %+v, want ErrWorkerPanic", res.Errors)
	}
}

func TestConfigValidation(t *testing.T) {
	validFile := fixturePath("users.csv")
	tests := []struct {
		name string
		cfg  Config
	}{
		{"empty files", Config{}},
		{"duplicate files", Config{Files: []string{validFile, "./" + validFile}}},
		{"negative workers", Config{Files: []string{validFile}, Workers: -1}},
		{"negative batch", Config{Files: []string{validFile}, BatchSize: -1}},
		{"excessive batch", Config{Files: []string{validFile}, BatchSize: MaxBatchSize + 1}},
		{"negative max readers", Config{Files: []string{validFile}, MaxReaders: -1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Run(context.Background(), tt.cfg); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("err = %v, want ErrInvalidConfig", err)
			}
		})
	}
}
