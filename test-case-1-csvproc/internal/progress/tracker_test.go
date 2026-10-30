package progress

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestTrackerHooksAndSnapshotConcurrency(t *testing.T) {
	tr := NewTracker()
	hooks := tr.Hooks()
	hooks.OnFilesTotal(10)
	hooks.OnBytesTotal(8000)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 250; j++ {
				hooks.OnBytesRead(2)
				hooks.OnRowsRead(2)
				hooks.OnRowsProcessed(1)
				hooks.OnRowsFailed(1)
			}
			hooks.OnFileCompleted()
		}()
	}
	wg.Wait()

	snap := tr.Snapshot()
	if snap.FilesTotal != 10 || snap.FilesCompleted != 8 {
		t.Fatalf("files = %d/%d, want 8/10", snap.FilesCompleted, snap.FilesTotal)
	}
	if snap.BytesTotal != 8000 || snap.BytesRead != 4000 || snap.Percent != 50.0 {
		t.Fatalf("bytes = %d/%d (%.1f%%), want 4000/8000 (50.0%%)", snap.BytesRead, snap.BytesTotal, snap.Percent)
	}
	if snap.RowsRead != 4000 || snap.RowsProcessed != 2000 || snap.RowsFailed != 2000 {
		t.Fatalf("rows read=%d ok=%d err=%d, want 4000/2000/2000", snap.RowsRead, snap.RowsProcessed, snap.RowsFailed)
	}
	if snap.RowsPerSec <= 0 {
		t.Fatalf("expected positive RowsPerSec, got %v", snap.RowsPerSec)
	}
	line := snap.FormatLine("[test]")
	if !strings.Contains(line, "50.0%") || !strings.Contains(line, "files=8/10") || !strings.Contains(line, "read=4000 ok=2000 err=2000") {
		t.Fatalf("unexpected format line: %s", line)
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestStartReporterTickFileEventAndIdempotentStop(t *testing.T) {
	tr := NewTracker()
	hooks := tr.Hooks()
	hooks.OnFilesTotal(2)
	hooks.OnBytesTotal(200)
	hooks.OnBytesRead(100)
	hooks.OnRowsRead(100)
	hooks.OnRowsProcessed(95)
	hooks.OnRowsFailed(5)

	var out syncBuffer
	ctx := context.Background()
	stop := tr.StartReporter(ctx, &out, 50*time.Millisecond)

	// Trigger event-driven file completion immediately
	hooks.OnFileCompleted()

	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(out.String(), "[progress]") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	hooks.OnBytesRead(150) // exceeds 200 -> clamped to 100.0%
	hooks.OnFileCompleted()
	stop()
	stop() // idempotent second call must not panic or block

	got := out.String()
	if !strings.Contains(got, "[progress]") {
		t.Fatalf("expected [progress] line in output, got:\n%s", got)
	}
	if !strings.Contains(got, "[done]") || !strings.Contains(got, "100.0%") {
		t.Fatalf("expected [done] with 100.0%% in output, got:\n%s", got)
	}
	goleak.VerifyNone(t)
}

func TestStartReporterExitsOnContextCancel(t *testing.T) {
	tr := NewTracker()
	hooks := tr.Hooks()
	hooks.OnFilesTotal(4)
	hooks.OnFileCompleted() // fallback percentage by file count when BytesTotal == 0

	var out syncBuffer
	ctx, cancel := context.WithCancel(context.Background())

	stop := tr.StartReporter(ctx, &out, time.Hour)
	cancel()
	stop()

	if !strings.Contains(out.String(), "[done]") || !strings.Contains(out.String(), "25.0%") {
		t.Fatalf("expected [done] with 25.0%% on context cancel, got: %q", out.String())
	}
	goleak.VerifyNone(t)
}

func TestStartReporterDisabledWhenZeroIntervalOrNilWriter(t *testing.T) {
	tr := NewTracker()
	var out syncBuffer
	stop1 := tr.StartReporter(context.Background(), &out, 0)
	stop1()
	if out.String() != "" {
		t.Fatalf("expected no output when interval=0, got %q", out.String())
	}

	stop2 := tr.StartReporter(context.Background(), nil, 10*time.Millisecond)
	stop2()
}
