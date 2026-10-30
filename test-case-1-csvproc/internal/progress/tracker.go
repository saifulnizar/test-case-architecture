// Package progress provides lock-free atomic counters and a hybrid
// event-driven + time-driven reporter to track real-time CSV processing
// progress (including 0.0% - 100.0% byte percentage) without slowing down
// reader or worker goroutines.
package progress

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"csvproc/internal/processor"
)

// Snapshot represents a point-in-time reading of all pipeline counters.
type Snapshot struct {
	FilesTotal     int64
	FilesCompleted int64
	BytesTotal     int64
	BytesRead      int64
	Percent        float64
	RowsRead       int64
	RowsProcessed  int64
	RowsFailed     int64
	Elapsed        time.Duration
	RowsPerSec     float64
}

// Tracker maintains lock-free atomic counters for concurrent readers and workers,
// plus a non-blocking buffered notification channel so file-completion events
// immediately trigger a progress update even on fast/small runs.
type Tracker struct {
	start          time.Time
	filesTotal     atomic.Int64
	filesCompleted atomic.Int64
	bytesTotal     atomic.Int64
	bytesRead      atomic.Int64
	rowsRead       atomic.Int64
	rowsProcessed  atomic.Int64
	rowsFailed     atomic.Int64
	fileEventCh    chan struct{}
}

// NewTracker initializes a Tracker with the current start time.
func NewTracker() *Tracker {
	return &Tracker{
		start:       time.Now(),
		fileEventCh: make(chan struct{}, 1),
	}
}

// Hooks returns a processor.ProgressHooks struct wired to this Tracker's atomic
// counters so the processor pipeline can update progress lock-free.
func (t *Tracker) Hooks() processor.ProgressHooks {
	return processor.ProgressHooks{
		OnFilesTotal: func(total int64) { t.filesTotal.Store(total) },
		OnBytesTotal: func(total int64) { t.bytesTotal.Store(total) },
		OnBytesRead:  func(delta int64) { t.bytesRead.Add(delta) },
		OnFileCompleted: func() {
			t.filesCompleted.Add(1)
			// Non-blocking send: notify the reporter goroutine that a file finished
			// without ever blocking the reader goroutine.
			select {
			case t.fileEventCh <- struct{}{}:
			default:
			}
		},
		OnRowsRead:      func(delta int64) { t.rowsRead.Add(delta) },
		OnRowsProcessed: func(delta int64) { t.rowsProcessed.Add(delta) },
		OnRowsFailed:    func(delta int64) { t.rowsFailed.Add(delta) },
	}
}

// Snapshot reads the current atomic counters and computes percentage, elapsed
// time, and throughput.
func (t *Tracker) Snapshot() Snapshot {
	elapsed := time.Since(t.start)
	fTotal := t.filesTotal.Load()
	fDone := t.filesCompleted.Load()
	bTotal := t.bytesTotal.Load()
	bRead := t.bytesRead.Load()
	read := t.rowsRead.Load()
	proc := t.rowsProcessed.Load()
	fail := t.rowsFailed.Load()

	var pct float64
	switch {
	case bTotal > 0:
		pct = (float64(bRead) / float64(bTotal)) * 100.0
		if pct > 100.0 {
			pct = 100.0
		}
	case fTotal > 0:
		pct = (float64(fDone) / float64(fTotal)) * 100.0
	}

	var rps float64
	if secs := elapsed.Seconds(); secs > 0 {
		rps = float64(proc+fail) / secs
	}
	return Snapshot{
		FilesTotal:     fTotal,
		FilesCompleted: fDone,
		BytesTotal:     bTotal,
		BytesRead:      bRead,
		Percent:        pct,
		RowsRead:       read,
		RowsProcessed:  proc,
		RowsFailed:     fail,
		Elapsed:        elapsed,
		RowsPerSec:     rps,
	}
}

// FormatLine formats a Snapshot as a single human-readable status line with 0.0% - 100.0%.
func (s Snapshot) FormatLine(prefix string) string {
	return fmt.Sprintf(
		"%-10s %5.1f%% | files=%d/%d | read=%d ok=%d err=%d | rate=%.0f rows/s | elapsed=%s",
		prefix,
		s.Percent,
		s.FilesCompleted,
		s.FilesTotal,
		s.RowsRead,
		s.RowsProcessed,
		s.RowsFailed,
		s.RowsPerSec,
		s.Elapsed.Round(time.Millisecond),
	)
}

// StartReporter launches a background goroutine that writes progress lines to
// out both periodically (every interval via time.Ticker) AND whenever a file
// completes (event-driven). Calling the returned stop function stops the
// reporter, waits for the goroutine to exit, and writes one final "[done]" line.
//
// If interval <= 0 or out == nil, StartReporter is a no-op and returns a
// safe no-op stop function.
func (t *Tracker) StartReporter(ctx context.Context, out io.Writer, interval time.Duration) (stop func()) {
	if interval <= 0 || out == nil {
		return func() {}
	}

	stopCh := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				_, _ = fmt.Fprintln(out, t.Snapshot().FormatLine("[progress]"))
			case <-t.fileEventCh:
				_, _ = fmt.Fprintln(out, t.Snapshot().FormatLine("[progress]"))
			case <-stopCh:
				_, _ = fmt.Fprintln(out, t.Snapshot().FormatLine("[done]"))
				return
			case <-ctx.Done():
				_, _ = fmt.Fprintln(out, t.Snapshot().FormatLine("[done]"))
				return
			}
		}
	}()

	return func() {
		once.Do(func() {
			close(stopCh)
		})
		wg.Wait()
	}
}
