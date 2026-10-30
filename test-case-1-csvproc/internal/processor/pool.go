package processor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

const (
	// DefaultMaxReaders caps how many CSV files can be open simultaneously.
	DefaultMaxReaders = 8
	// jobsBufferPerWorker sizes the buffered jobs channel (Workers * 2).
	jobsBufferPerWorker = 2
)

// ProgressHooks defines optional atomic counter callbacks so the pipeline can
// report real-time progress (including 0-100% byte percentage and file completion
// events) without coupling to a concrete progress package.
type ProgressHooks struct {
	OnFilesTotal    func(total int64)
	OnBytesTotal    func(total int64)
	OnBytesRead     func(delta int64)
	OnFileCompleted func()
	OnRowsRead      func(delta int64)
	OnRowsProcessed func(delta int64)
	OnRowsFailed    func(delta int64)
}

// Config configures the concurrent CSV processor pipeline.
type Config struct {
	Files      []string
	Workers    int           // default runtime.NumCPU()
	BatchSize  int           // default DefaultBatchSize (500)
	MaxReaders int           // default min(len(Files), DefaultMaxReaders)
	MaxErrors  int           // default DefaultMaxErrorSamples (1000)
	Progress   ProgressHooks // optional callbacks for live progress tracking

	// Unexported hooks for deterministic concurrency tests.
	testReaderStart func(index int)
	testReaderEnd   func(index int)
	testRowHook     func(file string, row Row)
}

// Result holds the final merged output of a pipeline run.
type Result struct {
	Files       []FileStats `json:"files"`
	TotalErrors int64       `json:"total_errors"`
	Errors      []RowError  `json:"errors,omitempty"`
	Partial     bool        `json:"partial"`
}

func (c Config) validateAndNormalize() (Config, error) {
	if len(c.Files) == 0 {
		return c, fmt.Errorf("%w: no input files provided", ErrInvalidConfig)
	}
	seen := make(map[string]struct{}, len(c.Files))
	for _, f := range c.Files {
		clean := filepath.Clean(f)
		if _, exists := seen[clean]; exists {
			return c, fmt.Errorf("%w: duplicate file path %q", ErrInvalidConfig, f)
		}
		seen[clean] = struct{}{}
	}
	if c.Workers < 0 {
		return c, fmt.Errorf("%w: workers cannot be negative (%d)", ErrInvalidConfig, c.Workers)
	}
	if c.Workers == 0 {
		c.Workers = runtime.NumCPU()
	}
	if c.BatchSize < 0 || c.BatchSize > MaxBatchSize {
		return c, fmt.Errorf("%w: batch size %d out of range (1..%d)", ErrInvalidConfig, c.BatchSize, MaxBatchSize)
	}
	if c.BatchSize == 0 {
		c.BatchSize = DefaultBatchSize
	}
	if c.MaxReaders < 0 {
		return c, fmt.Errorf("%w: max readers cannot be negative (%d)", ErrInvalidConfig, c.MaxReaders)
	}
	if c.MaxReaders == 0 {
		c.MaxReaders = min(len(c.Files), DefaultMaxReaders)
	}
	c.MaxReaders = min(c.MaxReaders, len(c.Files))
	if c.MaxErrors <= 0 {
		c.MaxErrors = DefaultMaxErrorSamples
	}
	return c, nil
}

// Run executes the two-level concurrent CSV processing pipeline:
//
//  1. Pre-calculates total file sizes via os.Stat (O(1) metadata lookup, zero file
//     content loaded into RAM) so 0.0% - 100.0% progress is available while streaming.
//  2. Producer coordinator dispatches up to MaxReaders concurrent file readers.
//  3. Readers stream lines into Batch structs and send them to a buffered jobs
//     channel (capacity = Workers * 2), providing automatic backpressure.
//  4. A fixed pool of Workers consumes batches, validates rows, and updates
//     worker-local FileStats maps (zero mutex contention on the hot path).
//  5. Once all readers finish, the producer coordinator closes jobs; once all
//     workers drain/exit, Run merges the worker-local stats into Result.
func Run(ctx context.Context, cfg Config) (Result, error) {
	cfg, err := cfg.validateAndNormalize()
	if err != nil {
		return Result{}, err
	}

	if cfg.Progress.OnFilesTotal != nil {
		cfg.Progress.OnFilesTotal(int64(len(cfg.Files)))
	}
	if cfg.Progress.OnBytesTotal != nil {
		var totalBytes int64
		for _, path := range cfg.Files {
			if info, statErr := os.Stat(path); statErr == nil && !info.IsDir() {
				totalBytes += info.Size()
			}
		}
		cfg.Progress.OnBytesTotal(totalBytes)
	}

	errCollector := NewErrorCollector(cfg.MaxErrors)
	jobs := make(chan Batch, cfg.Workers*jobsBufferPerWorker)
	fileReports := make([]FileStats, len(cfg.Files))
	producerDone := make(chan struct{})

	go dispatchReaders(ctx, cfg, jobs, errCollector, fileReports, producerDone)

	workers := make([]*worker, cfg.Workers)
	var workerWG sync.WaitGroup
	for i := range workers {
		w := &worker{
			local:       make(map[string]*FileStats),
			errCollect:  errCollector,
			progress:    cfg.Progress,
			testRowHook: cfg.testRowHook,
		}
		workers[i] = w
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			w.run(ctx, jobs)
		}()
	}

	workerWG.Wait()
	<-producerDone

	// Merge worker-local FileStats maps into a single map by file path.
	mergedByFile := make(map[string]*FileStats, len(cfg.Files))
	for _, w := range workers {
		for path, st := range w.local {
			dst, ok := mergedByFile[path]
			if !ok {
				mergedByFile[path] = st
				continue
			}
			if mergeErr := dst.Merge(st); mergeErr != nil {
				return Result{}, mergeErr
			}
		}
	}

	partial := len(jobs) > 0
	for i := range fileReports {
		rep := &fileReports[i]
		if st, ok := mergedByFile[rep.Path]; ok {
			rep.RowsProcessed = st.RowsProcessed
			rep.RowsFailed += st.RowsFailed
			rep.Columns = st.Columns
		}
		if rep.Err != nil {
			rep.ErrorMsg = rep.Err.Error()
			if errors.Is(rep.Err, context.Canceled) || errors.Is(rep.Err, context.DeadlineExceeded) {
				partial = true
			}
		}
	}

	res := Result{
		Files:       fileReports,
		TotalErrors: errCollector.Total(),
		Errors:      errCollector.Samples(),
		Partial:     partial,
	}
	if partial {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return res, ctxErr
		}
		return res, context.Canceled
	}
	return res, nil
}

// dispatchReaders coordinates all file readers and is the SOLE owner/closer of
// the jobs channel. By acquiring the semaphore slot before spawning each reader
// and waiting on readerWG before closing jobs, "send on closed channel" is
// structurally impossible.
func dispatchReaders(
	ctx context.Context,
	cfg Config,
	jobs chan<- Batch,
	errCollector *ErrorCollector,
	reports []FileStats,
	done chan<- struct{},
) {
	var readerWG sync.WaitGroup
	defer close(done)
	defer close(jobs)
	defer readerWG.Wait()

	send := func(ctx context.Context, b Batch) error {
		select {
		case jobs <- b:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	sem := make(chan struct{}, cfg.MaxReaders)
	for i, path := range cfg.Files {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			for j := i; j < len(cfg.Files); j++ {
				reports[j] = FileStats{Path: cfg.Files[j], Err: ctx.Err()}
			}
			return
		}

		readerWG.Add(1)
		go func(idx int, filePath string) {
			defer readerWG.Done()
			defer func() { <-sem }()

			if cfg.testReaderStart != nil {
				cfg.testReaderStart(idx)
			}
			if cfg.testReaderEnd != nil {
				defer cfg.testReaderEnd(idx)
			}

			out, err := ReadCSVFile(
				ctx,
				filePath,
				cfg.BatchSize,
				errCollector,
				cfg.Progress.OnBytesRead,
				cfg.Progress.OnRowsRead,
				cfg.Progress.OnRowsFailed,
				send,
			)

			var cols []ColumnStats
			if out.Meta != nil {
				cols = newFileStats(out.Meta).Columns
			}
			reports[idx] = FileStats{
				Path:       filePath,
				RowsRead:   out.RowsRead,
				RowsFailed: out.ParseErrors,
				Columns:    cols,
				Err:        err,
			}
			if cfg.Progress.OnFileCompleted != nil && err == nil {
				cfg.Progress.OnFileCompleted()
			}
		}(i, path)
	}
}

type worker struct {
	local       map[string]*FileStats
	errCollect  *ErrorCollector
	progress    ProgressHooks
	testRowHook func(file string, row Row)
}

func (w *worker) run(ctx context.Context, jobs <-chan Batch) {
	for {
		select {
		case <-ctx.Done():
			return
		case b, ok := <-jobs:
			if !ok {
				return
			}
			w.processBatch(b)
		}
	}
}

func (w *worker) processBatch(b Batch) {
	st, ok := w.local[b.Meta.Path]
	if !ok {
		st = newFileStats(b.Meta)
		w.local[b.Meta.Path] = st
	}

	var okCount, failCount int64
	for _, row := range b.Rows {
		if err := w.processSingleRow(st, b.Meta.Path, row); err != nil {
			st.RowsFailed++
			failCount++
			w.errCollect.Add(NewRowError(b.Meta.Path, row.Line, err))
			continue
		}
		okCount++
	}

	if w.progress.OnRowsProcessed != nil && okCount > 0 {
		w.progress.OnRowsProcessed(okCount)
	}
	if w.progress.OnRowsFailed != nil && failCount > 0 {
		w.progress.OnRowsFailed(failCount)
	}
}

func (w *worker) processSingleRow(st *FileStats, path string, row Row) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrWorkerPanic, r)
		}
	}()
	if w.testRowHook != nil {
		w.testRowHook(path, row)
	}
	return st.ProcessRow(row)
}
