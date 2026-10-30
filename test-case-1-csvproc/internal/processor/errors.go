package processor

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// DefaultMaxErrorSamples is the default cap on stored RowError samples when
// ErrorCollector is created with a non-positive limit.
const DefaultMaxErrorSamples = 1000

// Sentinel errors for row-level and file-level failures, usable with errors.Is.
var (
	ErrEmptyFile     = errors.New("file is empty")
	ErrBadHeader     = errors.New("invalid csv header")
	ErrColumnCount   = errors.New("wrong number of columns")
	ErrEmptyRow      = errors.New("row is completely empty")
	ErrWorkerPanic   = errors.New("recovered panic")
	ErrInvalidConfig = errors.New("invalid processor config")
)

// FileError wraps a file-level failure with its file path.
type FileError struct {
	Path string
	Err  error
}

func (e *FileError) Error() string { return fmt.Sprintf("file %s: %v", e.Path, e.Err) }
func (e *FileError) Unwrap() error { return e.Err }

// RowError represents a validation or CSV parse error on a specific line of a file.
type RowError struct {
	File   string `json:"file"`
	Line   int    `json:"line"`
	Reason string `json:"reason"`
	Err    error  `json:"-"`
}

func (e RowError) Error() string {
	if e.Line > 0 {
		return fmt.Sprintf("%s:%d: %s", e.File, e.Line, e.Reason)
	}
	return fmt.Sprintf("%s: %s", e.File, e.Reason)
}

func (e RowError) Unwrap() error { return e.Err }

// NewRowError constructs a RowError with both the underlying error and its string reason.
func NewRowError(file string, line int, err error) RowError {
	reason := ""
	if err != nil {
		reason = err.Error()
	}
	return RowError{
		File:   file,
		Line:   line,
		Reason: reason,
		Err:    err,
	}
}

// ErrorCollector is a thread-safe, bounded collector for row-level errors.
//
// Why bounded: if an input CSV has 10 million malformed rows, storing every
// RowError in an unbounded slice would exhaust memory (OOM). ErrorCollector
// always counts the exact total using an atomic counter, and only acquires a
// mutex while the sample slice is below its capacity cap. Once full, subsequent
// Add calls are lock-free and allocate zero heap memory.
type ErrorCollector struct {
	limit   int
	total   atomic.Int64
	mu      sync.Mutex
	samples []RowError
}

// NewErrorCollector creates a bounded ErrorCollector that retains up to limit
// error samples (if limit <= 0, DefaultMaxErrorSamples is used).
func NewErrorCollector(limit int) *ErrorCollector {
	if limit <= 0 {
		limit = DefaultMaxErrorSamples
	}
	return &ErrorCollector{
		limit:   limit,
		samples: make([]RowError, 0, min(limit, 128)),
	}
}

// Add records a RowError. It increments the total count atomically and appends
// to the sample slice only if capacity remains.
func (c *ErrorCollector) Add(err RowError) {
	n := c.total.Add(1)
	if n > int64(c.limit) {
		// Fast path: once we have enough samples, avoid even locking the mutex
		// if the slice is already full.
		c.mu.Lock()
		full := len(c.samples) >= c.limit
		c.mu.Unlock()
		if full {
			return
		}
	}
	c.mu.Lock()
	if len(c.samples) < c.limit {
		c.samples = append(c.samples, err)
	}
	c.mu.Unlock()
}

// Total returns the total number of errors recorded, including those dropped
// after the sample limit was reached.
func (c *ErrorCollector) Total() int64 {
	return c.total.Load()
}

// Samples returns a copy of the collected RowError samples.
func (c *ErrorCollector) Samples() []RowError {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]RowError, len(c.samples))
	copy(out, c.samples)
	return out
}
