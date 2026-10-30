// Package processor implements a schema-agnostic, streaming concurrent CSV
// file processor using a bounded reader pool and a fixed worker pool.
package processor

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Row is a single raw CSV data record along with its 1-based physical line
// number in the source file (line 1 is the header).
type Row struct {
	Line   int
	Fields []string
}

// FileMeta holds immutable metadata for a single CSV file. It is created once
// when the reader parses the file header and shared read-only across batches.
type FileMeta struct {
	Path   string
	Header []string
}

// Batch groups multiple rows from a single file to amortize channel
// synchronization overhead across BatchSize records.
type Batch struct {
	Meta *FileMeta
	Rows []Row
}

// ColumnStats tracks statistics for a single CSV column across rows.
//
// Determinism note: floating-point addition is not associative, so summing
// float64 across concurrent workers would yield slight variations depending on
// batch scheduling. Therefore, Min and Max use float64 (order-independent),
// while exact summation uses int64 (IntSum) with overflow protection.
type ColumnStats struct {
	Name           string  `json:"name"`
	NonEmpty       int64   `json:"non_empty"`
	Empty          int64   `json:"empty"`
	NumericCount   int64   `json:"numeric_count"`
	Min            float64 `json:"min,omitempty"`
	Max            float64 `json:"max,omitempty"`
	IntCount       int64   `json:"int_count"`
	IntSum         int64   `json:"int_sum"`
	IntSumOverflow bool    `json:"int_sum_overflow,omitempty"`
}

// Observe records a single cell value into the column's statistics.
func (c *ColumnStats) Observe(raw string) {
	v := strings.TrimSpace(raw)
	if v == "" {
		c.Empty++
		return
	}
	c.NonEmpty++

	f, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return
	}
	if c.NumericCount == 0 {
		c.Min, c.Max = f, f
	} else {
		c.Min = math.Min(c.Min, f)
		c.Max = math.Max(c.Max, f)
	}
	c.NumericCount++

	if i, err := strconv.ParseInt(v, 10, 64); err == nil {
		c.IntCount++
		c.addInt(i)
	}
}

func (c *ColumnStats) addInt(v int64) {
	if c.IntSumOverflow {
		return
	}
	if (v > 0 && c.IntSum > math.MaxInt64-v) || (v < 0 && c.IntSum < math.MinInt64-v) {
		c.IntSumOverflow = true
		return
	}
	c.IntSum += v
}

func (c *ColumnStats) merge(other *ColumnStats) {
	c.NonEmpty += other.NonEmpty
	c.Empty += other.Empty
	if other.NumericCount > 0 {
		if c.NumericCount == 0 {
			c.Min, c.Max = other.Min, other.Max
		} else {
			c.Min = math.Min(c.Min, other.Min)
			c.Max = math.Max(c.Max, other.Max)
		}
		c.NumericCount += other.NumericCount
	}
	c.IntCount += other.IntCount
	if other.IntSumOverflow {
		c.IntSumOverflow = true
	}
	c.addInt(other.IntSum)
}

// FileStats holds the processing outcome and per-column statistics for one file.
type FileStats struct {
	Path          string        `json:"path"`
	RowsRead      int64         `json:"rows_read"`
	RowsProcessed int64         `json:"rows_processed"`
	RowsFailed    int64         `json:"rows_failed"`
	Columns       []ColumnStats `json:"columns,omitempty"`
	Err           error         `json:"-"`
	ErrorMsg      string        `json:"error,omitempty"`
}

// newFileStats initializes a FileStats slice of ColumnStats from a file header.
func newFileStats(meta *FileMeta) *FileStats {
	cols := make([]ColumnStats, len(meta.Header))
	for i, h := range meta.Header {
		cols[i].Name = strings.Clone(h)
	}
	return &FileStats{
		Path:    strings.Clone(meta.Path),
		Columns: cols,
	}
}

// ProcessRow validates and aggregates a single CSV row into the file's stats.
// It returns a descriptive error if the row is invalid (wrong column count or
// completely blank), leaving the column aggregates untouched.
func (fs *FileStats) ProcessRow(row Row) error {
	if len(row.Fields) != len(fs.Columns) {
		return fmt.Errorf("%w: got %d columns, want %d", ErrColumnCount, len(row.Fields), len(fs.Columns))
	}
	if isBlankRow(row.Fields) {
		return ErrEmptyRow
	}
	fs.RowsProcessed++
	for i := range fs.Columns {
		fs.Columns[i].Observe(row.Fields[i])
	}
	return nil
}

// Merge combines another worker-local FileStats for the same file into fs.
func (fs *FileStats) Merge(other *FileStats) error {
	if len(fs.Columns) != len(other.Columns) {
		return fmt.Errorf("merge %q: column count mismatch (%d vs %d)", fs.Path, len(fs.Columns), len(other.Columns))
	}
	fs.RowsProcessed += other.RowsProcessed
	fs.RowsFailed += other.RowsFailed
	for i := range fs.Columns {
		fs.Columns[i].merge(&other.Columns[i])
	}
	return nil
}

func isBlankRow(fields []string) bool {
	for _, f := range fields {
		if strings.TrimSpace(f) != "" {
			return false
		}
	}
	return true
}

// NormalizeHeader strips an optional UTF-8 BOM from the first column and trims
// leading/trailing whitespace from all column names.
func NormalizeHeader(raw []string) []string {
	out := make([]string, len(raw))
	for i, col := range raw {
		if i == 0 {
			col = strings.TrimPrefix(col, "\ufeff")
		}
		out[i] = strings.TrimSpace(col)
	}
	return out
}
