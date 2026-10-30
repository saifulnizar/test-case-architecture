package processor

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
)

const (
	// DefaultBatchSize is the number of rows grouped into a single channel send.
	DefaultBatchSize = 500
	// MaxBatchSize prevents an excessively large -batch-size flag from
	// pre-allocating gigabytes of slice capacity per reader.
	MaxBatchSize = 100_000

	readBufferSize = 64 * 1024 // 64 KiB streaming read buffer per active file
	ctxCheckMask   = 1023      // check ctx.Err() every 1024 rows even if send doesn't block
)

// SendBatchFunc delivers a Batch downstream to the jobs channel and respects ctx.
type SendBatchFunc func(ctx context.Context, b Batch) error

// ReadOutcome summarizes what a single file reader accomplished before finishing.
type ReadOutcome struct {
	Meta        *FileMeta
	RowsRead    int64
	ParseErrors int64
}

// byteCounterReader wraps an io.Reader and reports the number of bytes read
// from the underlying file to an atomic callback. Because it sits underneath
// bufio.NewReaderSize(64 KiB), Read is only invoked once per 64 KiB chunk,
// adding zero per-row overhead while enabling 0.0% - 100.0% byte progress.
type byteCounterReader struct {
	r      io.Reader
	onRead func(delta int64)
}

func (b *byteCounterReader) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if n > 0 && b.onRead != nil {
		b.onRead(int64(n))
	}
	return n, err
}

// ReadCSVFile streams a single CSV file line-by-line, parses its header on line 1,
// groups subsequent records into batches of size batchSize, and sends each batch
// via send.
//
// Memory guarantee: ReadCSVFile never loads the entire file into memory. At any
// moment it holds only one 64 KiB bufio buffer and at most one Batch of rows.
// After passing a Batch to send, it allocates a fresh slice backing array so the
// worker receiving the Batch exclusively owns the slice (zero data race).
func ReadCSVFile(
	ctx context.Context,
	path string,
	batchSize int,
	errCollector *ErrorCollector,
	onBytesRead func(delta int64),
	onRowsRead func(delta int64),
	onRowsFailed func(delta int64),
	send SendBatchFunc,
) (out ReadOutcome, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &FileError{Path: path, Err: fmt.Errorf("%w: %v", ErrWorkerPanic, r)}
		}
	}()

	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	if batchSize > MaxBatchSize {
		return out, fmt.Errorf("%w: batch size %d exceeds max %d", ErrInvalidConfig, batchSize, MaxBatchSize)
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}

	f, err := os.Open(path)
	if err != nil {
		var pe *fs.PathError
		if errors.As(err, &pe) {
			err = pe.Err
		}
		return out, &FileError{Path: path, Err: fmt.Errorf("open: %w", err)}
	}
	defer func() { _ = f.Close() }()

	var srcReader io.Reader = f
	if onBytesRead != nil {
		srcReader = &byteCounterReader{r: f, onRead: onBytesRead}
	}

	cr := csv.NewReader(bufio.NewReaderSize(srcReader, readBufferSize))
	// Disable encoding/csv's automatic column-count rejection so a single row with
	// missing/extra columns is handled by the worker as a row-level validation
	// error with exact context, rather than halting or poisoning the reader.
	cr.FieldsPerRecord = -1

	rawHeader, err := cr.Read()
	switch {
	case errors.Is(err, io.EOF):
		return out, &FileError{Path: path, Err: ErrEmptyFile}
	case err != nil:
		return out, &FileError{Path: path, Err: fmt.Errorf("%w: %w", ErrBadHeader, err)}
	}

	header := NormalizeHeader(rawHeader)
	if len(header) == 0 || isBlankRow(header) {
		return out, &FileError{Path: path, Err: fmt.Errorf("%w: header has no column names", ErrBadHeader)}
	}

	meta := &FileMeta{Path: path, Header: header}
	out.Meta = meta

	rows := make([]Row, 0, batchSize)
	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		if err := send(ctx, Batch{Meta: meta, Rows: rows}); err != nil {
			return err
		}
		rows = make([]Row, 0, batchSize)
		return nil
	}

	for {
		if out.RowsRead&ctxCheckMask == 0 {
			if err := ctx.Err(); err != nil {
				return out, err
			}
		}

		rec, err := cr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			var pe *csv.ParseError
			if !errors.As(err, &pe) {
				return out, &FileError{Path: path, Err: fmt.Errorf("read: %w", err)}
			}
			out.RowsRead++
			out.ParseErrors++
			if onRowsRead != nil {
				onRowsRead(1)
			}
			if onRowsFailed != nil {
				onRowsFailed(1)
			}
			if errCollector != nil {
				errCollector.Add(NewRowError(path, pe.StartLine, pe.Err))
			}
			continue
		}

		line, _ := cr.FieldPos(0)
		rows = append(rows, Row{Line: line, Fields: rec})
		out.RowsRead++
		if onRowsRead != nil {
			onRowsRead(1)
		}

		if len(rows) == batchSize {
			if err := flush(); err != nil {
				return out, err
			}
		}
	}

	if err := flush(); err != nil {
		return out, err
	}
	return out, nil
}
