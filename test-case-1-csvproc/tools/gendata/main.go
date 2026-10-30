// Command gendata generates deterministic multi-file CSV datasets of varying
// column schemas and sizes (with a configurable bad-row rate) for testing and
// benchmarking csvproc.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
)

func main() {
	var (
		outDir    string
		files     int
		rows      int
		errorRate float64
		seed      int64
	)

	flag.StringVar(&outDir, "out", "./data", "Output directory for generated CSV files")
	flag.IntVar(&files, "files", 12, "Number of CSV files to generate")
	flag.IntVar(&rows, "rows", 50_000, "Number of rows per CSV file")
	flag.Float64Var(&errorRate, "error-rate", 0.01, "Fraction of intentionally malformed rows (0.0 to 1.0)")
	flag.Int64Var(&seed, "seed", 2026, "Deterministic random seed")
	flag.Parse()

	if err := generate(outDir, files, rows, errorRate, seed); err != nil {
		fmt.Fprintf(os.Stderr, "gendata error: %v\n", err)
		os.Exit(1)
	}
}

func generate(outDir string, files, rows int, errorRate float64, seed int64) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	schemas := []struct {
		prefix string
		header string
		write  func(w *bufio.Writer, r *rand.Rand, rowIdx int)
	}{
		{
			prefix: "sales",
			header: "tx_id,outlet_id,qty,amount,channel\n",
			write: func(w *bufio.Writer, r *rand.Rand, rowIdx int) {
				channels := [...]string{"pos", "online", "partner", ""}
				fmt.Fprintf(w, "TX-%d,OUT-%02d,%d,%.2f,%s\n",
					rowIdx, r.Intn(20)+1, r.Intn(10)+1, float64(r.Intn(50000)+1000)/100.0, channels[r.Intn(len(channels))])
			},
		},
		{
			prefix: "inventory",
			header: "sku,warehouse,stock,reorder_level\n",
			write: func(w *bufio.Writer, r *rand.Rand, rowIdx int) {
				fmt.Fprintf(w, "SKU-%05d,WH-%d,%d,%d\n",
					rowIdx%10000, r.Intn(5)+1, r.Intn(500), r.Intn(50))
			},
		},
		{
			prefix: "telemetry",
			header: "device_id,latency_ms,cpu_pct,status,region\n",
			write: func(w *bufio.Writer, r *rand.Rand, rowIdx int) {
				regions := [...]string{"id-jkt", "id-sby", "id-bdg", "sg-1"}
				fmt.Fprintf(w, "DEV-%04d,%d,%.1f,OK,%s\n",
					rowIdx%2000, r.Intn(250)+1, r.Float64()*100.0, regions[r.Intn(len(regions))])
			},
		},
	}

	for fIdx := 0; fIdx < files; fIdx++ {
		sc := schemas[fIdx%len(schemas)]
		fileName := fmt.Sprintf("%s_%02d.csv", sc.prefix, fIdx+1)
		fullPath := filepath.Join(outDir, fileName)

		f, err := os.Create(fullPath)
		if err != nil {
			return err
		}

		bw := bufio.NewWriterSize(f, 128*1024)
		_, _ = bw.WriteString(sc.header)
		rng := rand.New(rand.NewSource(seed + int64(fIdx)))

		for rIdx := 1; rIdx <= rows; rIdx++ {
			if errorRate > 0 && rng.Float64() < errorRate {
				if rIdx%2 == 0 {
					_, _ = bw.WriteString("malformed_row_too_few_cols\n")
				} else {
					_, _ = bw.WriteString(" , , \n")
				}
				continue
			}
			sc.write(bw, rng, rIdx)
		}

		if err := bw.Flush(); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}

	fmt.Printf("Generated %d CSV files (%d rows each, %.1f%% error rate) in %s\n",
		files, rows, errorRate*100, outDir)
	return nil
}
