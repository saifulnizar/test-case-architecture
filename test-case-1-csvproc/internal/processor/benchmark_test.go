package processor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkPipeline(b *testing.B) {
	dir := b.TempDir()
	const numFiles = 8
	const rowsPerFile = 5_000

	var sb strings.Builder
	sb.WriteString("id,outlet,qty,amount,region\n")
	for i := 0; i < rowsPerFile; i++ {
		fmt.Fprintf(&sb, "%d,OUT-%d,%d,%d.50,ID-JKT\n", i, i%20, (i%10)+1, (i%500)*100)
	}
	payload := []byte(sb.String())

	var files []string
	for i := 0; i < numFiles; i++ {
		p := filepath.Join(dir, fmt.Sprintf("bench_%d.csv", i))
		if err := os.WriteFile(p, payload, 0o600); err != nil {
			b.Fatal(err)
		}
		files = append(files, p)
	}

	for _, workers := range []int{1, 4, 8} {
		for _, batchSize := range []int{50, 500, 2000} {
			name := fmt.Sprintf("workers=%d/batch=%d", workers, batchSize)
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(payload) * numFiles))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, err := Run(context.Background(), Config{
						Files:      files,
						Workers:    workers,
						BatchSize:  batchSize,
						MaxReaders: 8,
					})
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
