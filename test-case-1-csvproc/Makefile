.PHONY: build test race cover bench vet fmt gendata run run-large docker-build clean

GO ?= go
BINARY := bin/csvproc

build:
	@mkdir -p bin
	$(GO) build -o $(BINARY) ./cmd/csvproc

test:
	$(GO) test ./...

race:
	$(GO) test -race -count=3 ./...

cover:
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -n 1
	$(GO) tool cover -html=coverage.out -o coverage.html

bench:
	$(GO) test -bench=BenchmarkPipeline -benchmem ./internal/processor

vet:
	$(GO) vet ./...

fmt:
	gofmt -s -w .

gendata:
	$(GO) run ./tools/gendata -out ./data -files 12 -rows 1000000 -error-rate 0.01

run: build
	./$(BINARY) -input ./testdata -workers 4 -batch-size 500 -max-readers 4 -output report.json

run-large: build
	./$(BINARY) -input ./data -workers 4 -batch-size 500 -max-readers 4 -progress-interval 200ms -output report.json

docker-build:
	docker build -t csvproc:latest .

clean:
	rm -rf bin/ data/ report.json coverage.out coverage.html
