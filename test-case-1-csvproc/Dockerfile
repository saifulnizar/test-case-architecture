# Stage 1: Build static binaries
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Cache dependencies before copying source code
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/csvproc ./cmd/csvproc && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/gendata ./tools/gendata

# Stage 2: Minimal non-root runtime image
FROM alpine:3.21

RUN adduser -D -u 10001 appuser && \
    mkdir -p /app/data /app/output && \
    chown -R appuser:appuser /app

WORKDIR /app
COPY --from=builder /out/csvproc /usr/local/bin/csvproc
COPY --from=builder /out/gendata /usr/local/bin/gendata
COPY --chown=appuser:appuser testdata ./testdata

USER appuser

ENTRYPOINT ["csvproc"]
CMD ["-input", "/app/testdata", "-output", "/app/output/report.json"]
