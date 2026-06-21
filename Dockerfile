# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Cache dependencies first
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY cmd/    ./cmd/
COPY internal/ ./internal/
COPY pkg/   ./pkg/

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /hapartition ./cmd/hapartition/

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /hapartition /hapartition

EXPOSE 6379 8080 7946

ENTRYPOINT ["/hapartition"]
