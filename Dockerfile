# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Install git and ca-certificates
RUN apk add --no-cache git ca-certificates

# Copy module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o cnpj-etl ./cmd/cnpj-etl

# Final minimal runtime image
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata
ENV TZ=America/Sao_Paulo

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/cnpj-etl /app/cnpj-etl

# Create data directories
RUN mkdir -p /app/data/zip /app/data/extracted

VOLUME ["/app/data"]

EXPOSE 8080

ENTRYPOINT ["/app/cnpj-etl"]
