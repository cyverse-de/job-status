### Build stage
FROM golang:1.27 AS builder

WORKDIR /build

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary; let the build platform determine GOOS/GOARCH.
ENV CGO_ENABLED=0

RUN go build -ldflags='-w -s' -o job-status ./cmd/job-status

### Final stage - minimal runtime image
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /

COPY --from=builder /build/job-status /job-status

EXPOSE 60000

ENTRYPOINT ["/job-status"]
