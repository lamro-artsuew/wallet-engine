# Build stage
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /wallet-engine ./cmd/server

# Runtime stage
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata
RUN adduser -D -u 1000 appuser

WORKDIR /app
COPY --from=builder /wallet-engine .
COPY migrations/ ./migrations/
COPY config/ ./config/

USER appuser

EXPOSE 8080 9090

ENTRYPOINT ["/app/wallet-engine"]
