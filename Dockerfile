# Build stage
FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Copy dependency files
COPY go.mod go.sum ./
# If go.sum doesn't exist, we will create it or tidy it first
# Let's run go mod tidy during development, but copy it here.
# Since we haven't run go mod tidy yet, let's just copy go.mod first or create a dummy go.sum.
# Better to copy go.mod and then run go mod tidy inside the Dockerfile or copy everything.
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o main ./cmd/server

# Final stage
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/main .
COPY --from=builder /app/migrations ./migrations

EXPOSE 8080 9090

ENTRYPOINT ["./main"]
