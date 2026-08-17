package logger

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/grpclog"
)

var Log *slog.Logger

type ContextHandler struct {
	slog.Handler
}

// Handle extracts OpenTelemetry W3C trace context from the request context and appends trace_id and span_id attributes
func (h *ContextHandler) Handle(ctx context.Context, r slog.Record) error {
	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", spanContext.TraceID().String()),
			slog.String("span_id", spanContext.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

func Init(level string) {
	var programLevel slog.Level
	switch strings.ToLower(level) {
	case "debug":
		programLevel = slog.LevelDebug
	case "info":
		programLevel = slog.LevelInfo
	case "warn":
		programLevel = slog.LevelWarn
	case "error":
		programLevel = slog.LevelError
	default:
		programLevel = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: programLevel,
	}

	var handler slog.Handler
	if os.Getenv("ENV") == "production" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	// Wrap handler in ContextHandler to parse trace context
	Log = slog.New(&ContextHandler{Handler: handler})
	slog.SetDefault(Log)

	// Discard verbose info logs from google.golang.org/grpc dependency logger
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, os.Stderr, os.Stderr))
}
