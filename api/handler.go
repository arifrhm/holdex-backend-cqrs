package api

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	"github.com/holdex/epic-fermi/api/graphql"
	"github.com/holdex/epic-fermi/internal/eventstore"
	"github.com/holdex/epic-fermi/internal/query"
)

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests processed",
		},
		[]string{"path", "method", "status"},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Latency of HTTP requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"path", "method"},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(httpRequestDuration)
}

type limiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

type IPRateLimiter struct {
	ips map[string]*limiterEntry
	mu  sync.RWMutex
	r   rate.Limit
	b   int
}

func NewIPRateLimiter(ctx context.Context, r rate.Limit, b int) *IPRateLimiter {
	limiter := &IPRateLimiter{
		ips: make(map[string]*limiterEntry),
		r:   r,
		b:   b,
	}
	go limiter.startSweeper(ctx, 1*time.Minute, 10*time.Minute)
	return limiter
}

func (i *IPRateLimiter) startSweeper(ctx context.Context, interval, maxAge time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			i.mu.Lock()
			for ip, entry := range i.ips {
				if time.Since(entry.lastSeen) > maxAge {
					delete(i.ips, ip)
				}
			}
			i.mu.Unlock()
		}
	}
}

func (i *IPRateLimiter) GetLimiter(ip string) *rate.Limiter {
	i.mu.RLock()
	entry, exists := i.ips[ip]
	i.mu.RUnlock()

	if exists {
		// Only update lastSeen under write lock if it was updated more than a second ago
		if time.Since(entry.lastSeen) > time.Second {
			i.mu.Lock()
			entry.lastSeen = time.Now()
			i.mu.Unlock()
		}
		return entry.limiter
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	// Double-check existence under write lock
	entry, exists = i.ips[ip]
	if !exists {
		entry = &limiterEntry{
			limiter:  rate.NewLimiter(i.r, i.b),
			lastSeen: time.Now(),
		}
		i.ips[ip] = entry
	} else {
		entry.lastSeen = time.Now()
	}

	return entry.limiter
}

// NewHTTPHandler configures the HTTP routes including GraphQL, Playground, Metrics, and Health Check
func NewHTTPHandler(ctx context.Context, qs *query.Service, es eventstore.EventStore, pool *pgxpool.Pool, rdb *redis.Client, rateRPS float64, rateBurst int) http.Handler {
	mux := http.NewServeMux()

	// GraphQL Server resolver setup
	gqlServer := handler.NewDefaultServer(graphql.NewExecutableSchema(graphql.Config{
		Resolvers: &graphql.Resolver{
			QueryService: qs,
			EventStore:   es,
		},
	}))

	// Instanitate Rate Limiter using dynamic configuration values
	limiter := NewIPRateLimiter(ctx, rate.Limit(rateRPS), rateBurst)

	// Versioned GraphQL Handlers
	playgroundHandler := RateLimitMiddleware(limiter)(playground.Handler("GraphQL Playground", "/v1/query"))
	queryHandler := RateLimitMiddleware(limiter)(graphql.DataloaderMiddleware(qs)(gqlServer))

	// Routes
	mux.Handle("/", playgroundHandler)
	mux.Handle("/v1/playground", playgroundHandler)
	mux.Handle("/v1/query", queryHandler)
	mux.Handle("/metrics", promhttp.Handler())

	// Legacy backward compatibility routes
	mux.Handle("/query", queryHandler)

	// Health Check Route
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		reqCtx := r.Context()

		// Check Postgres
		if err := pool.Ping(reqCtx); err != nil {
			http.Error(w, "Database connection unhealthy: " + err.Error(), http.StatusInternalServerError)
			return
		}

		// Check Redis
		if err := rdb.Ping(reqCtx).Err(); err != nil {
			http.Error(w, "Redis connection unhealthy: " + err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	// Wrap handler in Tracing and Prometheus metrics instrumentation middlewares
	return OTelTracingMiddleware(PrometheusMiddleware(mux))
}

// OTelTracingMiddleware extracts or starts a W3C OpenTelemetry span context per HTTP request
func OTelTracingMiddleware(next http.Handler) http.Handler {
	propagator := propagation.TraceContext{}
	tracer := otel.Tracer("holdex-portfolio")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 1. Extract W3C Trace Context from incoming HTTP request headers
		ctx := propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))

		// 2. Always start a server-side child span
		ctx, span := tracer.Start(ctx, r.Method+" "+r.URL.Path, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()

		// 3. Inject tracecontext back to response headers so downstream clients can read it
		propagator.Inject(ctx, propagation.HeaderCarrier(w.Header()))

		// 4. Pass execution to next handler with tracing context
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RateLimitMiddleware enforces token-bucket rate limits per client IP address
func RateLimitMiddleware(limiter *IPRateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := r.Header.Get("X-Forwarded-For")
			if ip != "" {
				parts := strings.Split(ip, ",")
				ip = strings.TrimSpace(parts[len(parts)-1])
			}
			if ip == "" {
				var err error
				ip, _, err = net.SplitHostPort(r.RemoteAddr)
				if err != nil {
					ip = r.RemoteAddr
				}
			}

			lim := limiter.GetLimiter(ip)
			if !lim.Allow() {
				http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// PrometheusMiddleware records count and latency metrics for all HTTP endpoints
func PrometheusMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(rw, r)

		duration := time.Since(start).Seconds()
		statusStr := strconv.Itoa(rw.statusCode)

		// Normalize path to prevent cardinality explosion
		normalizedPath := r.URL.Path
		if strings.HasPrefix(normalizedPath, "/v1/query") || strings.HasPrefix(normalizedPath, "/query") {
			normalizedPath = "/v1/query"
		} else if strings.HasPrefix(normalizedPath, "/v1/playground") || normalizedPath == "/" {
			normalizedPath = "/v1/playground"
		} else if normalizedPath == "/metrics" {
			normalizedPath = "/metrics"
		} else if normalizedPath == "/healthz" {
			normalizedPath = "/healthz"
		} else {
			normalizedPath = "other"
		}

		httpRequestDuration.WithLabelValues(normalizedPath, r.Method).Observe(duration)
		httpRequestsTotal.WithLabelValues(normalizedPath, r.Method, statusStr).Inc()
	})
}
