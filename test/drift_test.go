package test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/holdex/epic-fermi/api"
	grpcapi "github.com/holdex/epic-fermi/api/grpc"
	pb "github.com/holdex/epic-fermi/api/grpc/proto"
	"github.com/holdex/epic-fermi/internal/cache"
	"github.com/holdex/epic-fermi/internal/domain"
	"github.com/holdex/epic-fermi/internal/projection"
	"github.com/holdex/epic-fermi/internal/query"
)

type MockEventStore struct {
	mock.Mock
}

func (m *MockEventStore) AppendEvents(ctx context.Context, aggregateID string, expectedVersion int, events []domain.Event) error {
	args := m.Called(ctx, aggregateID, expectedVersion, events)
	return args.Error(0)
}

func (m *MockEventStore) LoadEvents(ctx context.Context, aggregateID string) ([]domain.Event, error) {
	args := m.Called(ctx, aggregateID)
	return args.Get(0).([]domain.Event), args.Error(1)
}

func (m *MockEventStore) GetLatestEvent(ctx context.Context, aggregateID string) (domain.Event, error) {
	args := m.Called(ctx, aggregateID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(domain.Event), args.Error(1)
}

func (m *MockEventStore) Subscribe(ctx context.Context, eventTypes ...string) (<-chan domain.Event, error) {
	args := m.Called(ctx, eventTypes)
	return args.Get(0).(<-chan domain.Event), args.Error(1)
}

func TestAPIPayloadDrift(t *testing.T) {
	dbDSN := os.Getenv("DB_DSN")
	redisAddr := os.Getenv("REDIS_ADDR")

	if dbDSN == "" {
		dbDSN = "postgres://holdex_user:holdex_password@localhost:5433/holdex_db?sslmode=disable"
	}
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try connecting to Postgres
	pool, err := pgxpool.New(ctx, dbDSN)
	if err != nil {
		t.Skip("Postgres not available, skipping drift test: ", err)
		return
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skip("Postgres ping failed, skipping drift test: ", err)
		return
	}

	// Try connecting to Redis
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("Redis ping failed, skipping drift test: ", err)
		return
	}

	// Clean up database tables and seed test data
	_, err = pool.Exec(ctx, "TRUNCATE TABLE price_history, market_summaries CASCADE")
	require.NoError(t, err)

	repo := projection.NewRepository(pool)
	cacheMgr := cache.NewCache(rdb)
	cachedRepo := query.NewCachedRepository(repo, cacheMgr)
	queryService := query.NewService(cachedRepo)
	mockStore := new(MockEventStore)

	// Seed read model with exact expected data matching snapshots
	// Fixed last_updated to avoid timestamp differences causing drift failures
	fixedTime, err := time.Parse(time.RFC3339, "2026-08-10T12:00:00Z")
	require.NoError(t, err)

	err = repo.UpdateMarketSummary(ctx, projection.MarketSummary{
		CoinID:         "bitcoin",
		Symbol:         "btc",
		Name:           "Bitcoin",
		CurrentPrice:   65000.0,
		MarketCap:      1300000000000.0,
		Volume24h:      35000000000.0,
		PriceChange24h: 2.1,
		LastUpdated:    fixedTime,
	})
	require.NoError(t, err)

	err = repo.AddPriceHistory(ctx, "bitcoin", 64500.0, fixedTime.Add(-10*time.Minute))
	require.NoError(t, err)
	err = repo.AddPriceHistory(ctx, "bitcoin", 65000.0, fixedTime)
	require.NoError(t, err)

	t.Run("GraphQL Drift Check", func(t *testing.T) {
		handler := api.NewHTTPHandler(ctx, queryService, mockStore, pool, rdb, 1000.0, 1000)
		ts := httptest.NewServer(handler)
		defer ts.Close()

		// Send GraphQL query request
		queryStr := `{"query":"{ marketSummaries(limit: 1) { coinId symbol name currentPrice marketCap volume24h priceChange24h priceHistory(limit: 2) { price recordedAt } } }"}`
		resp, err := http.Post(ts.URL+"/query", "application/json", bytes.NewBufferString(queryStr))
		require.NoError(t, err)
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		// Read expected snapshot
		expectedSnapshot, err := os.ReadFile("../testdata/snapshots/graphql_markets.json")
		require.NoError(t, err)

		// Normalize JSON for comparison
		var actualJSON, expectedJSON interface{}
		err = json.Unmarshal(body, &actualJSON)
		require.NoError(t, err)

		err = json.Unmarshal(expectedSnapshot, &expectedJSON)
		require.NoError(t, err)

		assert.Equal(t, expectedJSON, actualJSON, "GraphQL response payload has drifted from the snapshot!")
	})

	t.Run("gRPC Drift Check", func(t *testing.T) {
		// gRPC server setup using bufconn
		lis := bufconn.Listen(1024 * 1024)
		s := grpc.NewServer(
			grpc.UnaryInterceptor(grpcapi.OTelUnaryServerInterceptor()),
			grpc.StreamInterceptor(grpcapi.OTelStreamServerInterceptor()),
		)
		srv := grpcapi.NewServer(queryService, mockStore)
		pb.RegisterMarketServiceServer(s, srv)

		go func() {
			if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
				panic(err)
			}
		}()
		defer s.GracefulStop()

		conn, err := grpc.NewClient("passthrough://bufconn",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return lis.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)
		defer conn.Close()

		client := pb.NewMarketServiceClient(conn)

		res, err := client.GetMarketSummaries(ctx, &pb.GetMarketSummariesRequest{Limit: 1, Offset: 0})
		require.NoError(t, err)

		// Marshal gRPC message to json
		actualBytes, err := json.Marshal(res)
		require.NoError(t, err)

		// Read expected snapshot
		expectedSnapshot, err := os.ReadFile("../testdata/snapshots/grpc_market_data.json")
		require.NoError(t, err)

		var actualJSON, expectedJSON interface{}
		err = json.Unmarshal(actualBytes, &actualJSON)
		require.NoError(t, err)

		err = json.Unmarshal(expectedSnapshot, &expectedJSON)
		require.NoError(t, err)

		assert.Equal(t, expectedJSON, actualJSON, "gRPC response payload has drifted from the snapshot!")
	})
}
