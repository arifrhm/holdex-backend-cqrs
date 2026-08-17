package grpc

import (
	"context"
	"net"
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

	"github.com/holdex/epic-fermi/internal/cache"
	"github.com/holdex/epic-fermi/internal/domain"
	"github.com/holdex/epic-fermi/internal/projection"
	"github.com/holdex/epic-fermi/internal/query"
	pb "github.com/holdex/epic-fermi/api/grpc/proto"
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

func TestGRPCServer_Integration(t *testing.T) {
	dbDSN := os.Getenv("DB_DSN")
	redisAddr := os.Getenv("REDIS_ADDR")

	if dbDSN == "" {
		dbDSN = "postgres://holdex_user:holdex_password@localhost:5433/holdex_db?sslmode=disable"
	}
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try connecting to Postgres
	pool, err := pgxpool.New(ctx, dbDSN)
	if err != nil {
		t.Skip("Postgres not available, skipping integration test: ", err)
		return
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skip("Postgres ping failed, skipping integration test: ", err)
		return
	}

	// Try connecting to Redis
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skip("Redis ping failed, skipping integration test: ", err)
		return
	}

	// Setup databases and seed some data
	_, err = pool.Exec(ctx, "TRUNCATE TABLE price_history, market_summaries CASCADE")
	require.NoError(t, err)

	repo := NewRepository(pool)
	cacheMgr := cache.NewCache(rdb)
	cachedRepo := query.NewCachedRepository(repo, cacheMgr)
	queryService := query.NewService(cachedRepo)
	mockStore := new(MockEventStore)

	// Seed read model
	err = repo.UpdateMarketSummary(ctx, projection.MarketSummary{
		CoinID:         "bitcoin",
		Symbol:         "btc",
		Name:           "Bitcoin",
		CurrentPrice:   65000.0,
		MarketCap:      1300000000000.0,
		Volume24h:      35000000000.0,
		PriceChange24h: 2.1,
		LastUpdated:    time.Now(),
	})
	require.NoError(t, err)

	// In-memory gRPC listener
	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer(
		grpc.UnaryInterceptor(OTelUnaryServerInterceptor()),
		grpc.StreamInterceptor(OTelStreamServerInterceptor()),
	)
	srv := NewServer(queryService, mockStore)
	pb.RegisterMarketServiceServer(s, srv)

	go func() {
		if err := s.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			panic(err)
		}
	}()
	defer s.GracefulStop()

	// Dial in-memory server
	conn, err := grpc.NewClient("passthrough://bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := pb.NewMarketServiceClient(conn)

	t.Run("GetMarketSummary", func(t *testing.T) {
		res, err := client.GetMarketSummary(ctx, &pb.GetMarketSummaryRequest{CoinId: "bitcoin"})
		require.NoError(t, err)
		assert.Equal(t, "bitcoin", res.GetCoinId())
		assert.Equal(t, "btc", res.GetSymbol())
		assert.Equal(t, 65000.0, res.GetCurrentPrice())
	})

	t.Run("GetMarketSummaries", func(t *testing.T) {
		res, err := client.GetMarketSummaries(ctx, &pb.GetMarketSummariesRequest{Limit: 10, Offset: 0})
		require.NoError(t, err)
		assert.Len(t, res.GetSummaries(), 1)
		assert.Equal(t, "bitcoin", res.GetSummaries()[0].GetCoinId())
	})
}

// Stub function to mock repo functions in local tests if needed
func NewRepository(pool *pgxpool.Pool) *projection.Repository {
	return projection.NewRepository(pool)
}
