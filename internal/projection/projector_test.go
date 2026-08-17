package projection

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/holdex/epic-fermi/internal/cache"
	"github.com/holdex/epic-fermi/internal/domain"
	"github.com/holdex/epic-fermi/internal/domain/market"
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
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
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
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(<-chan domain.Event), args.Error(1)
}

func TestProjector_Integration(t *testing.T) {
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

	// Clean up database tables
	_, err = pool.Exec(ctx, "TRUNCATE TABLE price_history, market_summaries CASCADE")
	require.NoError(t, err)

	repo := NewRepository(pool)
	cacheMgr := cache.NewCache(rdb)
	mockStore := new(MockEventStore)
	projector := NewProjector(mockStore, repo, cacheMgr)

	coinID := "ethereum"
	payload := market.NewDataFetchedPayload{
		CoinID:         coinID,
		Symbol:         "eth",
		Name:           "Ethereum",
		CurrentPrice:   3000.0,
		MarketCap:      360000000000.0,
		Volume24h:      15000000000.0,
		PriceChange24h: -1.2,
		LastUpdated:    time.Now().Truncate(time.Microsecond),
	}

	event, err := market.NewNewDataFetchedEvent(coinID, 1, payload)
	require.NoError(t, err)

	t.Run("Project NewDataFetched Event", func(t *testing.T) {
		err := projector.Project(ctx, event)
		require.NoError(t, err)

		// Verify read model table was updated
		summary, err := repo.GetMarketSummary(ctx, coinID)
		require.NoError(t, err)
		assert.Equal(t, coinID, summary.CoinID)
		assert.Equal(t, "eth", summary.Symbol)
		assert.Equal(t, 3000.0, summary.CurrentPrice)

		// Verify price history was appended
		history, err := repo.GetPriceHistory(ctx, coinID, 10)
		require.NoError(t, err)
		require.Len(t, history, 1)
		assert.Equal(t, 3000.0, history[0].Price)
	})
}
