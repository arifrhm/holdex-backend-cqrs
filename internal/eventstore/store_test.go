package eventstore

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/holdex/epic-fermi/internal/domain"
	"github.com/holdex/epic-fermi/internal/domain/market"
)

func TestEventStore_Integration(t *testing.T) {
	// Skip if not running in integration test environment or if services are not available
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

	// Clean up database tables before test
	_, err = pool.Exec(ctx, "TRUNCATE TABLE events RESTART IDENTITY CASCADE")
	require.NoError(t, err)

	store := NewStore(pool, rdb)

	coinID := "bitcoin"
	payload := market.NewDataFetchedPayload{
		CoinID:         coinID,
		Symbol:         "btc",
		Name:           "Bitcoin",
		CurrentPrice:   50000.0,
		MarketCap:      1000000000.0,
		Volume24h:      20000000.0,
		PriceChange24h: 1.5,
		LastUpdated:    time.Now().Truncate(time.Microsecond),
	}

	event, err := market.NewNewDataFetchedEvent(coinID, 1, payload)
	require.NoError(t, err)

	t.Run("Append and Load Events", func(t *testing.T) {
		err := store.AppendEvents(ctx, coinID, 0, []domain.Event{event})
		require.NoError(t, err)

		loaded, err := store.LoadEvents(ctx, coinID)
		require.NoError(t, err)
		require.Len(t, loaded, 1)

		loadedEvent, ok := loaded[0].(market.MarketEvent)
		require.True(t, ok)
		assert.Equal(t, coinID, loadedEvent.AggregateID())
		assert.Equal(t, 1, loadedEvent.Version())
		assert.Equal(t, "NewDataFetched", loadedEvent.EventType())
		assert.Equal(t, payload.CurrentPrice, loadedEvent.Payload.CurrentPrice)
		assert.Equal(t, payload.Symbol, loadedEvent.Payload.Symbol)
	})

	t.Run("Optimistic Concurrency Control", func(t *testing.T) {
		// Appending an event with the same expected version should fail
		event2, err := market.NewNewDataFetchedEvent(coinID, 2, payload)
		require.NoError(t, err)

		// Expected version is 0 (which has current version 1) - this should fail
		err = store.AppendEvents(ctx, coinID, 0, []domain.Event{event2})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), ErrConcurrencyConflict.Error())

		// Appending with correct version 1 should succeed
		err = store.AppendEvents(ctx, coinID, 1, []domain.Event{event2})
		assert.NoError(t, err)
	})
}
