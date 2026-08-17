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

func TestOutboxPublisher_Integration(t *testing.T) {
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
	_, err = pool.Exec(ctx, "TRUNCATE TABLE price_history, market_summaries, events RESTART IDENTITY CASCADE")
	require.NoError(t, err)

	store := NewStore(pool, rdb)
	publisher := NewOutboxPublisher(pool, rdb, 50*time.Millisecond)

	// Subscribe to Redis events
	eventsChan, err := store.Subscribe(ctx, market.NewDataFetchedEvent)
	require.NoError(t, err)

	// Start outbox publisher
	pubCtx, pubCancel := context.WithCancel(ctx)
	defer pubCancel()
	go func() {
		_ = publisher.Start(pubCtx)
	}()

	coinID := "bitcoin"
	payload := market.NewDataFetchedPayload{
		CoinID:         coinID,
		Symbol:         "btc",
		Name:           "Bitcoin",
		CurrentPrice:   60000.0,
		LastUpdated:    time.Now().Truncate(time.Microsecond),
	}

	event, err := market.NewNewDataFetchedEvent(coinID, 1, payload)
	require.NoError(t, err)

	// Append event which writes to outbox_events
	err = store.AppendEvents(ctx, coinID, 0, []domain.Event{event})
	require.NoError(t, err)

	// Verify that the event is picked up by the OutboxPublisher and published to Redis
	select {
	case ev := <-eventsChan:
		assert.Equal(t, coinID, ev.AggregateID())
		assert.Equal(t, market.NewDataFetchedEvent, ev.EventType())
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for outbox event to be published to Redis")
	}

	// Verify outbox table is empty now
	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM outbox_events").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}
