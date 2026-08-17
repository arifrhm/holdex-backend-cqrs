package graphql

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/holdex/epic-fermi/internal/cache"
	"github.com/holdex/epic-fermi/internal/projection"
	"github.com/holdex/epic-fermi/internal/query"
)

func TestPriceHistoryDataloader_Integration(t *testing.T) {
	dbDSN := os.Getenv("DB_DSN")
	if dbDSN == "" {
		dbDSN = "postgres://holdex_user:holdex_password@localhost:5433/holdex_db?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Connect to Postgres
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

	// Setup repository and service
	repo := projection.NewRepository(pool)
	rdb := cache.NewCache(nil)
	cachedRepo := query.NewCachedRepository(repo, rdb)
	service := query.NewService(cachedRepo)

	// Clean up database tables
	_, err = pool.Exec(ctx, "TRUNCATE TABLE price_history, market_summaries RESTART IDENTITY CASCADE")
	require.NoError(t, err)

	// Seed test data
	now := time.Now().Truncate(time.Microsecond)
	err = repo.AddPriceHistory(ctx, "bitcoin", 61000.0, now.Add(-5*time.Minute))
	require.NoError(t, err)
	err = repo.AddPriceHistory(ctx, "bitcoin", 62000.0, now)
	require.NoError(t, err)

	err = repo.AddPriceHistory(ctx, "ethereum", 31000.0, now.Add(-5*time.Minute))
	require.NoError(t, err)
	err = repo.AddPriceHistory(ctx, "ethereum", 32000.0, now)
	require.NoError(t, err)

	// Initialize Dataloader
	dl := NewPriceHistoryDataloader(service, 10*time.Millisecond)

	// Fetch concurrently to verify batching
	var wg sync.WaitGroup
	wg.Add(2)

	var btcRes, ethRes []*PricePoint
	var btcErr, ethErr error

	go func() {
		defer wg.Done()
		btcRes, btcErr = dl.Load(ctx, "bitcoin", 2)
	}()

	go func() {
		defer wg.Done()
		ethRes, ethErr = dl.Load(ctx, "ethereum", 2)
	}()

	wg.Wait()

	require.NoError(t, btcErr)
	require.NoError(t, ethErr)

	// Verify bitcoin results
	require.Len(t, btcRes, 2)
	assert.Equal(t, 62000.0, btcRes[0].Price)
	assert.Equal(t, 61000.0, btcRes[1].Price)

	// Verify ethereum results
	require.Len(t, ethRes, 2)
	assert.Equal(t, 32000.0, ethRes[0].Price)
	assert.Equal(t, 31000.0, ethRes[1].Price)
}
