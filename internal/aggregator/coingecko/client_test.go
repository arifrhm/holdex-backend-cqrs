package coingecko

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPClient_FetchMarketData(t *testing.T) {
	mockResponse := `[
		{
			"id": "bitcoin",
			"symbol": "btc",
			"name": "Bitcoin",
			"current_price": 58340.50,
			"market_cap": 1150000000000.0,
			"total_volume": 28000000000.0,
			"price_change_percentage_24h": -2.45,
			"last_updated": "2026-08-10T12:00:00Z"
		}
	]`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/coins/markets", r.URL.Path)
		assert.Equal(t, "usd", r.URL.Query().Get("vs_currency"))
		assert.Equal(t, "bitcoin", r.URL.Query().Get("ids"))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, "", 2*time.Second)

	ctx := context.Background()
	data, err := client.FetchMarketData(ctx, []string{"bitcoin"})
	require.NoError(t, err)
	require.Len(t, data, 1)

	coin := data[0]
	assert.Equal(t, "bitcoin", coin.ID)
	assert.Equal(t, "btc", coin.Symbol)
	assert.Equal(t, "Bitcoin", coin.Name)
	assert.Equal(t, 58340.50, coin.CurrentPrice)
	assert.Equal(t, 1150000000000.0, coin.MarketCap)
	assert.Equal(t, 28000000000.0, coin.TotalVolume)
	assert.Equal(t, -2.45, coin.PriceChange24h)
	assert.Equal(t, "2026-08-10T12:00:00Z", coin.LastUpdated.Format(time.RFC3339))
}

func TestHTTPClient_Resiliency(t *testing.T) {
	var callCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		// Return 429 rate limit
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	// High cooldown to prevent cooldown during retry sleep
	client := NewHTTPClient(server.URL, "", 2*time.Second)
	client.cb.cooldown = 10 * time.Second

	ctx := context.Background()

	// Call 1: should execute 3 attempts and then fail, tripping circuit breaker
	_, err := client.FetchMarketData(ctx, []string{"bitcoin"})
	assert.Error(t, err)
	assert.Equal(t, 3, callCount, "Expected exactly 3 retry attempts")

	// Call 2: since circuit breaker is tripped (Open), it should immediately reject without calling server
	_, err = client.FetchMarketData(ctx, []string{"bitcoin"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "circuit breaker is open")
	assert.Equal(t, 3, callCount, "Should not call server when circuit breaker is open")

	// Simulate cooldown period passing by shifting lastActivity back
	client.cb.lastActivity = time.Now().Add(-15 * time.Second)

	// Call 3: should allow 1 request in half-open state, fail, trip back to Open, and reject further retries immediately
	_, err = client.FetchMarketData(ctx, []string{"bitcoin"})
	assert.Error(t, err)
	assert.Equal(t, 4, callCount, "Expected exactly 1 server call in half-open state before tripping back to Open")
}

