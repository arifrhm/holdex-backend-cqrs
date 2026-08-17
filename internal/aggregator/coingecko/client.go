package coingecko

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type MarketData struct {
	ID             string    `json:"id"`
	Symbol         string    `json:"symbol"`
	Name           string    `json:"name"`
	CurrentPrice   float64   `json:"current_price"`
	MarketCap      float64   `json:"market_cap"`
	TotalVolume    float64   `json:"total_volume"`
	PriceChange24h float64   `json:"price_change_percentage_24h"`
	LastUpdated    time.Time `json:"last_updated"`
}

type Client interface {
	FetchMarketData(ctx context.Context, coinIDs []string) ([]MarketData, error)
}

type CircuitState int

const (
	StateClosed CircuitState = iota
	StateOpen
	StateHalfOpen
)

type CircuitBreaker struct {
	mu           sync.RWMutex
	state        CircuitState
	failures     int
	successes    int
	lastActivity time.Time
	cooldown     time.Duration
	threshold    int
	halfOpenInFlight bool
}

func NewCircuitBreaker(threshold int, cooldown time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:        StateClosed,
		cooldown:     cooldown,
		threshold:    threshold,
		lastActivity: time.Now(),
	}
}

func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == StateOpen {
		if time.Since(cb.lastActivity) > cb.cooldown {
			cb.state = StateHalfOpen
			cb.lastActivity = time.Now()
			cb.halfOpenInFlight = true
			return true
		}
		return false
	}
	if cb.state == StateHalfOpen {
		if cb.halfOpenInFlight {
			return false
		}
		cb.halfOpenInFlight = true
		return true
	}
	return true
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastActivity = time.Now()
	cb.halfOpenInFlight = false
	if cb.state == StateClosed {
		cb.failures++
		if cb.failures >= cb.threshold {
			cb.state = StateOpen
		}
	} else if cb.state == StateHalfOpen {
		cb.state = StateOpen
	}
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastActivity = time.Now()
	cb.halfOpenInFlight = false
	if cb.state == StateHalfOpen {
		cb.successes++
		if cb.successes >= 2 {
			cb.state = StateClosed
			cb.failures = 0
			cb.successes = 0
		}
	} else if cb.state == StateClosed {
		cb.failures = 0
	}
}

type HTTPClient struct {
	baseURL    string
	httpClient *http.Client
	apiKey     string
	cb         *CircuitBreaker
}

func NewHTTPClient(baseURL string, apiKey string, timeout time.Duration) *HTTPClient {
	if baseURL == "" {
		baseURL = "https://api.coingecko.com/api/v3"
	}
	return &HTTPClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		apiKey: apiKey,
		cb:     NewCircuitBreaker(3, 30*time.Second),
	}
}

func (c *HTTPClient) FetchMarketData(ctx context.Context, coinIDs []string) ([]MarketData, error) {
	if len(coinIDs) == 0 {
		return nil, nil
	}

	u, err := url.Parse(c.baseURL + "/coins/markets")
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	q := u.Query()
	q.Set("vs_currency", "usd")
	q.Set("ids", strings.Join(coinIDs, ","))
	q.Set("order", "market_cap_desc")
	q.Set("per_page", "250")
	q.Set("page", "1")
	q.Set("sparkline", "false")
	q.Set("price_change_percentage", "24h")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if c.apiKey != "" {
		req.Header.Set("x-cg-demo-api-key", c.apiKey)
	}

	resp, err := c.executeWithRetry(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("api execution failed: %w", err)
	}
	defer resp.Body.Close()

	var data []MarketData
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("failed to decode response payload: %w", err)
	}

	return data, nil
}

func (c *HTTPClient) executeWithRetry(ctx context.Context, req *http.Request) (*http.Response, error) {
	var resp *http.Response
	var err error

	backoff := 500 * time.Millisecond
	maxBackoff := 5 * time.Second

	for i := 0; i < 3; i++ {
		if !c.cb.Allow() {
			return nil, fmt.Errorf("circuit breaker is open, rejecting API request")
		}

		clonedReq := req.Clone(ctx)
		resp, err = c.httpClient.Do(clonedReq)
		if err == nil {
			if resp.StatusCode == http.StatusOK {
				c.cb.RecordSuccess()
				return resp, nil
			}
			resp.Body.Close()

			if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
				c.cb.RecordFailure()
			} else {
				return nil, fmt.Errorf("coingecko returned client error status code: %d", resp.StatusCode)
			}
		} else {
			c.cb.RecordFailure()
		}

		// Check if we should retry (i.e. connection errors or 429/5xx status codes)
		if i == 2 {
			break
		}

		// Calculate jittered exponential backoff
		jitter := time.Duration(rand.Intn(100)) * time.Millisecond
		sleepTime := backoff + jitter
		if sleepTime > maxBackoff {
			sleepTime = maxBackoff
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(sleepTime):
		}

		backoff *= 2
	}

	if err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("coingecko request failed after retries")
}
