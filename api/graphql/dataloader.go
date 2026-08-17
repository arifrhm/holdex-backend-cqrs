package graphql

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/holdex/epic-fermi/internal/query"
)

type dataloaderKey string

const dataloaderContextKey dataloaderKey = "priceHistoryDataloader"

type DataloaderKey struct {
	CoinID string
	Limit  int
}

type dlResult struct {
	points []*PricePoint
	err    error
}

type PriceHistoryDataloader struct {
	queryService *query.Service
	mu           sync.Mutex
	cache        map[DataloaderKey][]*PricePoint
	keys         []DataloaderKey
	listeners    map[DataloaderKey][]chan dlResult
	wait         time.Duration
}

func NewPriceHistoryDataloader(qs *query.Service, wait time.Duration) *PriceHistoryDataloader {
	if wait <= 0 {
		wait = 5 * time.Millisecond
	}
	return &PriceHistoryDataloader{
		queryService: qs,
		cache:        make(map[DataloaderKey][]*PricePoint),
		listeners:    make(map[DataloaderKey][]chan dlResult),
		wait:         wait,
	}
}

// DataloaderMiddleware injects a new dataloader instance per HTTP request context
func DataloaderMiddleware(qs *query.Service) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			dl := NewPriceHistoryDataloader(qs, 5*time.Millisecond)
			ctx := context.WithValue(r.Context(), dataloaderContextKey, dl)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// ForContext retrieves the dataloader from the context
func ForContext(ctx context.Context) *PriceHistoryDataloader {
	dl, _ := ctx.Value(dataloaderContextKey).(*PriceHistoryDataloader)
	return dl
}

// Load loads the price history for a given coinID and limit, batching multiple concurrent calls
func (dl *PriceHistoryDataloader) Load(ctx context.Context, coinID string, limit int) ([]*PricePoint, error) {
	key := DataloaderKey{CoinID: coinID, Limit: limit}

	dl.mu.Lock()
	if val, ok := dl.cache[key]; ok {
		dl.mu.Unlock()
		return val, nil
	}

	ch := make(chan dlResult, 1)
	dl.listeners[key] = append(dl.listeners[key], ch)

	isFirst := len(dl.keys) == 0
	dl.keys = append(dl.keys, key)
	dl.mu.Unlock()

	if isFirst {
		go dl.dispatch()
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		return res.points, res.err
	}
}

func (dl *PriceHistoryDataloader) dispatch() {
	time.Sleep(dl.wait)

	dl.mu.Lock()
	keys := dl.keys
	listeners := dl.listeners
	dl.keys = nil
	dl.listeners = make(map[DataloaderKey][]chan dlResult)
	dl.mu.Unlock()

	if len(keys) == 0 {
		return
	}

	// Group keys by Limit to batch queries with the same limit parameter together
	uniqueCoins := make(map[DataloaderKey]bool)
	var coinIDsByLimit map[int][]string = make(map[int][]string)

	for _, k := range keys {
		if !uniqueCoins[k] {
			uniqueCoins[k] = true
			coinIDsByLimit[k.Limit] = append(coinIDsByLimit[k.Limit], k.CoinID)
		}
	}

	// Perform batch requests for each limit group
	for limit, coins := range coinIDsByLimit {
		batchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		res, err := dl.queryService.GetPriceHistories(batchCtx, coins, limit)
		cancel()

		// Distribute results to all waiting listeners for this limit group
		for _, coin := range coins {
			key := DataloaderKey{CoinID: coin, Limit: limit}
			var points []*PricePoint

			if err == nil {
				if dbPoints, exists := res[coin]; exists {
					for _, dp := range dbPoints {
						points = append(points, &PricePoint{
							Price:      dp.Price,
							RecordedAt: dp.RecordedAt.UTC().Format(time.RFC3339),
						})
					}
				}
			}

			if err == nil {
				dl.mu.Lock()
				dl.cache[key] = points
				dl.mu.Unlock()
			}

			if lis, ok := listeners[key]; ok {
				for _, ch := range lis {
					ch <- dlResult{points: points, err: err}
				}
			}
		}
	}
}
