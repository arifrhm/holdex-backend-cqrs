package query

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/holdex/epic-fermi/internal/cache"
	"github.com/holdex/epic-fermi/internal/projection"
	"golang.org/x/sync/singleflight"
)

type CachedRepository struct {
	dbRepo *projection.Repository
	cache  *cache.Cache
	sf     singleflight.Group
}

func NewCachedRepository(dbRepo *projection.Repository, cache *cache.Cache) *CachedRepository {
	return &CachedRepository{
		dbRepo: dbRepo,
		cache:  cache,
	}
}

func (r *CachedRepository) GetMarketSummaries(ctx context.Context, limit, offset int) ([]projection.MarketSummary, error) {
	cacheKey := fmt.Sprintf("cache:summaries:%d:%d", limit, offset)
	var summaries []projection.MarketSummary

	found, err := r.cache.Get(ctx, cacheKey, &summaries)
	if err == nil && found {
		return summaries, nil
	}

	val, err, _ := r.sf.Do(cacheKey, func() (interface{}, error) {
		var doubleChecked []projection.MarketSummary
		if found, cErr := r.cache.Get(ctx, cacheKey, &doubleChecked); cErr == nil && found {
			return doubleChecked, nil
		}

		res, dbErr := r.dbRepo.GetMarketSummaries(ctx, limit, offset)
		if dbErr != nil {
			return nil, dbErr
		}

		if sErr := r.cache.Set(ctx, cacheKey, res, 10*time.Second); sErr != nil {
			slog.Warn("Failed to update market summaries cache", "error", sErr, "key", cacheKey)
		}
		return res, nil
	})
	if err != nil {
		return nil, err
	}

	return val.([]projection.MarketSummary), nil
}

func (r *CachedRepository) GetMarketSummary(ctx context.Context, coinID string) (*projection.MarketSummary, error) {
	cacheKey := fmt.Sprintf("cache:summary:%s", coinID)
	var summary projection.MarketSummary

	found, err := r.cache.Get(ctx, cacheKey, &summary)
	if err == nil && found {
		return &summary, nil
	}

	val, err, _ := r.sf.Do(cacheKey, func() (interface{}, error) {
		var doubleChecked projection.MarketSummary
		if found, cErr := r.cache.Get(ctx, cacheKey, &doubleChecked); cErr == nil && found {
			return &doubleChecked, nil
		}

		res, dbErr := r.dbRepo.GetMarketSummary(ctx, coinID)
		if dbErr != nil {
			return nil, dbErr
		}

		if sErr := r.cache.Set(ctx, cacheKey, res, 10*time.Second); sErr != nil {
			slog.Warn("Failed to update market summary cache", "error", sErr, "key", cacheKey)
		}
		return res, nil
	})
	if err != nil {
		return nil, err
	}

	return val.(*projection.MarketSummary), nil
}

func (r *CachedRepository) GetPriceHistory(ctx context.Context, coinID string, limit int) ([]projection.PricePoint, error) {
	cacheKey := fmt.Sprintf("cache:history:%s:%d", coinID, limit)
	var history []projection.PricePoint

	found, err := r.cache.Get(ctx, cacheKey, &history)
	if err == nil && found {
		return history, nil
	}

	val, err, _ := r.sf.Do(cacheKey, func() (interface{}, error) {
		var doubleChecked []projection.PricePoint
		if found, cErr := r.cache.Get(ctx, cacheKey, &doubleChecked); cErr == nil && found {
			return doubleChecked, nil
		}

		res, dbErr := r.dbRepo.GetPriceHistory(ctx, coinID, limit)
		if dbErr != nil {
			return nil, dbErr
		}

		if sErr := r.cache.Set(ctx, cacheKey, res, 30*time.Second); sErr != nil {
			slog.Warn("Failed to update price history cache", "error", sErr, "key", cacheKey)
		}
		return res, nil
	})
	if err != nil {
		return nil, err
	}

	return val.([]projection.PricePoint), nil
}

func (r *CachedRepository) GetPriceHistories(ctx context.Context, coinIDs []string, limit int) (map[string][]projection.PricePoint, error) {
	result := make(map[string][]projection.PricePoint)
	var misses []string

	for _, coinID := range coinIDs {
		cacheKey := fmt.Sprintf("cache:history:%s:%d", coinID, limit)
		var history []projection.PricePoint

		found, err := r.cache.Get(ctx, cacheKey, &history)
		if err == nil && found {
			result[coinID] = history
		} else {
			misses = append(misses, coinID)
		}
	}

	if len(misses) > 0 {
		sortedMisses := make([]string, len(misses))
		copy(sortedMisses, misses)
		sort.Strings(sortedMisses)
		sfKey := fmt.Sprintf("sf:histories:%d:%v", limit, sortedMisses)

		val, err, _ := r.sf.Do(sfKey, func() (interface{}, error) {
			innerRes := make(map[string][]projection.PricePoint)
			var stillMisses []string
			for _, m := range misses {
				cacheKey := fmt.Sprintf("cache:history:%s:%d", m, limit)
				var history []projection.PricePoint
				if found, cErr := r.cache.Get(ctx, cacheKey, &history); cErr == nil && found {
					innerRes[m] = history
				} else {
					stillMisses = append(stillMisses, m)
				}
			}

			if len(stillMisses) > 0 {
				dbRes, dbErr := r.dbRepo.GetPriceHistories(ctx, stillMisses, limit)
				if dbErr != nil {
					return nil, dbErr
				}
				for coinID, history := range dbRes {
					innerRes[coinID] = history
					cacheKey := fmt.Sprintf("cache:history:%s:%d", coinID, limit)
					if sErr := r.cache.Set(ctx, cacheKey, history, 30*time.Second); sErr != nil {
						slog.Warn("Failed to update price histories cache", "error", sErr, "key", cacheKey)
					}
				}
			}
			return innerRes, nil
		})
		if err != nil {
			return nil, err
		}

		sfRes := val.(map[string][]projection.PricePoint)
		for k, v := range sfRes {
			result[k] = v
		}
	}

	return result, nil
}
