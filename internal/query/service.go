package query

import (
	"context"

	"github.com/holdex/epic-fermi/internal/projection"
)

// Repository defines the query read-model interface
type Repository interface {
	GetMarketSummaries(ctx context.Context, limit, offset int) ([]projection.MarketSummary, error)
	GetMarketSummary(ctx context.Context, coinID string) (*projection.MarketSummary, error)
	GetPriceHistory(ctx context.Context, coinID string, limit int) ([]projection.PricePoint, error)
	GetPriceHistories(ctx context.Context, coinIDs []string, limit int) (map[string][]projection.PricePoint, error)
}

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{
		repo: repo,
	}
}

// GetMarketSummaries retrieves all market summaries
func (s *Service) GetMarketSummaries(ctx context.Context, limit, offset int) ([]projection.MarketSummary, error) {
	return s.repo.GetMarketSummaries(ctx, limit, offset)
}

// GetMarketSummary retrieves a single market summary
func (s *Service) GetMarketSummary(ctx context.Context, coinID string) (*projection.MarketSummary, error) {
	return s.repo.GetMarketSummary(ctx, coinID)
}

// GetPriceHistory retrieves historical price points
func (s *Service) GetPriceHistory(ctx context.Context, coinID string, limit int) ([]projection.PricePoint, error) {
	return s.repo.GetPriceHistory(ctx, coinID, limit)
}

// GetPriceHistories retrieves historical price points for a list of coins in batch
func (s *Service) GetPriceHistories(ctx context.Context, coinIDs []string, limit int) (map[string][]projection.PricePoint, error) {
	return s.repo.GetPriceHistories(ctx, coinIDs, limit)
}


