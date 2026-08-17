package aggregator

import (
	"context"
	"log/slog"
	"time"

	"github.com/holdex/epic-fermi/internal/aggregator/coingecko"
)

type Service struct {
	client        coingecko.Client
	processor     *Processor
	coinIDs       []string
	pollInterval  time.Duration
	logger        *slog.Logger
}

func NewService(client coingecko.Client, processor *Processor, coinIDs []string, pollInterval time.Duration) *Service {
	return &Service{
		client:       client,
		processor:    processor,
		coinIDs:      coinIDs,
		pollInterval: pollInterval,
		logger:       slog.Default().With("component", "aggregator"),
	}
}

func (s *Service) Start(ctx context.Context) error {
	s.logger.Info("Starting aggregator service", "coins", s.coinIDs, "interval", s.pollInterval)

	// Run initial fetch immediately
	s.fetchAndProcess(ctx)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Stopping aggregator service")
			return ctx.Err()
		case <-time.After(s.pollInterval):
			s.fetchAndProcess(ctx)
		}
	}
}

func (s *Service) fetchAndProcess(ctx context.Context) {
	s.logger.Debug("Fetching market data from CoinGecko")
	data, err := s.client.FetchMarketData(ctx, s.coinIDs)
	if err != nil {
		s.logger.Error("Failed to fetch market data", "error", err)
		return
	}

	s.logger.Debug("Fetched market data records", "count", len(data))
	for _, record := range data {
		if ctx.Err() != nil {
			s.logger.Info("Context cancelled, aborting remaining record processing")
			return
		}
		err := s.processor.ProcessMarketData(ctx, record)
		if err != nil {
			s.logger.Error("Failed to process market data", "coin", record.ID, "error", err)
			continue
		}
		s.logger.Debug("Successfully processed market data", "coin", record.ID, "price", record.CurrentPrice)
	}
}
