package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/holdex/epic-fermi/internal/cache"
	"github.com/holdex/epic-fermi/internal/domain"
	"github.com/holdex/epic-fermi/internal/domain/market"
	"github.com/holdex/epic-fermi/internal/eventstore"
)

type Projector struct {
	store  eventstore.EventStore
	repo   *Repository
	cache  *cache.Cache
	logger *slog.Logger
}

func NewProjector(store eventstore.EventStore, repo *Repository, cache *cache.Cache) *Projector {
	return &Projector{
		store:  store,
		repo:   repo,
		cache:  cache,
		logger: slog.Default().With("component", "projector"),
	}
}

// Start runs the projector loop, reading from the database events table with checkpoints
func (p *Projector) Start(ctx context.Context) error {
	p.logger.Info("Starting event projector daemon")

	lastID, err := p.repo.GetLastEventID(ctx, "portfolio_projector")
	if err != nil {
		p.logger.Error("Failed to load projector checkpoint", "error", err)
		return err
	}

	pollInterval := 100 * time.Millisecond
	currentInterval := pollInterval
	maxInterval := 2 * time.Second

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Stopping event projector daemon")
			return ctx.Err()
		default:
			records, err := p.repo.GetNewEvents(ctx, lastID, 100)
			if err != nil {
				p.logger.Error("Failed to fetch new events for projection", "error", err)
				currentInterval = 1 * time.Second
			} else if len(records) > 0 {
				p.logger.Debug("Processing events batch", "count", len(records))
				var processedCount int
				var projectErr error

				for _, rec := range records {
					ev, err := deserializeEvent(rec)
					if err != nil {
						p.logger.Error("Failed to deserialize event, skipping", "event_id", rec.ID, "error", err)
						lastID = rec.ID
						processedCount++
						continue
					}

					if err := p.Project(ctx, ev); err != nil {
						p.logger.Error("Failed to project event, retrying later", "event_id", rec.ID, "error", err)
						projectErr = err
						break
					}

					lastID = rec.ID
					processedCount++
				}

				// Only save checkpoint if at least one event was successfully processed/skipped
				if processedCount > 0 {
					checkpointCtx := ctx
					var cancel context.CancelFunc
					if ctx.Err() != nil {
						checkpointCtx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
					}
					if err := p.repo.UpdateLastEventID(checkpointCtx, "portfolio_projector", lastID); err != nil {
						p.logger.Error("Failed to save projector checkpoint", "last_event_id", lastID, "error", err)
					}
					if cancel != nil {
						cancel()
					}
				}

				if projectErr != nil {
					// Exponentially backoff on projection errors to avoid tight loops
					currentInterval *= 2
					if currentInterval < 500*time.Millisecond {
						currentInterval = 500 * time.Millisecond
					}
					if currentInterval > maxInterval {
						currentInterval = maxInterval
					}
				} else {
					currentInterval = pollInterval
				}
			} else {
				currentInterval *= 2
				if currentInterval > maxInterval {
					currentInterval = maxInterval
				}
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(currentInterval):
			}
		}
	}
}

// Project processes a single event and updates the read models
func (p *Projector) Project(ctx context.Context, ev domain.Event) error {
	if me, ok := ev.(market.MarketEvent); ok {
		summary := MarketSummary{
			CoinID:         me.Payload.CoinID,
			Symbol:         me.Payload.Symbol,
			Name:           me.Payload.Name,
			CurrentPrice:   me.Payload.CurrentPrice,
			MarketCap:      me.Payload.MarketCap,
			Volume24h:      me.Payload.Volume24h,
			PriceChange24h: me.Payload.PriceChange24h,
			LastUpdated:    me.Payload.LastUpdated,
		}

		if err := p.repo.UpdateMarketSummary(ctx, summary); err != nil {
			return err
		}

		if err := p.repo.AddPriceHistory(ctx, summary.CoinID, summary.CurrentPrice, summary.LastUpdated); err != nil {
			return err
		}

		if err := p.cache.Delete(ctx, "cache:summary:"+summary.CoinID); err != nil {
			p.logger.Warn("Failed to invalidate cache for summary", "coin_id", summary.CoinID, "error", err)
		}

		p.logger.Debug("Projected event to read models", "coin_id", summary.CoinID, "price", summary.CurrentPrice)
	}
	return nil
}

func deserializeEvent(rec EventRecord) (domain.Event, error) {
	if rec.Type == market.NewDataFetchedEvent {
		var payload market.NewDataFetchedPayload
		if err := json.Unmarshal(rec.Data, &payload); err != nil {
			return nil, err
		}
		base := domain.BaseEvent{
			Type:    rec.Type,
			AggID:   rec.AggID,
			AggType: "Market",
			Vers:    rec.Version,
			Time:    rec.CreatedAt,
			Payload: rec.Data,
		}
		return market.MarketEvent{
			BaseEvent: base,
			Payload:   payload,
		}, nil
	}
	return nil, fmt.Errorf("unknown event type: %s", rec.Type)
}
