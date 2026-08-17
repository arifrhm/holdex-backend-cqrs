package aggregator

import (
	"context"
	"fmt"

	"github.com/holdex/epic-fermi/internal/aggregator/coingecko"
	"github.com/holdex/epic-fermi/internal/domain/market"
	"github.com/holdex/epic-fermi/internal/eventstore"
)

type Processor struct {
	store eventstore.EventStore
}

func NewProcessor(store eventstore.EventStore) *Processor {
	return &Processor{store: store}
}

func (p *Processor) ProcessMarketData(ctx context.Context, data coingecko.MarketData) error {
	// 1. Hydrate the aggregate using only the latest event
	latestEvent, err := p.store.GetLatestEvent(ctx, data.ID)
	if err != nil {
		return fmt.Errorf("failed to load latest event for %s: %w", data.ID, err)
	}

	agg := market.NewMarketAggregate(data.ID)
	if latestEvent != nil {
		agg.Apply(latestEvent, false)
	}

	// 2. Create the command
	cmd := market.FetchMarketDataCommand{
		CoinID:         data.ID,
		Symbol:         data.Symbol,
		Name:           data.Name,
		CurrentPrice:   data.CurrentPrice,
		MarketCap:      data.MarketCap,
		Volume24h:      data.TotalVolume,
		PriceChange24h: data.PriceChange24h,
		LastUpdated:    data.LastUpdated,
	}

	// 3. Process command using aggregate to generate new event
	event, err := agg.HandleCommand(cmd)
	if err != nil {
		return fmt.Errorf("failed to handle fetch command: %w", err)
	}

	// 4. Apply event to aggregate locally
	agg.Apply(event, true)

	// 5. Save aggregate uncommitted events to event store with optimistic concurrency check
	err = p.store.AppendEvents(ctx, agg.ID(), agg.Version()-1, agg.GetUncommittedEvents())
	if err != nil {
		return fmt.Errorf("failed to append events for %s: %w", data.ID, err)
	}

	return nil
}
