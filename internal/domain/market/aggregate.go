package market

import (
	"errors"
	"fmt"

	"github.com/holdex/epic-fermi/internal/domain"
)

type MarketAggregate struct {
	domain.BaseAggregateRoot
	Symbol         string
	Name           string
	CurrentPrice   float64
	MarketCap      float64
	Volume24h      float64
	PriceChange24h float64
}

func NewMarketAggregate(coinID string) *MarketAggregate {
	return &MarketAggregate{
		BaseAggregateRoot: domain.BaseAggregateRoot{
			IDStr:   coinID,
			TypeStr: "Market",
			Vers:    0,
		},
	}
}

// HandleCommand processes commands and generates events
func (a *MarketAggregate) HandleCommand(cmd interface{}) (domain.Event, error) {
	switch c := cmd.(type) {
	case FetchMarketDataCommand:
		if c.CoinID == "" {
			return nil, errors.New("coin ID cannot be empty")
		}
		if c.CurrentPrice < 0 {
			return nil, errors.New("current price cannot be negative")
		}

		payload := NewDataFetchedPayload{
			CoinID:         c.CoinID,
			Symbol:         c.Symbol,
			Name:           c.Name,
			CurrentPrice:   c.CurrentPrice,
			MarketCap:      c.MarketCap,
			Volume24h:      c.Volume24h,
			PriceChange24h: c.PriceChange24h,
			LastUpdated:    c.LastUpdated,
		}

		event, err := NewNewDataFetchedEvent(c.CoinID, a.Version()+1, payload)
		if err != nil {
			return nil, fmt.Errorf("failed to create NewDataFetched event: %w", err)
		}
		return event, nil
	default:
		return nil, fmt.Errorf("unknown command type: %T", cmd)
	}
}

// Apply updates the aggregate state by applying an event
func (a *MarketAggregate) Apply(event domain.Event, isNew bool) {
	if isNew {
		a.RaiseEvent(event)
	}

	a.Vers = event.Version()

	// Since we only have NewDataFetchedEvent for now:
	if event.EventType() == NewDataFetchedEvent {
		// In a real application, we might unmarshal the data and update fields,
		// but since the event itself has the payload if it's already instantiated as MarketEvent:
		if me, ok := event.(MarketEvent); ok {
			a.Symbol = me.Payload.Symbol
			a.Name = me.Payload.Name
			a.CurrentPrice = me.Payload.CurrentPrice
			a.MarketCap = me.Payload.MarketCap
			a.Volume24h = me.Payload.Volume24h
			a.PriceChange24h = me.Payload.PriceChange24h
		}
	}
}
