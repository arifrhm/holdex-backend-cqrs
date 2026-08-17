package market

import (
	"encoding/json"
	"time"

	"github.com/holdex/epic-fermi/internal/domain"
)

const (
	NewDataFetchedEvent = "NewDataFetched"
)

type NewDataFetchedPayload struct {
	CoinID         string    `json:"coin_id"`
	Symbol         string    `json:"symbol"`
	Name           string    `json:"name"`
	CurrentPrice   float64   `json:"current_price"`
	MarketCap      float64   `json:"market_cap"`
	Volume24h      float64   `json:"volume_24h"`
	PriceChange24h float64   `json:"price_change_24h"`
	LastUpdated    time.Time `json:"last_updated"`
}

type MarketEvent struct {
	domain.BaseEvent
	Payload NewDataFetchedPayload
}

func NewNewDataFetchedEvent(coinID string, version int, payload NewDataFetchedPayload) (domain.Event, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	base := domain.NewBaseEvent(coinID, "Market", NewDataFetchedEvent, version, data)
	return MarketEvent{
		BaseEvent: base,
		Payload:   payload,
	}, nil
}
