package market

import "time"

// FetchMarketDataCommand requests fetching new market data for an asset
type FetchMarketDataCommand struct {
	CoinID         string
	Symbol         string
	Name           string
	CurrentPrice   float64
	MarketCap      float64
	Volume24h      float64
	PriceChange24h float64
	LastUpdated    time.Time
}
