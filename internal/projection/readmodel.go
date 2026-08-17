package projection

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MarketSummary struct {
	CoinID         string    `json:"coin_id"`
	Symbol         string    `json:"symbol"`
	Name           string    `json:"name"`
	CurrentPrice   float64   `json:"current_price"`
	MarketCap      float64   `json:"market_cap"`
	Volume24h      float64   `json:"volume_24h"`
	PriceChange24h float64   `json:"price_change_24h"`
	LastUpdated    time.Time `json:"last_updated"`
}

type PricePoint struct {
	Price      float64   `json:"price"`
	RecordedAt time.Time `json:"recorded_at"`
}

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// UpdateMarketSummary updates the projected market summary for a coin (upsert)
func (r *Repository) UpdateMarketSummary(ctx context.Context, summary MarketSummary) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO market_summaries (coin_id, symbol, name, current_price, market_cap, volume_24h, price_change_24h, last_updated)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (coin_id) DO UPDATE SET
			 symbol = EXCLUDED.symbol,
			 name = EXCLUDED.name,
			 current_price = EXCLUDED.current_price,
			 market_cap = EXCLUDED.market_cap,
			 volume_24h = EXCLUDED.volume_24h,
			 price_change_24h = EXCLUDED.price_change_24h,
			 last_updated = EXCLUDED.last_updated`,
		summary.CoinID, summary.Symbol, summary.Name, summary.CurrentPrice, summary.MarketCap, summary.Volume24h, summary.PriceChange24h, summary.LastUpdated,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert market summary: %w", err)
	}
	return nil
}

// AddPriceHistory records a new spot price point for a coin in the historical read model
func (r *Repository) AddPriceHistory(ctx context.Context, coinID string, price float64, recordedAt time.Time) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO price_history (coin_id, price, recorded_at)
		 VALUES ($1, $2, $3)`,
		coinID, price, recordedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert price history: %w", err)
	}
	return nil
}

// GetMarketSummaries retrieves list of all projected summaries
func (r *Repository) GetMarketSummaries(ctx context.Context, limit, offset int) ([]MarketSummary, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := r.pool.Query(ctx,
		`SELECT coin_id, symbol, name, current_price, market_cap, volume_24h, price_change_24h, last_updated
		 FROM market_summaries
		 ORDER BY market_cap DESC NULLS LAST
		 LIMIT $1 OFFSET $2`,
		limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query market summaries: %w", err)
	}
	defer rows.Close()

	var summaries []MarketSummary
	for rows.Next() {
		var s MarketSummary
		err := rows.Scan(
			&s.CoinID, &s.Symbol, &s.Name, &s.CurrentPrice,
			&s.MarketCap, &s.Volume24h, &s.PriceChange24h, &s.LastUpdated,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan market summary: %w", err)
		}
		summaries = append(summaries, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during market summaries iteration: %w", err)
	}

	return summaries, nil
}

// GetMarketSummary retrieves summary for a single coin
func (r *Repository) GetMarketSummary(ctx context.Context, coinID string) (*MarketSummary, error) {
	var s MarketSummary
	err := r.pool.QueryRow(ctx,
		`SELECT coin_id, symbol, name, current_price, market_cap, volume_24h, price_change_24h, last_updated
		 FROM market_summaries
		 WHERE coin_id = $1`,
		coinID,
	).Scan(
		&s.CoinID, &s.Symbol, &s.Name, &s.CurrentPrice,
		&s.MarketCap, &s.Volume24h, &s.PriceChange24h, &s.LastUpdated,
	)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// GetPriceHistory retrieves the recent price points for a coin
func (r *Repository) GetPriceHistory(ctx context.Context, coinID string, limit int) ([]PricePoint, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := r.pool.Query(ctx,
		`SELECT price, recorded_at
		 FROM price_history
		 WHERE coin_id = $1
		 ORDER BY recorded_at DESC
		 LIMIT $2`,
		coinID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query price history: %w", err)
	}
	defer rows.Close()

	var points []PricePoint
	for rows.Next() {
		var p PricePoint
		err := rows.Scan(&p.Price, &p.RecordedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan price point: %w", err)
		}
		points = append(points, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during price history iteration: %w", err)
	}

	return points, nil
}

// GetPriceHistories retrieves historical price points for multiple coin IDs in a single batch query using SQL window partitioning
func (r *Repository) GetPriceHistories(ctx context.Context, coinIDs []string, limit int) (map[string][]PricePoint, error) {
	if limit <= 0 {
		limit = 100
	}

	res := make(map[string][]PricePoint)
	if len(coinIDs) == 0 {
		return res, nil
	}

	rows, err := r.pool.Query(ctx,
		`SELECT coin_id, price, recorded_at FROM (
			SELECT coin_id, price, recorded_at, ROW_NUMBER() OVER (PARTITION BY coin_id ORDER BY recorded_at DESC) as rn
			FROM price_history
			WHERE coin_id = ANY($1)
		 ) t WHERE rn <= $2`,
		coinIDs, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query batch price histories: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var coinID string
		var p PricePoint
		err := rows.Scan(&coinID, &p.Price, &p.RecordedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan batch price point: %w", err)
		}
		res[coinID] = append(res[coinID], p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during batch price histories iteration: %w", err)
	}

	return res, nil
}

type EventRecord struct {
	ID        int64
	Type      string
	AggID     string
	Version   int
	CreatedAt time.Time
	Data      []byte
}

func (r *Repository) GetLastEventID(ctx context.Context, name string) (int64, error) {
	var lastID int64
	err := r.pool.QueryRow(ctx, "SELECT last_event_id FROM projector_checkpoints WHERE projector_name = $1", name).Scan(&lastID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return lastID, err
}

func (r *Repository) UpdateLastEventID(ctx context.Context, name string, lastID int64) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO projector_checkpoints (projector_name, last_event_id, updated_at)
		 VALUES ($1, $2, NOW())
		 ON CONFLICT (projector_name) DO UPDATE SET
			 last_event_id = GREATEST(projector_checkpoints.last_event_id, EXCLUDED.last_event_id),
			 updated_at = EXCLUDED.updated_at`,
		name, lastID,
	)
	return err
}

func (r *Repository) GetNewEvents(ctx context.Context, lastID int64, limit int) ([]EventRecord, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, event_type, aggregate_id, version, created_at, data
		 FROM events
		 WHERE id > $1
		 ORDER BY id ASC
		 LIMIT $2`,
		lastID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []EventRecord
	for rows.Next() {
		var rec EventRecord
		if err := rows.Scan(&rec.ID, &rec.Type, &rec.AggID, &rec.Version, &rec.CreatedAt, &rec.Data); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func (r *Repository) PrunePriceHistory(ctx context.Context, retention time.Duration) error {
	cutoff := time.Now().Add(-retention)
	_, err := r.pool.Exec(ctx, "DELETE FROM price_history WHERE recorded_at < $1", cutoff)
	return err
}

