package eventstore

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type OutboxPublisher struct {
	pool         *pgxpool.Pool
	rdb          *redis.Client
	pollInterval time.Duration
	logger       *slog.Logger
}

func NewOutboxPublisher(pool *pgxpool.Pool, rdb *redis.Client, pollInterval time.Duration) *OutboxPublisher {
	if pollInterval <= 0 {
		pollInterval = 100 * time.Millisecond
	}
	return &OutboxPublisher{
		pool:         pool,
		rdb:          rdb,
		pollInterval: pollInterval,
		logger:       slog.Default().With("component", "outbox_publisher"),
	}
}

// Start initiates the polling loop with adaptive backoff to retrieve and publish outbox events to Redis Pub/Sub
func (p *OutboxPublisher) Start(ctx context.Context) error {
	p.logger.Info("Starting outbox publisher daemon", "base_poll_interval", p.pollInterval)

	currentInterval := p.pollInterval
	maxInterval := 2 * time.Second

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Stopping outbox publisher daemon")
			return ctx.Err()
		default:
			count, err := p.publishBatch(ctx)
			if err != nil {
				p.logger.Error("Failed to publish outbox batch, backing off...", "error", err)
				// Backoff on failure
				currentInterval = 1 * time.Second
			} else if count > 0 {
				// Reset to base poll interval immediately on activity
				currentInterval = p.pollInterval
			} else {
				// Exponentially backoff when idle
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

type outboxRecord struct {
	ID      int64
	Payload string
}

// publishBatch publishes a batch of outbox events and returns the number of events published, or an error
func (p *OutboxPublisher) publishBatch(ctx context.Context) (int, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Query pending outbox entries with FOR UPDATE SKIP LOCKED
	rows, err := tx.Query(ctx,
		`SELECT id, payload
		 FROM outbox_events
		 ORDER BY id ASC
		 LIMIT 100
		 FOR UPDATE SKIP LOCKED`,
	)
	if err != nil {
		return 0, fmt.Errorf("failed to query outbox: %w", err)
	}
	defer rows.Close()

	var records []outboxRecord
	for rows.Next() {
		var rec outboxRecord
		if err := rows.Scan(&rec.ID, &rec.Payload); err != nil {
			return 0, fmt.Errorf("failed to scan outbox row: %w", err)
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("error during outbox rows iteration: %w", err)
	}
	rows.Close() // Close early to release lock / connection resource

	if len(records) == 0 {
		return 0, nil
	}

	p.logger.Debug("Processing outbox events batch", "count", len(records))

	var publishedIDs []int64
	var publishErr error

	for _, rec := range records {
		// 1. Publish to Redis Pub/Sub
		err := p.rdb.Publish(ctx, RedisEventChannel, rec.Payload).Err()
		if err != nil {
			publishErr = fmt.Errorf("failed to publish event %d to Redis: %w", rec.ID, err)
			break
		}
		publishedIDs = append(publishedIDs, rec.ID)
		p.logger.Debug("Successfully published outbox event", "outbox_id", rec.ID)
	}

	// 2. Bulk delete successfully published records in the same transaction
	if len(publishedIDs) > 0 {
		_, err = tx.Exec(ctx, "DELETE FROM outbox_events WHERE id = ANY($1)", publishedIDs)
		if err != nil {
			return len(publishedIDs), fmt.Errorf("failed to delete outbox records: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return len(publishedIDs), fmt.Errorf("failed to commit transaction: %w", err)
	}

	if publishErr != nil {
		return len(publishedIDs), publishErr
	}

	return len(records), nil
}
