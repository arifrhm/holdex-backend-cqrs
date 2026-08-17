package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/holdex/epic-fermi/internal/domain"
	"github.com/holdex/epic-fermi/internal/domain/market"
)

var ErrConcurrencyConflict = errors.New("optimistic concurrency conflict: version already exists")

const RedisEventChannel = "holdex_events"

type EventStore interface {
	AppendEvents(ctx context.Context, aggregateID string, expectedVersion int, events []domain.Event) error
	LoadEvents(ctx context.Context, aggregateID string) ([]domain.Event, error)
	GetLatestEvent(ctx context.Context, aggregateID string) (domain.Event, error)
	Subscribe(ctx context.Context, eventTypes ...string) (<-chan domain.Event, error)
}

type Store struct {
	pool *pgxpool.Pool
	rdb  *redis.Client
}

func NewStore(pool *pgxpool.Pool, rdb *redis.Client) *Store {
	return &Store{
		pool: pool,
		rdb:  rdb,
	}
}

// AppendEvents appends a slice of events to the store for a given aggregate
func (s *Store) AppendEvents(ctx context.Context, aggregateID string, expectedVersion int, events []domain.Event) error {
	if len(events) == 0 {
		return nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Acquire a transaction-level advisory lock on the aggregate ID to prevent concurrent version updates
	h := fnv.New64a()
	h.Write([]byte(aggregateID))
	lockKey := int64(h.Sum64())

	_, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockKey)
	if err != nil {
		return fmt.Errorf("failed to acquire advisory lock: %w", err)
	}

	// Verify current version matches expected version
	var currentVersion int
	err = tx.QueryRow(ctx, "SELECT COALESCE(MAX(version), 0) FROM events WHERE aggregate_id = $1", aggregateID).Scan(&currentVersion)
	if err != nil {
		return fmt.Errorf("failed to query current version: %w", err)
	}

	if expectedVersion >= 0 && currentVersion != expectedVersion {
		return fmt.Errorf("%w (expected: %d, current: %d)", ErrConcurrencyConflict, expectedVersion, currentVersion)
	}

	for _, event := range events {
		var eventID int64
		err = tx.QueryRow(ctx,
			`INSERT INTO events (aggregate_id, aggregate_type, event_type, version, data, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 RETURNING id`,
			event.AggregateID(), event.AggregateType(), event.EventType(), event.Version(), event.Data(), event.Timestamp(),
		).Scan(&eventID)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" { // Unique violation
				return ErrConcurrencyConflict
			}
			return fmt.Errorf("failed to insert event: %w", err)
		}

		// Serialize event wrapper for outbox payload
		payload, err := s.serializeEvent(event)
		if err != nil {
			return fmt.Errorf("failed to serialize event for outbox: %w", err)
		}

		// Write to outbox_events table inside the same transaction
		_, err = tx.Exec(ctx,
			`INSERT INTO outbox_events (event_id, payload)
			 VALUES ($1, $2)`,
			eventID, payload,
		)
		if err != nil {
			return fmt.Errorf("failed to insert outbox event: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// LoadEvents loads all events for a given aggregate ID
func (s *Store) LoadEvents(ctx context.Context, aggregateID string) ([]domain.Event, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT event_type, version, created_at, data
		 FROM events
		 WHERE aggregate_id = $1
		 ORDER BY version ASC`,
		aggregateID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	var events []domain.Event
	for rows.Next() {
		var eventType string
		var version int
		var createdAt time.Time
		var data []byte

		if err := rows.Scan(&eventType, &version, &createdAt, &data); err != nil {
			return nil, fmt.Errorf("failed to scan event row: %w", err)
		}

		event, err := s.deserializeEvent(eventType, aggregateID, version, createdAt, data)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during events iteration: %w", err)
	}

	return events, nil
}

// GetLatestEvent retrieves only the latest event for a given aggregate ID (O(1) optimization)
func (s *Store) GetLatestEvent(ctx context.Context, aggregateID string) (domain.Event, error) {
	var eventType string
	var version int
	var createdAt time.Time
	var data []byte

	err := s.pool.QueryRow(ctx,
		`SELECT event_type, version, created_at, data
		 FROM events
		 WHERE aggregate_id = $1
		 ORDER BY version DESC
		 LIMIT 1`,
		aggregateID,
	).Scan(&eventType, &version, &createdAt, &data)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query latest event: %w", err)
	}

	event, err := s.deserializeEvent(eventType, aggregateID, version, createdAt, data)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize latest event: %w", err)
	}
	return event, nil
}

// Subscribe returns a channel of live events matching specified types, with automatic reconnection
func (s *Store) Subscribe(ctx context.Context, eventTypes ...string) (<-chan domain.Event, error) {
	eventsChan := make(chan domain.Event, 100)

	go func() {
		defer close(eventsChan)

		backoff := 100 * time.Millisecond
		maxBackoff := 5 * time.Second

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			pubsub := s.rdb.Subscribe(ctx, RedisEventChannel)
			
			// Wait to see if subscription is established
			_, err := pubsub.Receive(ctx)
			if err != nil {
				pubsub.Close()
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
					continue
				}
			}

			// Reset backoff on success
			backoff = 100 * time.Millisecond

			ch := pubsub.Channel()
			
		loop:
			for {
				select {
				case <-ctx.Done():
					pubsub.Close()
					return
				case msg, ok := <-ch:
					if !ok {
						break loop
					}

					event, err := s.deserializeRawJSON([]byte(msg.Payload))
					if err != nil {
						continue
					}

					// Filter event type if filter is specified
					if len(eventTypes) > 0 {
						matched := false
						for _, et := range eventTypes {
							if event.EventType() == et {
								matched = true
								break
							}
						}
						if !matched {
							continue
						}
					}

					select {
					case <-ctx.Done():
						pubsub.Close()
						return
					case eventsChan <- event:
					}
				}
			}
			pubsub.Close()

			// Sleep briefly before reconnecting
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
		}
	}()

	return eventsChan, nil
}

type eventWrapper struct {
	Type        string    `json:"type"`
	AggregateID string    `json:"aggregate_id"`
	Version     int       `json:"version"`
	Timestamp   time.Time `json:"timestamp"`
	Data        []byte    `json:"data"`
}

func (s *Store) serializeEvent(event domain.Event) (string, error) {
	w := eventWrapper{
		Type:        event.EventType(),
		AggregateID: event.AggregateID(),
		Version:     event.Version(),
		Timestamp:   event.Timestamp(),
		Data:        event.Data(),
	}
	bytes, err := json.Marshal(w)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func (s *Store) deserializeRawJSON(data []byte) (domain.Event, error) {
	var w eventWrapper
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	return s.deserializeEvent(w.Type, w.AggregateID, w.Version, w.Timestamp, w.Data)
}

func (s *Store) deserializeEvent(eventType string, aggID string, version int, timestamp time.Time, data []byte) (domain.Event, error) {
	switch eventType {
	case market.NewDataFetchedEvent:
		var payload market.NewDataFetchedPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, err
		}
		base := domain.BaseEvent{
			Type:    eventType,
			AggID:   aggID,
			AggType: "Market",
			Vers:    version,
			Time:    timestamp,
			Payload: data,
		}
		return market.MarketEvent{
			BaseEvent: base,
			Payload:   payload,
		}, nil
	default:
		return nil, fmt.Errorf("unknown event type: %s", eventType)
	}
}
