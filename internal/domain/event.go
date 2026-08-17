package domain

import "time"

// Event is the interface that all domain events must implement
type Event interface {
	EventType() string
	AggregateID() string
	AggregateType() string
	Version() int
	Timestamp() time.Time
	Data() []byte
}

// BaseEvent provides a common implementation of Event interface
type BaseEvent struct {
	Type      string    `json:"event_type"`
	AggID     string    `json:"aggregate_id"`
	AggType   string    `json:"aggregate_type"`
	Vers      int       `json:"version"`
	Time      time.Time `json:"timestamp"`
	Payload   []byte    `json:"-"`
}

func (e BaseEvent) EventType() string      { return e.Type }
func (e BaseEvent) AggregateID() string    { return e.AggID }
func (e BaseEvent) AggregateType() string  { return e.AggType }
func (e BaseEvent) Version() int           { return e.Vers }
func (e BaseEvent) Timestamp() time.Time   { return e.Time }
func (e BaseEvent) Data() []byte           { return e.Payload }

func NewBaseEvent(aggID, aggType, eventType string, version int, payload []byte) BaseEvent {
	return BaseEvent{
		Type:    eventType,
		AggID:   aggID,
		AggType: aggType,
		Vers:    version,
		Time:    time.Now(),
		Payload: payload,
	}
}
