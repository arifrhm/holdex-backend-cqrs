package aggregator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/holdex/epic-fermi/internal/aggregator/coingecko"
	"github.com/holdex/epic-fermi/internal/domain"
)

type MockEventStore struct {
	mock.Mock
}

func (m *MockEventStore) AppendEvents(ctx context.Context, aggregateID string, expectedVersion int, events []domain.Event) error {
	args := m.Called(ctx, aggregateID, expectedVersion, events)
	return args.Error(0)
}

func (m *MockEventStore) LoadEvents(ctx context.Context, aggregateID string) ([]domain.Event, error) {
	args := m.Called(ctx, aggregateID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]domain.Event), args.Error(1)
}

func (m *MockEventStore) GetLatestEvent(ctx context.Context, aggregateID string) (domain.Event, error) {
	args := m.Called(ctx, aggregateID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(domain.Event), args.Error(1)
}

func (m *MockEventStore) Subscribe(ctx context.Context, eventTypes ...string) (<-chan domain.Event, error) {
	args := m.Called(ctx, eventTypes)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(<-chan domain.Event), args.Error(1)
}

func TestProcessor_ProcessMarketData(t *testing.T) {
	mockStore := new(MockEventStore)
	processor := NewProcessor(mockStore)

	ctx := context.Background()
	coinID := "bitcoin"
	now := time.Now()

	// Setup mock behavior
	mockStore.On("GetLatestEvent", ctx, coinID).Return(nil, nil)
	mockStore.On("AppendEvents", ctx, coinID, 0, mock.Anything).Return(nil)

	record := coingecko.MarketData{
		ID:             coinID,
		Symbol:         "btc",
		Name:           "Bitcoin",
		CurrentPrice:   60000.0,
		MarketCap:      1200000000000.0,
		TotalVolume:    30000000000.0,
		PriceChange24h: 3.2,
		LastUpdated:    now,
	}

	err := processor.ProcessMarketData(ctx, record)
	assert.NoError(t, err)

	mockStore.AssertExpectations(t)
}
