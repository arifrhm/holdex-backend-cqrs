package grpc

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/holdex/epic-fermi/api/grpc/proto"
	"github.com/holdex/epic-fermi/internal/domain/market"
	"github.com/holdex/epic-fermi/internal/eventstore"
	"github.com/holdex/epic-fermi/internal/projection"
	"github.com/holdex/epic-fermi/internal/query"
)

type Server struct {
	pb.UnimplementedMarketServiceServer
	queryService *query.Service
	eventStore   eventstore.EventStore
}

func NewServer(qs *query.Service, es eventstore.EventStore) *Server {
	return &Server{
		queryService: qs,
		eventStore:   es,
	}
}

func (s *Server) GetMarketSummaries(ctx context.Context, req *pb.GetMarketSummariesRequest) (*pb.GetMarketSummariesResponse, error) {
	limit := int(req.GetLimit())
	offset := int(req.GetOffset())

	summaries, err := s.queryService.GetMarketSummaries(ctx, limit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get market summaries: %v", err)
	}

	pbSummaries := make([]*pb.MarketSummary, len(summaries))
	for i, summary := range summaries {
		pbSummaries[i] = mapSummaryToPB(summary)
	}

	return &pb.GetMarketSummariesResponse{
		Summaries: pbSummaries,
	}, nil
}

func (s *Server) GetMarketSummary(ctx context.Context, req *pb.GetMarketSummaryRequest) (*pb.MarketSummary, error) {
	summary, err := s.queryService.GetMarketSummary(ctx, req.GetCoinId())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "market summary not found for coin %s", req.GetCoinId())
		}
		return nil, status.Errorf(codes.Internal, "failed to get market summary: %v", err)
	}

	if summary == nil {
		return nil, status.Errorf(codes.NotFound, "market summary not found for coin %s", req.GetCoinId())
	}

	return mapSummaryToPB(*summary), nil
}

func (s *Server) GetPriceHistory(ctx context.Context, req *pb.GetPriceHistoryRequest) (*pb.GetPriceHistoryResponse, error) {
	limit := int(req.GetLimit())
	history, err := s.queryService.GetPriceHistory(ctx, req.GetCoinId(), limit)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "price history not found for coin %s", req.GetCoinId())
		}
		return nil, status.Errorf(codes.Internal, "failed to get price history: %v", err)
	}

	pbHistory := make([]*pb.PricePoint, len(history))
	for i, point := range history {
		pbHistory[i] = &pb.PricePoint{
			Price:      point.Price,
			RecordedAt: point.RecordedAt.UTC().Format(time.RFC3339),
		}
	}

	return &pb.GetPriceHistoryResponse{
		History: pbHistory,
	}, nil
}

func (s *Server) StreamMarketUpdates(req *pb.StreamMarketUpdatesRequest, stream pb.MarketService_StreamMarketUpdatesServer) error {
	ctx := stream.Context()
	eventsChan, err := s.eventStore.Subscribe(ctx, market.NewDataFetchedEvent)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to subscribe to events: %v", err)
	}

	filterCoinID := req.GetCoinId()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-eventsChan:
			if !ok {
				return nil
			}

			if me, ok := ev.(market.MarketEvent); ok {
				// Apply optional filter
				if filterCoinID != "" && filterCoinID != me.Payload.CoinID {
					continue
				}

				summary := &pb.MarketSummary{
					CoinId:          me.Payload.CoinID,
					Symbol:          me.Payload.Symbol,
					Name:            me.Payload.Name,
					CurrentPrice:    me.Payload.CurrentPrice,
					MarketCap:       me.Payload.MarketCap,
					Volume_24H:      me.Payload.Volume24h,
					PriceChange_24H: me.Payload.PriceChange24h,
					LastUpdated:     me.Payload.LastUpdated.UTC().Format(time.RFC3339),
				}

				if err := stream.Send(summary); err != nil {
					return err
				}
			}
		}
	}
}

func mapSummaryToPB(s projection.MarketSummary) *pb.MarketSummary {
	return &pb.MarketSummary{
		CoinId:          s.CoinID,
		Symbol:          s.Symbol,
		Name:            s.Name,
		CurrentPrice:    s.CurrentPrice,
		MarketCap:       s.MarketCap,
		Volume_24H:      s.Volume24h,
		PriceChange_24H: s.PriceChange24h,
		LastUpdated:     s.LastUpdated.UTC().Format(time.RFC3339),
	}
}
