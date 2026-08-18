package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/holdex/epic-fermi/api"
	grpcapi "github.com/holdex/epic-fermi/api/grpc"
	pb "github.com/holdex/epic-fermi/api/grpc/proto"
	"github.com/holdex/epic-fermi/internal/aggregator"
	"github.com/holdex/epic-fermi/internal/aggregator/coingecko"
	"github.com/holdex/epic-fermi/internal/cache"
	"github.com/holdex/epic-fermi/internal/config"
	"github.com/holdex/epic-fermi/internal/database"
	"github.com/holdex/epic-fermi/internal/eventstore"
	"github.com/holdex/epic-fermi/internal/projection"
	"github.com/holdex/epic-fermi/internal/query"
	"github.com/holdex/epic-fermi/pkg/logger"
)

func main() {
	if err := run(); err != nil {
		slog.Error("Application terminated with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.Load()
	logger.Init(cfg.LogLevel)

	slog.Info("Starting Holdex Portfolio backend", "config", cfg.String())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize databases
	writePool, readPool, rdb, err := initDatabases(ctx, cfg)
	if err != nil {
		return fmt.Errorf("database initialization failed: %w", err)
	}
	defer writePool.Close()
	defer readPool.Close()
	defer rdb.Close()

	// 2. Instantiate services
	store := eventstore.NewStore(writePool, rdb)
	cacheMgr := cache.NewCache(rdb)
	projRepo := projection.NewRepository(readPool)
	cachedRepo := query.NewCachedRepository(projRepo, cacheMgr)
	queryService := query.NewService(cachedRepo)

	projector := projection.NewProjector(store, projection.NewRepository(writePool), cacheMgr)
	outboxPublisher := eventstore.NewOutboxPublisher(writePool, rdb, 100*time.Millisecond)

	cgClient := coingecko.NewHTTPClient("", cfg.CoinGeckoAPIKey, 10*time.Second)
	processor := aggregator.NewProcessor(store)
	aggregatorDaemon := aggregator.NewService(cgClient, processor, cfg.CoinGeckoCoins, cfg.CoinGeckoPollInterval)

	// 3. Start background daemons
	var wg sync.WaitGroup
	startBackgroundDaemons(ctx, &wg, outboxPublisher, projector, aggregatorDaemon, writePool)

	// 4. Start HTTP and gRPC servers
	httpServer, grpcServer, err := startServers(ctx, cfg, queryService, store, readPool, rdb)
	if err != nil {
		return fmt.Errorf("failed to start servers: %w", err)
	}

	// 5. Handle graceful shutdown
	handleGracefulShutdown(cancel, &wg, httpServer, grpcServer)
	return nil
}

func initDatabases(ctx context.Context, cfg *config.Config) (*pgxpool.Pool, *pgxpool.Pool, *redis.Client, error) {
	// Run migrations
	if !cfg.SkipMigrations {
		slog.Info("Running migrations...")
		err := database.RunMigrations(ctx, cfg.DBDSN, "migrations")
		if err != nil {
			return nil, nil, nil, fmt.Errorf("migration run failed: %w", err)
		}
		slog.Info("Migrations applied successfully")
	} else {
		slog.Info("Migrations skipped by configuration")
	}

	// Connect write pool
	writeConfig, err := pgxpool.ParseConfig(cfg.DBDSN)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse database DSN config: %w", err)
	}
	writeConfig.MaxConns = int32(cfg.DBMaxConns)
	writeConfig.MinConns = int32(cfg.DBMinConns)
	writeConfig.MaxConnIdleTime = cfg.DBMaxConnIdleTime

	slog.Info("Connecting to PostgreSQL (Writer)", 
		"max_conns", writeConfig.MaxConns, 
		"min_conns", writeConfig.MinConns, 
		"idle_time", writeConfig.MaxConnIdleTime,
	)

	writePool, err := pgxpool.NewWithConfig(ctx, writeConfig)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to connect to Postgres write pool: %w", err)
	}

	if err := writePool.Ping(ctx); err != nil {
		writePool.Close()
		return nil, nil, nil, fmt.Errorf("database write pool ping failed: %w", err)
	}

	// Connect read pool
	readConfig, err := pgxpool.ParseConfig(cfg.DBDSNRead)
	if err != nil {
		writePool.Close()
		return nil, nil, nil, fmt.Errorf("failed to parse database DSN read config: %w", err)
	}
	readConfig.MaxConns = int32(cfg.DBMaxConns)
	readConfig.MinConns = int32(cfg.DBMinConns)
	readConfig.MaxConnIdleTime = cfg.DBMaxConnIdleTime

	slog.Info("Connecting to PostgreSQL (Reader)", 
		"max_conns", readConfig.MaxConns, 
		"min_conns", readConfig.MinConns, 
		"idle_time", readConfig.MaxConnIdleTime,
	)

	readPool, err := pgxpool.NewWithConfig(ctx, readConfig)
	if err != nil {
		writePool.Close()
		return nil, nil, nil, fmt.Errorf("failed to connect to Postgres read pool: %w", err)
	}

	if err := readPool.Ping(ctx); err != nil {
		writePool.Close()
		readPool.Close()
		return nil, nil, nil, fmt.Errorf("database read pool ping failed: %w", err)
	}

	// Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		writePool.Close()
		readPool.Close()
		rdb.Close()
		return nil, nil, nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return writePool, readPool, rdb, nil
}

func startBackgroundDaemons(ctx context.Context, wg *sync.WaitGroup, outboxPublisher *eventstore.OutboxPublisher, projector *projection.Projector, aggregatorDaemon *aggregator.Service, writePool *pgxpool.Pool) {
	// Start Outbox Publisher background daemon
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := outboxPublisher.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("Outbox publisher stopped with error", "error", err)
		}
	}()

	// Start Projection engine background loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := projector.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("Projector stopped with error", "error", err)
		}
	}()

	// Start Aggregator background daemon
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := aggregatorDaemon.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("Aggregator daemon stopped with error", "error", err)
		}
	}()

	// Start Price History Pruning background loop (runs once a day, prunes data older than 7 days)
	wg.Add(1)
	go func() {
		defer wg.Done()
		slog.Info("Starting price history pruner daemon")
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()

		writeRepo := projection.NewRepository(writePool)

		// Run immediate prune on startup
		if err := writeRepo.PrunePriceHistory(ctx, 7*24*time.Hour); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("Failed to prune price history on startup", "error", err)
		}

		for {
			select {
			case <-ctx.Done():
				slog.Info("Stopping price history pruner daemon")
				return
			case <-ticker.C:
				if err := writeRepo.PrunePriceHistory(ctx, 7*24*time.Hour); err != nil {
					slog.Error("Failed to prune price history", "error", err)
				} else {
					slog.Info("Successfully pruned historical price points older than 7 days")
				}
			}
		}
	}()
}

func startServers(ctx context.Context, cfg *config.Config, queryService *query.Service, store eventstore.EventStore, readPool *pgxpool.Pool, rdb *redis.Client) (*http.Server, *grpc.Server, error) {
	// Instantiate Rate Limiter for gRPC and HTTP
	grpcLimiter := api.NewIPRateLimiter(ctx, rate.Limit(cfg.RateLimitRPS), cfg.RateLimitBurst)

	// Start gRPC Server with Rate Limiting (SEC-01) and Tracing
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			grpcapi.OTelUnaryServerInterceptor(),
			grpcapi.RateLimitUnaryServerInterceptor(grpcLimiter.GetLimiter("grpc-global")),
		),
		grpc.ChainStreamInterceptor(
			grpcapi.OTelStreamServerInterceptor(),
			grpcapi.RateLimitStreamServerInterceptor(grpcLimiter.GetLimiter("grpc-global")),
		),
	)
	grpcSrvImpl := grpcapi.NewServer(queryService, store)
	pb.RegisterMarketServiceServer(grpcServer, grpcSrvImpl)
	reflection.Register(grpcServer)

	grpcListener, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen on gRPC port %s: %w", cfg.GRPCPort, err)
	}

	go func() {
		slog.Info("gRPC server listening", "port", cfg.GRPCPort)
		if err := grpcServer.Serve(grpcListener); err != nil && err != grpc.ErrServerStopped {
			slog.Error("gRPC server stopped", "error", err)
		}
	}()

	// Start HTTP Server (GraphQL + Playground + Healthz)
	httpHandler := api.NewHTTPHandler(ctx, queryService, store, readPool, rdb, cfg.RateLimitRPS, cfg.RateLimitBurst, cfg.TrustProxy, cfg.APIKey)
	httpServer := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: httpHandler,
	}

	go func() {
		slog.Info("HTTP server listening", "port", cfg.Port)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server stopped with error", "error", err)
		}
	}()

	return httpServer, grpcServer, nil
}

func handleGracefulShutdown(cancel context.CancelFunc, wg *sync.WaitGroup, httpServer *http.Server, grpcServer *grpc.Server) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	slog.Info("Shutdown signal received, shutting down gracefully...")

	// Cancel context to stop background loops
	cancel()

	// Wait for background daemons to exit gracefully
	slog.Info("Waiting for background daemons to terminate...")
	wg.Wait()
	slog.Info("Background daemons stopped.")

	// Shutdown HTTP Server with a timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
	} else {
		slog.Info("HTTP server stopped cleanly")
	}

	// Stop gRPC server gracefully with a timeout fallback
	slog.Info("Stopping gRPC server gracefully...")
	grpcStopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcStopped)
	}()

	select {
	case <-grpcStopped:
		slog.Info("gRPC server stopped cleanly")
	case <-time.After(5 * time.Second):
		slog.Warn("gRPC server graceful stop timed out, forcing stop...")
		grpcServer.Stop()
	}

	slog.Info("Server exited cleanly")
}
