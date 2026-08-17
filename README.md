# Holdex Portfolio — Real-Time Economic Data Aggregation Backend

[![Go Version](https://img.shields.io/github/go-mod/go-version/holdex/epic-fermi?color=blue&logo=go)](https://golang.org/)
[![Docker Build](https://img.shields.io/badge/docker-build-blue?logo=docker)](https://www.docker.com/)
[![CI Status](https://img.shields.io/badge/ci-passing-brightgreen?logo=github-actions)](https://github.com/)

A Go-based, production-ready backend system for real-time economic data aggregation, demonstrating high-performance software engineering using **Event Sourcing** and **CQRS (Command Query Responsibility Segregation)** patterns.

Features real-time ingestion from CoinGecko API, a PostgreSQL-backed Event Store with optimistic concurrency control, an event projector to build optimized read models, Redis query caching with automated invalidation, and dual API exposure via **GraphQL** and **gRPC**.

---

## System Architecture

```mermaid
flowchart TD
    subgraph "Write Side (Command)"
        CG[CoinGecko API] -->|Periodic Ingestion| Daemon[Aggregator Service]
        Daemon -->|FetchMarketDataCommand| Agg[Market Aggregate]
        Agg -->|Generate Events| ES[Event Store - Postgres]
    end

    subgraph Real-Time Dispatcher
        ES -->|Publish| RedisPubSub[(Redis Pub/Sub)]
    end

    subgraph Projection Engine
        RedisPubSub -->|Subscribe| Projector[Projector Service]
        Projector -->|Update Read Models| PostgresRead[(Read Models - Postgres)]
        Projector -->|Invalidate Detail Cache| RedisCache[(Redis Cache)]
    end

    subgraph "Read Side (Query)"
        UserGQL[GraphQL Client] -->|HTTP POST| GQL[GraphQL Server]
        UserGRPC[gRPC Client] -->|gRPC TCP| GRPC[gRPC Server]
        
        GQL -->|Query| QS[Query Service]
        GRPC -->|Query| QS[Query Service]
        
        QS -->|Check Cache| RedisCache
        RedisCache -->|Cache Miss| PostgresRead
    end
```

---

## Quick Start

The entire stack (PostgreSQL, Redis, migrations, background aggregator, projector, and servers) runs seamlessly with Docker Compose.

### Running with Docker Compose

1. Clone the repository:
   ```bash
   git clone https://github.com/holdex/epic-fermi.git
   cd epic-fermi
   ```

2. Start the services:
   ```bash
   docker-compose up --build
   ```

3. Verification:
   - **GraphQL Playground**: Access at `http://localhost:8080/` (routes to Playground, queries route to `/query`).
   - **GraphQL Health Endpoint**: `http://localhost:8080/healthz` (returns `OK` with status 200).
   - **gRPC Server**: Running at `localhost:9090`.

---

## Development Workflow

A `Makefile` is provided to simplify development and testing workflows.

| Command | Action |
|:---|:---|
| `make setup` | Install development tool dependencies (`gqlgen`, `protoc` Go plugins) |
| `make generate` | Regenerate GraphQL resolvers and compile Protobuf files |
| `make dev` | Run the Go server locally in development mode |
| `make docker-up` | Spin up PostgreSQL and Redis instances in the background |
| `make docker-down` | Tear down docker services and volumes |
| `make test` | Run all unit and integration tests (skips integration if DB unavailable) |
| `make migrate` | Run Postgres schema migrations locally |
| `make drift-check` | Run snapshot-based API payload drift detection tests |

---

## API Documentation & Examples

### 1. GraphQL API (Port 8080)

Open `http://localhost:8080/` in your browser to access the GraphQL Playground.

#### Get Market Summaries (Query)
```graphql
query GetMarkets {
  marketSummaries(limit: 10, offset: 0) {
    coinId
    symbol
    name
    currentPrice
    marketCap
    volume24h
    priceChange24h
    lastUpdated
  }
}
```

#### Get Price History Chart (Query)
```graphql
query GetChart {
  priceHistory(coinId: "bitcoin", limit: 30) {
    price
    recordedAt
  }
}
```

#### Subscribe to Live Price Updates (Subscription)
```graphql
subscription OnPriceChange {
  marketPriceUpdated(coinId: "bitcoin") {
    coinId
    currentPrice
    priceChange24h
    lastUpdated
  }
}
```

---

### 2. gRPC API (Port 9090)

The gRPC server supports Reflection, enabling you to inspect it with tools like `grpcurl`.

#### Get Market Summaries
```bash
grpcurl -plaintext -d '{"limit": 5, "offset": 0}' localhost:9090 market.v1.MarketService/GetMarketSummaries
```

#### Stream Live Price Updates (Server Streaming)
```bash
grpcurl -plaintext -d '{"coin_id": "bitcoin"}' localhost:9090 market.v1.MarketService/StreamMarketUpdates
```

---

## Architecture Deep-Dive

### Event Sourcing Core (`/internal/eventstore`)
- PostgreSQL is utilized as the source-of-truth Event Log.
- Optimistic Concurrency Control (OCC) is enforced using a composite unique constraint on `(aggregate_id, version)`. If concurrent commands attempt to append identical versions, the database transaction aborts with a unique constraint violation, throwing `ErrConcurrencyConflict`.
- Real-time updates are published across nodes using a Redis Pub/Sub backplane.

### CQRS Read Models & Projections (`/internal/projection`)
- The **Write Side** appends raw fetch events (`NewDataFetched`) containing spot ticker data.
- The **Projection Engine** (`Projector`) runs in a background thread, subscribing to the event channel. It updates the read-optimized tables (`market_summaries` and `price_history`) inside transactions.
- Once projections update, the projector invalidates the corresponding cached records in Redis, ensuring high query-path freshness.

### Caching (`/internal/cache`)
- Read-path queries (`Query Service`) lookup pre-computed projected views.
- Active queries are cached in Redis with a short TTL (10s for ticker summaries, 30s for historical series) to survive burst load.
- Invalidation is triggered reactively by the projector upon receiving new events.

---

## CI/CD, Pre-Commit Hooks & Drift Detection

### Pre-Commit Hooks
Local checks are configured via `.pre-commit-config.yaml` to ensure code hygiene and security before commits are made:
- **Hygiene**: Trailing whitespace removal, end-of-file newline enforcement, YAML/JSON validation, large file limits.
- **Security**: Gitleaks secrets scanner and private key detection.
- **Go Specifics**: Enforces `golangci-lint` formatting and lint checks.
- **Custom Rules**: Validates branch naming conventions and requires issue ticket references for TODOs/FIXMEs.
- Run locally via `make lint`.

### GitHub Actions Quality Gates (`ci.yml`)
The unified CI pipeline executes 7 parallel jobs to act as a **Production Quality Gate**:
1. **Lint**: Enforces formatting and pre-commit checks.
2. **Unit Tests**: Runs unit test suites with coverage reports uploaded to Codecov.
3. **Integration Tests**: Spins up Postgres & Redis, runs database migrations, and validates store/projection packages.
4. **Security Scanning**: Conducts `govulncheck` vulnerability scans and CodeQL analysis for Go.
5. **API Drift Check**: Spins up local services in CI and executes snapshot validation testing.
6. **Build**: Builds production-ready static Go binary and uploads build artifacts.
7. **All Checks Passed**: Dummy boundary check summarizing job outcomes for branch protection rules.

### Payload Drift Detection
API payload snapshot checks are implemented inside Go test cases in `test/drift_test.go`. The test seeds a database record, runs the servers in-memory via `httptest` and `bufconn` loopbacks, sends GraphQL and gRPC requests, and performs strict JSON comparison against `testdata/snapshots/`.
- Fails if the actual serialized response differs from the expected JSON snapshot.
- Run locally via `make drift-check`.

---

## Kubernetes Local Development (k3s / k3d)

The project includes raw Kubernetes manifests inside the `k8s/` directory. This configuration allows you to run and verify the entire microservices stack (PostgreSQL, Redis, migrations, application, and Ingress) in a local Kubernetes cluster like **k3s** or **k3d**.

### 1. Build & Import Image

For the Kubernetes cluster to find the application image without pulling from an external registry, you must build the image locally and import it into your cluster's image cache:

**If using k3d (k3s in Docker):**
```bash
make k8s-import-k3d
```

**If using a native k3s installation:**
```bash
make k8s-import-k3s
```

### 2. Deploy Services

Apply the PostgreSQL, Redis, ConfigMap, Secrets, App, and Ingress manifests:
```bash
make k8s-deploy
```
This target:
1. Deploys Postgres with a `PersistentVolumeClaim`.
2. Deploys Redis.
3. Configures environmental properties via `ConfigMap` and database connection string via base64 `Secret`.
4. Deploys the main Go application with readiness/liveness probes.
5. Deploys an Ingress route mapped to port 8080.

### 3. Expose & Access Services

- **GraphQL / HTTP API**: Accessible via `http://localhost/` (or the cluster IP of the Ingress controller).
- **gRPC API**: Port forward the gRPC service to communicate from outside the cluster:
  ```bash
  kubectl port-forward svc/holdex-app 9090:9090
  ```

### 4. Undeploy Services

Clean up the deployments:
```bash
make k8s-undeploy
```

---

## GCP Deployment Guide

This project is fully containerized and designed for simple deployment to Google Cloud Platform:

1. **Database**: Use **GCP Cloud SQL** (PostgreSQL 16) with Private IP.
2. **Cache & Broker**: Use **Memorystore for Redis**.
3. **Application**: Deploy the container to **GCP Cloud Run**.
   - Build the container: `gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/holdex-app`
   - Deploy:
     ```bash
     gcloud run deploy holdex-app \
       --image gcr.io/YOUR_PROJECT_ID/holdex-app \
       --platform managed \
       --vpc-connector YOUR_VPC_CONNECTOR \
       --set-env-vars="DB_DSN=postgres://user:password@CLOUD_SQL_PRIVATE_IP:5432/holdex_db,REDIS_ADDR=REDIS_PRIVATE_IP:6379"
     ```
