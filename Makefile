.PHONY: setup generate run dev docker-up docker-down test lint migrate drift-check proto help

help:
	@echo "Available commands:"
	@echo "  setup        - Install tools (gqlgen, protoc-gen-go, etc.)"
	@echo "  generate     - Generate GraphQL and gRPC code"
	@echo "  dev          - Run server locally in development mode"
	@echo "  docker-up    - Start PostgreSQL & Redis services in background"
	@echo "  docker-down  - Stop Docker services"
	@echo "  test         - Run unit tests"
	@echo "  migrate      - Run database migrations"
	@echo "  drift-check  - Run drift detection tests"

setup:
	go install github.com/99designs/gqlgen@v0.17.49
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.59.1
	go mod tidy

generate:
	@echo "Generating GraphQL resolver stubs and models..."
	go run github.com/99designs/gqlgen generate
	@echo "Generating Protobuf files..."
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		api/grpc/proto/market.proto

dev:
	go run cmd/server/main.go

docker-up:
	docker-compose up -d postgres redis

docker-down:
	docker-compose down -v

test:
	go test -v -race ./...

migrate:
	go run cmd/migrate/main.go up

migrate-down:
	go run cmd/migrate/main.go down

migrate-rollback:
	go run cmd/migrate/main.go down
	go run cmd/migrate/main.go up

drift-check:
	./scripts/drift_check.sh

lint:
	golangci-lint run ./...
	pre-commit run --all-files

k8s-build:
	docker build -t holdex-app:latest .

k8s-import-k3d: k8s-build
	k3d image import holdex-app:latest -c mycluster

k8s-import-k3s: k8s-build
	docker save holdex-app:latest | sudo k3s ctr images import -

k8s-deploy:
	kubectl apply -f k8s/postgres.yaml
	kubectl apply -f k8s/redis.yaml
	@echo "Waiting for database deployments..."
	kubectl rollout status deployment/postgres
	kubectl rollout status deployment/redis
	kubectl apply -f k8s/app.yaml
	kubectl apply -f k8s/ingress.yaml

k8s-undeploy:
	kubectl delete -f k8s/
