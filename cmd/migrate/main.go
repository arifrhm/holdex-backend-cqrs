package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/holdex/epic-fermi/internal/database"
)

func main() {
	dbDSN := os.Getenv("DB_DSN")
	if dbDSN == "" {
		dbDSN = "postgres://holdex_user:holdex_password@localhost:5433/holdex_db?sslmode=disable"
	}

	migrationsDir := os.Getenv("MIGRATIONS_DIR")
	if migrationsDir == "" {
		migrationsDir = "migrations"
	}

	action := "up"
	if len(os.Args) > 1 {
		action = os.Args[1]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if action == "up" {
		err := database.RunMigrations(ctx, dbDSN, migrationsDir)
		if err != nil {
			log.Fatalf("Migration up failed: %v", err)
		}
		log.Println("Migrations completed successfully.")
	} else if action == "down" {
		err := database.RollbackMigrations(ctx, dbDSN, migrationsDir)
		if err != nil {
			log.Fatalf("Migration rollback failed: %v", err)
		}
		log.Println("Migrations rolled back successfully.")
	} else {
		log.Fatalf("Unknown command: %s. Only 'up' and 'down' are supported.", action)
	}
}
