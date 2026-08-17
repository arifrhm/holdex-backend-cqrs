package database

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// RunMigrations applies all migrations in the migrations directory to the database
func RunMigrations(ctx context.Context, dbDSN string, migrationsDir string) error {
	conn, err := pgx.Connect(ctx, dbDSN)
	if err != nil {
		return fmt.Errorf("unable to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	// Create schema_migrations table if not exists
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version VARCHAR(255) PRIMARY KEY,
			applied_at TIMESTAMPTZ DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Find migration files matching *.up.sql
	pattern := filepath.Join(migrationsDir, "*.up.sql")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to glob migration files: %w", err)
	}

	sort.Strings(files)

	for _, file := range files {
		version := filepath.Base(file)

		// Check if already applied
		var exists bool
		err = conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)", version).Scan(&exists)
		if err != nil {
			return fmt.Errorf("failed to check migration state: %w", err)
		}

		if exists {
			log.Printf("Migration %s already applied.", version)
			continue
		}

		log.Printf("Applying migration %s...", version)
		content, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("failed to read migration file %s: %w", file, err)
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		_, err = tx.Exec(ctx, string(content))
		if err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("failed to execute migration %s: %w", version, err)
		}

		_, err = tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", version)
		if err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("failed to record migration %s: %w", version, err)
		}

		err = tx.Commit(ctx)
		if err != nil {
			return fmt.Errorf("failed to commit migration %s: %w", version, err)
		}
		log.Printf("Successfully applied migration %s.", version)
	}

	return nil
}

// RollbackMigrations rolls back all migrations in reverse order
func RollbackMigrations(ctx context.Context, dbDSN string, migrationsDir string) error {
	conn, err := pgx.Connect(ctx, dbDSN)
	if err != nil {
		return fmt.Errorf("unable to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	// Check if migrations table exists
	var tableExists bool
	err = conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = 'schema_migrations'
		)
	`).Scan(&tableExists)
	if err != nil || !tableExists {
		log.Println("Migrations table does not exist. No rollbacks required.")
		return nil
	}

	// Read all applied migrations
	rows, err := conn.Query(ctx, "SELECT version FROM schema_migrations ORDER BY version DESC")
	if err != nil {
		return fmt.Errorf("failed to query applied migrations: %w", err)
	}
	defer rows.Close()

	var versions []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return fmt.Errorf("failed to scan migration version: %w", err)
		}
		versions = append(versions, v)
	}
	rows.Close()

	for _, upVersion := range versions {
		// Map up.sql version to down.sql
		downVersion := strings.Replace(upVersion, ".up.sql", ".down.sql", 1)
		file := filepath.Join(migrationsDir, downVersion)

		log.Printf("Rolling back migration %s...", downVersion)
		content, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("failed to read rollback migration file %s: %w", file, err)
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin rollback transaction: %w", err)
		}

		_, err = tx.Exec(ctx, string(content))
		if err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("failed to execute rollback migration %s: %w", downVersion, err)
		}

		_, err = tx.Exec(ctx, "DELETE FROM schema_migrations WHERE version = $1", upVersion)
		if err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("failed to delete migration record %s: %w", upVersion, err)
		}

		err = tx.Commit(ctx)
		if err != nil {
			return fmt.Errorf("failed to commit rollback transaction: %w", err)
		}
		log.Printf("Successfully rolled back migration %s.", downVersion)
	}

	return nil
}
