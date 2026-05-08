package store

import (
	"context"
	"database/sql"
	"fmt"
)

// RLS (Row Level Security) helpers for the Postgres stores.
//
// Comby v3 ships RLS-ready schemas: events, commands, and snapshots all carry
// tenant_uuid (and workspace_uuid where applicable) columns so policies can
// enforce tenant isolation at the database layer.
//
// To enable RLS in production:
//
//  1. Run EnablePostgresRLS once against the database (DDL).
//  2. Make sure the application connects as a non-owner role that does NOT have
//     BYPASSRLS. The owner role used for migrations should retain BYPASSRLS for
//     schema changes and background jobs.
//  3. Wrap every read/write operation in a transaction that begins with
//     SET LOCAL app.tenant_uuid = '<tenantUuid>'. Use WithRLSSession as a helper.
//
// SQLite does not support RLS — the workspace_uuid/tenant_uuid columns exist
// for app-layer filtering parity only.

// EnablePostgresRLS enables Row Level Security and installs RESTRICTIVE
// tenant-isolation policies on the events, commands, and snapshots tables.
//
// Idempotent: safe to call multiple times. Uses CREATE POLICY IF NOT EXISTS
// semantics where supported; otherwise drops and recreates policies.
//
// The policies require the session variable app.tenant_uuid to be set
// (via SET LOCAL) before each query. If unset, current_setting throws and
// the RESTRICTIVE policy denies access (fail-closed).
func EnablePostgresRLS(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		// events
		`ALTER TABLE events ENABLE ROW LEVEL SECURITY`,
		`ALTER TABLE events FORCE ROW LEVEL SECURITY`,
		`DROP POLICY IF EXISTS comby_tenant_isolation ON events`,
		`CREATE POLICY comby_tenant_isolation ON events
			AS RESTRICTIVE
			FOR ALL
			USING (tenant_uuid = current_setting('app.tenant_uuid'))
			WITH CHECK (tenant_uuid = current_setting('app.tenant_uuid'))`,

		// commands
		`ALTER TABLE commands ENABLE ROW LEVEL SECURITY`,
		`ALTER TABLE commands FORCE ROW LEVEL SECURITY`,
		`DROP POLICY IF EXISTS comby_tenant_isolation ON commands`,
		`CREATE POLICY comby_tenant_isolation ON commands
			AS RESTRICTIVE
			FOR ALL
			USING (tenant_uuid = current_setting('app.tenant_uuid'))
			WITH CHECK (tenant_uuid = current_setting('app.tenant_uuid'))`,

		// snapshots
		`ALTER TABLE snapshots ENABLE ROW LEVEL SECURITY`,
		`ALTER TABLE snapshots FORCE ROW LEVEL SECURITY`,
		`DROP POLICY IF EXISTS comby_tenant_isolation ON snapshots`,
		`CREATE POLICY comby_tenant_isolation ON snapshots
			AS RESTRICTIVE
			FOR ALL
			USING (tenant_uuid = current_setting('app.tenant_uuid'))
			WITH CHECK (tenant_uuid = current_setting('app.tenant_uuid'))`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("EnablePostgresRLS failed at %q: %w", stmt, err)
		}
	}
	return nil
}

// DisablePostgresRLS removes RLS policies and disables RLS on all comby tables.
// Useful for diagnostics and migrations. Do not run in production unless intended.
func DisablePostgresRLS(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`DROP POLICY IF EXISTS comby_tenant_isolation ON events`,
		`DROP POLICY IF EXISTS comby_tenant_isolation ON commands`,
		`DROP POLICY IF EXISTS comby_tenant_isolation ON snapshots`,
		`ALTER TABLE events DISABLE ROW LEVEL SECURITY`,
		`ALTER TABLE commands DISABLE ROW LEVEL SECURITY`,
		`ALTER TABLE snapshots DISABLE ROW LEVEL SECURITY`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("DisablePostgresRLS failed at %q: %w", stmt, err)
		}
	}
	return nil
}

// WithRLSSession runs fn inside a transaction that has app.tenant_uuid set
// via SET LOCAL. The variable is automatically scoped to the transaction —
// subsequent transactions on the same connection are not affected.
//
// Usage:
//
//	err := WithRLSSession(ctx, db, tenantUuid, func(tx *sql.Tx) error {
//	    rows, err := tx.QueryContext(ctx, "SELECT ... FROM events WHERE ...")
//	    // ... process rows
//	    return nil
//	})
//
// If RLS is enabled and the SET LOCAL value is not present, RESTRICTIVE
// policies fail-closed (deny all rows).
func WithRLSSession(ctx context.Context, db *sql.DB, tenantUuid string, fn func(tx *sql.Tx) error) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
			return
		}
		err = tx.Commit()
	}()

	if _, err = tx.ExecContext(ctx, "SELECT set_config('app.tenant_uuid', $1, true)", tenantUuid); err != nil {
		return err
	}

	return fn(tx)
}
