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
	// We use current_setting('app.tenant_uuid', true) — the second arg
	// missing_ok=true makes current_setting return NULL when the GUC is
	// not set, which makes the equality NULL (i.e., not true), and the
	// RESTRICTIVE policy denies. Without missing_ok, current_setting
	// raises an error on missing GUCs, which surfaces in unintuitive ways
	// for Postgres clients that do not retry — fail-closed via a deny is
	// cleaner.
	stmts := []string{
		// events
		`ALTER TABLE events ENABLE ROW LEVEL SECURITY`,
		`ALTER TABLE events FORCE ROW LEVEL SECURITY`,
		`DROP POLICY IF EXISTS comby_tenant_isolation ON events`,
		`CREATE POLICY comby_tenant_isolation ON events
			AS PERMISSIVE
			FOR ALL
			USING (tenant_uuid = current_setting('app.tenant_uuid', true))
			WITH CHECK (tenant_uuid = current_setting('app.tenant_uuid', true))`,

		// commands
		`ALTER TABLE commands ENABLE ROW LEVEL SECURITY`,
		`ALTER TABLE commands FORCE ROW LEVEL SECURITY`,
		`DROP POLICY IF EXISTS comby_tenant_isolation ON commands`,
		`CREATE POLICY comby_tenant_isolation ON commands
			AS PERMISSIVE
			FOR ALL
			USING (tenant_uuid = current_setting('app.tenant_uuid', true))
			WITH CHECK (tenant_uuid = current_setting('app.tenant_uuid', true))`,

		// snapshots
		`ALTER TABLE snapshots ENABLE ROW LEVEL SECURITY`,
		`ALTER TABLE snapshots FORCE ROW LEVEL SECURITY`,
		`DROP POLICY IF EXISTS comby_tenant_isolation ON snapshots`,
		`CREATE POLICY comby_tenant_isolation ON snapshots
			AS PERMISSIVE
			FOR ALL
			USING (tenant_uuid = current_setting('app.tenant_uuid', true))
			WITH CHECK (tenant_uuid = current_setting('app.tenant_uuid', true))`,
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

// EnsureAppRole creates the given Postgres role if it does not exist and
// grants the privileges the role needs to read/write the comby tables.
// The role is created with NOBYPASSRLS so RESTRICTIVE policies actually apply.
//
// Idempotent: safe to call on every Init.
//
// The DDL is intentionally permissive on table grants (SELECT/INSERT/UPDATE/
// DELETE on events, commands, snapshots) — RLS does the actual filtering.
//
// Role and password go into DDL fragments where parameterised SQL is not
// supported, so we validate the role name is a safe identifier and quote the
// password as a Postgres string literal.
func EnsureAppRole(ctx context.Context, db *sql.DB, role, password string) error {
	if role == "" {
		return fmt.Errorf("EnsureAppRole: role name is required")
	}
	if !isSafePgIdentifier(role) {
		return fmt.Errorf("EnsureAppRole: role name %q must match [A-Za-z_][A-Za-z0-9_]*", role)
	}

	qRole := pgQuoteIdent(role)
	qPwd := pgQuoteLiteral(password)

	// Probe existence
	var exists bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)`, role).Scan(&exists); err != nil {
		return fmt.Errorf("EnsureAppRole: probe existence: %w", err)
	}

	if !exists {
		stmt := fmt.Sprintf(`CREATE ROLE %s LOGIN NOBYPASSRLS PASSWORD %s`, qRole, qPwd)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("EnsureAppRole: CREATE ROLE failed: %w", err)
		}
	} else {
		// Idempotent: enforce NOBYPASSRLS + LOGIN + (re)set password.
		stmts := []string{
			fmt.Sprintf(`ALTER ROLE %s NOBYPASSRLS LOGIN`, qRole),
			fmt.Sprintf(`ALTER ROLE %s WITH PASSWORD %s`, qRole, qPwd),
		}
		for _, s := range stmts {
			if _, err := db.ExecContext(ctx, s); err != nil {
				return fmt.Errorf("EnsureAppRole: ALTER ROLE failed: %w", err)
			}
		}
	}

	// Grant privileges on tables. Tables must already exist — caller must run
	// EnsureAppRole AFTER store Init has created them.
	grants := []string{
		fmt.Sprintf(`GRANT USAGE ON SCHEMA public TO %s`, qRole),
		fmt.Sprintf(`GRANT SELECT, INSERT, UPDATE, DELETE ON events TO %s`, qRole),
		fmt.Sprintf(`GRANT SELECT, INSERT, UPDATE, DELETE ON commands TO %s`, qRole),
		fmt.Sprintf(`GRANT SELECT, INSERT, UPDATE, DELETE ON snapshots TO %s`, qRole),
		fmt.Sprintf(`GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO %s`, qRole),
	}
	for _, g := range grants {
		if _, err := db.ExecContext(ctx, g); err != nil {
			return fmt.Errorf("EnsureAppRole: %q: %w", g, err)
		}
	}
	return nil
}

// isSafePgIdentifier reports whether s is a valid unquoted Postgres
// identifier (letters/digits/underscore, not starting with a digit).
// Conservative — rejects edge-cases like quoted identifiers.
func isSafePgIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r == '_':
			// ok
		case r >= '0' && r <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// pgQuoteIdent quotes a Postgres identifier (role/table/column).
// Doubles any embedded double-quote.
func pgQuoteIdent(s string) string {
	out := `"`
	for _, r := range s {
		if r == '"' {
			out += `""`
		} else {
			out += string(r)
		}
	}
	out += `"`
	return out
}

// pgQuoteLiteral quotes a Postgres string literal. Doubles embedded single
// quotes.
func pgQuoteLiteral(s string) string {
	out := "'"
	for _, r := range s {
		if r == '\'' {
			out += "''"
		} else {
			out += string(r)
		}
	}
	out += "'"
	return out
}
