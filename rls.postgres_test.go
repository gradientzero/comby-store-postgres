package store_test

// End-to-end tests for Postgres Row Level Security integration.
//
// These tests assume a Postgres container is reachable at
// localhost:5432 with user=postgres password=mysecretpassword db=postgres
// (matching the convention of the other test files in this package).
//
// The tests share one database, so each one TRUNCATEs the comby tables
// at start and end, and disables RLS on cleanup so subsequent runs of
// the existing TestEventStore* / TestCommandStore* / TestSnapshotStore*
// tests are not affected.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	store "github.com/gradientzero/comby-store-postgres"
	"github.com/gradientzero/comby/v3"
	_ "github.com/lib/pq"
)

const (
	rlsTestHost     = "localhost"
	rlsTestPort     = 5432
	rlsTestOwner    = "postgres"
	rlsTestOwnerPwd = "mysecretpassword"
	rlsTestDB       = "postgres"

	rlsTestAppRole = "comby_app_rls_test"
	rlsTestAppPwd  = "apppassword"
)

// rlsOwnerDB opens a direct owner connection for setup/teardown.
func rlsOwnerDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		rlsTestHost, rlsTestPort, rlsTestOwner, rlsTestOwnerPwd, rlsTestDB)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open owner db: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping owner db: %v", err)
	}
	return db
}

// rlsResetState wipes comby tables and disables RLS so the next test
// (or the existing non-RLS tests) starts from a clean slate. Tables are
// kept (so we don't reprovision them every time) — only the data and
// policies are removed.
func rlsResetState(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()

	// Drop policies + RLS first so TRUNCATE succeeds even if something is
	// in a weird state.
	_ = store.DisablePostgresRLS(ctx, db)
	for _, table := range []string{"events", "commands", "snapshots"} {
		// TABLE may not exist on first run — ignore errors.
		_, _ = db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE;", table))
	}
}

// rlsAppDB opens a direct connection as the test app role.
func rlsAppDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		rlsTestHost, rlsTestPort, rlsTestAppRole, rlsTestAppPwd, rlsTestDB)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open app db: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping app db: %v", err)
	}
	return db
}

// makeRLSEventStore initialises an event store with the RLS app role
// option set, ready for tenant-scoped ops.
func makeRLSEventStore(t *testing.T) comby.EventStore {
	t.Helper()
	es := store.NewEventStorePostgres(rlsTestHost, rlsTestPort, rlsTestOwner, rlsTestOwnerPwd, rlsTestDB)
	if err := es.Init(context.Background(),
		comby.EventStoreOptionWithRLSAppRole(rlsTestAppRole, rlsTestAppPwd),
	); err != nil {
		t.Fatalf("event store init: %v", err)
	}
	return es
}

// newEvent builds a minimal Event ready for store.Create.
func newEvent(tenantUuid, aggregateUuid, dataType string) comby.Event {
	evt := comby.NewBaseEvent()
	_ = evt.SetEventUuid(comby.NewUuid())
	_ = evt.SetTenantUuid(tenantUuid)
	_ = evt.SetAggregateUuid(aggregateUuid)
	_ = evt.SetDomain("Test")
	_ = evt.SetDomainEvtName(dataType)
	_ = evt.SetVersion(1)
	_ = evt.SetCreatedAt(time.Now().UnixNano())
	_ = evt.SetDomainEvtBytes([]byte(`{"hello":"world"}`))
	return evt
}

// ============================================================================
// Test 1: RLS-off path is unchanged (backwards compatibility)
// ============================================================================

// TestRLS_OffPathBackwardsCompat: when WithRLSAppRole is NOT set, the store
// behaves exactly as before — single owner connection, no RLS policies, no
// app role. This is what every existing non-RLS test relies on.
func TestRLS_OffPathBackwardsCompat(t *testing.T) {
	owner := rlsOwnerDB(t)
	defer owner.Close()
	rlsResetState(t, owner)
	t.Cleanup(func() { rlsResetState(t, owner) })

	ctx := context.Background()
	es := store.NewEventStorePostgres(rlsTestHost, rlsTestPort, rlsTestOwner, rlsTestOwnerPwd, rlsTestDB)
	if err := es.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer es.Close(ctx)

	tenant := comby.NewUuid()
	if err := es.Create(ctx, comby.EventStoreCreateOptionWithEvent(newEvent(tenant, comby.NewUuid(), "X"))); err != nil {
		t.Fatal(err)
	}

	evts, total, err := es.List(ctx, comby.EventStoreListOptionWithTenantUuid(tenant))
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(evts) != 1 {
		t.Fatalf("expected 1 event, got total=%d len=%d", total, len(evts))
	}

	// Verify RLS was NOT enabled on the events table.
	var enabled bool
	row := owner.QueryRow(`SELECT relrowsecurity FROM pg_class WHERE relname='events'`)
	if err := row.Scan(&enabled); err != nil {
		t.Fatal(err)
	}
	if enabled {
		t.Fatal("expected RLS to be DISABLED on events table when WithRLSAppRole is not set")
	}
}

// ============================================================================
// Test 2: Init with RLS option provisions the role and policies
// ============================================================================

// TestRLS_InitProvisionsAppRoleAndPolicies: after Init with WithRLSAppRole,
// the events/commands/snapshots tables have RLS enabled and the comby_app
// role exists with NOBYPASSRLS.
func TestRLS_InitProvisionsAppRoleAndPolicies(t *testing.T) {
	owner := rlsOwnerDB(t)
	defer owner.Close()
	rlsResetState(t, owner)
	t.Cleanup(func() { rlsResetState(t, owner) })

	es := makeRLSEventStore(t)
	defer es.Close(context.Background())

	// RLS enabled on events
	for _, table := range []string{"events", "commands", "snapshots"} {
		var rels, forced bool
		row := owner.QueryRow(`SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname=$1`, table)
		if err := row.Scan(&rels, &forced); err != nil {
			t.Fatalf("table %s pg_class: %v", table, err)
		}
		if !rels {
			t.Fatalf("expected RLS enabled on %s", table)
		}
		if !forced {
			t.Fatalf("expected RLS FORCED on %s", table)
		}
		// Policy exists?
		var policyName string
		row = owner.QueryRow(`SELECT policyname FROM pg_policies WHERE tablename=$1 AND policyname='comby_tenant_isolation'`, table)
		if err := row.Scan(&policyName); err != nil {
			t.Fatalf("table %s missing comby_tenant_isolation policy: %v", table, err)
		}
	}

	// app role exists, NOBYPASSRLS
	var rolBypassRLS bool
	row := owner.QueryRow(`SELECT rolbypassrls FROM pg_roles WHERE rolname=$1`, rlsTestAppRole)
	if err := row.Scan(&rolBypassRLS); err != nil {
		t.Fatalf("app role %q missing: %v", rlsTestAppRole, err)
	}
	if rolBypassRLS {
		t.Fatalf("expected app role %q to be NOBYPASSRLS, got BYPASSRLS=true", rlsTestAppRole)
	}
}

// ============================================================================
// Test 3: App pool without SET LOCAL is fail-closed
// ============================================================================

// TestRLS_AppPoolFailClosedWithoutSession: a raw connection as the app role
// without app.tenant_uuid set must see 0 rows on SELECT and fail on INSERT.
// This is the policy's fail-closed property — without a session value the
// USING clause cannot evaluate cleanly, and RESTRICTIVE policies deny.
func TestRLS_AppPoolFailClosedWithoutSession(t *testing.T) {
	owner := rlsOwnerDB(t)
	defer owner.Close()
	rlsResetState(t, owner)
	t.Cleanup(func() { rlsResetState(t, owner) })

	ctx := context.Background()

	// Init RLS so the role + policies exist.
	es := makeRLSEventStore(t)
	defer es.Close(ctx)

	// Seed one event via the public API (tenant=A) so there's data to read.
	tenantA := comby.NewUuid()
	if err := es.Create(ctx, comby.EventStoreCreateOptionWithEvent(newEvent(tenantA, comby.NewUuid(), "X"))); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Direct app-role connection, no SET LOCAL.
	app := rlsAppDB(t)
	defer app.Close()

	// SELECT returns 0 (fail-closed)
	row := app.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`)
	var n int
	if err := row.Scan(&n); err != nil {
		// The query itself can also error out under RLS; either is acceptable.
		t.Logf("count error (fail-closed via error): %v", err)
	} else if n != 0 {
		t.Fatalf("expected 0 rows visible without SET LOCAL, got %d", n)
	}

	// INSERT must fail (WITH CHECK denies because USING denies first)
	_, err := app.ExecContext(ctx,
		`INSERT INTO events (uuid, tenant_uuid, domain, aggregate_uuid, version, created_at, data_type, data_bytes)
		 VALUES ($1, $2, 'Test', $3, 1, 0, 'X', '{}')`,
		comby.NewUuid(), tenantA, comby.NewUuid())
	if err == nil {
		t.Fatal("expected INSERT to fail when app.tenant_uuid is unset")
	}
}

// ============================================================================
// Test 4: Tenant isolation on reads
// ============================================================================

// TestRLS_TenantIsolationOnEvents: with RLS enabled, listing events filtered
// by tenant A only sees A's events; B sees only B's. The store routes the
// LIST through the app pool wrapped in WithRLSSession, so the RESTRICTIVE
// policy enforces the boundary.
func TestRLS_TenantIsolationOnEvents(t *testing.T) {
	owner := rlsOwnerDB(t)
	defer owner.Close()
	rlsResetState(t, owner)
	t.Cleanup(func() { rlsResetState(t, owner) })

	ctx := context.Background()
	es := makeRLSEventStore(t)
	defer es.Close(ctx)

	tenantA := comby.NewUuid()
	tenantB := comby.NewUuid()
	for i := 0; i < 3; i++ {
		if err := es.Create(ctx, comby.EventStoreCreateOptionWithEvent(newEvent(tenantA, comby.NewUuid(), "A"))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		if err := es.Create(ctx, comby.EventStoreCreateOptionWithEvent(newEvent(tenantB, comby.NewUuid(), "B"))); err != nil {
			t.Fatal(err)
		}
	}

	// Query as A
	_, totalA, err := es.List(ctx, comby.EventStoreListOptionWithTenantUuid(tenantA))
	if err != nil {
		t.Fatal(err)
	}
	if totalA != 3 {
		t.Fatalf("expected 3 events for tenant A, got %d", totalA)
	}

	// Query as B
	_, totalB, err := es.List(ctx, comby.EventStoreListOptionWithTenantUuid(tenantB))
	if err != nil {
		t.Fatal(err)
	}
	if totalB != 5 {
		t.Fatalf("expected 5 events for tenant B, got %d", totalB)
	}
}

// ============================================================================
// Test 5: Write-side WITH CHECK enforces tenant boundary
// ============================================================================

// TestRLS_WriteCheckRejectsForeignTenant: when the store is on the app pool
// with SET LOCAL app.tenant_uuid=A, an attempt to INSERT a row with
// tenant_uuid=B is rejected by the WITH CHECK clause of the RESTRICTIVE
// policy. We simulate this by calling WithRLSSession directly (bypassing
// the store's automatic routing which would have set tenant_uuid=B).
func TestRLS_WriteCheckRejectsForeignTenant(t *testing.T) {
	owner := rlsOwnerDB(t)
	defer owner.Close()
	rlsResetState(t, owner)
	t.Cleanup(func() { rlsResetState(t, owner) })

	es := makeRLSEventStore(t)
	defer es.Close(context.Background())

	tenantA := comby.NewUuid()
	tenantB := comby.NewUuid()

	// Open the app pool directly; force a SET LOCAL for tenantA but try to
	// INSERT a row claiming tenantB. This is the threat model: a legitimate
	// session for A is hijacked into trying to write to B.
	app := rlsAppDB(t)
	defer app.Close()

	err := store.WithRLSSession(context.Background(), app, tenantA, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(),
			`INSERT INTO events (uuid, tenant_uuid, domain, aggregate_uuid, version, created_at, data_type, data_bytes)
			 VALUES ($1, $2, 'Test', $3, 1, 0, 'X', '{}')`,
			comby.NewUuid(), tenantB, comby.NewUuid())
		return err
	})
	if err == nil {
		t.Fatal("expected INSERT with foreign tenant to fail (WITH CHECK)")
	}
	if !strings.Contains(err.Error(), "row-level security") && !strings.Contains(err.Error(), "violates") {
		t.Logf("INSERT failed as expected with: %v", err)
	}
}

// ============================================================================
// Test 6: WithRLSBypass(ctx) routes to owner pool
// ============================================================================

// TestRLS_BypassCtxRoutesToOwner: with RLS on, a List call carrying a
// WithRLSBypass context skips the app pool and runs on owner — useful for
// readmodel restoration and cross-tenant reactors.
func TestRLS_BypassCtxRoutesToOwner(t *testing.T) {
	owner := rlsOwnerDB(t)
	defer owner.Close()
	rlsResetState(t, owner)
	t.Cleanup(func() { rlsResetState(t, owner) })

	ctx := context.Background()
	es := makeRLSEventStore(t)
	defer es.Close(ctx)

	tenantA := comby.NewUuid()
	tenantB := comby.NewUuid()
	if err := es.Create(ctx, comby.EventStoreCreateOptionWithEvent(newEvent(tenantA, comby.NewUuid(), "A"))); err != nil {
		t.Fatal(err)
	}
	if err := es.Create(ctx, comby.EventStoreCreateOptionWithEvent(newEvent(tenantB, comby.NewUuid(), "B"))); err != nil {
		t.Fatal(err)
	}

	// Without tenant filter and without bypass: under the strict reading,
	// would route to owner (tenant filter empty) — let's confirm.
	_, total, err := es.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Fatalf("expected owner-pool list to see 2 events, got %d", total)
	}

	// With explicit bypass: same result, owner pool again.
	bypassCtx := comby.WithRLSBypass(ctx)
	_, total, err = es.List(bypassCtx)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Fatalf("expected bypass list to see 2 events, got %d", total)
	}
}

// ============================================================================
// Test 7: Bridge tenant boundary
// ============================================================================

// TestRLS_BridgeTenantBoundary: events written under tenant_uuid=<bridge>
// are only visible when the SET LOCAL value is the bridge uuid. A
// stakeholder of the bridge (with their own tenant_uuid) cannot read bridge
// events through their tenant session — exactly matching the v3 design
// principle that the bridge is its own tenant_uuid for RLS purposes.
func TestRLS_BridgeTenantBoundary(t *testing.T) {
	owner := rlsOwnerDB(t)
	defer owner.Close()
	rlsResetState(t, owner)
	t.Cleanup(func() { rlsResetState(t, owner) })

	ctx := context.Background()
	es := makeRLSEventStore(t)
	defer es.Close(ctx)

	bridgeUuid := comby.NewUuid()
	stakeholderUuid := comby.NewUuid()

	// Bridge has its own events (e.g. shared workspace data)
	for i := 0; i < 4; i++ {
		if err := es.Create(ctx, comby.EventStoreCreateOptionWithEvent(newEvent(bridgeUuid, comby.NewUuid(), "BridgeData"))); err != nil {
			t.Fatal(err)
		}
	}
	// Stakeholder has its own events in its home tenant
	if err := es.Create(ctx, comby.EventStoreCreateOptionWithEvent(newEvent(stakeholderUuid, comby.NewUuid(), "HomeData"))); err != nil {
		t.Fatal(err)
	}

	// Stakeholder session → only home events
	_, totalHome, err := es.List(ctx, comby.EventStoreListOptionWithTenantUuid(stakeholderUuid))
	if err != nil {
		t.Fatal(err)
	}
	if totalHome != 1 {
		t.Fatalf("stakeholder should see 1 home event, got %d", totalHome)
	}

	// Bridge session → only bridge events
	_, totalBridge, err := es.List(ctx, comby.EventStoreListOptionWithTenantUuid(bridgeUuid))
	if err != nil {
		t.Fatal(err)
	}
	if totalBridge != 4 {
		t.Fatalf("bridge tenant should see 4 events, got %d", totalBridge)
	}

	// Sanity: app pool used directly with stakeholder session does NOT see
	// bridge events even when explicitly asking for them by aggregate uuid
	// (since RLS hides them at the row level). This is the strongest
	// guarantee the test bundle proves.
	app := rlsAppDB(t)
	defer app.Close()
	if err := store.WithRLSSession(ctx, app, stakeholderUuid, func(tx *sql.Tx) error {
		var n int
		row := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE tenant_uuid=$1`, bridgeUuid)
		if err := row.Scan(&n); err != nil {
			return err
		}
		if n != 0 {
			return fmt.Errorf("expected stakeholder session to see 0 bridge events even when filtering by bridge uuid, got %d", n)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// ============================================================================
// Test 8: RLS applies to all three tables (events, commands, snapshots)
// ============================================================================

// TestRLS_AppliesToAllThreeTables: enable RLS once via the EventStore, then
// also run a CommandStore + SnapshotStore against the same DB (sharing the
// app role + policies that EventStore.Init created). All three must enforce
// the tenant boundary.
func TestRLS_AppliesToAllThreeTables(t *testing.T) {
	owner := rlsOwnerDB(t)
	defer owner.Close()
	rlsResetState(t, owner)
	t.Cleanup(func() { rlsResetState(t, owner) })

	ctx := context.Background()

	// EventStore drives the policy install + role provisioning.
	es := makeRLSEventStore(t)
	defer es.Close(ctx)

	// CommandStore — also configure with the same RLS app role so it gets
	// the second pool.
	cs := store.NewCommandStorePostgres(rlsTestHost, rlsTestPort, rlsTestOwner, rlsTestOwnerPwd, rlsTestDB)
	if err := cs.Init(ctx, comby.CommandStoreOptionWithRLSAppRole(rlsTestAppRole, rlsTestAppPwd)); err != nil {
		t.Fatal(err)
	}
	defer cs.Close(ctx)

	// SnapshotStore — same.
	ss := store.NewSnapshotStorePostgres(rlsTestHost, rlsTestPort, rlsTestOwner, rlsTestOwnerPwd, rlsTestDB,
		store.SnapshotStorePostgresWithRLSAppRole(rlsTestAppRole, rlsTestAppPwd))
	if err := ss.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer ss.Close(ctx)

	tenantA := comby.NewUuid()
	tenantB := comby.NewUuid()

	// Create one event for A
	if err := es.Create(ctx, comby.EventStoreCreateOptionWithEvent(newEvent(tenantA, comby.NewUuid(), "X"))); err != nil {
		t.Fatal(err)
	}

	// Create one command for A (build the command directly).
	cmd := comby.NewBaseCommand()
	_ = cmd.SetCommandUuid(comby.NewUuid())
	_ = cmd.SetTenantUuid(tenantA)
	_ = cmd.SetDomain("Test")
	_ = cmd.SetDomainCmdName("TestCmd")
	_ = cmd.SetCreatedAt(time.Now().UnixNano())
	_ = cmd.SetDomainCmdBytes([]byte(`{}`))
	if err := cs.Create(ctx, comby.CommandStoreCreateOptionWithCommand(cmd)); err != nil {
		t.Fatal(err)
	}

	// Create one snapshot for A
	if err := ss.Save(ctx, &comby.SnapshotStoreModel{
		AggregateUuid: comby.NewUuid(),
		TenantUuid:    tenantA,
		Domain:        "Test",
		Version:       1,
		Data:          []byte(`{}`),
		CreatedAt:     time.Now().UnixNano(),
	}); err != nil {
		t.Fatal(err)
	}

	// Verify tenant A sees its rows; tenant B sees nothing.
	app := rlsAppDB(t)
	defer app.Close()

	checkCount := func(tenantUuid string, expect map[string]int) {
		t.Helper()
		if err := store.WithRLSSession(ctx, app, tenantUuid, func(tx *sql.Tx) error {
			for table, want := range expect {
				var n int
				row := tx.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
				if err := row.Scan(&n); err != nil {
					return fmt.Errorf("scan %s: %w", table, err)
				}
				if n != want {
					return fmt.Errorf("tenant=%s table=%s want=%d got=%d", tenantUuid, table, want, n)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	checkCount(tenantA, map[string]int{"events": 1, "commands": 1, "snapshots": 1})
	checkCount(tenantB, map[string]int{"events": 0, "commands": 0, "snapshots": 0})
}

// ============================================================================
// Test 9: Reset runs as owner (truncates across tenants)
// ============================================================================

// TestRLS_ResetRunsAsOwner: Reset truncates the table — necessarily
// cross-tenant. It must run on the owner pool (BYPASSRLS) to succeed
// regardless of the active app session.
func TestRLS_ResetRunsAsOwner(t *testing.T) {
	owner := rlsOwnerDB(t)
	defer owner.Close()
	rlsResetState(t, owner)
	t.Cleanup(func() { rlsResetState(t, owner) })

	ctx := context.Background()
	es := makeRLSEventStore(t)
	defer es.Close(ctx)

	tenantA := comby.NewUuid()
	tenantB := comby.NewUuid()
	if err := es.Create(ctx, comby.EventStoreCreateOptionWithEvent(newEvent(tenantA, comby.NewUuid(), "X"))); err != nil {
		t.Fatal(err)
	}
	if err := es.Create(ctx, comby.EventStoreCreateOptionWithEvent(newEvent(tenantB, comby.NewUuid(), "Y"))); err != nil {
		t.Fatal(err)
	}

	if err := es.Reset(ctx); err != nil {
		t.Fatalf("reset: %v", err)
	}

	// Owner-pool count should be 0 across all tenants.
	row := owner.QueryRow(`SELECT COUNT(*) FROM events`)
	var n int
	if err := row.Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 events after Reset, got %d", n)
	}
}

// ============================================================================
// Test 10: Sanity — DisablePostgresRLS reopens access
// ============================================================================

// TestRLS_DisablePostgresRLS: sanity check that the suite is not green
// because of policy bugs — DisablePostgresRLS should restore unrestricted
// access for the same app role.
func TestRLS_DisablePostgresRLS(t *testing.T) {
	owner := rlsOwnerDB(t)
	defer owner.Close()
	rlsResetState(t, owner)
	t.Cleanup(func() { rlsResetState(t, owner) })

	ctx := context.Background()
	es := makeRLSEventStore(t)
	defer es.Close(ctx)

	tenantA := comby.NewUuid()
	if err := es.Create(ctx, comby.EventStoreCreateOptionWithEvent(newEvent(tenantA, comby.NewUuid(), "X"))); err != nil {
		t.Fatal(err)
	}

	app := rlsAppDB(t)
	defer app.Close()

	// Without SET LOCAL: 0
	row := app.QueryRow(`SELECT COUNT(*) FROM events`)
	var nBefore int
	if err := row.Scan(&nBefore); err != nil {
		// fail-closed via error counts as "blocked"
		nBefore = 0
	}
	if nBefore != 0 {
		t.Fatalf("with RLS on, expected app role to see 0 rows without SET LOCAL, got %d", nBefore)
	}

	// Disable RLS
	if err := store.DisablePostgresRLS(ctx, owner); err != nil {
		t.Fatalf("disable RLS: %v", err)
	}

	// Now app role should see the row.
	row = app.QueryRow(`SELECT COUNT(*) FROM events`)
	var nAfter int
	if err := row.Scan(&nAfter); err != nil {
		t.Fatal(err)
	}
	if nAfter != 1 {
		t.Fatalf("after DisableRLS expected 1 row visible to app role, got %d", nAfter)
	}
}

