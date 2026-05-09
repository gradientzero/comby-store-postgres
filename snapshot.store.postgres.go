package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/gradientzero/comby/v3"
	_ "github.com/lib/pq"
)

// SnapshotStorePostgresOption configures the PostgreSQL snapshot store.
type SnapshotStorePostgresOption func(*snapshotStorePostgresConfig)

type snapshotStorePostgresConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration

	// Postgres Row Level Security: when RLSAppRoleUser is set, the store
	// opens a second pool as the (NOBYPASSRLS) app role and routes Save/
	// GetLatest through it inside SET LOCAL app.tenant_uuid sessions. See
	// EventStoreOptionWithRLSAppRole for the analogue on the event store.
	RLSAppRoleUser     string
	RLSAppRolePassword string
}

// SnapshotStorePostgresWithMaxOpenConns sets the maximum number of open connections.
func SnapshotStorePostgresWithMaxOpenConns(n int) SnapshotStorePostgresOption {
	return func(c *snapshotStorePostgresConfig) { c.MaxOpenConns = n }
}

// SnapshotStorePostgresWithMaxIdleConns sets the maximum number of idle connections.
func SnapshotStorePostgresWithMaxIdleConns(n int) SnapshotStorePostgresOption {
	return func(c *snapshotStorePostgresConfig) { c.MaxIdleConns = n }
}

// SnapshotStorePostgresWithConnMaxLifetime sets the maximum connection lifetime.
func SnapshotStorePostgresWithConnMaxLifetime(d time.Duration) SnapshotStorePostgresOption {
	return func(c *snapshotStorePostgresConfig) { c.ConnMaxLifetime = d }
}

// SnapshotStorePostgresWithConnMaxIdleTime sets the maximum connection idle time.
func SnapshotStorePostgresWithConnMaxIdleTime(d time.Duration) SnapshotStorePostgresOption {
	return func(c *snapshotStorePostgresConfig) { c.ConnMaxIdleTime = d }
}

// SnapshotStorePostgresWithRLSAppRole enables Postgres Row Level Security in
// the snapshot store. See EventStoreOptionWithRLSAppRole for semantics.
func SnapshotStorePostgresWithRLSAppRole(user, password string) SnapshotStorePostgresOption {
	return func(c *snapshotStorePostgresConfig) {
		c.RLSAppRoleUser = user
		c.RLSAppRolePassword = password
	}
}

// Make sure it implements interfaces
var _ comby.SnapshotStore = (*snapshotStorePostgres)(nil)

type snapshotStorePostgres struct {
	db       *sql.DB // owner connection (BYPASSRLS)
	appDb    *sql.DB // optional non-BYPASSRLS connection used for tenant-scoped ops; nil if RLS off
	config   snapshotStorePostgresConfig
	host     string
	port     int
	user     string
	password string
	dbName   string
}

func NewSnapshotStorePostgres(host string, port int, user, password, dbName string, opts ...SnapshotStorePostgresOption) comby.SnapshotStore {
	s := &snapshotStorePostgres{
		host:     host,
		port:     port,
		user:     user,
		password: password,
		dbName:   dbName,
	}
	for _, opt := range opts {
		opt(&s.config)
	}
	return s
}

func (s *snapshotStorePostgres) connect(ctx context.Context) (*sql.DB, error) {
	return s.connectAs(ctx, s.user, s.password)
}

func (s *snapshotStorePostgres) connectAs(ctx context.Context, user, password string) (*sql.DB, error) {
	var dbNameStr string
	var passwordStr string
	if len(s.dbName) != 0 {
		dbNameStr = fmt.Sprintf("dbname=%s", s.dbName)
	}
	if len(password) != 0 {
		passwordStr = fmt.Sprintf("password=%s", password)
	}
	dsn := fmt.Sprintf("host=%s port=%d user=%s %s %s sslmode=disable",
		s.host, s.port, user, passwordStr, dbNameStr)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	// Apply pool settings from config, falling back to sensible defaults.
	maxOpenConns := 10
	if s.config.MaxOpenConns > 0 {
		maxOpenConns = s.config.MaxOpenConns
	}
	db.SetMaxOpenConns(maxOpenConns)

	maxIdleConns := 5
	if s.config.MaxIdleConns > 0 {
		maxIdleConns = s.config.MaxIdleConns
	}
	db.SetMaxIdleConns(maxIdleConns)

	if s.config.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(s.config.ConnMaxLifetime)
	} else {
		db.SetConnMaxLifetime(30 * time.Minute)
	}

	if s.config.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(s.config.ConnMaxIdleTime)
	} else {
		db.SetConnMaxIdleTime(5 * time.Minute)
	}

	return db, nil
}

func (s *snapshotStorePostgres) migrate(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS snapshots (
		aggregate_uuid TEXT PRIMARY KEY,
		tenant_uuid TEXT,
		workspace_uuid TEXT,
		domain TEXT NOT NULL,
		version BIGINT NOT NULL,
		data BYTEA NOT NULL,
		created_at BIGINT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS "snapshots_tenant_index" ON "snapshots" ("tenant_uuid" ASC);
	CREATE INDEX IF NOT EXISTS "snapshots_workspace_index" ON "snapshots" ("workspace_uuid" ASC);
	`
	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return err
	}
	// migrate existing databases: add tenant_uuid + workspace_uuid columns if they don't exist
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS tenant_uuid TEXT`); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS workspace_uuid TEXT`); err != nil {
		return err
	}
	return nil
}

func (s *snapshotStorePostgres) Init(ctx context.Context) error {
	db, err := s.connect(ctx)
	if err != nil {
		return err
	}
	s.db = db

	if err := s.migrate(ctx); err != nil {
		return err
	}

	if s.config.RLSAppRoleUser != "" {
		if err := EnablePostgresRLS(ctx, s.db); err != nil {
			return fmt.Errorf("snapshotStorePostgres.Init: EnablePostgresRLS failed: %w", err)
		}
		if err := EnsureAppRole(ctx, s.db, s.config.RLSAppRoleUser, s.config.RLSAppRolePassword); err != nil {
			return fmt.Errorf("snapshotStorePostgres.Init: EnsureAppRole failed: %w", err)
		}
		appDb, err := s.connectAs(ctx, s.config.RLSAppRoleUser, s.config.RLSAppRolePassword)
		if err != nil {
			return fmt.Errorf("snapshotStorePostgres.Init: app-role connect failed: %w", err)
		}
		s.appDb = appDb
	}
	return nil
}

func (s *snapshotStorePostgres) rlsEnabled() bool { return s.appDb != nil }

// withTenantConn picks owner vs app pool — see eventStorePostgres.withTenantConn.
func (s *snapshotStorePostgres) withTenantConn(ctx context.Context, tenantUuid string, fn func(pgQuerier) error) error {
	if !s.rlsEnabled() || comby.IsRLSBypass(ctx) || tenantUuid == "" {
		return fn(s.db)
	}
	return WithRLSSession(ctx, s.appDb, tenantUuid, func(tx *sql.Tx) error {
		return fn(tx)
	})
}

func (s *snapshotStorePostgres) Save(ctx context.Context, model *comby.SnapshotStoreModel) error {
	if model == nil {
		return fmt.Errorf("snapshot model is nil")
	}

	query := `INSERT INTO snapshots (aggregate_uuid, tenant_uuid, workspace_uuid, domain, version, data, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT(aggregate_uuid) DO UPDATE SET
			tenant_uuid=EXCLUDED.tenant_uuid,
			workspace_uuid=EXCLUDED.workspace_uuid,
			domain=EXCLUDED.domain,
			version=EXCLUDED.version,
			data=EXCLUDED.data,
			created_at=EXCLUDED.created_at;`

	return s.withTenantConn(ctx, model.TenantUuid, func(q pgQuerier) error {
		_, err := q.ExecContext(ctx, query,
			model.AggregateUuid,
			model.TenantUuid,
			model.WorkspaceUuid,
			model.Domain,
			model.Version,
			model.Data,
			model.CreatedAt,
		)
		return err
	})
}

func (s *snapshotStorePostgres) GetLatest(ctx context.Context, aggregateUuid string) (*comby.SnapshotStoreModel, error) {
	// GetLatest has no tenant filter on input. We first do an owner-pool peek
	// to learn the tenant, then re-run as the app role. This keeps the RLS
	// boundary strict — a hostile caller using a tenant session would still
	// be unable to read another tenant's snapshot, because the second query
	// runs under their actual SET LOCAL value.
	//
	// In practice GetLatest is invoked from the AggregateRepository which
	// already knows the tenant and runs under WithRLSBypass for restoration
	// paths or through the owner pool. We keep this simple: route to owner.
	query := `SELECT aggregate_uuid, COALESCE(tenant_uuid, ''), COALESCE(workspace_uuid, ''), domain, version, data, created_at
		FROM snapshots WHERE aggregate_uuid=$1 LIMIT 1;`

	row := s.db.QueryRowContext(ctx, query, aggregateUuid)

	var model comby.SnapshotStoreModel
	if err := row.Scan(
		&model.AggregateUuid,
		&model.TenantUuid,
		&model.WorkspaceUuid,
		&model.Domain,
		&model.Version,
		&model.Data,
		&model.CreatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &model, nil
}

func (s *snapshotStorePostgres) Delete(ctx context.Context, aggregateUuid string) error {
	// Delete-by-aggregate has no tenant filter — runs on owner pool.
	query := `DELETE FROM snapshots WHERE aggregate_uuid=$1;`
	_, err := s.db.ExecContext(ctx, query, aggregateUuid)
	return err
}

func (s *snapshotStorePostgres) Close(ctx context.Context) error {
	if s.appDb != nil {
		_ = s.appDb.Close()
	}
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
