package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/gradientzero/comby/v2"
	_ "github.com/lib/pq"
)

// SnapshotStorePostgresOption configures the PostgreSQL snapshot store.
type SnapshotStorePostgresOption func(*snapshotStorePostgresConfig)

type snapshotStorePostgresConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
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

// Make sure it implements interfaces
var _ comby.SnapshotStore = (*snapshotStorePostgres)(nil)

type snapshotStorePostgres struct {
	db       *sql.DB
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
	var dbNameStr string
	var passwordStr string
	if len(s.dbName) != 0 {
		dbNameStr = fmt.Sprintf("dbname=%s", s.dbName)
	}
	if len(s.password) != 0 {
		passwordStr = fmt.Sprintf("password=%s", s.password)
	}
	dsn := fmt.Sprintf("host=%s port=%d user=%s %s %s sslmode=disable",
		s.host, s.port, s.user, passwordStr, dbNameStr)

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
		domain TEXT NOT NULL,
		version BIGINT NOT NULL,
		data BYTEA NOT NULL,
		created_at BIGINT NOT NULL
	);
	`
	_, err := s.db.ExecContext(ctx, query)
	return err
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
	return nil
}

func (s *snapshotStorePostgres) Save(ctx context.Context, model *comby.SnapshotStoreModel) error {
	if model == nil {
		return fmt.Errorf("snapshot model is nil")
	}

	query := `INSERT INTO snapshots (aggregate_uuid, domain, version, data, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT(aggregate_uuid) DO UPDATE SET
			domain=EXCLUDED.domain,
			version=EXCLUDED.version,
			data=EXCLUDED.data,
			created_at=EXCLUDED.created_at;`

	_, err := s.db.ExecContext(ctx, query,
		model.AggregateUuid,
		model.Domain,
		model.Version,
		model.Data,
		model.CreatedAt,
	)
	return err
}

func (s *snapshotStorePostgres) GetLatest(ctx context.Context, aggregateUuid string) (*comby.SnapshotStoreModel, error) {
	query := `SELECT aggregate_uuid, domain, version, data, created_at
		FROM snapshots WHERE aggregate_uuid=$1 LIMIT 1;`

	row := s.db.QueryRowContext(ctx, query, aggregateUuid)

	var model comby.SnapshotStoreModel
	if err := row.Scan(
		&model.AggregateUuid,
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
	query := `DELETE FROM snapshots WHERE aggregate_uuid=$1;`
	_, err := s.db.ExecContext(ctx, query, aggregateUuid)
	return err
}

func (s *snapshotStorePostgres) Close(ctx context.Context) error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
