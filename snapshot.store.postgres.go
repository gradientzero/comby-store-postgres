package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/gradientzero/comby/v2"
	_ "github.com/lib/pq"
)

// Make sure it implements interfaces
var _ comby.SnapshotStore = (*snapshotStorePostgres)(nil)

type snapshotStorePostgres struct {
	db       *sql.DB
	host     string
	port     int
	user     string
	password string
	dbName   string
}

func NewSnapshotStorePostgres(host string, port int, user, password, dbName string) comby.SnapshotStore {
	return &snapshotStorePostgres{
		host:     host,
		port:     port,
		user:     user,
		password: password,
		dbName:   dbName,
	}
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

	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(100)

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
