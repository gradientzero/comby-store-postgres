package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/gradientzero/comby-store-postgres/internal"
	"github.com/gradientzero/comby/v3"
	_ "github.com/lib/pq"
)

// Make sure it implements interfaces
var _ comby.EventStore = (*eventStorePostgres)(nil)

// pgQuerier abstracts *sql.DB and *sql.Tx so the same query code can run
// with or without an RLS-scoped transaction wrapper.
type pgQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type eventStorePostgres struct {
	options comby.EventStoreOptions
	db      *sql.DB // owner connection (BYPASSRLS)
	appDb   *sql.DB // optional non-BYPASSRLS connection used for tenant-scoped ops; nil if RLS is off

	// potgres specific options
	host     string
	port     int
	user     string
	password string
	dbName   string
}

func NewEventStorePostgres(host string, port int, user, password, dbName string, opts ...comby.EventStoreOption) comby.EventStore {
	es := &eventStorePostgres{
		host:     host,
		port:     port,
		user:     user,
		password: password,
		dbName:   dbName,
	}
	for _, opt := range opts {
		if _, err := opt(&es.options); err != nil {
			return nil
		}
	}
	return es
}

func (es *eventStorePostgres) connect(ctx context.Context) (*sql.DB, error) {
	return es.connectAs(ctx, es.user, es.password)
}

func (es *eventStorePostgres) connectAs(ctx context.Context, user, password string) (*sql.DB, error) {
	var dbNameStr string
	var passwordStr string
	dbName := es.dbName
	if len(dbName) != 0 {
		dbNameStr = fmt.Sprintf("dbname=%s", dbName)
	}
	if len(password) != 0 {
		passwordStr = fmt.Sprintf("password=%s", password)
	}
	dsn := fmt.Sprintf("host=%s port=%d user=%s %s %s sslmode=disable",
		es.host, es.port, user, passwordStr, dbNameStr)

	// create postgres connection
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	// open connection by ping
	if err := db.Ping(); err != nil {
		return nil, err
	}

	// PostgreSQL supports full MVCC — concurrent reads and writes are safe.
	// Apply pool settings from options, falling back to sensible defaults.
	maxOpenConns := 25
	if es.options.MaxOpenConns > 0 {
		maxOpenConns = es.options.MaxOpenConns
	}
	db.SetMaxOpenConns(maxOpenConns)

	maxIdleConns := 5
	if es.options.MaxIdleConns > 0 {
		maxIdleConns = es.options.MaxIdleConns
	}
	db.SetMaxIdleConns(maxIdleConns)

	if es.options.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(es.options.ConnMaxLifetime)
	} else {
		db.SetConnMaxLifetime(30 * time.Minute)
	}

	if es.options.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(es.options.ConnMaxIdleTime)
	} else {
		db.SetConnMaxIdleTime(5 * time.Minute)
	}

	return db, nil
}

func (es *eventStorePostgres) migrate(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS events (
		id SERIAL PRIMARY KEY,
		instance_id INTEGER,
		uuid TEXT,
		tenant_uuid TEXT,
		workspace_uuid TEXT,
		command_uuid TEXT,
		domain TEXT,
		aggregate_uuid TEXT,
		version INTEGER,
		created_at BIGINT,
		data_type TEXT,
		data_bytes TEXT,
		req_ctx TEXT
	);
	CREATE INDEX IF NOT EXISTS "tenant_index" ON "events" (
		"tenant_uuid" ASC
	);
	CREATE INDEX IF NOT EXISTS "workspace_index" ON "events" (
		"workspace_uuid" ASC
	);
	CREATE INDEX IF NOT EXISTS "aggregate_uuid_index" ON "events" (
		"aggregate_uuid" ASC
	);
	CREATE UNIQUE INDEX IF NOT EXISTS "uuid_index" ON "events" (
		"uuid" ASC
	);
	CREATE INDEX IF NOT EXISTS "created_at_index" ON "events" (
		"created_at" ASC
	);
	`
	if _, err := es.db.ExecContext(ctx, query); err != nil {
		return err
	}

	// migrate existing databases: add req_ctx column if it doesn't exist
	if _, err := es.db.ExecContext(ctx, `ALTER TABLE events ADD COLUMN IF NOT EXISTS req_ctx TEXT`); err != nil {
		return err
	}

	// migrate existing databases: add workspace_uuid column if it doesn't exist
	if _, err := es.db.ExecContext(ctx, `ALTER TABLE events ADD COLUMN IF NOT EXISTS workspace_uuid TEXT`); err != nil {
		return err
	}

	return nil
}

// fullfilling EventStore interface
func (es *eventStorePostgres) Init(ctx context.Context, opts ...comby.EventStoreOption) error {
	for _, opt := range opts {
		if _, err := opt(&es.options); err != nil {
			return err
		}
	}

	// connect to db (or create new one) as the owner role
	if db, err := es.connect(ctx); err != nil {
		return err
	} else {
		es.db = db
	}

	// auto-migrate table
	if !es.options.ReadOnly {
		if err := es.migrate(ctx); err != nil {
			return err
		}
	}

	// optional RLS app-role pool — only when EventStoreOptionWithRLSAppRole was set
	if es.options.RLSAppRoleUser != "" {
		// Ensure RLS policies are present on events/commands/snapshots.
		// Idempotent.
		if err := EnablePostgresRLS(ctx, es.db); err != nil {
			return fmt.Errorf("eventStorePostgres.Init: EnablePostgresRLS failed: %w", err)
		}
		// Ensure the app role exists with NOBYPASSRLS + table grants.
		if err := EnsureAppRole(ctx, es.db, es.options.RLSAppRoleUser, es.options.RLSAppRolePassword); err != nil {
			return fmt.Errorf("eventStorePostgres.Init: EnsureAppRole failed: %w", err)
		}
		// Open the app-role pool.
		if appDb, err := es.connectAs(ctx, es.options.RLSAppRoleUser, es.options.RLSAppRolePassword); err != nil {
			return fmt.Errorf("eventStorePostgres.Init: app-role connect failed: %w", err)
		} else {
			es.appDb = appDb
		}
	}
	return nil
}

// rlsEnabled reports whether the store has been configured with an RLS app
// role and an active app-role pool.
func (es *eventStorePostgres) rlsEnabled() bool {
	return es.appDb != nil
}

// withTenantConn picks the correct connection pool and runs fn against it.
//
//   - If RLS is off, ctx is bypass-tagged, or tenantUuid is empty (cross-tenant
//     query), fn runs on the owner pool (es.db) directly.
//   - Otherwise fn runs on es.appDb inside a transaction whose
//     app.tenant_uuid is set to tenantUuid via SET LOCAL. The RESTRICTIVE
//     RLS policies on events/commands/snapshots then constrain reads and
//     writes to that tenant.
//
// The fn callback receives a pgQuerier so it can use either *sql.DB or
// *sql.Tx polymorphically.
func (es *eventStorePostgres) withTenantConn(ctx context.Context, tenantUuid string, fn func(pgQuerier) error) error {
	if !es.rlsEnabled() || comby.IsRLSBypass(ctx) || tenantUuid == "" {
		return fn(es.db)
	}
	return WithRLSSession(ctx, es.appDb, tenantUuid, func(tx *sql.Tx) error {
		return fn(tx)
	})
}

func (es *eventStorePostgres) Create(ctx context.Context, opts ...comby.EventStoreCreateOption) error {
	createOpts := comby.EventStoreCreateOptions{
		Event: nil,
	}
	for _, opt := range opts {
		if _, err := opt(&createOpts); err != nil {
			return err
		}
	}

	if es.options.ReadOnly {
		return fmt.Errorf("'%s' failed to create event - instance is readonly", es.String())
	}

	var evt comby.Event = createOpts.Event
	if evt == nil {
		return fmt.Errorf("'%s' failed to create event - event is nil", es.String())
	}
	if len(evt.GetEventUuid()) < 1 {
		return fmt.Errorf("'%s' failed to create event - event uuid is invalid", es.String())
	}

	// sql statement
	dbRecord, err := internal.BaseEventToDbEvent(evt)
	if err != nil {
		return err
	}

	// encrypt domain data if crypto service is provided
	if es.options.CryptoService != nil {
		if err := es.encryptDomainData(dbRecord); err != nil {
			return err
		}
	}

	query := `INSERT INTO events (
	instance_id,
	uuid,
	tenant_uuid,
	workspace_uuid,
	command_uuid,
	domain,
	aggregate_uuid,
	version,
	created_at,
	data_type,
	data_bytes,
	req_ctx
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12);`

	return es.withTenantConn(ctx, dbRecord.TenantUuid, func(q pgQuerier) error {
		_, err := q.ExecContext(
			ctx,
			query,
			dbRecord.InstanceId,
			dbRecord.Uuid,
			dbRecord.TenantUuid,
			dbRecord.WorkspaceUuid,
			dbRecord.CommandUuid,
			dbRecord.Domain,
			dbRecord.AggregateUuid,
			dbRecord.Version,
			dbRecord.CreatedAt,
			dbRecord.DataType,
			dbRecord.DataBytes,
			dbRecord.ReqCtx,
		)
		return err
	})
}

func (es *eventStorePostgres) Get(ctx context.Context, opts ...comby.EventStoreGetOption) (comby.Event, error) {
	getOpts := comby.EventStoreGetOptions{}
	for _, opt := range opts {
		if _, err := opt(&getOpts); err != nil {
			return nil, err
		}
	}

	if len(getOpts.EventUuid) == 0 {
		return nil, fmt.Errorf("'%s' failed to get event - event uuid is required", es.String())
	}

	query := `SELECT id, instance_id, uuid, tenant_uuid, COALESCE(workspace_uuid, ''), command_uuid, domain,
		aggregate_uuid, version, created_at, data_type, data_bytes, COALESCE(req_ctx, '')
		FROM events WHERE uuid=$1 LIMIT 1;`

	// Get-by-uuid has no tenant filter — must run on owner pool. Callers that
	// need tenant scoping should use List(WithTenantUuid).
	row := es.db.QueryRowContext(ctx, query, getOpts.EventUuid)
	if row.Err() != nil {
		return nil, row.Err()
	}

	// extract record
	var dbRecord internal.Event
	if err := row.Scan(
		&dbRecord.ID,
		&dbRecord.InstanceId,
		&dbRecord.Uuid,
		&dbRecord.TenantUuid,
		&dbRecord.WorkspaceUuid,
		&dbRecord.CommandUuid,
		&dbRecord.Domain,
		&dbRecord.AggregateUuid,
		&dbRecord.Version,
		&dbRecord.CreatedAt,
		&dbRecord.DataType,
		&dbRecord.DataBytes,
		&dbRecord.ReqCtx,
	); err != nil {
		// Catch errors
		switch {
		case err == sql.ErrNoRows:
			return nil, nil
		case err != nil:
			return nil, err
		}
	}

	// decrypt domain data if crypto service is provided
	if es.options.CryptoService != nil {
		if err := es.decryptDomainData(&dbRecord); err != nil {
			return nil, err
		}
	}

	// db record to event
	evt, err := internal.DbEventToBaseEvent(&dbRecord)
	if err != nil {
		return nil, err
	}
	return evt, err
}

func (es *eventStorePostgres) List(ctx context.Context, opts ...comby.EventStoreListOption) ([]comby.Event, int64, error) {
	listOpts := comby.EventStoreListOptions{
		Before:    -1,
		After:     -1,
		Offset:    0,
		Limit:     100,
		OrderBy:   "created_at",
		Ascending: true,
	}
	for _, opt := range opts {
		if _, err := opt(&listOpts); err != nil {
			return nil, 0, err
		}
	}

	// prepare statement: (do NOT used them for Query/QueryContext)
	// 1. see different syntax for postgres:
	// http://go-database-sql.org/prepared.html#parameter-placeholder-syntax
	// 2. db.Query and db.QueryContext for some reason it does not work as expected
	// (seems to be something internally in database/sql because for SQLite and Postgres
	// simply does not return the expected result after sending new values to prepared statement)
	var whereSQL string = ""
	var whereList []string = []string{}
	var args []any
	paramIdx := 0
	if len(listOpts.TenantUuid) > 0 {
		paramIdx++
		whereList = append(whereList, fmt.Sprintf("tenant_uuid=$%d", paramIdx))
		args = append(args, listOpts.TenantUuid)
	}
	if len(listOpts.AggregateUuid) > 0 {
		paramIdx++
		whereList = append(whereList, fmt.Sprintf("aggregate_uuid=$%d", paramIdx))
		args = append(args, listOpts.AggregateUuid)
	}
	if len(listOpts.DataType) > 0 {
		paramIdx++
		whereList = append(whereList, fmt.Sprintf("data_type=$%d", paramIdx))
		args = append(args, listOpts.DataType)
	}
	if len(listOpts.Domains) > 0 {
		placeholders := make([]string, len(listOpts.Domains))
		for i, d := range listOpts.Domains {
			paramIdx++
			placeholders[i] = fmt.Sprintf("$%d", paramIdx)
			args = append(args, d)
		}
		whereList = append(whereList, fmt.Sprintf("domain IN (%s)", strings.Join(placeholders, ",")))
	}
	if listOpts.Before >= 0 {
		paramIdx++
		whereList = append(whereList, fmt.Sprintf("created_at<$%d", paramIdx))
		args = append(args, listOpts.Before)
	}
	if listOpts.After >= 0 {
		paramIdx++
		whereList = append(whereList, fmt.Sprintf("created_at>$%d", paramIdx))
		args = append(args, listOpts.After)
	}

	// note the first empty character(s) below
	for index, where := range whereList {
		if index == 0 {
			whereSQL = fmt.Sprintf(" WHERE %s", where)
		} else {
			whereSQL = fmt.Sprintf("%s AND %s", whereSQL, where)
		}
	}

	// prepare orderby statement
	var orderBySQL string = ""
	if len(listOpts.OrderBy) > 0 {
		if listOpts.Ascending {
			orderBySQL = fmt.Sprintf(" ORDER BY %s ASC", listOpts.OrderBy)
		} else {
			orderBySQL = fmt.Sprintf(" ORDER BY %s DESC", listOpts.OrderBy)
		}
	}

	// prepare limit/offset statements
	var limitSQL string = ""
	var offsetSQL string = ""
	if listOpts.Limit >= 0 {
		limitSQL = fmt.Sprintf(" LIMIT %d", listOpts.Limit)
	}
	if listOpts.Offset >= 0 {
		offsetSQL = fmt.Sprintf(" OFFSET %d", listOpts.Offset)
	}

	// build queries
	var queryTotalQuery string = fmt.Sprintf("SELECT COUNT(id) FROM events%s;", whereSQL)
	var query string = fmt.Sprintf("SELECT id, instance_id, uuid, tenant_uuid, COALESCE(workspace_uuid, ''), command_uuid, domain, aggregate_uuid, version, created_at, data_type, data_bytes, COALESCE(req_ctx, '') FROM events%s%s%s%s;", whereSQL, orderBySQL, limitSQL, offsetSQL)

	// route the count + list through the same connection. If TenantUuid is set,
	// we run on the app pool with SET LOCAL app.tenant_uuid; otherwise on owner.
	var queryTotal int64
	var dbRecords []*internal.Event
	if rlsErr := es.withTenantConn(ctx, listOpts.TenantUuid, func(q pgQuerier) error {
		// count
		var row *sql.Row
		if len(args) > 0 {
			row = q.QueryRowContext(ctx, queryTotalQuery, args...)
		} else {
			row = q.QueryRowContext(ctx, queryTotalQuery)
		}
		if err := row.Err(); err != nil {
			return err
		}
		if err := row.Scan(&queryTotal); err != nil {
			return err
		}

		// list
		var rows *sql.Rows
		var err error
		if len(args) > 0 {
			rows, err = q.QueryContext(ctx, query, args...)
		} else {
			rows, err = q.QueryContext(ctx, query)
		}
		switch {
		case err == sql.ErrNoRows:
			return nil
		case err != nil:
			return err
		}
		if rows != nil {
			defer rows.Close()
		}

		for rows.Next() {
			var dbRecord internal.Event
			if err := rows.Scan(
				&dbRecord.ID,
				&dbRecord.InstanceId,
				&dbRecord.Uuid,
				&dbRecord.TenantUuid,
				&dbRecord.WorkspaceUuid,
				&dbRecord.CommandUuid,
				&dbRecord.Domain,
				&dbRecord.AggregateUuid,
				&dbRecord.Version,
				&dbRecord.CreatedAt,
				&dbRecord.DataType,
				&dbRecord.DataBytes,
				&dbRecord.ReqCtx,
			); err != nil {
				return err
			}
			dbRecords = append(dbRecords, &dbRecord)
		}
		if err = rows.Err(); err != nil {
			return err
		}
		return rows.Close()
	}); rlsErr != nil {
		return nil, 0, rlsErr
	}

	// decrypt domain data if crypto service is provided
	if es.options.CryptoService != nil {
		for _, dbRecord := range dbRecords {
			if err := es.decryptDomainData(dbRecord); err != nil {
				return nil, 0, err
			}
		}
	}

	// convert
	evts, err := internal.DbEventsToBaseEvents(dbRecords)
	if err != nil {
		return nil, 0, err
	}
	return evts, queryTotal, nil
}

func (es *eventStorePostgres) Update(ctx context.Context, opts ...comby.EventStoreUpdateOption) error {
	updateOpts := comby.EventStoreUpdateOptions{
		Event: nil,
	}
	for _, opt := range opts {
		if _, err := opt(&updateOpts); err != nil {
			return err
		}
	}
	if es.options.ReadOnly {
		return fmt.Errorf("'%s' failed to update event - instance is readonly", es.String())
	}

	var evt comby.Event = updateOpts.Event
	if evt == nil {
		return fmt.Errorf("'%s' failed to update event - event is nil", es.String())
	}
	if len(evt.GetEventUuid()) < 1 {
		return fmt.Errorf("'%s' failed to update event - event uuid is invalid", es.String())
	}

	// convert to db format
	dbRecord, err := internal.BaseEventToDbEvent(evt)
	if err != nil {
		return err
	}

	// encrypt domain data if crypto service is provided
	if es.options.CryptoService != nil {
		if err := es.encryptDomainData(dbRecord); err != nil {
			return err
		}
	}

	query := `UPDATE events SET
		instance_id=$1,
		tenant_uuid=$2,
		workspace_uuid=$3,
		command_uuid=$4,
		domain=$5,
		aggregate_uuid=$6,
		version=$7,
		created_at=$8,
		data_type=$9,
		data_bytes=$10,
		req_ctx=$11
	 WHERE uuid=$12;`

	return es.withTenantConn(ctx, dbRecord.TenantUuid, func(q pgQuerier) error {
		_, err := q.ExecContext(ctx,
			query,
			dbRecord.InstanceId,
			dbRecord.TenantUuid,
			dbRecord.WorkspaceUuid,
			dbRecord.CommandUuid,
			dbRecord.Domain,
			dbRecord.AggregateUuid,
			dbRecord.Version,
			dbRecord.CreatedAt,
			dbRecord.DataType,
			dbRecord.DataBytes,
			dbRecord.ReqCtx,
			dbRecord.Uuid)
		return err
	})
}

func (es *eventStorePostgres) Delete(ctx context.Context, opts ...comby.EventStoreDeleteOption) error {
	deleteOpts := comby.EventStoreDeleteOptions{}
	for _, opt := range opts {
		if _, err := opt(&deleteOpts); err != nil {
			return err
		}
	}
	if es.options.ReadOnly {
		return fmt.Errorf("'%s' failed to delete event - instance is readonly", es.String())
	}

	var eventUuid string = deleteOpts.EventUuid
	if len(eventUuid) < 1 {
		return fmt.Errorf("'%s' failed to delete event - event uuid '%s' is invalid", es.String(), eventUuid)
	}

	// run query with parameterized values. Delete-by-uuid has no tenant
	// filter so it runs on the owner pool. Tenant-scoped admin tools should
	// use a tenant-aware reset/delete-by-aggregate flow instead.
	query := "DELETE FROM events WHERE uuid=$1;"
	_, err := es.db.ExecContext(ctx, query, eventUuid)
	return err
}

func (es *eventStorePostgres) Total(ctx context.Context) int64 {
	// run query (no args to not using prepared statement)
	row := es.db.QueryRowContext(ctx, `SELECT COUNT(id) FROM events;`)
	if err := row.Err(); err != nil {
		return 0
	}
	// extract record
	var dbTotal int64
	if err := row.Scan(&dbTotal); err != nil {
		return 0
	}
	return dbTotal
}

func (es *eventStorePostgres) UniqueList(ctx context.Context, opts ...comby.EventStoreUniqueListOption) ([]string, int64, error) {
	listOpts := comby.EventStoreUniqueListOptions{
		DbField:   "tenant_uuid",
		Offset:    0,
		Limit:     100,
		Ascending: true,
	}
	for _, opt := range opts {
		if _, err := opt(&listOpts); err != nil {
			return nil, 0, err
		}
	}

	// prepare where
	var whereSQL string = ""
	var whereList []string = []string{}
	var args []any
	paramIdx := 0
	if len(listOpts.TenantUuid) > 0 {
		paramIdx++
		whereList = append(whereList, fmt.Sprintf("tenant_uuid=$%d", paramIdx))
		args = append(args, listOpts.TenantUuid)
	}
	if len(listOpts.Domain) > 0 {
		paramIdx++
		whereList = append(whereList, fmt.Sprintf("domain=$%d", paramIdx))
		args = append(args, listOpts.Domain)
	}

	// note the first empty character(s) below
	for index, where := range whereList {
		if index == 0 {
			whereSQL = fmt.Sprintf(" WHERE %s", where)
		} else {
			whereSQL = fmt.Sprintf("%s AND %s", whereSQL, where)
		}
	}

	// prepare orderby
	var orderBySQL string = ""
	if len(listOpts.DbField) > 0 {
		if listOpts.Ascending {
			orderBySQL = fmt.Sprintf(" ORDER BY %s ASC", listOpts.DbField)
		} else {
			orderBySQL = fmt.Sprintf(" ORDER BY %s DESC", listOpts.DbField)
		}
	}

	// prepare limit/offset statements
	var limitSQL string = ""
	var offsetSQL string = ""
	if listOpts.Limit >= 0 {
		limitSQL = fmt.Sprintf(" LIMIT %d", listOpts.Limit)
	}
	if listOpts.Offset >= 0 {
		offsetSQL = fmt.Sprintf(" OFFSET %d", listOpts.Offset)
	}

	// run query with parameterized values
	var query string = fmt.Sprintf("SELECT DISTINCT %s FROM events%s%s%s%s;", listOpts.DbField, whereSQL, orderBySQL, limitSQL, offsetSQL)
	var totalQuery string = fmt.Sprintf("SELECT COUNT(DISTINCT %s) FROM events%s;", listOpts.DbField, whereSQL)

	var dbUniqueValues []string
	var dbTotal int64
	if rlsErr := es.withTenantConn(ctx, listOpts.TenantUuid, func(q pgQuerier) error {
		var rows *sql.Rows
		var err error
		if len(args) > 0 {
			rows, err = q.QueryContext(ctx, query, args...)
		} else {
			rows, err = q.QueryContext(ctx, query)
		}
		switch {
		case err == sql.ErrNoRows:
			return nil
		case err != nil:
			return err
		}
		if rows != nil {
			defer rows.Close()
		}
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				return err
			}
			dbUniqueValues = append(dbUniqueValues, v)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}

		var row *sql.Row
		if len(args) > 0 {
			row = q.QueryRowContext(ctx, totalQuery, args...)
		} else {
			row = q.QueryRowContext(ctx, totalQuery)
		}
		if err := row.Err(); err != nil {
			return err
		}
		return row.Scan(&dbTotal)
	}); rlsErr != nil {
		return nil, 0, rlsErr
	}

	return dbUniqueValues, dbTotal, nil
}

func (es *eventStorePostgres) Close(ctx context.Context) error {
	if es.appDb != nil {
		_ = es.appDb.Close()
	}
	return es.db.Close()
}

func (es *eventStorePostgres) Options() comby.EventStoreOptions {
	return es.options
}

func (es *eventStorePostgres) String() string {
	return fmt.Sprintf("postgres://%s:***@%s:%d/%s", es.user, es.host, es.port, es.dbName)
}

func (es *eventStorePostgres) Info(ctx context.Context) (*comby.EventStoreInfoModel, error) {

	// run extra total query (no args to not using prepared statement)
	row := es.db.QueryRowContext(ctx, "SELECT COUNT(uuid) FROM events;")
	if err := row.Err(); err != nil {
		return nil, err
	}
	// extract record
	var dbTotal int64
	if err := row.Scan(&dbTotal); err != nil {
		return nil, err
	}

	// run extra total query (no args to not using prepared statement)
	row = es.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(created_at), 0) FROM events;")
	if err := row.Err(); err != nil {
		return nil, err
	}
	// extract record
	var dbLastCreatedAt int64
	if err := row.Scan(&dbLastCreatedAt); err != nil {
		return nil, err
	}

	return &comby.EventStoreInfoModel{
		StoreType:         "postgres",
		LastItemCreatedAt: dbLastCreatedAt,
		NumItems:          dbTotal,
		ConnectionInfo:    fmt.Sprintf("postgres://%s:***@%s:%d/%s", es.user, es.host, es.port, es.dbName),
	}, nil
}

func (es *eventStorePostgres) Reset(ctx context.Context) error {
	if es.options.ReadOnly {
		return fmt.Errorf("'%s' failed to reset - instance is readonly", es.String())
	}
	query := "TRUNCATE TABLE events CASCADE;"
	if _, err := es.db.Exec(query); err != nil {
		return err
	}
	return nil
}

func (es *eventStorePostgres) encryptDomainData(dbRecord *internal.Event) error {
	if es.options.CryptoService == nil {
		return fmt.Errorf("'%s' failed - crypto service is nil", es.String())
	}
	domainData := []byte(dbRecord.DataBytes)
	if len(domainData) < 1 {
		return fmt.Errorf("'%s' failed - domain data is empty", es.String())
	}
	if encryptedData, err := es.options.CryptoService.Encrypt(domainData); err != nil {
		return fmt.Errorf("'%s' failed - failed to encrypt domain data: %w", es.String(), err)
	} else {
		dbRecord.DataBytes = hex.EncodeToString(encryptedData)
	}
	return nil
}

func (es *eventStorePostgres) decryptDomainData(dbRecord *internal.Event) error {
	if es.options.CryptoService == nil {
		return fmt.Errorf("'%s' failed - crypto service is nil", es.String())
	}
	encryptedData, err := hex.DecodeString(dbRecord.DataBytes)
	if err != nil {
		return fmt.Errorf("'%s' failed - failed to decode hex domain data: %w", es.String(), err)
	}
	if len(encryptedData) < 1 {
		return fmt.Errorf("'%s' failed - encrypted domain data is empty", es.String())
	}
	if decryptedData, err := es.options.CryptoService.Decrypt(encryptedData); err != nil {
		return fmt.Errorf("'%s' failed - failed to decrypt domain data: %w", es.String(), err)
	} else {
		dbRecord.DataBytes = string(decryptedData)
	}
	return nil
}
