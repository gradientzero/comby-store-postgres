package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"

	"github.com/gradientzero/comby-store-postgres/internal"
	"github.com/gradientzero/comby/v2"
	_ "github.com/lib/pq"
)

// Make sure it implements interfaces
var _ comby.CommandStore = (*commandStorePostgres)(nil)

type commandStorePostgres struct {
	options comby.CommandStoreOptions
	db      *sql.DB

	// potgres specific options
	host     string
	port     int
	user     string
	password string
	dbName   string
}

func NewCommandStorePostgres(host string, port int, user, password, dbName string, opts ...comby.CommandStoreOption) comby.CommandStore {
	cs := &commandStorePostgres{
		host:     host,
		port:     port,
		user:     user,
		password: password,
		dbName:   dbName,
	}
	for _, opt := range opts {
		if _, err := opt(&cs.options); err != nil {
			return nil
		}
	}
	return cs
}

func (cs *commandStorePostgres) connect(ctx context.Context) (*sql.DB, error) {

	var dbNameStr string
	var passwordStr string
	dbName := cs.dbName
	if len(dbName) != 0 {
		dbNameStr = fmt.Sprintf("dbname=%s", dbName)
	}
	pw := cs.password
	if len(pw) != 0 {
		passwordStr = fmt.Sprintf("password=%s", pw)
	}
	dsn := fmt.Sprintf("host=%s port=%d user=%s %s %s sslmode=disable",
		cs.host, cs.port, cs.user, passwordStr, dbNameStr)

	// create postgres connection
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	// open connection by ping
	if err := db.Ping(); err != nil {
		return nil, err
	}

	// otherwise, return the connection
	return db, nil
}

func (cs *commandStorePostgres) migrate(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS commands (
		id SERIAL PRIMARY KEY,
		instance_id INTEGER,
		uuid TEXT,
		tenant_uuid TEXT,
		domain TEXT,
		created_at BIGINT,
		data_type TEXT,
		data_bytes TEXT,
		req_ctx TEXT
	);
	CREATE INDEX IF NOT EXISTS "tenant_index" ON "commands" (
		"tenant_uuid" ASC
	);
	`
	_, err := cs.db.ExecContext(ctx, query)
	return err
}

// fullfilling CommandStore interface
func (cs *commandStorePostgres) Init(ctx context.Context, opts ...comby.CommandStoreOption) error {
	for _, opt := range opts {
		if _, err := opt(&cs.options); err != nil {
			return err
		}
	}

	// connect to db (or create new one)
	if db, err := cs.connect(ctx); err != nil {
		return err
	} else {
		cs.db = db
	}

	// auto-migrate table
	if !cs.options.ReadOnly {
		if err := cs.migrate(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (cs *commandStorePostgres) Create(ctx context.Context, opts ...comby.CommandStoreCreateOption) error {
	createOpts := comby.CommandStoreCreateOptions{
		Command: nil,
	}
	for _, opt := range opts {
		if _, err := opt(&createOpts); err != nil {
			return err
		}
	}
	if cs.options.ReadOnly {
		return fmt.Errorf("'%s' failed to create command - instance is readonly", cs.String())
	}
	var cmd comby.Command = createOpts.Command
	if cmd == nil {
		return fmt.Errorf("'%s' failed to create command - command is nil", cs.String())
	}
	if len(cmd.GetCommandUuid()) < 1 {
		return fmt.Errorf("'%s' failed to create command - command uuid is invalid", cs.String())
	}

	// sql statement
	dbRecord, err := internal.BaseCommandToDbCommand(cmd)
	if err != nil {
		return err
	}

	// encrypt domain data if crypto service is provided
	if cs.options.CryptoService != nil {
		if err := cs.encryptDomainData(dbRecord); err != nil {
			return err
		}
	}

	// sql begin transaction
	tx, err := cs.db.Begin()
	if err != nil {
		return err
	}

	// prepare statement
	query := `INSERT INTO commands (
		instance_id, 
		uuid, 
		tenant_uuid,
		domain,
		created_at,
		data_type,
		data_bytes,
		req_ctx
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8);`
	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}

	// execute statement
	_, err = stmt.ExecContext(
		ctx,
		dbRecord.InstanceId,
		dbRecord.Uuid,
		dbRecord.TenantUuid,
		dbRecord.Domain,
		dbRecord.CreatedAt,
		dbRecord.DataType,
		dbRecord.DataBytes,
		dbRecord.ReqCtx,
	)
	if err != nil {
		return err
	}

	// close statement
	err = stmt.Close()
	if err != nil {
		return err
	}

	// commit statement
	return tx.Commit()
}

func (cs *commandStorePostgres) Get(ctx context.Context, opts ...comby.CommandStoreGetOption) (comby.Command, error) {
	getOpts := comby.CommandStoreGetOptions{}
	for _, opt := range opts {
		if _, err := opt(&getOpts); err != nil {
			return nil, err
		}
	}

	// prepare query
	var query string = "SELECT * FROM commands LIMIT 1;"
	if len(getOpts.CommandUuid) > 0 {
		query = fmt.Sprintf("SELECT * FROM commands WHERE uuid='%s' LIMIT 1;", getOpts.CommandUuid)
	}

	// run query (no args to not using prepared statement)
	row := cs.db.QueryRowContext(ctx, query)
	if row.Err() != nil {
		return nil, row.Err()
	}

	// extract record
	var dbRecord internal.Command
	if err := row.Scan(
		&dbRecord.ID,
		&dbRecord.InstanceId,
		&dbRecord.Uuid,
		&dbRecord.TenantUuid,
		&dbRecord.Domain,
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
	if cs.options.CryptoService != nil {
		if err := cs.decryptDomainData(&dbRecord); err != nil {
			return nil, err
		}
	}

	// db record to command
	cmd, err := internal.DbCommandToBaseCommand(&dbRecord)
	if err != nil {
		return nil, err
	}
	return cmd, err
}

func (cs *commandStorePostgres) List(ctx context.Context, opts ...comby.CommandStoreListOption) ([]comby.Command, int64, error) {
	listOpts := comby.CommandStoreListOptions{
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
	if len(listOpts.TenantUuid) > 0 {
		whereList = append(whereList, fmt.Sprintf("tenant_uuid='%s'", listOpts.TenantUuid))
	}
	if len(listOpts.Domain) > 0 {
		whereList = append(whereList, fmt.Sprintf("domain='%s'", listOpts.Domain))
	}
	if len(listOpts.DataType) > 0 {
		whereList = append(whereList, fmt.Sprintf("data_type='%s'", listOpts.DataType))
	}
	if listOpts.Before >= 0 {
		whereList = append(whereList, fmt.Sprintf("created_at<%d", listOpts.Before))
	}
	if listOpts.After >= 0 {
		whereList = append(whereList, fmt.Sprintf("created_at>%d", listOpts.After))
	}

	// note the first empty character(s) below
	for index, where := range whereList {
		if index == 0 {
			whereSQL = fmt.Sprintf(" WHERE %s", where)
		} else {
			whereSQL = fmt.Sprintf("%s AND %s", whereSQL, where)
		}
	}

	// count the total number of records for this query
	var queryTotal int64
	var queryTotalQuery string = fmt.Sprintf("SELECT COUNT(id) FROM commands%s;", whereSQL)
	row := cs.db.QueryRowContext(ctx, queryTotalQuery)
	if err := row.Err(); err != nil {
		return nil, 0, err
	}
	// extract record
	if err := row.Scan(&queryTotal); err != nil {
		return nil, 0, err
	}

	// prepare orderby
	var orderBySQL string = ""
	if len(listOpts.OrderBy) > 0 {
		if listOpts.Ascending {
			orderBySQL = fmt.Sprintf(" ORDER BY %s ASC", listOpts.OrderBy)
		} else {
			orderBySQL = fmt.Sprintf(" ORDER BY %s DESC", listOpts.OrderBy)
		}
	}

	// prepare limit/offset
	var limitSQL string = ""
	var offsetSQL string = ""
	if listOpts.Limit >= 0 {
		limitSQL = fmt.Sprintf(" LIMIT %d", listOpts.Limit)
	}
	if listOpts.Offset >= 0 {
		offsetSQL = fmt.Sprintf(" OFFSET %d", listOpts.Offset)
	}

	// run query (no args to not using prepared statement - see above for more info)
	var query string = fmt.Sprintf("SELECT * FROM commands%s%s%s%s;", whereSQL, orderBySQL, limitSQL, offsetSQL)
	rows, err := cs.db.QueryContext(ctx, query)
	switch {
	case err == sql.ErrNoRows:
		return nil, queryTotal, nil
	case err != nil:
		return nil, 0, err
	}
	if rows != nil {
		defer rows.Close()
	}

	// extract results
	var dbRecords []*internal.Command
	for rows.Next() {
		var dbRecord internal.Command
		if err := rows.Scan(
			&dbRecord.ID,
			&dbRecord.InstanceId,
			&dbRecord.Uuid,
			&dbRecord.TenantUuid,
			&dbRecord.Domain,
			&dbRecord.CreatedAt,
			&dbRecord.DataType,
			&dbRecord.DataBytes,
			&dbRecord.ReqCtx,
		); err != nil {
			return nil, 0, err
		}
		dbRecords = append(dbRecords, &dbRecord)
	}
	if err := rows.Close(); err != nil {
		return nil, 0, err
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	// decrypt domain data if crypto service is provided
	if cs.options.CryptoService != nil {
		for _, dbRecord := range dbRecords {
			if err := cs.decryptDomainData(dbRecord); err != nil {
				return nil, 0, err
			}
		}
	}

	// convert
	cmds, err := internal.DbCommandsToBaseCommands(dbRecords)
	if err != nil {
		return nil, 0, err
	}
	return cmds, queryTotal, err
}

func (cs *commandStorePostgres) Update(ctx context.Context, opts ...comby.CommandStoreUpdateOption) error {
	updateOpts := comby.CommandStoreUpdateOptions{
		Command: nil,
	}
	for _, opt := range opts {
		if _, err := opt(&updateOpts); err != nil {
			return err
		}
	}
	if cs.options.ReadOnly {
		return fmt.Errorf("'%s' failed to update command - instance is readonly", cs.String())
	}
	var cmd comby.Command = updateOpts.Command
	if cmd == nil {
		return fmt.Errorf("'%s' failed to update command - command is nil", cs.String())
	}
	if len(cmd.GetCommandUuid()) < 1 {
		return fmt.Errorf("'%s' failed to update command - command uuid is invalid", cs.String())
	}

	// convert to db format
	dbRecord, err := internal.BaseCommandToDbCommand(cmd)
	if err != nil {
		return err
	}

	// encrypt domain data if crypto service is provided
	if cs.options.CryptoService != nil {
		if err := cs.encryptDomainData(dbRecord); err != nil {
			return err
		}
	}

	// sql begin transaction
	tx, err := cs.db.Begin()
	if err != nil {
		return err
	}

	// prepare statement
	query := `UPDATE commands SET
		instance_id=$1, 
		tenant_uuid=$2,
		domain=$3,
		created_at=$4,
		data_type=$5,
		data_bytes=$6,
		req_ctx=$7
	 WHERE uuid=$8;`
	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}

	// execute statement
	_, err = stmt.ExecContext(ctx,
		dbRecord.InstanceId,
		dbRecord.TenantUuid,
		dbRecord.Domain,
		dbRecord.CreatedAt,
		dbRecord.DataType,
		dbRecord.DataBytes,
		dbRecord.ReqCtx,
		dbRecord.Uuid)
	if err != nil {
		return err
	}

	// close statement
	err = stmt.Close()
	if err != nil {
		return err
	}

	// commit statement
	return tx.Commit()
}

func (cs *commandStorePostgres) Delete(ctx context.Context, opts ...comby.CommandStoreDeleteOption) error {
	deleteOpts := comby.CommandStoreDeleteOptions{}
	for _, opt := range opts {
		if _, err := opt(&deleteOpts); err != nil {
			return err
		}
	}
	if cs.options.ReadOnly {
		return fmt.Errorf("'%s' failed to delete command - instance is readonly", cs.String())
	}
	var commandUuid string = deleteOpts.CommandUuid
	if len(commandUuid) < 1 {
		return fmt.Errorf("'%s' failed to delete command - command uuid '%s' is invalid", cs.String(), commandUuid)
	}

	// run query (no args to not using prepared statement)
	query := fmt.Sprintf("DELETE FROM commands WHERE uuid='%s';", commandUuid)
	_, err := cs.db.ExecContext(ctx, query)
	return err
}

func (cs *commandStorePostgres) Total(ctx context.Context) int64 {
	// run query (no args to not using prepared statement)
	row := cs.db.QueryRowContext(ctx, `SELECT COUNT(id) FROM commands;`)
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

func (cs *commandStorePostgres) Close(ctx context.Context) error {
	return cs.db.Close()
}
func (cs *commandStorePostgres) Options() comby.CommandStoreOptions {
	return cs.options
}

func (cs *commandStorePostgres) String() string {
	return fmt.Sprintf("postgres://%s:***@%s:%d/%s", cs.user, cs.host, cs.port, cs.dbName)
}

func (cs *commandStorePostgres) Info(ctx context.Context) (*comby.CommandStoreInfoModel, error) {

	// run extra total query (no args to not using prepared statement)
	var totalQuery string = fmt.Sprintf("SELECT COUNT(uuid) FROM commands;")
	row := cs.db.QueryRowContext(ctx, totalQuery)
	if err := row.Err(); err != nil {
		return nil, err
	}
	// extract record
	var dbTotal int64
	if err := row.Scan(&dbTotal); err != nil {
		return nil, err
	}

	// run extra total query (no args to not using prepared statement)
	var lastEventQuery string = fmt.Sprintf("SELECT MAX(created_at) FROM commands;")
	row = cs.db.QueryRowContext(ctx, lastEventQuery)
	if err := row.Err(); err != nil {
		return nil, err
	}
	// extract record
	var dbLastCreatedAt int64
	if err := row.Scan(&dbLastCreatedAt); err != nil {
		return nil, err
	}

	return &comby.CommandStoreInfoModel{
		StoreType:         "postgres",
		LastItemCreatedAt: dbLastCreatedAt,
		NumItems:          dbTotal,
		ConnectionInfo:    fmt.Sprintf("postgres://%s:***@%s:%d/%s", cs.user, cs.host, cs.port, cs.dbName),
	}, nil
}

func (cs *commandStorePostgres) Reset(ctx context.Context) error {
	if cs.options.ReadOnly {
		return fmt.Errorf("'%s' failed to reset - instance is readonly", cs.String())
	}
	query := "TRUNCATE TABLE commands CASCADE;"
	if _, err := cs.db.Exec(query); err != nil {
		return err
	}
	return nil
}

func (cs *commandStorePostgres) encryptDomainData(dbRecord *internal.Command) error {
	if cs.options.CryptoService == nil {
		return fmt.Errorf("'%s' failed - crypto service is nil", cs.String())
	}
	domainData := []byte(dbRecord.DataBytes)
	if len(domainData) < 1 {
		return fmt.Errorf("'%s' failed - domain data is empty", cs.String())
	}
	if encryptedData, err := cs.options.CryptoService.Encrypt(domainData); err != nil {
		return fmt.Errorf("'%s' failed - failed to encrypt domain data: %w", cs.String(), err)
	} else {
		dbRecord.DataBytes = hex.EncodeToString(encryptedData)
	}
	return nil
}

func (cs *commandStorePostgres) decryptDomainData(dbRecord *internal.Command) error {
	if cs.options.CryptoService == nil {
		return fmt.Errorf("'%s' failed - crypto service is nil", cs.String())
	}
	encryptedData, err := hex.DecodeString(dbRecord.DataBytes)
	if err != nil {
		return fmt.Errorf("'%s' failed - failed to decode hex domain data: %w", cs.String(), err)
	}
	if len(encryptedData) < 1 {
		return fmt.Errorf("'%s' failed - encrypted domain data is empty", cs.String())
	}
	if decryptedData, err := cs.options.CryptoService.Decrypt(encryptedData); err != nil {
		return fmt.Errorf("'%s' failed - failed to decrypt domain data: %w", cs.String(), err)
	} else {
		dbRecord.DataBytes = string(decryptedData)
	}
	return nil
}
