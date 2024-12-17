package internal

import (
	"database/sql"
)

type Command struct {
	// system fields
	ID sql.NullInt64

	// fields
	InstanceId int64  `json:"instance_id"`
	Uuid       string `json:"uuid"`
	TenantUuid string `json:"tenant_uuid"`
	Domain     string `json:"domain"`
	CreatedAt  int64  `json:"created_at"`
	DataType   string `json:"data_type"`
	DataBytes  string `json:"data_bytes"`
	ReqCtx     string `json:"req_ctx"`
}

type Event struct {
	// system fields
	ID sql.NullInt64

	// fields
	InstanceId    int64  `json:"instance_id"`
	Uuid          string `json:"uuid"`
	TenantUuid    string `json:"tenant_uuid"`
	CommandUuid   string `json:"command_uuid"`
	Domain        string `json:"domain"`
	AggregateUuid string `json:"aggregate_uuid"`
	Version       int64  `json:"version"`
	CreatedAt     int64  `json:"created_at"`
	DataType      string `json:"data_type"`
	DataBytes     string `json:"data_bytes"`
}
