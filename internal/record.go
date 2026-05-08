package internal

import (
	"database/sql"
)

type Command struct {
	// system fields
	ID sql.NullInt64

	// fields
	InstanceId    int64  `json:"_instance_id"`
	Uuid          string `json:"_uuid"`
	TenantUuid    string `json:"_tenant_uuid"`
	WorkspaceUuid string `json:"_workspace_uuid"`
	Domain        string `json:"_domain"`
	CreatedAt     int64  `json:"_created_at"`
	DataType      string `json:"_data_type"`
	DataBytes     string `json:"_data_bytes"`
	ReqCtx        string `json:"_req_ctx"`
}

type Event struct {
	// system fields
	ID sql.NullInt64

	// fields
	InstanceId    int64  `json:"_instance_id"`
	Uuid          string `json:"_uuid"`
	TenantUuid    string `json:"_tenant_uuid"`
	WorkspaceUuid string `json:"_workspace_uuid"`
	CommandUuid   string `json:"_command_uuid"`
	Domain        string `json:"_domain"`
	AggregateUuid string `json:"_aggregate_uuid"`
	Version       int64  `json:"_version"`
	CreatedAt     int64  `json:"_created_at"`
	DataType      string `json:"_data_type"`
	DataBytes     string `json:"_data_bytes"`
	ReqCtx        string `json:"_req_ctx"`
}
