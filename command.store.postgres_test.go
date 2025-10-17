package store_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	store "github.com/gradientzero/comby-store-postgres"
	"github.com/gradientzero/comby/v2"
)

func TestCommandStore1(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init store
	commandStore := store.NewCommandStorePostgres("localhost", 5432, "postgres", "mysecretpassword", "postgres")
	if err = commandStore.Init(ctx,
		comby.CommandStoreOptionWithAttribute("key1", "value"),
	); err != nil {
		t.Fatal(err)
	}

	// check if the attribute is set
	if v := commandStore.Options().Attributes.Get("key1"); v != nil {
		if v != "value" {
			t.Fatalf("wrong value: %q", v)
		}
	} else {
		t.Fatalf("missing key")
	}

	// reset database
	if err := commandStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// check info with empty store
	if m, err := commandStore.Info(ctx); err != nil {
		t.Fatalf("failed to get info: %v", err)
	} else {
		if m.LastItemCreatedAt != 0 {
			t.Fatalf("wrong last item created at %d", m.LastItemCreatedAt)
		}
	}

	// check totals
	if commandStore.Total(ctx) != 0 {
		t.Fatalf("wrong total %d", commandStore.Total(ctx))
	}

	// Create test domain data
	type TestDomainCommand struct {
		Name  string
		Value int
	}

	// Create values
	cmd1 := &comby.BaseCommand{
		InstanceId:  1,
		CommandUuid: comby.NewUuid(),
		TenantUuid:  "TenantUuid_1",
		Domain:      "Domain_1",
		CreatedAt:   1000,
		DomainCmd: &TestDomainCommand{
			Name:  "TestCommand1",
			Value: 100,
		},
	}
	if err := commandStore.Create(ctx,
		comby.CommandStoreCreateOptionWithCommand(cmd1),
	); err != nil {
		t.Fatal(err)
	}
	cmd2 := &comby.BaseCommand{
		InstanceId:  1,
		CommandUuid: comby.NewUuid(),
		TenantUuid:  "TenantUuid_2",
		Domain:      "Domain_2",
		CreatedAt:   1000,
		DomainCmd: &TestDomainCommand{
			Name:  "TestCommand2",
			Value: 200,
		},
	}
	if err := commandStore.Create(ctx,
		comby.CommandStoreCreateOptionWithCommand(cmd2),
		comby.CommandStoreCreateOptionWithAttribute("anyKey1", "anyValue1"),
	); err != nil {
		t.Fatal(err)
	}

	// check totals
	if commandStore.Total(ctx) != 2 {
		t.Fatalf("wrong total %d", commandStore.Total(ctx))
	}

	// Get a value and verify all fields
	if _cmd1, err := commandStore.Get(ctx,
		comby.CommandStoreGetOptionWithCommandUuid(cmd1.CommandUuid),
	); err != nil {
		t.Fatal(err)
	} else {
		if _cmd1.GetTenantUuid() != "TenantUuid_1" {
			t.Fatalf("wrong tenant uuid: %q", _cmd1.GetTenantUuid())
		}
		if _cmd1.GetDomain() != "Domain_1" {
			t.Fatalf("wrong domain: %q", _cmd1.GetDomain())
		}
		if _cmd1.GetInstanceId() != 1 {
			t.Fatalf("wrong instance id: %d", _cmd1.GetInstanceId())
		}
		// Verify data_bytes is not empty
		if len(_cmd1.GetDomainCmdBytes()) == 0 {
			t.Fatalf("data_bytes is empty")
		}
		// Verify data_type is set
		if len(_cmd1.GetDomainCmdName()) == 0 {
			t.Fatalf("data_type is empty")
		}
	}

	// List all commands
	if evts, total, err := commandStore.List(ctx); err == nil {
		if len(evts) != 2 {
			t.Fatalf("wrong number of commands: %d", len(evts))
		}
		if int64(len(evts)) != total {
			t.Fatalf("wrong number of totals: %d", total)
		}
	}

	// Delete an event
	if err := commandStore.Delete(ctx,
		comby.CommandStoreDeleteOptionWithCommandUuid(cmd1.CommandUuid),
	); err != nil {
		t.Fatal(err)
	}

	// check totals
	if commandStore.Total(ctx) != 1 {
		t.Fatalf("wrong total %d", commandStore.Total(ctx))
	}

	// reset database
	if err := commandStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := commandStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

func TestCommandStoreEncrypted(t *testing.T) {
	var err error
	ctx := context.Background()

	// create crypto service
	key := []byte("12345678901234567890123456789012")
	cryptoService, _ := comby.NewCryptoService(key)

	// setup and init store
	commandStore := store.NewCommandStorePostgres("localhost", 5432, "postgres", "mysecretpassword", "postgres")
	if err = commandStore.Init(ctx,
		comby.CommandStoreOptionWithCryptoService(cryptoService),
	); err != nil {
		t.Fatal(err)
	}

	// create domain data to encrypt/decrypt
	type MyDomainCommand struct {
		String  string
		Int     int64
		Boolean bool
	}
	domainData := &MyDomainCommand{
		String:  "string",
		Int:     123,
		Boolean: true,
	}

	// Create values
	cmd1 := &comby.BaseCommand{
		CommandUuid: comby.NewUuid(),
		TenantUuid:  "TenantUuid_1",
		Domain:      "Domain_1",
		CreatedAt:   1000,
		DomainCmd:   domainData,
	}
	if err := commandStore.Create(ctx,
		comby.CommandStoreCreateOptionWithCommand(cmd1),
	); err != nil {
		t.Fatal(err)
	}

	// Get a value
	if _cmd1, err := commandStore.Get(ctx,
		comby.CommandStoreGetOptionWithCommandUuid(cmd1.CommandUuid),
	); err != nil {
		t.Fatal(err)
	} else {
		_domainData := &MyDomainCommand{}
		_domainData, _ = comby.Deserialize(_cmd1.GetDomainCmdBytes(), _domainData)
		if _domainData.String != "string" {
			t.Fatalf("wrong value: %q", _domainData.String)
		}
		if _domainData.Int != 123 {
			t.Fatalf("wrong value: %q", _domainData.Int)
		}
		if _domainData.Boolean != true {
			t.Fatalf("wrong value")
		}
	}

	// List all commands
	if cmds, _, err := commandStore.List(ctx); err == nil {
		_cmd1 := cmds[0]
		_domainData := &MyDomainCommand{}
		_domainData, _ = comby.Deserialize(_cmd1.GetDomainCmdBytes(), _domainData)
		if _domainData.String != "string" {
			t.Fatalf("wrong value: %q", _domainData.String)
		}
		if _domainData.Int != 123 {
			t.Fatalf("wrong value: %q", _domainData.Int)
		}
		if _domainData.Boolean != true {
			t.Fatalf("wrong value")
		}
	}

	// Update event
	domainData.String = "string2"
	domainData.Int = 456
	domainData.Boolean = false
	cmd1.DomainCmd = domainData

	// Delete an event
	if err := commandStore.Update(ctx,
		comby.CommandStoreUpdateOptionWithCommand(cmd1),
	); err != nil {
		t.Fatal(err)
	}

	// Get a value
	if _cmd1, err := commandStore.Get(ctx,
		comby.CommandStoreGetOptionWithCommandUuid(cmd1.CommandUuid),
	); err != nil {
		t.Fatal(err)
	} else {
		_domainData := &MyDomainCommand{}
		_domainData, _ = comby.Deserialize(_cmd1.GetDomainCmdBytes(), _domainData)
		if _domainData.String != "string2" {
			t.Fatalf("wrong value: %q", _domainData.String)
		}
		if _domainData.Int != 456 {
			t.Fatalf("wrong value: %q", _domainData.Int)
		}
		if _domainData.Boolean != false {
			t.Fatalf("wrong value")
		}
	}

	// reset database
	if err := commandStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := commandStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

func TestCommandStoreFieldLoading(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init store
	commandStore := store.NewCommandStorePostgres("localhost", 5432, "postgres", "mysecretpassword", "postgres")
	if err = commandStore.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Define a domain command type with various fields
	type ComplexDomainCommand struct {
		StringField  string
		IntField     int64
		BoolField    bool
		FloatField   float64
		ArrayField   []string
		NestedObject struct {
			Name  string
			Value int
		}
	}

	// Create a comprehensive test command with all fields populated
	testData := &ComplexDomainCommand{
		StringField: "test-string-value",
		IntField:    42,
		BoolField:   true,
		FloatField:  3.14159,
		ArrayField:  []string{"item1", "item2", "item3"},
		NestedObject: struct {
			Name  string
			Value int
		}{
			Name:  "nested",
			Value: 999,
		},
	}

	// Create request context
	reqCtx := &comby.RequestContext{
		SenderTenantUuid:    "sender-tenant-123",
		SenderIdentityUuid:  "sender-identity-456",
		TargetAggregateUuid: "target-aggregate-789",
	}

	cmd := &comby.BaseCommand{
		InstanceId:  123,
		CommandUuid: comby.NewUuid(),
		TenantUuid:  "tenant-uuid-123",
		Domain:      "test-domain",
		CreatedAt:   1234567890,
		DomainCmd:   testData,
		ReqCtx:      reqCtx,
	}

	// Create the command
	if err := commandStore.Create(ctx,
		comby.CommandStoreCreateOptionWithCommand(cmd),
	); err != nil {
		t.Fatal(err)
	}

	// Test 1: Get the command and verify ALL fields are loaded
	t.Run("Get - Verify all fields loaded", func(t *testing.T) {
		loadedCmd, err := commandStore.Get(ctx,
			comby.CommandStoreGetOptionWithCommandUuid(cmd.CommandUuid),
		)
		if err != nil {
			t.Fatalf("failed to get command: %v", err)
		}
		if loadedCmd == nil {
			t.Fatal("loaded command is nil")
		}

		// Verify all base fields
		if loadedCmd.GetInstanceId() != 123 {
			t.Errorf("InstanceId: expected 123, got %d", loadedCmd.GetInstanceId())
		}
		if loadedCmd.GetCommandUuid() != cmd.CommandUuid {
			t.Errorf("CommandUuid: expected %s, got %s", cmd.CommandUuid, loadedCmd.GetCommandUuid())
		}
		if loadedCmd.GetTenantUuid() != "tenant-uuid-123" {
			t.Errorf("TenantUuid: expected 'tenant-uuid-123', got %s", loadedCmd.GetTenantUuid())
		}
		if loadedCmd.GetDomain() != "test-domain" {
			t.Errorf("Domain: expected 'test-domain', got %s", loadedCmd.GetDomain())
		}
		if loadedCmd.GetCreatedAt() != 1234567890 {
			t.Errorf("CreatedAt: expected 1234567890, got %d", loadedCmd.GetCreatedAt())
		}

		// CRITICAL: Verify data_bytes is not empty
		dataBytes := loadedCmd.GetDomainCmdBytes()
		if len(dataBytes) == 0 {
			t.Fatal("CRITICAL: data_bytes is empty after loading")
		}
		t.Logf("data_bytes length: %d bytes", len(dataBytes))
		t.Logf("data_bytes content (first 100 chars): %s", string(dataBytes[:minCmd(100, len(dataBytes))]))

		// Verify data_type is set
		dataType := loadedCmd.GetDomainCmdName()
		if len(dataType) == 0 {
			t.Fatal("CRITICAL: data_type is empty after loading")
		}
		t.Logf("data_type: %s", dataType)

		// Deserialize and verify the domain data
		loadedData := &ComplexDomainCommand{}
		loadedData, err = comby.Deserialize(dataBytes, loadedData)
		if err != nil {
			t.Fatalf("failed to deserialize domain data: %v", err)
		}

		// Verify all domain data fields
		if loadedData.StringField != "test-string-value" {
			t.Errorf("StringField: expected 'test-string-value', got %s", loadedData.StringField)
		}
		if loadedData.IntField != 42 {
			t.Errorf("IntField: expected 42, got %d", loadedData.IntField)
		}
		if loadedData.BoolField != true {
			t.Errorf("BoolField: expected true, got %v", loadedData.BoolField)
		}
		if loadedData.FloatField != 3.14159 {
			t.Errorf("FloatField: expected 3.14159, got %f", loadedData.FloatField)
		}
		if len(loadedData.ArrayField) != 3 {
			t.Errorf("ArrayField length: expected 3, got %d", len(loadedData.ArrayField))
		}
		if loadedData.NestedObject.Name != "nested" {
			t.Errorf("NestedObject.Name: expected 'nested', got %s", loadedData.NestedObject.Name)
		}
		if loadedData.NestedObject.Value != 999 {
			t.Errorf("NestedObject.Value: expected 999, got %d", loadedData.NestedObject.Value)
		}

		// Verify request context is loaded
		if loadedCmd.GetReqCtx() == nil {
			t.Fatal("request context is nil")
		}
		if loadedCmd.GetReqCtx().SenderTenantUuid != "sender-tenant-123" {
			t.Errorf("ReqCtx.SenderTenantUuid: expected 'sender-tenant-123', got %s", loadedCmd.GetReqCtx().SenderTenantUuid)
		}
		if loadedCmd.GetReqCtx().SenderIdentityUuid != "sender-identity-456" {
			t.Errorf("ReqCtx.SenderIdentityUuid: expected 'sender-identity-456', got %s", loadedCmd.GetReqCtx().SenderIdentityUuid)
		}
		if loadedCmd.GetReqCtx().TargetAggregateUuid != "target-aggregate-789" {
			t.Errorf("ReqCtx.TargetAggregateUuid: expected 'target-aggregate-789', got %s", loadedCmd.GetReqCtx().TargetAggregateUuid)
		}
	})

	// Test 2: List commands and verify ALL fields are loaded
	t.Run("List - Verify all fields loaded", func(t *testing.T) {
		cmds, total, err := commandStore.List(ctx)
		if err != nil {
			t.Fatalf("failed to list commands: %v", err)
		}
		if total != 1 {
			t.Fatalf("expected 1 command, got %d", total)
		}
		if len(cmds) != 1 {
			t.Fatalf("expected 1 command in list, got %d", len(cmds))
		}

		loadedCmd := cmds[0]

		// Verify all base fields
		if loadedCmd.GetInstanceId() != 123 {
			t.Errorf("InstanceId: expected 123, got %d", loadedCmd.GetInstanceId())
		}
		if loadedCmd.GetTenantUuid() != "tenant-uuid-123" {
			t.Errorf("TenantUuid: expected 'tenant-uuid-123', got %s", loadedCmd.GetTenantUuid())
		}

		// CRITICAL: Verify data_bytes is not empty in List
		dataBytes := loadedCmd.GetDomainCmdBytes()
		if len(dataBytes) == 0 {
			t.Fatal("CRITICAL: data_bytes is empty after List()")
		}
		t.Logf("List - data_bytes length: %d bytes", len(dataBytes))

		// Verify data_type is set
		dataType := loadedCmd.GetDomainCmdName()
		if len(dataType) == 0 {
			t.Fatal("CRITICAL: data_type is empty after List()")
		}

		// Deserialize and verify the domain data
		loadedData := &ComplexDomainCommand{}
		loadedData, err = comby.Deserialize(dataBytes, loadedData)
		if err != nil {
			t.Fatalf("failed to deserialize domain data from List: %v", err)
		}

		// Verify critical fields
		if loadedData.StringField != "test-string-value" {
			t.Errorf("List - StringField: expected 'test-string-value', got %s", loadedData.StringField)
		}
		if loadedData.IntField != 42 {
			t.Errorf("List - IntField: expected 42, got %d", loadedData.IntField)
		}

		// Verify request context is loaded
		if loadedCmd.GetReqCtx() == nil {
			t.Fatal("request context is nil")
		}
	})

	// reset database
	if err := commandStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := commandStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

// Helper function to create test commands with all fields populated (except runtime fields)
func createTestCommand(tenantUuid, domain string, createdAt int64) comby.Command {
	cmd := comby.NewBaseCommand()
	cmd.SetInstanceId(1)
	cmd.SetTenantUuid(tenantUuid)
	cmd.SetDomain(domain)
	cmd.SetDomainCmdName(fmt.Sprintf("TestCommand_%d", createdAt))
	cmd.SetDomainCmdBytes([]byte(fmt.Sprintf("test-data-%d", createdAt)))
	cmd.SetCreatedAt(createdAt)
	return cmd
}

// Helper function for min (used in test assertions)
func minCmd(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Test: PostgreSQL to PostgreSQL - Validate complete command data is copied (including DomainCmdBytes)
// This test specifically addresses the reported issue where data_bytes are missing after copying
// between PostgreSQL command stores.
func TestSyncCommandStore_ValidateCompleteCommandData(t *testing.T) {
	ctx := context.Background()

	// Create PostgreSQL command stores
	source := store.NewCommandStorePostgres("localhost", 5432, "postgres", "mysecretpassword", "postgres")
	destination := store.NewCommandStorePostgres("localhost", 5432, "postgres", "mysecretpassword", "postgres")

	// Initialize stores
	if err := source.Init(ctx); err != nil {
		t.Fatalf("Failed to init source PostgreSQL store: %v", err)
	}
	defer source.Close(ctx)

	if err := destination.Init(ctx); err != nil {
		t.Fatalf("Failed to init destination PostgreSQL store: %v", err)
	}
	defer destination.Close(ctx)

	// Reset both stores to ensure clean state
	if err := source.Reset(ctx); err != nil {
		t.Fatalf("Failed to reset source store: %v", err)
	}
	if err := destination.Reset(ctx); err != nil {
		t.Fatalf("Failed to reset destination store: %v", err)
	}

	// Create test commands with all fields populated and various data sizes
	startTime := time.Now().Unix()
	testCommands := []comby.Command{
		createTestCommand("tenant-1", "user-domain", startTime),
		createTestCommand("tenant-1", "order-domain", startTime+1),
		createTestCommand("tenant-2", "product-domain", startTime+2),
		createTestCommand("tenant-2", "inventory-domain", startTime+3),
		createTestCommand("tenant-3", "payment-domain", startTime+4),
	}

	// Set DomainCmdBytes with different sizes to ensure all data is copied
	testCommands[0].SetDomainCmdBytes([]byte("small-payload"))
	testCommands[1].SetDomainCmdBytes([]byte("medium-payload-with-more-content-for-testing"))
	// Create large payload with actual text content (PostgreSQL doesn't accept null bytes in text fields)
	largePayload := "large-payload-"
	for i := 0; i < 500; i++ {
		largePayload += "x"
	}
	testCommands[2].SetDomainCmdBytes([]byte(largePayload))
	testCommands[3].SetDomainCmdBytes([]byte(`{"type":"json","data":{"key":"value","nested":{"field":"data"}}}`))
	// Create very large payload with actual text content
	veryLargePayload := "very-large-payload-"
	for i := 0; i < 2000; i++ {
		veryLargePayload += "y"
	}
	testCommands[4].SetDomainCmdBytes([]byte(veryLargePayload))

	// Additional field customization
	testCommands[0].SetDomainCmdName("CreateUserCommand")
	testCommands[1].SetDomainCmdName("PlaceOrderCommand")
	testCommands[2].SetDomainCmdName("AddProductCommand")
	testCommands[3].SetDomainCmdName("UpdateInventoryCommand")
	testCommands[4].SetDomainCmdName("ProcessPaymentCommand")

	// Store all commands in source
	for i, cmd := range testCommands {
		if err := source.Create(ctx, comby.CommandStoreCreateOptionWithCommand(cmd)); err != nil {
			t.Fatalf("Failed to create command %d in source PostgreSQL store: %v", i, err)
		}
	}

	// Verify source has all commands
	sourceTotal := source.Total(ctx)
	if sourceTotal != int64(len(testCommands)) {
		t.Fatalf("Source store should have %d commands, got %d", len(testCommands), sourceTotal)
	}

	// Sync from PostgreSQL source to PostgreSQL destination
	err := comby.SyncCommandStore(ctx, source, destination)
	if err != nil {
		t.Fatalf("Sync from PostgreSQL to PostgreSQL failed: %v", err)
	}

	// Verify count matches
	destTotal := destination.Total(ctx)
	if sourceTotal != destTotal {
		t.Fatalf("Command count mismatch after sync: source=%d, destination=%d", sourceTotal, destTotal)
	}

	// Retrieve all commands from both stores for field-by-field comparison
	sourceCommands, _, err := source.List(ctx, comby.CommandStoreListOptionOrderBy("created_at"), comby.CommandStoreListOptionAscending(true))
	if err != nil {
		t.Fatalf("Failed to list source commands: %v", err)
	}

	destCommands, _, err := destination.List(ctx, comby.CommandStoreListOptionOrderBy("created_at"), comby.CommandStoreListOptionAscending(true))
	if err != nil {
		t.Fatalf("Failed to list destination commands: %v", err)
	}

	if len(sourceCommands) != len(destCommands) {
		t.Fatalf("Command list length mismatch: source=%d, destination=%d", len(sourceCommands), len(destCommands))
	}

	// Validate EVERY field for EVERY command
	for i := 0; i < len(sourceCommands); i++ {
		srcCmd := sourceCommands[i]
		dstCmd := destCommands[i]

		// Instance ID
		if srcCmd.GetInstanceId() != dstCmd.GetInstanceId() {
			t.Errorf("Command %d: InstanceId mismatch: source=%d, dest=%d", i, srcCmd.GetInstanceId(), dstCmd.GetInstanceId())
		}

		// Command UUID
		if srcCmd.GetCommandUuid() != dstCmd.GetCommandUuid() {
			t.Errorf("Command %d: CommandUuid mismatch: source=%s, dest=%s", i, srcCmd.GetCommandUuid(), dstCmd.GetCommandUuid())
		}

		// Tenant UUID
		if srcCmd.GetTenantUuid() != dstCmd.GetTenantUuid() {
			t.Errorf("Command %d: TenantUuid mismatch: source=%s, dest=%s", i, srcCmd.GetTenantUuid(), dstCmd.GetTenantUuid())
		}

		// Domain
		if srcCmd.GetDomain() != dstCmd.GetDomain() {
			t.Errorf("Command %d: Domain mismatch: source=%s, dest=%s", i, srcCmd.GetDomain(), dstCmd.GetDomain())
		}

		// Domain Command Name
		if srcCmd.GetDomainCmdName() != dstCmd.GetDomainCmdName() {
			t.Errorf("Command %d: DomainCmdName mismatch: source=%s, dest=%s", i, srcCmd.GetDomainCmdName(), dstCmd.GetDomainCmdName())
		}

		// Created At
		if srcCmd.GetCreatedAt() != dstCmd.GetCreatedAt() {
			t.Errorf("Command %d: CreatedAt mismatch: source=%d, dest=%d", i, srcCmd.GetCreatedAt(), dstCmd.GetCreatedAt())
		}

		// CRITICAL TEST: DomainCmdBytes (data_bytes) - THIS IS THE REPORTED BUG!
		srcBytes := srcCmd.GetDomainCmdBytes()
		dstBytes := dstCmd.GetDomainCmdBytes()

		// Check if bytes exist
		if len(srcBytes) == 0 {
			t.Errorf("Command %d: Source DomainCmdBytes is empty! This should not happen.", i)
		}

		if len(dstBytes) == 0 {
			t.Errorf("Command %d: CRITICAL BUG - Destination DomainCmdBytes is empty! data_bytes were not copied from PostgreSQL source.", i)
		}

		// Check length matches
		if len(srcBytes) != len(dstBytes) {
			t.Errorf("Command %d: DomainCmdBytes length mismatch: source=%d bytes, dest=%d bytes",
				i, len(srcBytes), len(dstBytes))
			t.Errorf("  Source data (first 100 chars): %q", string(srcBytes[:minCmd(100, len(srcBytes))]))
			if len(dstBytes) > 0 {
				t.Errorf("  Dest data (first 100 chars): %q", string(dstBytes[:minCmd(100, len(dstBytes))]))
			} else {
				t.Errorf("  Dest data: EMPTY (BUG!)")
			}
		}

		// Check content matches byte-for-byte
		if len(srcBytes) > 0 && len(dstBytes) > 0 {
			if string(srcBytes) != string(dstBytes) {
				t.Errorf("Command %d: DomainCmdBytes content mismatch!", i)
				t.Errorf("  Source (%d bytes): %q", len(srcBytes), string(srcBytes[:minCmd(200, len(srcBytes))]))
				t.Errorf("  Dest (%d bytes): %q", len(dstBytes), string(dstBytes[:minCmd(200, len(dstBytes))]))
			}
		}
	}
}
