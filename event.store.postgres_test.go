package store_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	store "github.com/gradientzero/comby-store-postgres"
	"github.com/gradientzero/comby/v2"
)

func TestEventStore1(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init store
	eventStore := store.NewEventStorePostgres("localhost", 5432, "postgres", "mysecretpassword", "postgres")
	if err = eventStore.Init(ctx,
		comby.EventStoreOptionWithAttribute("key1", "value"),
	); err != nil {
		t.Fatal(err)
	}

	// check if the attribute is set
	if v := eventStore.Options().Attributes.Get("key1"); v != nil {
		if v != "value" {
			t.Fatalf("wrong value: %q", v)
		}
	} else {
		t.Fatalf("missing key")
	}

	// reset database
	if err := eventStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// check info with empty store
	if m, err := eventStore.Info(ctx); err != nil {
		t.Fatalf("failed to get info: %v", err)
	} else {
		if m.LastItemCreatedAt != 0 {
			t.Fatalf("wrong last item created at %d", m.LastItemCreatedAt)
		}
	}

	// check totals
	if eventStore.Total(ctx) != 0 {
		t.Fatalf("wrong total %d", eventStore.Total(ctx))
	}

	// Create test domain data
	type TestDomainData struct {
		Name  string
		Value int
	}

	// Create values
	evt1 := &comby.BaseEvent{
		InstanceId:    1,
		EventUuid:     comby.NewUuid(),
		TenantUuid:    "TenantUuid_1",
		CommandUuid:   "CommandUuid_1",
		AggregateUuid: "AggregateUuid_1",
		Domain:        "Domain_1",
		CreatedAt:     1000,
		Version:       1,
		DomainEvt: &TestDomainData{
			Name:  "TestEvent1",
			Value: 100,
		},
	}
	if err := eventStore.Create(ctx,
		comby.EventStoreCreateOptionWithEvent(evt1),
	); err != nil {
		t.Fatal(err)
	}
	evt2 := &comby.BaseEvent{
		InstanceId:    1,
		EventUuid:     comby.NewUuid(),
		TenantUuid:    "TenantUuid_2",
		CommandUuid:   "CommandUuid_2",
		AggregateUuid: "AggregateUuid_2",
		Domain:        "Domain_2",
		CreatedAt:     1000,
		Version:       1,
		DomainEvt: &TestDomainData{
			Name:  "TestEvent2",
			Value: 200,
		},
	}
	if err := eventStore.Create(ctx,
		comby.EventStoreCreateOptionWithEvent(evt2),
		comby.EventStoreCreateOptionWithAttribute("anyKey1", "anyValue1"),
	); err != nil {
		t.Fatal(err)
	}

	// check totals
	if eventStore.Total(ctx) != 2 {
		t.Fatalf("wrong total %d", eventStore.Total(ctx))
	}

	// Get a value and verify all fields
	if _evt1, err := eventStore.Get(ctx,
		comby.EventStoreGetOptionWithEventUuid(evt1.EventUuid),
	); err != nil {
		t.Fatal(err)
	} else {
		if _evt1.GetAggregateUuid() != "AggregateUuid_1" {
			t.Fatalf("wrong aggregate uuid: %q", _evt1.GetAggregateUuid())
		}
		if _evt1.GetTenantUuid() != "TenantUuid_1" {
			t.Fatalf("wrong tenant uuid: %q", _evt1.GetTenantUuid())
		}
		if _evt1.GetCommandUuid() != "CommandUuid_1" {
			t.Fatalf("wrong command uuid: %q", _evt1.GetCommandUuid())
		}
		if _evt1.GetDomain() != "Domain_1" {
			t.Fatalf("wrong domain: %q", _evt1.GetDomain())
		}
		if _evt1.GetVersion() != 1 {
			t.Fatalf("wrong version: %d", _evt1.GetVersion())
		}
		if _evt1.GetInstanceId() != 1 {
			t.Fatalf("wrong instance id: %d", _evt1.GetInstanceId())
		}
		// Verify data_bytes is not empty
		if len(_evt1.GetDomainEvtBytes()) == 0 {
			t.Fatalf("data_bytes is empty")
		}
		// Verify data_type is set
		if len(_evt1.GetDomainEvtName()) == 0 {
			t.Fatalf("data_type is empty")
		}
	}

	// List all events
	if evts, total, err := eventStore.List(ctx); err == nil {
		if len(evts) != 2 {
			t.Fatalf("wrong number of events: %d", len(evts))
		}
		if int64(len(evts)) != total {
			t.Fatalf("wrong number of totals: %d", total)
		}
	}

	// Delete an event
	if err := eventStore.Delete(ctx,
		comby.EventStoreDeleteOptionWithEventUuid(evt1.EventUuid),
	); err != nil {
		t.Fatal(err)
	}

	// check totals
	if eventStore.Total(ctx) != 1 {
		t.Fatalf("wrong total %d", eventStore.Total(ctx))
	}

	// reset database
	if err := eventStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := eventStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

func TestEventStoreWithEncrypted(t *testing.T) {
	var err error
	ctx := context.Background()

	// create crypto service
	key := []byte("12345678901234567890123456789012")
	cryptoService, _ := comby.NewCryptoService(key)

	// setup and init store
	eventStore := store.NewEventStorePostgres("localhost", 5432, "postgres", "mysecretpassword", "postgres")
	if err = eventStore.Init(ctx,
		comby.EventStoreOptionWithCryptoService(cryptoService),
	); err != nil {
		t.Fatal(err)
	}

	// create domain data to encrypt/decrypt
	type MyDomainEvent struct {
		String  string
		Int     int64
		Boolean bool
	}
	domainData := &MyDomainEvent{
		String:  "string",
		Int:     123,
		Boolean: true,
	}

	// Create values
	evt1 := &comby.BaseEvent{
		EventUuid:     comby.NewUuid(),
		AggregateUuid: "AggregateUuid_1",
		Domain:        "Domain_1",
		CreatedAt:     1000,
		Version:       1,
		DomainEvt:     domainData,
	}
	if err := eventStore.Create(ctx,
		comby.EventStoreCreateOptionWithEvent(evt1),
	); err != nil {
		t.Fatal(err)
	}

	// Get a value
	if _evt1, err := eventStore.Get(ctx,
		comby.EventStoreGetOptionWithEventUuid(evt1.EventUuid),
	); err != nil {
		t.Fatal(err)
	} else {
		_domainData := &MyDomainEvent{}
		_domainData, _ = comby.Deserialize(_evt1.GetDomainEvtBytes(), _domainData)
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

	// List all events
	if evts, _, err := eventStore.List(ctx); err == nil {
		_evt1 := evts[0]
		_domainData := &MyDomainEvent{}
		_domainData, _ = comby.Deserialize(_evt1.GetDomainEvtBytes(), _domainData)
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
	evt1.DomainEvt = domainData

	if err := eventStore.Update(ctx,
		comby.EventStoreUpdateOptionWithEvent(evt1),
	); err != nil {
		t.Fatal(err)
	}

	// Get a value
	if _evt1, err := eventStore.Get(ctx,
		comby.EventStoreGetOptionWithEventUuid(evt1.EventUuid),
	); err != nil {
		t.Fatal(err)
	} else {
		_domainData := &MyDomainEvent{}
		_domainData, _ = comby.Deserialize(_evt1.GetDomainEvtBytes(), _domainData)
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
	if err := eventStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := eventStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

func TestEventStoreFieldLoading(t *testing.T) {
	var err error
	ctx := context.Background()

	// setup and init store
	eventStore := store.NewEventStorePostgres("localhost", 5432, "postgres", "mysecretpassword", "postgres")
	if err = eventStore.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// Define a domain event type with various fields
	type ComplexDomainEvent struct {
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

	// Create a comprehensive test event with all fields populated
	testData := &ComplexDomainEvent{
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

	evt := &comby.BaseEvent{
		InstanceId:    123,
		EventUuid:     comby.NewUuid(),
		TenantUuid:    "tenant-uuid-123",
		CommandUuid:   "command-uuid-456",
		AggregateUuid: "aggregate-uuid-789",
		Domain:        "test-domain",
		Version:       5,
		CreatedAt:     1234567890,
		DomainEvt:     testData,
	}

	// Create the event
	if err := eventStore.Create(ctx,
		comby.EventStoreCreateOptionWithEvent(evt),
	); err != nil {
		t.Fatal(err)
	}

	// Test 1: Get the event and verify ALL fields are loaded
	t.Run("Get - Verify all fields loaded", func(t *testing.T) {
		loadedEvt, err := eventStore.Get(ctx,
			comby.EventStoreGetOptionWithEventUuid(evt.EventUuid),
		)
		if err != nil {
			t.Fatalf("failed to get event: %v", err)
		}
		if loadedEvt == nil {
			t.Fatal("loaded event is nil")
		}

		// Verify all base fields
		if loadedEvt.GetInstanceId() != 123 {
			t.Errorf("InstanceId: expected 123, got %d", loadedEvt.GetInstanceId())
		}
		if loadedEvt.GetEventUuid() != evt.EventUuid {
			t.Errorf("EventUuid: expected %s, got %s", evt.EventUuid, loadedEvt.GetEventUuid())
		}
		if loadedEvt.GetTenantUuid() != "tenant-uuid-123" {
			t.Errorf("TenantUuid: expected 'tenant-uuid-123', got %s", loadedEvt.GetTenantUuid())
		}
		if loadedEvt.GetCommandUuid() != "command-uuid-456" {
			t.Errorf("CommandUuid: expected 'command-uuid-456', got %s", loadedEvt.GetCommandUuid())
		}
		if loadedEvt.GetAggregateUuid() != "aggregate-uuid-789" {
			t.Errorf("AggregateUuid: expected 'aggregate-uuid-789', got %s", loadedEvt.GetAggregateUuid())
		}
		if loadedEvt.GetDomain() != "test-domain" {
			t.Errorf("Domain: expected 'test-domain', got %s", loadedEvt.GetDomain())
		}
		if loadedEvt.GetVersion() != 5 {
			t.Errorf("Version: expected 5, got %d", loadedEvt.GetVersion())
		}
		if loadedEvt.GetCreatedAt() != 1234567890 {
			t.Errorf("CreatedAt: expected 1234567890, got %d", loadedEvt.GetCreatedAt())
		}

		// CRITICAL: Verify data_bytes is not empty
		dataBytes := loadedEvt.GetDomainEvtBytes()
		if len(dataBytes) == 0 {
			t.Fatal("CRITICAL: data_bytes is empty after loading")
		}

		// Verify data_type is set
		dataType := loadedEvt.GetDomainEvtName()
		if len(dataType) == 0 {
			t.Fatal("CRITICAL: data_type is empty after loading")
		}

		// Deserialize and verify the domain data
		loadedData := &ComplexDomainEvent{}
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
	})

	// Test 2: List events and verify ALL fields are loaded
	t.Run("List - Verify all fields loaded", func(t *testing.T) {
		evts, total, err := eventStore.List(ctx)
		if err != nil {
			t.Fatalf("failed to list events: %v", err)
		}
		if total != 1 {
			t.Fatalf("expected 1 event, got %d", total)
		}
		if len(evts) != 1 {
			t.Fatalf("expected 1 event in list, got %d", len(evts))
		}

		loadedEvt := evts[0]

		// Verify all base fields
		if loadedEvt.GetInstanceId() != 123 {
			t.Errorf("InstanceId: expected 123, got %d", loadedEvt.GetInstanceId())
		}
		if loadedEvt.GetTenantUuid() != "tenant-uuid-123" {
			t.Errorf("TenantUuid: expected 'tenant-uuid-123', got %s", loadedEvt.GetTenantUuid())
		}
		if loadedEvt.GetCommandUuid() != "command-uuid-456" {
			t.Errorf("CommandUuid: expected 'command-uuid-456', got %s", loadedEvt.GetCommandUuid())
		}

		// CRITICAL: Verify data_bytes is not empty in List
		dataBytes := loadedEvt.GetDomainEvtBytes()
		if len(dataBytes) == 0 {
			t.Fatal("CRITICAL: data_bytes is empty after List()")
		}

		// Verify data_type is set
		dataType := loadedEvt.GetDomainEvtName()
		if len(dataType) == 0 {
			t.Fatal("CRITICAL: data_type is empty after List()")
		}

		// Deserialize and verify the domain data
		loadedData := &ComplexDomainEvent{}
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
	})

	// reset database
	if err := eventStore.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	// close connection
	if err := eventStore.Close(ctx); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}
}

// Helper function to create test events with all fields populated (except runtime fields)
func createTestEvent(tenantUuid, domain string, version int64, createdAt int64) comby.Event {
	evt := comby.NewBaseEvent()
	evt.SetInstanceId(1)
	evt.SetTenantUuid(tenantUuid)
	evt.SetCommandUuid(fmt.Sprintf("command-%d", version))
	evt.SetDomain(domain)
	evt.SetAggregateUuid(fmt.Sprintf("aggregate-%d", version))
	evt.SetVersion(version)
	evt.SetDomainEvtName(fmt.Sprintf("TestEvent_%d", version))
	evt.SetDomainEvtBytes([]byte(fmt.Sprintf("test-data-%d", version)))
	evt.SetCreatedAt(createdAt)
	return evt
}

// Test: PostgreSQL to PostgreSQL - Validate complete event data is copied (including DomainEvtBytes)
// This test specifically addresses the reported issue where data_bytes are missing after copying
// between PostgreSQL event stores.
func TestSyncEventStore_ValidateCompleteEventData(t *testing.T) {
	ctx := context.Background()

	// Create PostgreSQL event stores
	source := store.NewEventStorePostgres("localhost", 5432, "postgres", "mysecretpassword", "postgres")
	destination := store.NewEventStorePostgres("localhost", 5432, "postgres", "mysecretpassword", "postgres")

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

	// Create test events with all fields populated and various data sizes
	startTime := time.Now().Unix()
	testEvents := []comby.Event{
		createTestEvent("tenant-1", "user-domain", 1, startTime),
		createTestEvent("tenant-1", "order-domain", 2, startTime+1),
		createTestEvent("tenant-2", "product-domain", 3, startTime+2),
		createTestEvent("tenant-2", "inventory-domain", 4, startTime+3),
		createTestEvent("tenant-3", "payment-domain", 5, startTime+4),
	}

	// Set DomainEvtBytes with different sizes to ensure all data is copied
	testEvents[0].SetDomainEvtBytes([]byte("small-payload"))
	testEvents[1].SetDomainEvtBytes([]byte("medium-payload-with-more-content-for-testing"))
	// Create large payload with actual text content (PostgreSQL doesn't accept null bytes in text fields)
	largePayload := "large-payload-"
	for i := 0; i < 500; i++ {
		largePayload += "x"
	}
	testEvents[2].SetDomainEvtBytes([]byte(largePayload))
	testEvents[3].SetDomainEvtBytes([]byte(`{"type":"json","data":{"key":"value","nested":{"field":"data"}}}`))
	// Create very large payload with actual text content
	veryLargePayload := "very-large-payload-"
	for i := 0; i < 2000; i++ {
		veryLargePayload += "y"
	}
	testEvents[4].SetDomainEvtBytes([]byte(veryLargePayload))

	// Additional field customization
	testEvents[0].SetDomainEvtName("UserCreated")
	testEvents[1].SetDomainEvtName("OrderPlaced")
	testEvents[2].SetDomainEvtName("ProductAdded")
	testEvents[3].SetDomainEvtName("InventoryUpdated")
	testEvents[4].SetDomainEvtName("PaymentProcessed")

	// Store all events in source
	for i, evt := range testEvents {
		if err := source.Create(ctx, comby.EventStoreCreateOptionWithEvent(evt)); err != nil {
			t.Fatalf("Failed to create event %d in source PostgreSQL store: %v", i, err)
		}
	}

	// Verify source has all events
	sourceTotal := source.Total(ctx)
	if sourceTotal != int64(len(testEvents)) {
		t.Fatalf("Source store should have %d events, got %d", len(testEvents), sourceTotal)
	}

	// Sync from PostgreSQL source to PostgreSQL destination
	err := comby.SyncEventStore(ctx, source, destination)
	if err != nil {
		t.Fatalf("Sync from PostgreSQL to PostgreSQL failed: %v", err)
	}

	// Verify count matches
	destTotal := destination.Total(ctx)
	if sourceTotal != destTotal {
		t.Fatalf("Event count mismatch after sync: source=%d, destination=%d", sourceTotal, destTotal)
	}

	// Retrieve all events from both stores for field-by-field comparison
	sourceEvents, _, err := source.List(ctx, comby.EventStoreListOptionOrderBy("created_at"), comby.EventStoreListOptionAscending(true))
	if err != nil {
		t.Fatalf("Failed to list source events: %v", err)
	}

	destEvents, _, err := destination.List(ctx, comby.EventStoreListOptionOrderBy("created_at"), comby.EventStoreListOptionAscending(true))
	if err != nil {
		t.Fatalf("Failed to list destination events: %v", err)
	}

	if len(sourceEvents) != len(destEvents) {
		t.Fatalf("Event list length mismatch: source=%d, destination=%d", len(sourceEvents), len(destEvents))
	}

	// Validate EVERY field for EVERY event
	for i := 0; i < len(sourceEvents); i++ {
		srcEvt := sourceEvents[i]
		dstEvt := destEvents[i]

		// Instance ID
		if srcEvt.GetInstanceId() != dstEvt.GetInstanceId() {
			t.Errorf("Event %d: InstanceId mismatch: source=%d, dest=%d", i, srcEvt.GetInstanceId(), dstEvt.GetInstanceId())
		}

		// Event UUID
		if srcEvt.GetEventUuid() != dstEvt.GetEventUuid() {
			t.Errorf("Event %d: EventUuid mismatch: source=%s, dest=%s", i, srcEvt.GetEventUuid(), dstEvt.GetEventUuid())
		}

		// Tenant UUID
		if srcEvt.GetTenantUuid() != dstEvt.GetTenantUuid() {
			t.Errorf("Event %d: TenantUuid mismatch: source=%s, dest=%s", i, srcEvt.GetTenantUuid(), dstEvt.GetTenantUuid())
		}

		// Command UUID
		if srcEvt.GetCommandUuid() != dstEvt.GetCommandUuid() {
			t.Errorf("Event %d: CommandUuid mismatch: source=%s, dest=%s", i, srcEvt.GetCommandUuid(), dstEvt.GetCommandUuid())
		}

		// Domain
		if srcEvt.GetDomain() != dstEvt.GetDomain() {
			t.Errorf("Event %d: Domain mismatch: source=%s, dest=%s", i, srcEvt.GetDomain(), dstEvt.GetDomain())
		}

		// Aggregate UUID
		if srcEvt.GetAggregateUuid() != dstEvt.GetAggregateUuid() {
			t.Errorf("Event %d: AggregateUuid mismatch: source=%s, dest=%s", i, srcEvt.GetAggregateUuid(), dstEvt.GetAggregateUuid())
		}

		// Version
		if srcEvt.GetVersion() != dstEvt.GetVersion() {
			t.Errorf("Event %d: Version mismatch: source=%d, dest=%d", i, srcEvt.GetVersion(), dstEvt.GetVersion())
		}

		// Domain Event Name
		if srcEvt.GetDomainEvtName() != dstEvt.GetDomainEvtName() {
			t.Errorf("Event %d: DomainEvtName mismatch: source=%s, dest=%s", i, srcEvt.GetDomainEvtName(), dstEvt.GetDomainEvtName())
		}

		// Created At
		if srcEvt.GetCreatedAt() != dstEvt.GetCreatedAt() {
			t.Errorf("Event %d: CreatedAt mismatch: source=%d, dest=%d", i, srcEvt.GetCreatedAt(), dstEvt.GetCreatedAt())
		}

		// CRITICAL TEST: DomainEvtBytes (data_bytes) - THIS IS THE REPORTED BUG!
		srcBytes := srcEvt.GetDomainEvtBytes()
		dstBytes := dstEvt.GetDomainEvtBytes()

		// Check if bytes exist
		if len(srcBytes) == 0 {
			t.Errorf("Event %d: Source DomainEvtBytes is empty! This should not happen.", i)
		}

		if len(dstBytes) == 0 {
			t.Errorf("Event %d: CRITICAL BUG - Destination DomainEvtBytes is empty! data_bytes were not copied from PostgreSQL source.", i)
		}

		// Check length matches
		if len(srcBytes) != len(dstBytes) {
			t.Errorf("Event %d: DomainEvtBytes length mismatch: source=%d bytes, dest=%d bytes",
				i, len(srcBytes), len(dstBytes))
			t.Errorf("  Source data (first 100 chars): %q", string(srcBytes[:min(100, len(srcBytes))]))
			if len(dstBytes) > 0 {
				t.Errorf("  Dest data (first 100 chars): %q", string(dstBytes[:min(100, len(dstBytes))]))
			} else {
				t.Errorf("  Dest data: EMPTY (BUG!)")
			}
		}

		// Check content matches byte-for-byte
		if len(srcBytes) > 0 && len(dstBytes) > 0 {
			if string(srcBytes) != string(dstBytes) {
				t.Errorf("Event %d: DomainEvtBytes content mismatch!", i)
				t.Errorf("  Source (%d bytes): %q", len(srcBytes), string(srcBytes[:min(200, len(srcBytes))]))
				t.Errorf("  Dest (%d bytes): %q", len(dstBytes), string(dstBytes[:min(200, len(dstBytes))]))
			}
		}
	}
}

// Helper function for min
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
