package store_test

import (
	"context"
	"testing"

	"github.com/gradientzero/comby-store-postgres"
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
	if v, ok := eventStore.Options().Attributes.Get("key1"); ok {
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

	// check totals
	if eventStore.Total(ctx) != 0 {
		t.Fatalf("wrong total %d", eventStore.Total(ctx))
	}

	// Create values
	evt1 := &comby.BaseEvent{
		EventUuid:     comby.NewUuid(),
		AggregateUuid: "AggregateUuid_1",
		Domain:        "Domain_1",
		CreatedAt:     1000,
		Version:       1,
	}
	if err := eventStore.Create(ctx,
		comby.EventStoreCreateOptionWithEvent(evt1),
	); err != nil {
		t.Fatal(err)
	}
	evt2 := &comby.BaseEvent{
		EventUuid:     comby.NewUuid(),
		AggregateUuid: "AggregateUuid_2",
		Domain:        "Domain_2",
		CreatedAt:     1000,
		Version:       1,
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

	// Get a value
	if _evt1, err := eventStore.Get(ctx,
		comby.EventStoreGetOptionWithEventUuid(evt1.EventUuid),
	); err != nil {
		t.Fatal(err)
	} else {
		if _evt1.GetAggregateUuid() != "AggregateUuid_1" {
			t.Fatalf("wrong value: %q", _evt1)
		}
	}

	// List all events
	if evts, total, err := eventStore.List(ctx); err != nil {
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
