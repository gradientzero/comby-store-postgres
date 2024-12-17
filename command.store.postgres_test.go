package store_test

import (
	"context"
	"testing"

	"github.com/gradientzero/comby-store-postgres"
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
	if v, ok := commandStore.Options().Attributes.Get("key1"); ok {
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

	// check totals
	if commandStore.Total(ctx) != 0 {
		t.Fatalf("wrong total %d", commandStore.Total(ctx))
	}

	// Create values
	cmd1 := &comby.BaseCommand{
		CommandUuid: comby.NewUuid(),
		TenantUuid:  "TenantUuid_1",
		Domain:      "Domain_1",
		CreatedAt:   1000,
	}
	if err := commandStore.Create(ctx,
		comby.CommandStoreCreateOptionWithCommand(cmd1),
	); err != nil {
		t.Fatal(err)
	}
	cmd2 := &comby.BaseCommand{
		CommandUuid: comby.NewUuid(),
		TenantUuid:  "TenantUuid_2",
		Domain:      "Domain_2",
		CreatedAt:   1000,
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

	// Get a value
	if _cmd1, err := commandStore.Get(ctx,
		comby.CommandStoreGetOptionWithCommandUuid(cmd1.CommandUuid),
	); err != nil {
		t.Fatal(err)
	} else {
		if _cmd1.GetTenantUuid() != "TenantUuid_1" {
			t.Fatalf("wrong value: %q", _cmd1)
		}
	}

	// List all commands
	if evts, total, err := commandStore.List(ctx); err != nil {
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
